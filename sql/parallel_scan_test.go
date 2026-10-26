package sql

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"slopdb/storage"
)

func TestParallelScalarAggregateSnapshotAndAdmission(t *testing.T) {
	old := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(old)
	opt := storage.DefaultOptions()
	opt.DisableWALSync = true
	opt.CompressionType = storage.NoCompression
	l, err := storage.NewLSMTree(t.TempDir(), opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	s, err := NewSchema(l, 4096)
	if err != nil {
		t.Fatal(err)
	}
	statisticsSQL(t, s, "CREATE TABLE t(id INTEGER PRIMARY KEY,n INTEGER,p TEXT)")
	for base := 0; base < 8000; base += 100 {
		rows := make([]string, 100)
		for j := range rows {
			id := base + j
			rows[j] = fmt.Sprintf("(%d,%d,'%s')", id, id%17, strings.Repeat("z", 180))
		}
		statisticsSQL(t, s, "INSERT INTO t VALUES"+strings.Join(rows, ","))
	}
	if err = l.CompactRange(); err != nil {
		t.Fatal(err)
	}
	if err = s.StartStatement(); err != nil {
		t.Fatal(err)
	}
	defer s.EndStatement()
	info := s.tables["t"]
	lower := []byte(info.storagePrefix("t"))
	upper := prefixSuccessor(lower)
	ranges, release, err := s.parallelRanges(context.Background(), s.statementSnapshot, lower, upper, 256<<10)
	if err != nil || release == nil || len(ranges) < 2 {
		t.Fatalf("parallel plan: %d ranges, %v", len(ranges), err)
	}
	release()
	if got := s.parallelScanSlots.Load(); got != 0 {
		t.Fatalf("leaked CPU budget%d", got)
	}
	stmt := statisticsSelect(t, "SELECT COUNT(*),COUNT(n),MIN(n),MAX(n) FROM t WHERE id>=0")
	functions := make([]*FuncCallExpr, len(stmt.Columns))
	for i, c := range stmt.Columns {
		functions[i] = c.Expr.(*FuncCallExpr)
	}
	typed, ok := compileTypedAggregate(info, "t", "t", stmt.Where, nil, functions, nil)
	if !ok {
		t.Fatal("not typed")
	}
	parallel := &aggregateGroup{states: make([]aggregateState, len(functions))}
	used, err := s.parallelTypedAggregate(typed, info, map[string]bool{"id": true, "n": true, "p": false}, nil, map[string]*aggregateGroup{"": parallel})
	if !used || err != nil {
		t.Fatalf("parallel: used%v %v", used, err)
	}
	serial := &aggregateGroup{states: make([]aggregateState, len(functions))}
	if err = s.walkTypedAggregate(typed.clonePrivate(), info, map[string]bool{"id": true, "n": true, "p": false}, nil, map[string]*aggregateGroup{"": serial}, func() *aggregateGroup { return serial }); err != nil {
		t.Fatal(err)
	}
	for i, f := range functions {
		a, e := aggregateValue(&parallel.states[i], f)
		if e != nil {
			t.Fatal(e)
		}
		b, e := aggregateValue(&serial.states[i], f)
		if e != nil {
			t.Fatal(e)
		}
		if ValueCompare(a, b) != 0 {
			t.Fatalf("%s parallel%s serial%s", f.Name, a.String(), b.String())
		}
	}
	sumStmt := statisticsSelect(t, "SELECT SUM(n) FROM t")
	sumProgram, _ := compileTypedAggregate(info, "t", "t", nil, nil, []*FuncCallExpr{sumStmt.Columns[0].Expr.(*FuncCallExpr)}, nil)
	if used, e := s.parallelTypedAggregate(sumProgram, info, map[string]bool{"n": true}, nil, map[string]*aggregateGroup{"": {states: make([]aggregateState, 1)}}); used || e != nil {
		t.Fatalf("order-sensitive SUM parallelized: %v %v", used, e)
	}
	for _, query := range []string{"SELECT COUNT(X'AB') FROM t", "SELECT MIN(n || 'wide') FROM t"} {
		checked := statisticsSelect(t, query)
		fn := checked.Columns[0].Expr.(*FuncCallExpr)
		unsafe, _ := compileTypedAggregate(info, "t", "t", nil, nil, []*FuncCallExpr{fn}, nil)
		if unsafe != nil {
			if used, e := s.parallelTypedAggregate(unsafe, info, map[string]bool{"n": true}, nil, map[string]*aggregateGroup{"": {states: make([]aggregateState, 1)}}); used || e != nil {
				t.Fatalf("variable-width program parallelized: %s", query)
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = s.countRangeParallel(ctx, s.statementSnapshot, lower, upper); err == nil {
		t.Fatal("cancelled count succeeded")
	}
	if s.parallelScanSlots.Load() != 0 {
		t.Fatal("cancel leaked workers")
	}
}
