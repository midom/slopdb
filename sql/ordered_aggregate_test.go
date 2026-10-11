package sql

import (
	"context"
	"errors"
	"os"
	"testing"

	"slopdb/storage"
)

func TestTypedOrderedAggregateReusesSingleAccumulator(t *testing.T) {
	opts := storage.DefaultOptions()
	opts.DisableWAL = true
	l, err := storage.NewLSMTree(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	info := &TableInfo{Name: "t", PKColumns: []string{"g", "id"}, Columns: []ColumnDef{{Name: "g", ColType: ColInteger}, {Name: "id", ColType: ColInteger}, {Name: "n", ColType: ColInteger}}}
	for g := 0; g < 100; g++ {
		for id := 0; id < 3; id++ {
			row := map[string]Value{"g": NewIntValue(int64(g)), "id": NewIntValue(int64(id)), "n": NewIntValue(int64(g))}
			if err = l.Put(info.encodePK("t", row), encodeRow(row, info)); err != nil {
				t.Fatal(err)
			}
		}
	}
	statements, err := Parse("SELECT g,SUM(n) FROM t WHERE id<>1 GROUP BY g")
	if err != nil {
		t.Fatal(err)
	}
	stmt := statements[0].(*SelectStmt)
	aggregate := stmt.Columns[1].Expr.(*FuncCallExpr)
	typed, ok := compileTypedAggregate(info, "t", "t", stmt.Where, stmt.GroupBy, []*FuncCallExpr{aggregate}, map[string]bool{"g": true})
	if !ok {
		t.Fatal("typed aggregate unavailable")
	}
	s := &Schema{lsm: l, queryMemoryBytes: 512, queryTempDir: t.TempDir()}
	var held *aggregateGroup
	groups := 0
	var peak int64
	err = s.walkTypedOrderedAggregate(typed, info, map[string]bool{"g": true, "id": true, "n": true}, nil, func(g *aggregateGroup) error {
		if held != nil && held != g {
			t.Fatal("ordered aggregation allocated multiple accumulators")
		}
		held = g
		if bytes := aggregateGroupMemory(g); bytes > peak {
			peak = bytes
		}
		if g.inputs != 2 || len(g.baseValues) != 1 || g.baseValues[0].Int != int64(groups) || g.states[0].integer != int64(2*groups) {
			t.Fatalf("group %d: %+v", groups, g)
		}
		groups++
		return nil
	})
	if err != nil || groups != 100 {
		t.Fatalf("groups=%d error=%v", groups, err)
	}
	if peak > 512 {
		t.Fatalf("peak accumulator bytes=%d", peak)
	}
	// Cancel from an emission callback while an iterator and batch are active.
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	err = s.walkTypedOrderedAggregate(typed, info, map[string]bool{"g": true, "id": true, "n": true}, nil, func(*aggregateGroup) error { cancel(); return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-scan cancellation: %v", err)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOrderedAggregateInputRequiresClusteredPrefix(t *testing.T) {
	info := &TableInfo{PKColumns: []string{"a", "b"}}
	stmt := &SelectStmt{From: "t", FromAlias: "x"}
	a := &ColumnRefExpr{Name: "x.a"}
	b := &ColumnRefExpr{Name: "b"}
	cases := []struct {
		groups []Expression
		paths  []*accessPath
		want   bool
	}{
		{[]Expression{a}, nil, true}, {[]Expression{a, b}, nil, true}, {[]Expression{b}, nil, false},
		{[]Expression{a}, []*accessPath{{intervals: []coveringInterval{{}}}}, true},
		{[]Expression{a}, []*accessPath{{index: &IndexInfo{}, intervals: []coveringInterval{{}}}}, false},
		{[]Expression{a}, []*accessPath{{points: [][]byte{{1}}}}, false},
		{[]Expression{a}, []*accessPath{{intervals: []coveringInterval{{}, {}}}}, false},
	}
	for _, tc := range cases {
		if got := orderedAggregateInput(stmt, info, tc.groups, tc.paths); got != tc.want {
			t.Fatalf("groups=%v paths=%v got=%v", tc.groups, tc.paths, got)
		}
	}
}

func TestOrderedAggregateDistinctEngineLifetime(t *testing.T) {
	opts := storage.DefaultOptions()
	opts.DisableWAL = true
	l, err := storage.NewLSMTree(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	info := &TableInfo{Name: "t", PKColumns: []string{"g", "id"}, Columns: []ColumnDef{{Name: "g", ColType: ColInteger}, {Name: "id", ColType: ColInteger}, {Name: "n", ColType: ColInteger}}}
	for g := 0; g < 20; g++ {
		for id := 0; id < 10; id++ {
			row := map[string]Value{"g": NewIntValue(int64(g)), "id": NewIntValue(int64(id)), "n": NewIntValue(int64(id % 5))}
			if err = l.Put(info.encodePK("t", row), encodeRow(row, info)); err != nil {
				t.Fatal(err)
			}
		}
	}
	s := &Schema{lsm: l, tables: map[string]*TableInfo{"t": info}, queryMemoryBytes: 512, queryTempDir: t.TempDir()}
	t.Cleanup(s.clearSelectCaches)
	for _, query := range []string{"SELECT g,SUM(DISTINCT n) AS total,COUNT(DISTINCT n) AS c FROM t GROUP BY g", "SELECT g,SUM(DISTINCT ABS(n)) AS total,COUNT(DISTINCT ABS(n)) AS c FROM t GROUP BY g"} {
		statements, err := Parse(query)
		if err != nil {
			t.Fatal(err)
		}
		stmt := statements[0].(*SelectStmt)
		cols := []selectExprCol{{name: "g", expr: stmt.Columns[0].Expr}, {name: "total", expr: stmt.Columns[1].Expr}, {name: "c", expr: stmt.Columns[2].Expr}}
		rows, used, err := s.streamAggregateScan(stmt, info, cols)
		if err != nil || !used || len(rows) != 20 {
			t.Fatalf("%s: %d %v %v", query, len(rows), used, err)
		}
		for _, row := range rows {
			if row["total"].Int != 10 || row["c"].Int != 5 {
				t.Fatal(row)
			}
		}
		// streamAggregateScan has not run statement cleanup. Completed ordered
		// groups must already have relinquished every temporary engine themselves.
		if len(s.queryDisks) != 0 {
			t.Fatalf("%s retained %d completed-group engines", query, len(s.queryDisks))
		}
		entries, err := os.ReadDir(s.queryTempDir)
		if err != nil || len(entries) > 0 {
			t.Fatalf("completed-group files leaked: %v %v", entries, err)
		}
	}
	// During typed execution, each emission owns only its two DISTINCT states;
	// closing them leaves no references for the next accumulator reuse.
	statements, err := Parse("SELECT g,SUM(DISTINCT n),COUNT(DISTINCT n) FROM t GROUP BY g")
	if err != nil {
		t.Fatal(err)
	}
	stmt := statements[0].(*SelectStmt)
	funcs := []*FuncCallExpr{stmt.Columns[1].Expr.(*FuncCallExpr), stmt.Columns[2].Expr.(*FuncCallExpr)}
	typed, ok := compileTypedAggregate(info, "t", "t", nil, stmt.GroupBy, funcs, map[string]bool{"g": true})
	if !ok {
		t.Fatal("typed aggregate unavailable")
	}
	groups := 0
	err = s.walkTypedOrderedAggregate(typed, info, map[string]bool{"g": true, "n": true}, nil, func(g *aggregateGroup) error {
		if len(s.queryDisks) != 2 {
			t.Fatalf("group %d retained %d engines", groups, len(s.queryDisks))
		}
		disks := []*diskHash{g.states[0].distinctDisk, g.states[1].distinctDisk}
		closeAggregateDisks(g)
		if len(s.queryDisks) != 0 {
			t.Fatal("closed group retains statement engine ownership")
		}
		for _, disk := range disks {
			if !disk.closed || disk.index != nil || disk.lastRows != nil {
				t.Fatal("closed group retains temporary engine/cache")
			}
		}
		for _, state := range g.states {
			if state.distinctDisk != nil {
				t.Fatal("accumulator retains closed engine")
			}
		}
		groups++
		return nil
	})
	if err != nil || groups != 20 {
		t.Fatalf("typed lifetime: %d %v", groups, err)
	}
	// Error from HAVING must release the emitting group's engine too.
	statements, err = Parse("SELECT g,SUM(DISTINCT n) FROM t GROUP BY g HAVING SUM(DISTINCT n)>0 AND 1/(g-g)=1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Select(statements[0].(*SelectStmt)); err == nil {
		t.Fatal("HAVING error swallowed")
	}
	if len(s.queryDisks) != 0 {
		t.Fatal("error retains engine ownership")
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
}
