package sql

import (
	"context"
	"errors"
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
