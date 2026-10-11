package sql

import (
	"container/heap"
	"context"
	"errors"
	"math"
	"os"
	"sort"
	"testing"
)

func TestExternalSortMixedValuesAndStableTies(t *testing.T) {
	s := &Schema{queryMemoryBytes: 300, queryTempDir: t.TempDir()}
	values := []Value{NewNullValue(), NewIntValue(math.MaxInt64), NewRealValue(9007199254740992), NewIntValue(9007199254740993), NewTextValue("a"), NewBlobValue([]byte{0, 1}), NewBlobValue([]byte{0, 0}), NewIntValue(0), NewRealValue(math.Copysign(0, -1)), NewNullValue()}
	rows := make([]map[string]Value, len(values))
	for i, v := range values {
		rows[i] = map[string]Value{"v": v, "id": NewIntValue(int64(i))}
	}
	for _, order := range []OrderByClause{{Expr: &ColumnRefExpr{Name: "v"}}, {Expr: &ColumnRefExpr{Name: "v"}, Desc: true, NullsLast: true}, {Expr: &ColumnRefExpr{Name: "v"}, NullsFirst: true}} {
		expected := append([]map[string]Value(nil), rows...)
		sort.SliceStable(expected, func(i, j int) bool { return s.compareRows(expected[i], expected[j], []OrderByClause{order}) })
		got, _, err := s.sortBounded(rows, nil, []OrderByClause{order}, -1)
		if err != nil {
			t.Fatal(err)
		}
		for i := range got {
			if got[i]["id"].Int != expected[i]["id"].Int {
				t.Fatalf("sort differs: row %d got %v want %v", i, got[i], expected[i])
			}
		}
	}
	entries, err := os.ReadDir(s.queryTempDir)
	if err != nil || len(entries) > 0 {
		t.Fatalf("files leaked: %v %v", entries, err)
	}
}

func TestSpillMergeCancellationCleanup(t *testing.T) {
	dir := t.TempDir()
	s := &Schema{queryMemoryBytes: 200, queryTempDir: dir}
	sorter := newSpillSorter(s, func(a, b spillRecord) bool { return a.Seq < b.Seq })
	for i := 0; i < 100; i++ {
		if err := sorter.add(map[string]Value{"x": NewIntValue(int64(i))}, nil, ""); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	err := sorter.walk(func(spillRecord) error { cancel(); return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("merge cancellation: %v", err)
	}
	sorter.close()
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) > 0 {
		t.Fatalf("files leaked: %v %v", entries, err)
	}
}

// Both cases consume identical preallocated input. Top-K copies only retained
// rows; the full-sort case retains a copy of all 10,000 rows.
func BenchmarkTopKRetention(b *testing.B) {
	rows := make([]map[string]Value, 10000)
	for i := range rows {
		rows[i] = map[string]Value{"x": NewIntValue(int64((i * 7919) % 10000))}
	}
	less := func(a, c spillRecord) bool {
		if a.Row["x"].Int != c.Row["x"].Int {
			return a.Row["x"].Int < c.Row["x"].Int
		}
		return a.Seq < c.Seq
	}
	b.Run("TopK10", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			h := &topHeap{less: less}
			for i, row := range rows {
				rec := spillRecord{Row: row, Seq: int64(i)}
				if h.Len() < 10 {
					heap.Push(h, rec)
				} else if less(rec, h.rows[0]) {
					heap.Pop(h)
					heap.Push(h, rec)
				}
			}
		}
	})
	b.Run("FullSort", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			records := make([]spillRecord, len(rows))
			for i, row := range rows {
				records[i] = spillRecord{Row: copyRow(row), Seq: int64(i)}
			}
			sort.Slice(records, func(i, j int) bool { return less(records[i], records[j]) })
		}
	})
}
