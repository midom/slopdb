package sql

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
	"strings"
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

func TestIndexedDiskHashSeeksAndDuplicateBuckets(t *testing.T) {
	dir := t.TempDir()
	s := &Schema{queryMemoryBytes: 512, queryTempDir: dir, profile: &QueryProfile{}}
	h, err := newDiskHash(s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.close)
	payload := strings.Repeat("x", 1024)
	for i := 0; i < 512; i++ {
		key := fmt.Sprintf("key:%04d", i)
		if err = h.add(key, map[string]Value{"id": NewIntValue(int64(i)), "payload": NewTextValue(payload)}); err != nil {
			t.Fatal(err)
		}
	}
	special := []string{"", "a", "a\x00", "a\x00x", "\x00a", "\xff\x00"}
	for i, key := range special {
		if err = h.add(key, map[string]Value{"id": NewIntValue(int64(i)), "v": NewBlobValue([]byte{0, 255}), "null": NewNullValue()}); err != nil {
			t.Fatal(err)
		}
	}
	for j := 0; j < 3; j++ {
		if err = h.add("duplicates", map[string]Value{"id": NewIntValue(int64(j))}); err != nil {
			t.Fatal(err)
		}
	}
	// Every unique probe reads exactly one indexed record, including after SST
	// flushes. Unrelated buckets must never be decoded to find the matching row.
	before := h.rowsRead
	for i := 0; i < 512; i++ {
		found := 0
		err = h.walk(fmt.Sprintf("key:%04d", i), func(row map[string]Value) error {
			found++
			if row["id"].Int != int64(i) || row["payload"].Text != payload {
				t.Fatal(row)
			}
			return nil
		})
		if err != nil || found != 1 {
			t.Fatalf("probe %d: %d %v", i, found, err)
		}
	}
	if read := h.rowsRead - before; read != 512 {
		t.Fatalf("512 unique probes decoded %d records", read)
	}
	for i, key := range special {
		found := 0
		err = h.walk(key, func(row map[string]Value) error {
			found++
			if row["id"].Int != int64(i) || !row["null"].Null || !bytes.Equal(row["v"].Blob, []byte{0, 255}) {
				t.Fatal(row)
			}
			return nil
		})
		if err != nil || found != 1 {
			t.Fatalf("binary bucket %q: %d %v", key, found, err)
		}
	}
	before = h.rowsRead
	if err = h.walk("missing", func(map[string]Value) error { t.Fatal("missing bucket matched"); return nil }); err != nil {
		t.Fatal(err)
	}
	if h.rowsRead != before {
		t.Fatal("missing probe decoded other buckets")
	}
	var ids []int64
	err = h.walk("duplicates", func(row map[string]Value) error { ids = append(ids, row["id"].Int); return nil })
	if err != nil || !reflect.DeepEqual(ids, []int64{0, 1, 2}) {
		t.Fatalf("duplicates: %v %v", ids, err)
	}
	if stats := h.index.Stats(); stats.WALActive || stats.MemSize > 128<<10 || stats.BlockCacheBytes > 64<<10 {
		t.Fatalf("unbounded spill index or WAL enabled: %+v", stats)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	cancel()
	if err = h.walk("duplicates", func(map[string]Value) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	h.close()
	h.close()
	if s.profile.SpillBytes <= 0 || s.profile.SpillRuns <= 0 || s.profile.SpillRowsRead != h.rowsRead {
		t.Fatalf("profile: %+v", s.profile)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) > 0 {
		t.Fatalf("spill cleanup: %v %v", entries, err)
	}
}

func TestIndexedDiskHashAdjacentCacheInvalidation(t *testing.T) {
	s := &Schema{queryMemoryBytes: 1 << 20, queryTempDir: t.TempDir()}
	h, err := newDiskHash(s)
	if err != nil {
		t.Fatal(err)
	}
	defer h.close()
	if err = h.add("k", map[string]Value{"x": NewIntValue(1)}); err != nil {
		t.Fatal(err)
	}
	read := func() []int64 {
		t.Helper()
		var ids []int64
		if err = h.walk("k", func(row map[string]Value) error { ids = append(ids, row["x"].Int); return nil }); err != nil {
			t.Fatal(err)
		}
		return ids
	}
	if got := read(); !reflect.DeepEqual(got, []int64{1}) {
		t.Fatal(got)
	}
	before := h.rowsRead
	for i := 0; i < 10; i++ {
		if got := read(); !reflect.DeepEqual(got, []int64{1}) {
			t.Fatal(got)
		}
	}
	if h.rowsRead != before {
		t.Fatal("adjacent bucket was decoded repeatedly")
	}
	if err = h.add("k", map[string]Value{"x": NewIntValue(2)}); err != nil {
		t.Fatal(err)
	}
	if got := read(); !reflect.DeepEqual(got, []int64{1, 2}) {
		t.Fatal("cached bucket omitted new write", got)
	}
}

func TestIndexedSpillRejectsMalformedRows(t *testing.T) {
	for _, data := range [][]byte{nil, {128}, {1}, {1, 0}, {1, 0, 255}, {1, 0, binTypeInt64, 0}, {1, 0, binTypeText, 128}, {0, 1}} {
		if _, err := decodeSpillMap(data); err == nil {
			t.Fatalf("accepted malformed row: %x", data)
		}
	}
}

func TestQueryDiskCleanupDetachesOwnership(t *testing.T) {
	s := &Schema{queryMemoryBytes: 512, queryTempDir: t.TempDir()}
	var held []*diskHash
	for i := 0; i < 3; i++ {
		disk, err := newDiskHash(s)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, disk)
		s.queryDisks = append(s.queryDisks, disk)
		if err = disk.add("k", nil); err != nil {
			t.Fatal(err)
		}
	}
	s.clearSelectCaches()
	if s.queryDisks != nil {
		t.Fatal("statement retains ownership list")
	}
	for _, disk := range held {
		if !disk.closed || disk.index != nil {
			t.Fatal("statement cleanup skipped retained engine")
		}
		disk.close()
	}
	entries, err := os.ReadDir(s.queryTempDir)
	if err != nil || len(entries) > 0 {
		t.Fatalf("cleanup files: %v %v", entries, err)
	}
}
