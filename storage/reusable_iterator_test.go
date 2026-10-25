package storage

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sort"
	"testing"
)

func reusableTestTree(t *testing.T, memory BlockCacheMemory) *LSMTree {
	t.Helper()
	o := DefaultOptions()
	o.DisableWALSync = true
	o.PageSize = 512
	o.CompactionL0Trigger = 100
	o.BlockCacheMemory = memory
	o.BlockCacheSize = 1 << 20
	o.MaxOpenFiles = 1
	o.CompressionType = ZstdCompression
	l, err := NewLSMTree(filepath.Join(t.TempDir(), "db"), o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func checkReusableRows(t *testing.T, it Iterator, expected map[string][]byte, reverse bool) {
	t.Helper()
	defer it.Close()
	keys := make([]string, 0, len(expected))
	for k := range expected {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if reverse {
		sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	}
	var dst []byte
	var owned [][]byte
	for _, k := range keys {
		if !it.Valid() {
			t.Fatalf("iterator ended before %q: %v", k, IteratorError(it))
		}
		if string(it.Key().UserKey) != k || !bytes.Equal(it.Value(), expected[k]) {
			t.Fatalf("row = %q %q, want %q %q", it.Key().UserKey, it.Value(), k, expected[k])
		}
		dst = IteratorValueInto(it, dst)
		if !bytes.Equal(dst, expected[k]) {
			t.Fatalf("ValueInto row %q = %q", k, dst)
		}
		owned = append(owned, IteratorValueInto(it, nil))
		if reverse {
			it.Prev()
		} else {
			it.Next()
		}
	}
	if it.Valid() || IteratorError(it) != nil {
		t.Fatalf("extra row/error: %q %v", it.Key().UserKey, IteratorError(it))
	}
	it.Close()
	for i, k := range keys {
		if !bytes.Equal(owned[i], expected[k]) {
			t.Fatalf("caller-owned row %q changed after movement/close", k)
		}
	}
}

func TestReusableRangeSnapshotsPagesAndCompaction(t *testing.T) {
	for _, memory := range []BlockCacheMemory{BlockCacheHeap, BlockCacheMmap} {
		t.Run(string(memory), func(t *testing.T) {
			l := reusableTestTree(t, memory)
			old := make(map[string][]byte)
			latest := make(map[string][]byte)
			for i := 0; i < 160; i++ {
				k := fmt.Sprintf("key-%04d", i)
				v := []byte(fmt.Sprintf("old-%04d-%s", i, bytes.Repeat([]byte{'a' + byte(i%20)}, 210)))
				old[k], latest[k] = v, v
				if err := l.Put([]byte(k), v); err != nil {
					t.Fatal(err)
				}
				if i%40 == 39 {
					if err := l.Flush(); err != nil {
						t.Fatal(err)
					}
				}
			}
			snapshot := l.Snapshot()
			defer l.ReleaseSnapshot(snapshot)
			for i := 0; i < 160; i += 3 {
				k := fmt.Sprintf("key-%04d", i)
				v := []byte(fmt.Sprintf("updated-%04d-%s", i, bytes.Repeat([]byte{'z'}, 230)))
				latest[k] = v
				if err := l.Put([]byte(k), v); err != nil {
					t.Fatal(err)
				}
			}
			if err := l.Flush(); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 160; i += 7 {
				k := fmt.Sprintf("key-%04d", i)
				delete(latest, k)
				if err := l.Delete([]byte(k)); err != nil {
					t.Fatal(err)
				}
			}
			if err := l.DeleteRange([]byte("key-0050"), []byte("key-0070")); err != nil {
				t.Fatal(err)
			}
			for i := 50; i < 70; i++ {
				delete(latest, fmt.Sprintf("key-%04d", i))
			}
			// These iterators pin old files while compaction retires their view.
			pinned := l.NewIteratorAtRangeReusable(snapshot, nil, nil, false)
			if err := l.CompactRange(); err != nil {
				t.Fatal(err)
			}
			checkReusableRows(t, pinned, old, false)
			for _, reverse := range []bool{false, true} {
				checkReusableRows(t, l.NewIteratorAtRangeReusable(snapshot, nil, nil, reverse), old, reverse)
				checkReusableRows(t, l.NewIteratorRangeReusable(nil, nil, reverse), latest, reverse)
				bounded := make(map[string][]byte)
				for k, v := range latest {
					if k >= "key-0030" && k < "key-0100" {
						bounded[k] = v
					}
				}
				checkReusableRows(t, l.NewIteratorRangeReusable([]byte("key-0030"), []byte("key-0100"), reverse), bounded, reverse)
			}
		})
	}
}

func TestReusableRangeDirectionAndBorrowedSeek(t *testing.T) {
	l := reusableTestTree(t, BlockCacheMmap)
	for i := 0; i < 80; i++ {
		k := []byte(fmt.Sprintf("k%03d", i))
		if err := l.Put(k, bytes.Repeat(k, 60)); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.CompactRange(); err != nil {
		t.Fatal(err)
	}
	it := l.NewIteratorRangeReusable([]byte("k010"), []byte("k070"), false)
	defer it.Close()
	assertKey := func(want string) {
		t.Helper()
		if !it.Valid() || string(it.Key().UserKey) != want {
			t.Fatalf("key %q, want %q", it.Key().UserKey, want)
		}
	}
	assertKey("k010")
	it.Seek([]byte("k040"))
	assertKey("k040")
	it.Seek(it.Key().UserKey)
	assertKey("k040")
	it.Prev()
	assertKey("k039")
	it.Next()
	assertKey("k040")
	it.Prev()
	assertKey("k039")
	it.Seek([]byte("k069"))
	it.Next()
	if it.Valid() {
		t.Fatal("upper bound exposed")
	}
	it.Prev()
	assertKey("k069")
	it.Seek([]byte("k010"))
	it.Prev()
	if it.Valid() {
		t.Fatal("lower bound exposed")
	}
	it.Next()
	assertKey("k010")
	seekIteratorLast(it)
	assertKey("k069")
	it.Close()
	it.Next()
	it.Prev()
	it.Seek(nil)
	seekIteratorLast(it)
	if it.Valid() {
		t.Fatal("closed reusable iterator revived")
	}
}

func TestOrdinaryRangeOwnershipUnchanged(t *testing.T) {
	for _, memory := range []BlockCacheMemory{BlockCacheHeap, BlockCacheMmap} {
		t.Run(string(memory), func(t *testing.T) {
			l := reusableTestTree(t, memory)
			for i := 0; i < 50; i++ {
				k := []byte(fmt.Sprintf("k%03d", i))
				if err := l.Put(k, bytes.Repeat(k, 80)); err != nil {
					t.Fatal(err)
				}
			}
			if err := l.Flush(); err != nil {
				t.Fatal(err)
			}
			it := l.NewIteratorRange(nil, nil, false)
			var keys, values [][]byte
			for it.Valid() {
				keys = append(keys, it.Key().UserKey)
				values = append(values, it.Value())
				it.Next()
			}
			it.Close()
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			for i, k := range keys {
				want := fmt.Sprintf("k%03d", i)
				if string(k) != want || !bytes.Equal(values[i], bytes.Repeat([]byte(want), 80)) {
					t.Fatalf("ordinary iterator row %d changed", i)
				}
			}
		})
	}
}

func TestReusableIteratorScratchAndClosedDatabase(t *testing.T) {
	l := reusableTestTree(t, BlockCacheMmap)
	if err := l.Put([]byte("a"), bytes.Repeat([]byte{'x'}, 2*iteratorScratchLimit)); err != nil {
		t.Fatal(err)
	}
	if err := l.Put([]byte("b"), []byte("small")); err != nil {
		t.Fatal(err)
	}
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	it := l.NewIteratorRangeReusable(nil, nil, false).(*mergeIterator)
	if len(it.Value()) != 2*iteratorScratchLimit {
		t.Fatal("large row truncated")
	}
	owned := IteratorValueInto(it, nil)
	it.Next()
	if string(it.Value()) != "small" || cap(it.valueScratch)+cap(it.groupScratch) > iteratorScratchLimit {
		t.Fatal("oversized scratch retained", cap(it.valueScratch), cap(it.groupScratch))
	}
	it.Close()
	if it.lastVal != nil || it.lastKey.UserKey != nil || it.groupScratch != nil || it.valueScratch != nil || it.iters != nil {
		t.Fatal("closed iterator retained scratch/view")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if len(owned) != 2*iteratorScratchLimit || owned[0] != 'x' || owned[len(owned)-1] != 'x' {
		t.Fatal("ValueInto invalidated by close")
	}
	for _, reverse := range []bool{false, true} {
		closed := l.NewIteratorRangeReusable(nil, nil, reverse)
		if closed.Valid() || IteratorError(closed) == nil {
			t.Fatal("closed database did not report error")
		}
		closed.Close()
	}
}

func TestReusableIteratorDropsSkippedLargeValueScratch(t *testing.T) {
	l := reusableTestTree(t, BlockCacheMmap)
	for _, kv := range []struct {
		key   string
		value []byte
	}{
		{"a", []byte("first")}, {"b", bytes.Repeat([]byte{'x'}, 2*iteratorScratchLimit)}, {"c", []byte("last")},
	} {
		if err := l.Put([]byte(kv.key), kv.value); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := l.DeleteRange([]byte("b"), []byte("c")); err != nil {
		t.Fatal(err)
	}
	it := l.NewIteratorRangeReusable(nil, nil, false).(*mergeIterator)
	defer it.Close()
	if string(it.Key().UserKey) != "a" {
		t.Fatal("missing first row")
	}
	it.Next()
	if string(it.Key().UserKey) != "c" || string(it.Value()) != "last" {
		t.Fatal("range-deleted row exposed")
	}
	if cap(it.valueScratch)+cap(it.groupScratch) > iteratorScratchLimit {
		t.Fatal("skipped large row retained oversized scratch")
	}
}

// A wrapper exposing only the base interface exercises the helper's fallback,
// rather than inheriting an implementation-specific ValueInto method.
type baseValueIterator struct{ Iterator }

func TestIteratorValueIntoFallbackAndLargeKeys(t *testing.T) {
	l := reusableTestTree(t, BlockCacheMmap)
	large := append([]byte("a"), bytes.Repeat([]byte{'k'}, 2*iteratorScratchLimit)...)
	for _, key := range [][]byte{large, []byte("b"), []byte("c")} {
		if err := l.Put(key, bytes.Repeat(key[:1], 200)); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.CompactRange(); err != nil {
		t.Fatal(err)
	}
	for _, reverse := range []bool{false, true} {
		it := l.NewIteratorRangeReusable(nil, nil, reverse).(*mergeIterator)
		defer it.Close()
		if reverse {
			it.Seek(large)
		}
		if !bytes.Equal(it.Key().UserKey, large) {
			t.Fatal("large key truncated")
		}
		buf := make([]byte, 200)
		owned := IteratorValueInto(baseValueIterator{it}, buf[:0])
		if len(owned) != 200 || &owned[0] != &buf[0] {
			t.Fatal("fallback did not reuse caller buffer")
		}
		it.Next()
		if string(it.Key().UserKey) != "b" || cap(it.groupScratch)+cap(it.valueScratch) > iteratorScratchLimit {
			t.Fatal("large key scratch retained")
		}
		it.Prev()
		if !bytes.Equal(it.Key().UserKey, large) {
			t.Fatal("reverse lost large key")
		}
		it.Next()
		if string(it.Key().UserKey) != "b" {
			t.Fatal("direction switch lost row")
		}
		it.Close()
		if !bytes.Equal(owned, bytes.Repeat([]byte{'a'}, 200)) {
			t.Fatal("fallback value changed")
		}
		if len(IteratorValueInto(it, buf)) != 0 {
			t.Fatal("closed iterator returned a value")
		}
	}
}

// Measures encoded key/value allocation reuse after warming the same SSTable
// range; iterator setup is included, fixture creation and validation are not.
func BenchmarkReusableRangeIterator(b *testing.B) {
	o := DefaultOptions()
	o.DisableWALSync = true
	o.CompactionL0Trigger = 100
	o.BlockCacheSize = 8 << 20
	l, err := NewLSMTree(filepath.Join(b.TempDir(), "db"), o)
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	for i := 0; i < 1000; i++ {
		if err := l.Put([]byte(fmt.Sprintf("k%06d", i)), bytes.Repeat([]byte{'v'}, 240)); err != nil {
			b.Fatal(err)
		}
	}
	if err := l.CompactRange(); err != nil {
		b.Fatal(err)
	}
	for _, reusable := range []bool{false, true} {
		b.Run(fmt.Sprintf("reusable=%v", reusable), func(b *testing.B) {
			create := func() Iterator {
				if reusable {
					return l.NewIteratorRangeReusable(nil, nil, false)
				}
				return l.NewIteratorRange(nil, nil, false)
			}
			warm := create()
			count := 0
			for warm.Valid() {
				if len(warm.Value()) != 240 {
					b.Fatal("invalid fixture row")
				}
				count++
				warm.Next()
			}
			warm.Close()
			if count != 1000 {
				b.Fatal("invalid fixture count", count)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				it := create()
				n := 0
				for it.Valid() {
					if len(it.Key().UserKey) != 7 || len(it.Value()) != 240 {
						b.Fatal("invalid row")
					}
					n++
					it.Next()
				}
				if n != 1000 || IteratorError(it) != nil {
					b.Fatal("invalid scan", n, IteratorError(it))
				}
				it.Close()
			}
		})
	}
}
