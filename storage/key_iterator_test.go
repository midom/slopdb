package storage

import (
	"bytes"
	"fmt"
	"sort"
	"testing"
)

func checkKeyIterator(t *testing.T, it Iterator, keys []string, reverse bool) {
	t.Helper()
	defer it.Close()
	want := append([]string(nil), keys...)
	sort.Strings(want)
	if reverse {
		sort.Sort(sort.Reverse(sort.StringSlice(want)))
	}
	for _, key := range want {
		if !it.Valid() || string(it.Key().UserKey) != key || it.Value() != nil {
			t.Fatalf("key/value=%q %q, want %q nil (%v)", it.Key().UserKey, it.Value(), key, IteratorError(it))
		}
		if len(IteratorValueInto(it, make([]byte, 5))) != 0 {
			t.Fatal("key iterator copied a value")
		}
		if reverse {
			it.Prev()
		} else {
			it.Next()
		}
	}
	if it.Valid() || IteratorError(it) != nil {
		t.Fatal("key iterator extra row/error", IteratorError(it))
	}
}

func TestKeyIteratorsSnapshotsTombstonesPagesAndCompaction(t *testing.T) {
	for _, memory := range []BlockCacheMemory{BlockCacheHeap, BlockCacheMmap} {
		t.Run(string(memory), func(t *testing.T) {
			l := reusableTestTree(t, memory)
			var old, latest, bounded []string
			for i := 0; i < 120; i++ {
				key := fmt.Sprintf("k%03d", i)
				old = append(old, key)
				if err := l.Put([]byte(key), bytes.Repeat([]byte{byte(i)}, 300)); err != nil {
					t.Fatal(err)
				}
				if i%30 == 29 {
					if err := l.Flush(); err != nil {
						t.Fatal(err)
					}
				}
			}
			seq := l.Snapshot()
			defer l.ReleaseSnapshot(seq)
			pinned := l.NewIteratorAtRangeKeys(seq, nil, nil, false)
			for i := 0; i < 120; i++ {
				key := fmt.Sprintf("k%03d", i)
				if i%3 == 0 {
					if err := l.Put([]byte(key), bytes.Repeat([]byte{'z'}, 350)); err != nil {
						t.Fatal(err)
					}
				}
				if i%7 == 0 {
					if err := l.Delete([]byte(key)); err != nil {
						t.Fatal(err)
					}
				}
				if i%7 != 0 && (i < 40 || i >= 60) {
					latest = append(latest, key)
					if i >= 10 && i < 90 {
						bounded = append(bounded, key)
					}
				}
			}
			if err := l.DeleteRange([]byte("k040"), []byte("k060")); err != nil {
				t.Fatal(err)
			}
			if err := l.CompactRange(); err != nil {
				t.Fatal(err)
			}
			checkKeyIterator(t, pinned, old, false)
			for _, reverse := range []bool{false, true} {
				checkKeyIterator(t, l.NewIteratorAtRangeKeys(seq, nil, nil, reverse), old, reverse)
				checkKeyIterator(t, l.NewIteratorRangeKeys(nil, nil, reverse), latest, reverse)
				checkKeyIterator(t, l.NewIteratorRangeKeys([]byte("k010"), []byte("k090"), reverse), bounded, reverse)
			}
			it := l.NewIteratorRangeKeys(nil, nil, false).(*mergeIterator)
			it.Seek([]byte("k030"))
			it.Seek(it.Key().UserKey)
			if string(it.Key().UserKey) != "k030" {
				t.Fatal("borrowed seek changed position")
			}
			it.Prev()
			if string(it.Key().UserKey) != "k029" {
				t.Fatal("reverse changed position")
			}
			it.Next()
			if string(it.Key().UserKey) != "k030" {
				t.Fatal("direction switch changed position")
			}
			seekIteratorLast(it)
			if string(it.Key().UserKey) != "k118" {
				t.Fatal("last key changed")
			}
			if it.valueScratch != nil || it.lastVal != nil {
				t.Fatal("key iterator retained encoded values")
			}
			it.Close()
			it.Next()
			it.Prev()
			it.Seek(nil)
			if it.Valid() || it.groupScratch != nil {
				t.Fatal("closed key iterator retained scratch/revived")
			}
			ordinary := l.NewIteratorRange(nil, nil, false)
			if !ordinary.Valid() || len(ordinary.Value()) != 300 {
				t.Fatal("ordinary value contract changed")
			}
			ordinary.Close()
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			closed := l.NewIteratorRangeKeys(nil, nil, false)
			defer closed.Close()
			if closed.Valid() || IteratorError(closed) == nil {
				t.Fatal("closed key read succeeded")
			}
		})
	}
}

func TestKeyIteratorDoesNotCopyOversizedPayload(t *testing.T) {
	l := reusableTestTree(t, BlockCacheMmap)
	for _, key := range []string{"a", "b"} {
		if err := l.Put([]byte(key), bytes.Repeat([]byte{'x'}, 128<<10)); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.CompactRange(); err != nil {
		t.Fatal(err)
	}
	it := l.NewIteratorRangeKeys(nil, nil, false).(*mergeIterator)
	defer it.Close()
	for it.Valid() {
		if it.Value() != nil || it.valueScratch != nil {
			t.Fatal("key-only path copied oversized payload")
		}
		it.Next()
	}
}
