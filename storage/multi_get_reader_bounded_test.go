package storage

import (
	"bytes"
	"reflect"
	"testing"
)

func TestMultiGetAtForReaderBoundedExactBudgetDuplicatesAndMissing(t *testing.T) {
	tree := openReaderViewTest(t)
	if err := tree.WriteBatch([]Mutation{{Key: []byte("a"), Value: []byte("abc")}, {Key: []byte("b"), Value: []byte("de")}, {Key: []byte("empty"), Value: []byte{}}}); err != nil {
		t.Fatal(err)
	}
	reader := uint64(9)
	snapshot := tree.SnapshotForReader(reader)
	defer tree.ReleaseSnapshotForReader(reader, snapshot)
	keys := [][]byte{[]byte("missing"), []byte("a"), []byte("a"), []byte("empty"), []byte("b")}
	for _, budget := range []int{0, 2, 3, 7, 8, 9} {
		values, fit, err := tree.GetMultiAtForReaderBounded(keys, snapshot, reader, budget)
		if err != nil {
			t.Fatal(err)
		}
		if budget < 8 {
			if fit || values != nil {
				t.Fatalf("budget %d returned partial values: %#v fit=%v", budget, values, fit)
			}
			continue
		}
		want := []PointValue{{}, {Value: []byte("abc"), Found: true}, {Value: []byte("abc"), Found: true}, {Found: true}, {Value: []byte("de"), Found: true}}
		if !fit || !reflect.DeepEqual(values, want) {
			t.Fatalf("budget %d: %#v fit=%v", budget, values, fit)
		}
		values[1].Value[0] = 'X'
		if string(values[2].Value) != "abc" {
			t.Fatal("duplicates do not own independent copies")
		}
	}
	for _, keys := range [][][]byte{nil, {}, {[]byte("missing"), []byte("empty"), []byte("empty")}} {
		values, fit, err := tree.GetMultiAtForReaderBounded(keys, snapshot, reader, 0)
		legacy, legacyErr := tree.GetMultiAtForReader(keys, snapshot, reader)
		if err != nil || legacyErr != nil || !fit || !reflect.DeepEqual(values, legacy) {
			t.Fatalf("zero-byte batch: %#v fit=%v err=%v, want %#v err=%v", values, fit, err, legacy, legacyErr)
		}
	}
	if values, fit, err := tree.GetMultiAtForReaderBounded(keys, snapshot, reader, -1); err == nil || fit || values != nil {
		t.Fatalf("negative budget: %#v %v %v", values, fit, err)
	}
}

func TestMultiGetAtForReaderBoundedRejectsHugeValueBeforeCopy(t *testing.T) {
	tree := openReaderViewTest(t)
	if err := tree.Put([]byte("huge"), bytes.Repeat([]byte{'x'}, 1<<20)); err != nil {
		t.Fatal(err)
	}
	reader := uint64(3)
	snapshot := tree.SnapshotForReader(reader)
	defer tree.ReleaseSnapshotForReader(reader, snapshot)
	keys := [][]byte{[]byte("huge")}
	allocations := testing.AllocsPerRun(100, func() {
		values, fit, err := tree.GetMultiAtForReaderBounded(keys, snapshot, reader, 1)
		if err != nil || fit || values != nil {
			panic("oversized value accepted")
		}
	})
	// The result slice allocates once per call. Race builds can discard pooled
	// workspaces occasionally, but copying the huge payload would add another
	// allocation on every call. Allow pool misses, not a payload copy.
	if allocations >= 2 {
		t.Fatalf("oversized memtable value caused %g allocations, indicating a payload copy", allocations)
	}
}

func TestMultiGetAtForReaderBoundedSnapshotOwnershipAfterClose(t *testing.T) {
	for _, memory := range []BlockCacheMemory{BlockCacheHeap, BlockCacheMmap} {
		t.Run(string(memory), func(t *testing.T) {
			options := DefaultOptions()
			options.DisableWALSync = true
			options.BlockCacheMemory = memory
			tree, err := NewLSMTree(t.TempDir(), options)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = tree.Close() })
			if err := tree.Put([]byte("a"), []byte("old")); err != nil {
				t.Fatal(err)
			}
			if err := tree.Flush(); err != nil {
				t.Fatal(err)
			}
			reader := uint64(15)
			snapshot := tree.SnapshotForReader(reader)
			defer tree.ReleaseSnapshotForReader(reader, snapshot)
			if err := tree.Put([]byte("a"), []byte("new")); err != nil {
				t.Fatal(err)
			}
			if err := tree.CompactRange(); err != nil {
				t.Fatal(err)
			}
			keys := [][]byte{[]byte("a"), []byte("a"), []byte("missing")}
			if values, fit, err := tree.GetMultiAtForReaderBounded(keys, snapshot, reader, 5); err != nil || fit || values != nil {
				t.Fatalf("undersized SSTable budget: %#v %v %v", values, fit, err)
			}
			values, fit, err := tree.GetMultiAtForReaderBounded(keys, snapshot, reader, 6)
			if err != nil || !fit || len(values) != 3 || string(values[0].Value) != "old" || string(values[1].Value) != "old" || values[2].Found {
				t.Fatalf("pinned snapshot: %#v %v %v", values, fit, err)
			}
			values[0].Value[0] = 'X'
			if string(values[1].Value) != "old" {
				t.Fatal("duplicate SSTable values alias")
			}
			if err := tree.Close(); err != nil {
				t.Fatal(err)
			}
			if string(values[0].Value) != "Xld" || string(values[1].Value) != "old" {
				t.Fatal("tree close invalidated owned batch results")
			}
			for _, keys := range [][][]byte{nil, {[]byte("a")}} {
				if values, fit, err := tree.GetMultiAtForReaderBounded(keys, snapshot, reader, 100); err == nil || fit || values != nil {
					t.Fatalf("closed tree batch: %#v %v %v", values, fit, err)
				}
			}
		})
	}
}
