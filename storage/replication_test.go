package storage

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestReplicatedMixedRangeBatch(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "db")
	tree, e := NewLSMTree(directory, DefaultOptions())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = tree.Close() })
	if e = tree.WriteBatch([]Mutation{{Key: []byte("a"), Value: []byte("old a")}, {Key: []byte("m"), Value: []byte("old m")}, {Key: []byte("z"), Value: []byte("old z")}}); e != nil {
		t.Fatal(e)
	}
	before := tree.Snapshot()
	released := false
	defer func() {
		if !released {
			tree.ReleaseSnapshot(before)
		}
	}()
	// The replay checkpoint sorts inside the deletion range but has a later
	// sequence in the same atomic record and must remain visible.
	if e = tree.ApplyReplicated([]Mutation{{Key: []byte("a"), Value: []byte("z"), RangeDelete: true}, {Key: []byte("m"), Value: []byte("checkpoint")}}); e != nil {
		t.Fatal(e)
	}
	check := func(tree *LSMTree) {
		t.Helper()
		for key, want := range map[string]string{"a": "", "m": "checkpoint", "z": "old z"} {
			v, ok, e := tree.Get([]byte(key))
			if e != nil || ok != (want != "") || !bytes.Equal(v, []byte(want)) {
				t.Fatalf("%s: %q found=%v error=%v", key, v, ok, e)
			}
		}
	}
	check(tree)
	if v, ok, e := tree.GetAt([]byte("a"), before); e != nil || !ok || string(v) != "old a" {
		t.Fatalf("range deletion broke old snapshot: %q %v %v", v, ok, e)
	}
	tree.ReleaseSnapshot(before)
	released = true
	if e = tree.Close(); e != nil {
		t.Fatal(e)
	}
	tree, e = NewLSMTree(directory, DefaultOptions())
	if e != nil {
		t.Fatal(e)
	}
	check(tree)
	if e = tree.Close(); e != nil {
		t.Fatal(e)
	}
}
func TestReplicationHookRunsOutsideWriterLocks(t *testing.T) {
	tree, e := NewLSMTree(filepath.Join(t.TempDir(), "db"), DefaultOptions())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = tree.Close() })
	calls := 0
	queued := false
	tree.SetReplicationHooks(&ReplicationHooks{Write: func(batch []Mutation, callback func()) error {
		calls++
		if callback != nil {
			callback()
		}
		return tree.ApplyReplicated(batch)
	}, PageSize: func(size int64) error { return tree.SetReplicatedPageSize(size) }})
	if e = tree.WriteBatchWithEnqueue([]Mutation{{Key: []byte("k"), Value: []byte("v")}}, func() { queued = true }); e != nil {
		t.Fatal(e)
	}
	if !queued {
		t.Fatal("hook dropped enqueue callback")
	}
	if e = tree.Put([]byte("x"), []byte("y")); e != nil {
		t.Fatal(e)
	}
	if e = tree.Delete([]byte("x")); e != nil {
		t.Fatal(e)
	}
	if e = tree.DeleteRange([]byte("a"), []byte("z")); e != nil {
		t.Fatal(e)
	}
	if e = tree.SetPageSize(8192); e != nil {
		t.Fatal(e)
	}
	if calls != 4 || tree.PageSize() != 8192 {
		t.Fatalf("hook calls=%d page_size=%d", calls, tree.PageSize())
	}
	if _, found, e := tree.Get([]byte("k")); e != nil || found {
		t.Fatalf("hook lost range tombstone: %v %v", found, e)
	}
}
