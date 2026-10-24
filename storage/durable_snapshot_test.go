package storage

import "testing"

func TestRetainSnapshotForReader(t *testing.T) {
	options := DefaultOptions()
	options.DisableWALSync = true
	tree, err := NewLSMTree(t.TempDir(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tree.Close() })
	if err = tree.Put([]byte("key"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	seq := tree.Snapshot()
	if err = tree.Put([]byte("key"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err = tree.RetainSnapshotForReader(17, seq); err != nil {
		t.Fatal(err)
	}
	tree.ReleaseSnapshot(seq)
	if err = tree.CompactRange(); err != nil {
		t.Fatal(err)
	}
	value, found, err := tree.GetAtForReader([]byte("key"), seq, 17)
	if err != nil || !found || string(value) != "old" {
		t.Fatalf("copied pin lost versions: %q %v %v", value, found, err)
	}
	tree.ReleaseSnapshotForReader(17, seq)
	if err = tree.RetainSnapshotForReader(18, seq); err == nil {
		t.Fatal("registered an unpinned historical sequence")
	}
	if err = tree.CompactRange(); err != nil {
		t.Fatal(err)
	}
	value, found, err = tree.Get([]byte("key"))
	if err != nil || !found || string(value) != "new" {
		t.Fatalf("latest value lost: %q %v %v", value, found, err)
	}
	if err = tree.Close(); err != nil {
		t.Fatal(err)
	}
}
