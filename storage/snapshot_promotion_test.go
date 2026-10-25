package storage

import "testing"

func TestReaderSnapshotPromotionRetainsTransactionHistory(t *testing.T) {
	tree := openReaderViewTest(t)
	if err := tree.Put([]byte("k"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	seq := tree.SnapshotForReader(71)
	if err := tree.PromoteReaderSnapshot(71, seq); err != nil {
		t.Fatal(err)
	}
	tree.ReleaseSnapshotForReader(71, seq)
	if err := tree.PromoteReaderSnapshot(71, seq); err == nil {
		t.Fatal("promoted an unpinned reader")
	}
	if err := tree.Put([]byte("k"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := tree.CompactRange(); err != nil {
		t.Fatal(err)
	}
	v, found, err := tree.GetAt([]byte("k"), seq)
	if err != nil || !found || string(v) != "old" {
		t.Fatal(string(v), found, err)
	}
	tree.ReleaseSnapshot(seq)
	tree.snapshotMu.Lock()
	count := len(tree.snapshots)
	tree.snapshotMu.Unlock()
	if count != 0 {
		t.Fatalf("leaked transaction snapshots: %d", count)
	}
}
