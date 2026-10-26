package storage

import (
	"math"
	"testing"
)

func TestExistsAtForReaderMVCCAndRangeDelete(t *testing.T) {
	l := reusableTestTree(t, BlockCacheHeap)
	if err := l.Put([]byte("a"), []byte{}); err != nil {
		t.Fatal(err)
	}
	snapshot := l.SnapshotForReader(19)
	defer l.ReleaseSnapshotForReader(19, snapshot)
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := l.DeleteRange([]byte("a"), []byte("b")); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		snapshot uint64
		want     bool
	}{{0, false}, {snapshot, true}, {math.MaxUint64, false}} {
		found, err := l.ExistsAtForReader([]byte("a"), c.snapshot, 19)
		if err != nil || found != c.want {
			t.Fatalf("snapshot%d found%v want%v err%v", c.snapshot, found, c.want, err)
		}
	}
	if err := l.Put([]byte("a"), []byte("resurrected")); err != nil {
		t.Fatal(err)
	}
	if err := l.CompactRange(); err != nil {
		t.Fatal(err)
	}
	found, err := l.ExistsAtForReader([]byte("a"), math.MaxUint64, 19)
	if err != nil || !found {
		t.Fatal("resurrection", found, err)
	}
}
