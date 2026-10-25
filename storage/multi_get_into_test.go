package storage

import (
	"bytes"
	"testing"
)

func TestMultiGetForReaderIntoOwnershipSnapshotsAndReuse(t *testing.T) {
	for _, memory := range []BlockCacheMemory{BlockCacheHeap, BlockCacheMmap} {
		t.Run(string(memory), func(t *testing.T) {
			tree := reusableTestTree(t, memory)
			if err := tree.WriteBatch([]Mutation{{Key: []byte("a"), Value: []byte("old-a")}, {Key: []byte("b"), Value: []byte("old-b")}, {Key: []byte("empty"), Value: []byte{}}}); err != nil {
				t.Fatal(err)
			}
			if err := tree.Flush(); err != nil {
				t.Fatal(err)
			}
			reader := uint64(83)
			seq := tree.SnapshotForReader(reader)
			defer tree.ReleaseSnapshotForReader(reader, seq)
			if err := tree.WriteBatch([]Mutation{{Key: []byte("a"), Value: []byte("new-a")}, {Key: []byte("b"), Delete: true}}); err != nil {
				t.Fatal(err)
			}
			if err := tree.CompactRange(); err != nil {
				t.Fatal(err)
			}
			keys := [][]byte{[]byte("a"), []byte("a"), []byte("b"), []byte("missing"), []byte("empty")}
			dst := make([]PointValue, len(keys))
			for i := range dst {
				dst[i].Value = make([]byte, 0, 32)
			}
			array, first, duplicate := &dst[0], &dst[0].Value[:cap(dst[0].Value)][0], &dst[1].Value[:cap(dst[1].Value)][0]
			got, err := tree.GetMultiAtForReaderInto(keys, seq, reader, dst)
			if err != nil || len(got) != len(keys) || &got[0] != array || &got[0].Value[0] != first || &got[1].Value[0] != duplicate {
				t.Fatalf("buffers not reused: %+v %v", got, err)
			}
			if string(got[0].Value) != "old-a" || string(got[1].Value) != "old-a" || string(got[2].Value) != "old-b" || got[3].Found || !got[4].Found || len(got[4].Value) != 0 {
				t.Fatal("snapshot/order/missing/empty changed", got)
			}
			got[0].Value[0] = 'X'
			if string(got[1].Value) != "old-a" || string(keys[0]) != "a" {
				t.Fatal("duplicate or input key aliased a result")
			}
			ordinary, err := tree.GetMultiAtForReader(keys, seq, reader)
			if err != nil || string(ordinary[0].Value) != "old-a" {
				t.Fatal("caller mutation reached storage", err)
			}
			got, err = tree.GetMultiAtForReaderInto(keys[:3], ^uint64(0), reader, got)
			if err != nil || string(got[0].Value) != "new-a" || got[2].Found || dst[3].Value != nil || dst[4].Value != nil {
				t.Fatal("latest/shrinking batch failed", err)
			}
			if string(ordinary[0].Value) != "old-a" || string(ordinary[1].Value) != "old-a" {
				t.Fatal("ordinary GetMulti retained values were overwritten")
			}
			empty, err := tree.GetMultiAtForReaderInto(nil, seq, reader, got)
			if err != nil || len(empty) != 0 {
				t.Fatal("empty batch failed", err)
			}
			got, err = tree.GetMultiAtForReaderInto(keys, seq, reader, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := tree.Close(); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got[0].Value, []byte("old-a")) || !bytes.Equal(ordinary[2].Value, []byte("old-b")) {
				t.Fatal("close invalidated owned values")
			}
			if values, err := tree.GetMultiAtForReaderInto(keys, seq, reader, got); err == nil || values != nil {
				t.Fatal("closed database accepted batch")
			}
		})
	}
}
