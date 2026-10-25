package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func TestMultiGetAtForReaderSnapshotsOrderAndOwnership(t *testing.T) {
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
			if err := tree.WriteBatch([]Mutation{{Key: []byte("a"), Value: []byte("old-a")}, {Key: []byte("b"), Value: []byte("old-b")}, {Key: []byte("empty"), Value: []byte{}}}); err != nil {
				t.Fatal(err)
			}
			if err := tree.Flush(); err != nil {
				t.Fatal(err)
			}
			reader := uint64(7)
			before := tree.SnapshotForReader(reader)
			defer tree.ReleaseSnapshotForReader(reader, before)
			if err := tree.WriteBatch([]Mutation{{Key: []byte("a"), Value: []byte("new-a")}, {Key: []byte("b"), Delete: true}, {Key: []byte("c"), Value: []byte("new-c")}}); err != nil {
				t.Fatal(err)
			}
			if err := tree.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := tree.CompactRange(); err != nil {
				t.Fatal(err)
			}
			keys := [][]byte{[]byte("b"), []byte("missing"), []byte("a"), []byte("a"), []byte("c"), []byte("empty")}
			values, err := tree.GetMultiAtForReader(keys, before, reader)
			if err != nil || len(values) != len(keys) {
				t.Fatalf("snapshot batch: %v %v", values, err)
			}
			want := []PointValue{{Value: []byte("old-b"), Found: true}, {}, {Value: []byte("old-a"), Found: true}, {Value: []byte("old-a"), Found: true}, {}, {Found: true}}
			if !reflect.DeepEqual(values, want) {
				t.Fatalf("ordered snapshot values: %#v, want %#v", values, want)
			}
			// Each duplicate owns its own data; mutation cannot affect another
			// duplicate or a subsequent read from the same cached storage page.
			values[2].Value[0] = 'X'
			if string(values[3].Value) != "old-a" {
				t.Fatal("duplicate results alias one another")
			}
			again, err := tree.GetMultiAtForReader(keys, before, reader)
			if err != nil || !reflect.DeepEqual(again, want) {
				t.Fatalf("caller mutation reached storage: %#v %v", again, err)
			}
			latest, err := tree.GetMultiAtForReader(keys, ^uint64(0), reader)
			if err != nil || !latest[2].Found || string(latest[2].Value) != "new-a" || latest[0].Found || !latest[4].Found || string(latest[4].Value) != "new-c" {
				t.Fatalf("latest watermark clipping: %#v %v", latest, err)
			}
			legacy, err := tree.GetMultiAt(keys, before)
			if err != nil || !reflect.DeepEqual(legacy, again) {
				t.Fatalf("reader/legacy contract differs: %#v %#v %v", legacy, again, err)
			}
			if err := tree.Close(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(again, want) || string(latest[2].Value) != "new-a" {
				t.Fatal("tree close invalidated owned batch values")
			}
			if _, err := tree.GetMultiAtForReader(keys, before, reader); err == nil {
				t.Fatal("closed tree accepted batch")
			}
			if _, err := tree.GetMultiAtForReader(nil, before, reader); err == nil {
				t.Fatal("closed tree accepted empty batch")
			}
		})
	}
}

func TestMultiGetAtForReaderZeroSnapshotAndEmptyBatch(t *testing.T) {
	tree := openReaderViewTest(t)
	reader := uint64(0)
	zero := tree.SnapshotForReader(reader)
	defer tree.ReleaseSnapshotForReader(reader, zero)
	if zero != 0 {
		t.Fatal("fresh tree has committed rows")
	}
	if err := tree.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	values, err := tree.GetMultiAtForReader([][]byte{[]byte("key")}, zero, reader)
	if err != nil || len(values) != 1 || values[0].Found {
		t.Fatalf("sequence zero read latest data: %#v %v", values, err)
	}
	for _, keys := range [][][]byte{nil, {}} {
		values, err := tree.GetMultiAtForReader(keys, zero, reader)
		legacy, legacyErr := tree.GetMultiAt(keys, zero)
		if err != nil || legacyErr != nil || !reflect.DeepEqual(values, legacy) {
			t.Fatalf("empty batch contract differs: %#v %#v %v %v", values, legacy, err, legacyErr)
		}
	}
}

func TestMultiGetAtForReaderConcurrentPublicationAndShardCollisions(t *testing.T) {
	tree := openReaderViewTest(t)
	write := func(n uint64) error {
		value := make([]byte, 8)
		binary.LittleEndian.PutUint64(value, n)
		return tree.WriteBatch([]Mutation{{Key: []byte("a"), Value: value}, {Key: []byte("b"), Value: value}, {Key: []byte("c"), Value: value, Delete: n%2 == 1}})
	}
	if err := write(0); err != nil {
		t.Fatal(err)
	}
	if err := tree.Flush(); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		<-start
		for n := uint64(1); n <= 20; n++ {
			if err := write(n); err != nil {
				t.Error(err)
				return
			}
			if n%4 == 0 {
				if err := tree.Flush(); err != nil {
					t.Error(err)
					return
				}
			}
			if n%8 == 0 {
				if err := tree.CompactRange(); err != nil {
					t.Error(err)
					return
				}
			}
		}
	}()
	for worker := 0; worker < 32; worker++ {
		workers.Add(1)
		go func(reader uint64) {
			defer workers.Done()
			<-start
			keys := [][]byte{[]byte("b"), []byte("missing"), []byte("a"), []byte("a"), []byte("c")}
			for pass := 0; pass < 40; pass++ {
				seq := tree.SnapshotForReader(reader)
				values, err := tree.GetMultiAtForReader(keys, seq, reader)
				tree.ReleaseSnapshotForReader(reader, seq)
				if err != nil || len(values) != len(keys) {
					t.Errorf("batch: %#v %v", values, err)
					return
				}
				if !values[0].Found || !values[2].Found || !values[3].Found || values[1].Found || len(values[0].Value) != 8 || !bytes.Equal(values[0].Value, values[2].Value) || !bytes.Equal(values[2].Value, values[3].Value) {
					t.Errorf("incoherent atomic snapshot: %#v", values)
					return
				}
				n := binary.LittleEndian.Uint64(values[0].Value)
				if values[4].Found != (n%2 == 0) || (values[4].Found && !bytes.Equal(values[4].Value, values[0].Value)) {
					t.Errorf("delete disagrees with batch version %d: %#v", n, values)
					return
				}
				values[2].Value[0] ^= 255
				if !bytes.Equal(values[0].Value, values[3].Value) {
					t.Error(fmt.Errorf("duplicate aliases another result for reader %d", reader))
					return
				}
			}
		}(uint64(worker%4) + uint64(worker/4)*snapshotReaderShards)
	}
	close(start)
	workers.Wait()
}
