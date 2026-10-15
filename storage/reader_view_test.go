package storage

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func openReaderViewTest(t *testing.T) *LSMTree {
	t.Helper()
	o := DefaultOptions()
	o.DisableWALSync = true
	o.MaxOpenFiles = 1
	o.CompactionL0Trigger = 100
	l, err := NewLSMTree(filepath.Join(t.TempDir(), "database"), o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func TestReaderViewRetirementWaitsForEveryReference(t *testing.T) {
	l := openReaderViewTest(t)
	key := []byte("key")
	if err := l.Put(key, []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	seq := l.Snapshot()
	defer l.ReleaseSnapshot(seq)
	legacy, _ := l.acquireView()
	defer func() {
		if legacy != nil {
			legacy.release()
		}
	}()
	type pin struct {
		view  *readView
		shard uint64
	}
	pins := make([]pin, snapshotReaderShards*2)
	for i := range pins {
		v, _, shard := l.acquireViewForReader(uint64(i))
		if v != legacy {
			t.Fatal("readers acquired different initial views")
		}
		pins[i] = pin{v, shard}
	}
	if err := l.Put(key, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := l.CompactRange(); err != nil {
		t.Fatal(err)
	}
	if !legacy.retired.Load() || legacy.closed.Load() {
		t.Fatal("retired pinned view was released")
	}
	var wg sync.WaitGroup
	for _, p := range pins {
		wg.Add(1)
		go func(p pin) {
			defer wg.Done()
			defer p.view.releaseReader(p.shard)
			value, found, _, err := readPointFromView(p.view, key, seq, 0, false)
			if err != nil || !found || string(value) != "old" {
				t.Errorf("retired read: %q %v %v", value, found, err)
			}
		}(p)
	}
	wg.Wait()
	if legacy.closed.Load() {
		t.Fatal("reader release ignored remaining legacy reference")
	}
	old := legacy
	legacy.release()
	legacy = nil
	if !old.closed.Load() {
		t.Fatal("last reference did not retire view")
	}
	for _, slot := range old.files {
		_, err := slot.entry.reader.file.ReadAt(make([]byte, 1), 0)
		if !errors.Is(err, os.ErrClosed) {
			t.Fatalf("retired descriptor not closed: %v", err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReaderViewConcurrentPublicationAndSnapshotRetention(t *testing.T) {
	l := openReaderViewTest(t)
	keys := [][]byte{[]byte("a"), []byte("b")}
	write := func(n uint64) error {
		value := make([]byte, 8)
		binary.LittleEndian.PutUint64(value, n)
		return l.WriteBatch([]Mutation{{Key: keys[0], Value: value}, {Key: keys[1], Value: value}})
	}
	if err := write(0); err != nil {
		t.Fatal(err)
	}
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for n := uint64(1); n <= 40; n++ {
			if err := write(n); err != nil {
				t.Error(err)
				return
			}
			if n%4 == 0 {
				if err := l.Flush(); err != nil {
					t.Error(err)
					return
				}
			}
			if n%8 == 0 {
				if err := l.CompactRange(); err != nil {
					t.Error(err)
					return
				}
			}
		}
	}()
	for worker := 0; worker < 100; worker++ {
		wg.Add(1)
		go func(reader uint64) {
			defer wg.Done()
			<-start
			for n := 0; n < 100; n++ {
				seq := l.SnapshotForReader(reader)
				a, foundA, errA := l.GetAtForReader(keys[0], seq, reader)
				b, foundB, errB := l.GetAtForReader(keys[1], seq, reader)
				l.ReleaseSnapshotForReader(reader, seq)
				if errA != nil || errB != nil || !foundA || !foundB || len(a) != 8 || len(b) != 8 || binary.LittleEndian.Uint64(a) != binary.LittleEndian.Uint64(b) {
					t.Errorf("incoherent snapshot: %x %x %v %v", a, b, errA, errB)
					return
				}
				a[0] = 255 // the public reader-aware path also returns owned values
			}
		}(uint64(worker % 8))
	}
	close(start)
	wg.Wait()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := l.GetAtForReader(keys[0], 0, 1); err == nil {
		t.Fatal("closed tree accepted read")
	}
}
