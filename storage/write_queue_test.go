package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestQueuedBatchesKeepMetadataOrder(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.DisableWALSync = true
	l, err := NewLSMTree(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	var publication sync.Mutex
	publication.Lock()
	started := make(chan struct{})
	second := make(chan error, 1)
	go func() {
		close(started)
		publication.Lock()
		second <- l.WriteBatchWithEnqueue([]Mutation{{Key: []byte("counter"), Value: []byte("2")}, {Key: []byte("row2"), Value: []byte("second")}}, publication.Unlock)
	}()
	<-started
	err = l.WriteBatchWithEnqueue([]Mutation{{Key: []byte("counter"), Value: []byte("1")}, {Key: []byte("row1"), Value: []byte("first")}}, publication.Unlock)
	if err != nil {
		t.Fatal(err)
	}
	if err = <-second; err != nil {
		t.Fatal(err)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err = NewLSMTree(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	for key, want := range map[string]string{"counter": "2", "row1": "first", "row2": "second"} {
		got, found, e := l.Get([]byte(key))
		if e != nil || !found || string(got) != want {
			t.Fatalf("%s: got %q found=%v err=%v", key, got, found, e)
		}
	}
}

func TestQueuedCallbackRunsOnWALFailure(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.DisableWALSync = true
	l, err := NewLSMTree(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err = os.Mkdir(filepath.Join(dir, "WAL-1.log"), 0700); err != nil {
		t.Fatal(err)
	}
	called := false
	err = l.WriteBatchWithEnqueue([]Mutation{{Key: []byte("row"), Value: []byte("value")}}, func() { called = true })
	if !called || err == nil || l.Stats().SeqNum != 0 {
		t.Fatalf("queued=%v err=%v seq=%d", called, err, l.Stats().SeqNum)
	}
	if _, found, e := l.Get([]byte("row")); e != nil || found {
		t.Fatalf("failed row published: found=%v err=%v", found, e)
	}
}

// Hold enqueue callbacks until every request is linked. This makes the group
// limits observable without relying on the scheduler to build a large backlog.
func TestQueuedBacklogBoundsAndAtomicLargeBatch(t *testing.T) {
	for _, tc := range []struct {
		name                            string
		count, size, largeFirst, groups int
	}{
		{"transaction_limit", 600, 80, 0, 3},
		{"byte_limit", 20, 64 << 10, 0, 2},
		{"large_atomic_batch", 20, 64 << 10, 2 << 20, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := DefaultOptions()
			opts.DisableWALSync = true
			l, err := NewLSMTree(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			release := make(chan struct{})
			enqueued := make(chan struct{}, tc.count+1)
			errs := make(chan error, tc.count+1)
			enqueue := func(id, size int) {
				value := bytes.Repeat([]byte{byte(id)}, size)
				go func() {
					errs <- l.WriteBatchWithEnqueue([]Mutation{
						{Key: []byte(fmt.Sprintf("row-%04d", id)), Value: value},
						{Key: []byte(fmt.Sprintf("mirror-%04d", id)), Value: []byte{byte(id)}},
					}, func() { enqueued <- struct{}{}; <-release })
				}()
			}
			count := tc.count
			if tc.largeFirst > 0 {
				enqueue(tc.count, tc.largeFirst)
				<-enqueued
				count++
			}
			for i := 0; i < tc.count; i++ {
				enqueue(i, tc.size)
			}
			for i := 0; i < tc.count; i++ {
				<-enqueued
			}
			if l.writeGroups.Load() != 0 {
				t.Fatal("queued callback did not stop publication")
			}
			close(release)
			for i := 0; i < count; i++ {
				if err := <-errs; err != nil {
					t.Fatal(err)
				}
			}
			if l.writeGroups.Load() != int64(tc.groups) || l.writeBatches.Load() != int64(count) {
				t.Fatalf("groups=%d batches=%d", l.writeGroups.Load(), l.writeBatches.Load())
			}
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			l, err = NewLSMTree(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			for i := 0; i < count; i++ {
				size := tc.size
				if i == tc.count {
					size = tc.largeFirst
				}
				value, found, err := l.Get([]byte(fmt.Sprintf("row-%04d", i)))
				if err != nil || !found || !bytes.Equal(value, bytes.Repeat([]byte{byte(i)}, size)) {
					t.Fatalf("row %d not recovered", i)
				}
				mirror, found, err := l.Get([]byte(fmt.Sprintf("mirror-%04d", i)))
				if err != nil || !found || !bytes.Equal(mirror, []byte{byte(i)}) {
					t.Fatalf("mirror %d not recovered", i)
				}
			}
		})
	}
}

func TestQueuedProducerGapPreservesFirstLeader(t *testing.T) {
	q := new(batchQueue)
	q.init()
	first, second := new(queuedBatch), new(queuedBatch)
	first.ready.Add(1)
	second.ready.Add(1)
	a, b := &queuedLink{request: first}, &queuedLink{request: second}
	previous := q.tail.Swap(a)
	q.tail.Swap(b).next.Store(b)
	// The second producer is linked, but the first producer is not. Election
	// must wait for the first link rather than hand leadership to the second.
	started, done := make(chan struct{}), make(chan struct{})
	go func() { close(started); q.wake(); close(done) }()
	<-started
	previous.next.Store(a)
	<-done
	first.ready.Wait()
	if !first.lead || second.lead {
		t.Fatal("producer gap reordered leadership")
	}
}
