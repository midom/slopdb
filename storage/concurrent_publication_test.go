package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
)

func TestParallelSkipListInstallationWaitsForWholeGroup(t *testing.T) {
	l := openReaderViewTest(t)
	for _, key := range []string{"a", "b"} {
		if err := l.Put([]byte(key), []byte("old")); err != nil {
			t.Fatal(err)
		}
	}
	old := l.Snapshot()
	defer l.ReleaseSnapshot(old)
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWriters := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseWriters()
	done := make(chan error, 1)
	batch := []Mutation{{Key: []byte("a"), Value: []byte("new")}, {Key: []byte("b"), Value: []byte("new")}}
	go func() {
		done <- l.writeMutationsWithInstaller(batch, true, false, func(mem *MemTable, base uint64) {
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); mem.installSortedMutations(batch[:1], base); close(started) }()
			go func() { defer wg.Done(); <-release; mem.installSortedMutations(batch[1:], base+1) }()
			wg.Wait()
		})
	}()
	<-started
	// One new row is physically installed, but no reader can observe half a group.
	for _, key := range []string{"a", "b"} {
		v, ok, err := l.Get([]byte(key))
		if err != nil || !ok || string(v) != "old" {
			t.Fatal("partially installed group became visible", key, string(v), ok, err)
		}
	}
	releaseWriters()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "b"} {
		v, ok, err := l.Get([]byte(key))
		if err != nil || !ok || string(v) != "new" {
			t.Fatal("group was not published", key, string(v), ok, err)
		}
		v, ok, err = l.GetAt([]byte(key), old)
		if err != nil || !ok || string(v) != "old" {
			t.Fatal("retained snapshot changed", key, string(v), ok, err)
		}
	}
}

func TestConcurrentQueuedSkipListBatchesKeepSnapshotsAtomic(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		for _, noWAL := range []bool{false, true} {
			t.Run(fmt.Sprintf("parallel=%v/noWAL=%v", parallel, noWAL), func(t *testing.T) {
				opts := DefaultOptions()
				opts.ParallelMemTableWrites = parallel
				opts.DisableWAL = noWAL
				opts.DisableWALSync = true
				opts.WriteBufferSize = 64 << 10
				dir := t.TempDir()
				l, err := NewLSMTree(dir, opts)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = l.Close() }()
				const writers = 32
				const rounds = 100
				for w := 0; w < writers; w++ {
					if err = l.WriteBatch([]Mutation{{Key: []byte(fmt.Sprintf("%02da", w)), Value: make([]byte, 8)}, {Key: []byte(fmt.Sprintf("%02db", w)), Value: make([]byte, 8)}}); err != nil {
						t.Fatal(err)
					}
				}
				var wg sync.WaitGroup
				stop := make(chan struct{})
				readerDone := make(chan struct{})
				go func() {
					defer close(readerDone)
					for {
						select {
						case <-stop:
							return
						default:
						}
						snapshot := l.Snapshot()
						for w := 0; w < writers; w++ {
							a, okA, errA := l.GetAt([]byte(fmt.Sprintf("%02da", w)), snapshot)
							b, okB, errB := l.GetAt([]byte(fmt.Sprintf("%02db", w)), snapshot)
							if errA != nil || errB != nil || !okA || !okB || !bytes.Equal(a, b) {
								t.Error("snapshot saw a partial two-row transaction", w, snapshot)
								l.ReleaseSnapshot(snapshot)
								return
							}
						}
						l.ReleaseSnapshot(snapshot)
					}
				}()
				for w := 0; w < writers; w++ {
					wg.Add(1)
					go func(w int) {
						defer wg.Done()
						for n := 1; n <= rounds; n++ {
							var value [8]byte
							binary.LittleEndian.PutUint64(value[:], uint64(n))
							if e := l.WriteBatch([]Mutation{{Key: []byte(fmt.Sprintf("%02da", w)), Value: value[:]}, {Key: []byte(fmt.Sprintf("%02db", w)), Value: value[:]}}); e != nil {
								t.Error(e)
								return
							}
						}
					}(w)
				}
				wg.Wait()
				close(stop)
				<-readerDone
				if err = l.Close(); err != nil {
					t.Fatal(err)
				}
				l, err = NewLSMTree(dir, opts)
				if err != nil {
					t.Fatal(err)
				}
				for w := 0; w < writers; w++ {
					for _, suffix := range []string{"a", "b"} {
						v, ok, e := l.Get([]byte(fmt.Sprintf("%02d%s", w, suffix)))
						if e != nil || !ok || len(v) != 8 || binary.LittleEndian.Uint64(v) != rounds {
							t.Fatal("reopen lost completed transaction", w, suffix, v, ok, e)
						}
					}
				}
			})
		}
	}
}
