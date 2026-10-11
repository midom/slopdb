// Adapted from flush_review.go; see TESTING.md.
package regression

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"slopdb/storage"
)

func flushMust(testState *regressionEnv, e error) {
	testState.t.Helper()
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
}
func TestFlush(testT *testing.T) {
	testState := newRegressionEnv(testT)

	for mode := 0; mode < 3; mode++ {
		dir := testState.path(fmt.Sprintf("godb_flush_review_%d", mode))
		os.RemoveAll(dir)
		o := storage.DefaultOptions()
		o.WriteBufferSize = 4096
		o.BlockSize = 256
		o.CompactionL0Trigger = 3
		o.DisableWAL = mode == 1
		o.DisableWALSync = mode == 2
		l, e := testState.openStorage(dir, o)
		flushMust(testState, e)
		for i := 0; i < 200; i++ {
			flushMust(testState, l.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("old")))
		}
		snap := l.Snapshot()
		for i := 0; i < 200; i++ {
			flushMust(testState, l.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("new")))
		}
		flushMust(testState, l.Flush())
		s := l.Stats()
		if s.MemSize != 0 || s.ImmSize != 0 {
			testState.t.Fatalf("%v", "Flush left unflushed data")
		}
		for i := 0; i < 200; i++ {
			key := []byte(fmt.Sprintf("k%03d", i))
			v, ok, e := l.GetAt(key, snap)
			flushMust(testState, e)
			if !ok || string(v) != "old" {
				testState.t.Fatalf("%v", "snapshot after flush")
			}
		}
		var wg sync.WaitGroup
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					flushMust(testState, l.Put([]byte(fmt.Sprintf("w%d_%03d", w, i)), []byte("value")))
					if i%10 == 0 {
						flushMust(testState, l.Flush())
					}
				}
			}(w)
		}
		wg.Wait()
		l.ReleaseSnapshot(snap)
		flushMust(testState, l.Flush())
		flushMust(testState, l.CompactRange())
		flushMust(testState, l.Close())
		l, e = testState.openStorage(dir, o)
		flushMust(testState, e)
		it := l.NewIterator()
		n := 0
		for it.Valid() {
			n++
			it.Next()
		}
		flushMust(testState, storage.IteratorError(it))
		it.Close()
		if n != 400 {
			testState.t.Fatalf("%v", fmt.Sprintf("persisted %d keys", n))
		}
		flushMust(testState, l.Flush())
		flushMust(testState, l.Close())
		if e = l.Flush(); e == nil {
			testState.t.Fatalf("%v", "Flush after close succeeded")
		}
		testState.t.Logf("PASS mode%d flush durability, old snapshots, concurrent rotation/flush, compaction and reopen\n", mode)
	}
}
