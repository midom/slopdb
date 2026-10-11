// Adapted from /tmp/godb/atomic_memtable_review.go; see TESTING.md.
package regression

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"slopdb/storage"
)

func TestAtomicMemtable(testT *testing.T) {
	testState := newRegressionEnv(testT)
	var seq atomic.Uint64
	mt := storage.NewMemTable()
	var wg sync.WaitGroup
	for w := 0; w < 32; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for j := 0; j < 10000; j++ {
				k := []byte(fmt.Sprintf("w%02d:%05d", w, j))
				s := seq.Add(1)
				mt.Put(s, 1, k, k)
				v, ok := mt.Get(k)
				if !ok || string(v) != string(k) {
					testState.t.Fatalf("%v", fmt.Sprintf("mem key=%q seq=%d got=%q found=%v", k, s, v, ok))
				}
			}
		}(w)
	}
	wg.Wait()
	testState.t.Log(fmt.Sprint("PASS 320000 concurrent immutable memtable node insertions and point reads"))
}
