// Adapted from /tmp/godb/bulk_memtable_review.go; see TESTING.md.
package regression

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"

	"slopdb/storage"
)

func bulkMemtableMust(testState *regressionEnv, e error) {
	testState.t.Helper()
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
}
func bulkMemtableKey(testState *regressionEnv, i int) []byte {
	testState.t.Helper()
	return []byte(fmt.Sprintf("k%03d", i))
}
func bulkMemtableCheck(testState *regressionEnv, l *storage.LSMTree, w map[string]string, s *uint64) {
	testState.t.Helper()
	for i := 0; i < 100; i++ {
		var v []byte
		var ok bool
		var e error
		if s == nil {
			v, ok, e = l.Get(bulkMemtableKey(testState, i))
		} else {
			v, ok, e = l.GetAt(bulkMemtableKey(testState, i), *s)
		}
		bulkMemtableMust(testState, e)
		want, exists := w[string(bulkMemtableKey(testState, i))]
		if ok != exists || ok && string(v) != want {
			testState.t.Fatalf("%v", fmt.Sprintf("point k=%d got=%q exists=%v expected=%q/%v", i, v, ok, want, exists))
		}
	}
	it := l.NewIterator()
	if s != nil {
		it.Close()
		it = l.NewIteratorAt(*s)
	}
	n := 0
	for it.Valid() {
		if w[string(it.Key().UserKey)] != string(it.Value()) {
			testState.t.Fatalf("%v", "iterator version")
		}
		n++
		it.Next()
	}
	bulkMemtableMust(testState, storage.IteratorError(it))
	it.Close()
	if n != len(w) {
		testState.t.Fatalf("%v", "iterator count")
	}
}
func TestBulkMemtable(testT *testing.T) {
	testState := newRegressionEnv(testT)

	for mode := 0; mode < 3; mode++ {
		dir := testState.path(fmt.Sprintf("godb_bulk_memtable_%d", mode))
		bulkMemtableMust(testState, os.RemoveAll(dir))
		o := storage.DefaultOptions()
		o.DisableWAL = mode == 0
		o.DisableWALSync = mode == 1
		o.WriteBufferSize = 4096
		o.BlockSize = 256
		o.MaxOpenFiles = 1
		o.CompactionL0Trigger = 3
		l, e := testState.openStorage(dir, o)
		bulkMemtableMust(testState, e)
		want := map[string]string{}
		rng := rand.New(rand.NewSource(91))
		for i := 0; i < 100; i++ {
			want[string(bulkMemtableKey(testState, i))] = "old"
			bulkMemtableMust(testState, l.Put(bulkMemtableKey(testState, i), []byte("old")))
		}
		snap := l.Snapshot()
		old := map[string]string{}
		for k, v := range want {
			old[k] = v
		}
		for round := 0; round < 12; round++ {
			var b []storage.Mutation
			for i := 0; i < 400; i++ {
				id := rng.Intn(100)
				v := fmt.Sprintf("v%d:%d", round, i)
				del := i%7 == 0
				b = append(b, storage.Mutation{Key: bulkMemtableKey(testState, id), Value: []byte(v), Delete: del})
				if del {
					delete(want, string(bulkMemtableKey(testState, id)))
				} else {
					want[string(bulkMemtableKey(testState, id))] = v
				}
			}
			bulkMemtableMust(testState, l.WriteBatch(b))
			bulkMemtableCheck(testState, l, want, nil)
			bulkMemtableCheck(testState, l, old, &snap)
		}
		bulkMemtableMust(testState, l.DeleteRange(bulkMemtableKey(testState, 20), bulkMemtableKey(testState, 40)))
		for i := 20; i < 40; i++ {
			delete(want, string(bulkMemtableKey(testState, i)))
		}
		var b []storage.Mutation
		for i := 0; i < 100; i++ {
			b = append(b, storage.Mutation{Key: bulkMemtableKey(testState, i), Value: []byte("bulk")})
			want[string(bulkMemtableKey(testState, i))] = "bulk"
		}
		bulkMemtableMust(testState, l.WriteBatch(b))
		bulkMemtableCheck(testState, l, want, nil)
		bulkMemtableCheck(testState, l, old, &snap)
		l.ReleaseSnapshot(snap)
		bulkMemtableMust(testState, l.CompactRange())
		bulkMemtableMust(testState, l.Close())
		l, e = testState.openStorage(dir, o)
		bulkMemtableMust(testState, e)
		bulkMemtableCheck(testState, l, want, nil)
		bulkMemtableMust(testState, l.Close())
		testState.t.Logf("PASS bulk sorted repeated-key writes/deletes, old snapshots, ranges, iterators, compaction, persistence: mode %d\n", mode)
		dir = testState.path(fmt.Sprintf("godb_bulk_atomic_%d", mode))
		bulkMemtableMust(testState, os.RemoveAll(dir))
		l, e = testState.openStorage(dir, o)
		bulkMemtableMust(testState, e)
		done := make(chan struct{})
		readDone := make(chan struct{})
		go func() {
			defer close(readDone)
			for {
				select {
				case <-done:
					return
				default:
				}
				s := l.Snapshot()
				a, pa, e := l.GetAt([]byte("a"), s)
				bulkMemtableMust(testState, e)
				z, pz, e := l.GetAt([]byte("z"), s)
				bulkMemtableMust(testState, e)
				if pa != pz || !bytes.Equal(a, z) {
					testState.t.Fatalf("%v", "partially published bulk group")
				}
				l.ReleaseSnapshot(s)
			}
		}()
		var wg sync.WaitGroup
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for r := 0; r < 80; r++ {
					value := []byte(fmt.Sprintf("%d:%d", w, r))
					var batch []storage.Mutation
					for i := 0; i < 98; i++ {
						batch = append(batch, storage.Mutation{Key: bulkMemtableKey(testState, i), Value: value})
					}
					batch = append(batch, storage.Mutation{Key: []byte("z"), Value: value}, storage.Mutation{Key: []byte("a"), Value: value})
					bulkMemtableMust(testState, l.WriteBatch(batch))
				}
			}(w)
		}
		wg.Wait()
		close(done)
		<-readDone
		bulkMemtableMust(testState, l.Close())
		testState.t.Logf("PASS concurrent 100-key atomic bulk publications: mode %d\n", mode)
	}
}
