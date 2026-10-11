// Adapted from /tmp/godb/reverse_multi_review.go; see TESTING.md.
package regression

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"testing"

	"slopdb/storage"
)

func reverseAndMultiGetMust(testState *regressionEnv, e error) {
	testState.t.Helper()
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
}
func reverseAndMultiGetCheck(testState *regressionEnv, b bool, msg string) {
	testState.t.Helper()
	if !b {
		testState.t.Fatalf("%v", msg)
	}
}
func TestReverseAndMultiGet(testT *testing.T) {
	testState := newRegressionEnv(testT)

	for mode := 0; mode < 3; mode++ {
		dir := testState.path(fmt.Sprintf("godb_reverse_multi_%d", mode))
		reverseAndMultiGetMust(testState, os.RemoveAll(dir))
		o := storage.DefaultOptions()
		o.BlockSize = 128
		o.WriteBufferSize = 2048
		o.MaxOpenFiles = 1
		o.DisableWAL = mode == 1
		o.DisableWALSync = mode == 2
		l, e := testState.openStorage(dir, o)
		reverseAndMultiGetMust(testState, e)
		model := map[string]string{}
		r := rand.New(rand.NewSource(14))
		checks := 0
		verify := func(seq uint64, want map[string]string) {
			keys := []string{}
			for k := range want {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			it := l.NewIteratorAtSeek(seq, []byte{255, 255})
			for i := len(keys) - 1; i >= 0; i-- {
				it.Prev()
				reverseAndMultiGetCheck(testState, it.Valid() && string(it.Key().UserKey) == keys[i] && string(it.Value()) == want[keys[i]], fmt.Sprintf("reverse mismatch %d/%d %q want %q", mode, i, it.Key().UserKey, keys[i]))
			}
			it.Prev()
			reverseAndMultiGetCheck(testState, !it.Valid(), "reverse exhaustion")
			reverseAndMultiGetMust(testState, storage.IteratorError(it))
			it.Close()
			it = l.NewIteratorAt(seq)
			for i := 0; i < len(keys); i++ {
				reverseAndMultiGetCheck(testState, it.Valid() && string(it.Key().UserKey) == keys[i], "forward mismatch")
				if i > 0 {
					it.Prev()
					reverseAndMultiGetCheck(testState, it.Valid() && string(it.Key().UserKey) == keys[i-1], "direction reverse")
					it.Next()
					reverseAndMultiGetCheck(testState, it.Valid() && string(it.Key().UserKey) == keys[i], "direction forward")
				}
				it.Next()
			}
			reverseAndMultiGetCheck(testState, !it.Valid(), "forward exhaustion")
			it.Close()
			ks := make([][]byte, 0, len(keys)+2)
			for _, k := range keys {
				ks = append(ks, []byte(k))
			}
			ks = append(ks, []byte("missing"), []byte("missing"))
			values, e := l.GetMultiAt(ks, seq)
			reverseAndMultiGetMust(testState, e)
			for i, k := range keys {
				reverseAndMultiGetCheck(testState, values[i].Found && string(values[i].Value) == want[k], "multi snapshot")
			}
			reverseAndMultiGetCheck(testState, !values[len(keys)].Found && !values[len(keys)+1].Found, "missing batch")
			if len(keys) > 0 {
				same, e := l.GetMultiAt([][]byte{ks[0], ks[0]}, seq)
				reverseAndMultiGetMust(testState, e)
				reverseAndMultiGetCheck(testState, same[0].Found && same[1].Found, "duplicates")
				same[0].Value[0] ^= 1
				reverseAndMultiGetCheck(testState, bytes.Equal(same[1].Value, []byte(want[keys[0]])), "owned multi values")
			}
			checks++
		}
		for round := 0; round < 12; round++ {
			snap := l.Snapshot()
			old := map[string]string{}
			for k, v := range model {
				old[k] = v
			}
			for i := 0; i < 100; i++ {
				k := fmt.Sprintf("k%03d", r.Intn(80))
				if r.Intn(4) == 0 {
					reverseAndMultiGetMust(testState, l.Delete([]byte(k)))
					delete(model, k)
				} else {
					v := fmt.Sprintf("value%d-%d", round, i)
					reverseAndMultiGetMust(testState, l.Put([]byte(k), []byte(v)))
					model[k] = v
				}
			}
			if round%3 == 0 {
				reverseAndMultiGetMust(testState, l.DeleteRange([]byte("k020"), []byte("k040")))
				for k := range model {
					if k >= "k020" && k < "k040" {
						delete(model, k)
					}
				}
			}
			verify(snap, old)
			l.ReleaseSnapshot(snap)
			snap = l.Snapshot()
			verify(snap, model)
			l.ReleaseSnapshot(snap)
		}
		reverseAndMultiGetMust(testState, l.CompactRange())
		seq := l.Snapshot()
		verify(seq, model)
		l.ReleaseSnapshot(seq)
		reverseAndMultiGetMust(testState, l.Close())
		l, e = testState.openStorage(dir, o)
		reverseAndMultiGetMust(testState, e)
		seq = l.Snapshot()
		verify(seq, model)
		l.ReleaseSnapshot(seq)
		var wg sync.WaitGroup
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < 100; i++ {
					key := fmt.Sprintf("pair%02d", w)
					value := []byte(fmt.Sprint(i))
					reverseAndMultiGetMust(testState, l.WriteBatch([]storage.Mutation{{Key: []byte(key + "a"), Value: value}, {Key: []byte(key + "z"), Value: value}}))
					values, e := l.GetMulti([][]byte{[]byte(key + "a"), []byte(key + "z")})
					reverseAndMultiGetMust(testState, e)
					reverseAndMultiGetCheck(testState, values[0].Found && values[1].Found && bytes.Equal(values[0].Value, values[1].Value), "batch committed view")
				}
			}(w)
		}
		wg.Wait()
		reverseAndMultiGetMust(testState, l.Close())
		testState.t.Logf("PASS mode %d: %d snapshot/direction/range/owned-value checks and 800 concurrent multi reads\n", mode, checks)
	}
}
