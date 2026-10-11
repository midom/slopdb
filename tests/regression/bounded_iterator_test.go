// Adapted from bounded_iterator_review.go; see TESTING.md.
package regression

import (
	"bytes"
	"fmt"
	"maps"
	"math/rand"
	"os"
	"sort"
	"testing"

	"slopdb/storage"
)

var boundedIteratorsChecks int

func boundedIteratorsMust(testState *regressionEnv, e error) {
	testState.t.Helper()
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
}
func boundedIteratorsKey(testState *regressionEnv, n int) []byte {
	testState.t.Helper()
	return []byte(fmt.Sprintf("k%04d", n))
}
func TestBoundedIterators(testT *testing.T) {
	testState := newRegressionEnv(testT)

	for mode := 0; mode < 3; mode++ {
		dir := testState.path(fmt.Sprintf("godb_bounded_review_%d", mode))
		boundedIteratorsMust(testState, os.RemoveAll(dir))
		o := storage.DefaultOptions()
		o.WriteBufferSize = 1024
		o.BlockSize = 256
		o.MaxOpenFiles = 1
		o.CompactionL0Trigger = 3
		o.DisableWAL = mode == 1
		o.DisableWALSync = mode == 2
		l, e := testState.openStorage(dir, o)
		boundedIteratorsMust(testState, e)
		want := map[string]string{}
		rnd := rand.New(rand.NewSource(77))
		for i := 0; i < 400; i++ {
			v := fmt.Sprintf("v%d", i)
			boundedIteratorsMust(testState, l.Put(boundedIteratorsKey(testState, i), []byte(v)))
			want[string(boundedIteratorsKey(testState, i))] = v
		}
		snap := l.Snapshot()
		old := maps.Clone(want)
		for i := 0; i < 250; i++ {
			n := rnd.Intn(400)
			if i%7 == 0 {
				end := n + 1 + rnd.Intn(10)
				boundedIteratorsMust(testState, l.DeleteRange(boundedIteratorsKey(testState, n), boundedIteratorsKey(testState, end)))
				for k := range want {
					if k >= string(boundedIteratorsKey(testState, n)) && k < string(boundedIteratorsKey(testState, end)) {
						delete(want, k)
					}
				}
			} else {
				v := fmt.Sprintf("changed%d", i)
				boundedIteratorsMust(testState, l.Put(boundedIteratorsKey(testState, n), []byte(v)))
				want[string(boundedIteratorsKey(testState, n))] = v
			}
		}
		verify := func(snapshot uint64, w map[string]string) {
			for c := 0; c < 100; c++ {
				a, b := rnd.Intn(420)-10, rnd.Intn(420)-10
				if a > b {
					a, b = b, a
				}
				var expected []string
				for k := range w {
					if k >= string(boundedIteratorsKey(testState, a)) && k < string(boundedIteratorsKey(testState, b)) {
						expected = append(expected, k)
					}
				}
				sort.Strings(expected)
				for _, reverse := range []bool{false, true} {
					it := l.NewIteratorAtRange(snapshot, boundedIteratorsKey(testState, a), boundedIteratorsKey(testState, b), reverse)
					var got []string
					var held [][]byte
					for it.Valid() {
						k := string(it.Key().UserKey)
						if string(it.Value()) != w[k] {
							testState.t.Fatalf("%v", "wrong version")
						}
						held = append(held, it.Key().UserKey)
						got = append(got, k)
						if reverse {
							it.Prev()
						} else {
							it.Next()
						}
					}
					boundedIteratorsMust(testState, storage.IteratorError(it))
					if reverse {
						for i, j := 0, len(got)-1; i < j; i, j = i+1, j-1 {
							got[i], got[j] = got[j], got[i]
						}
					}
					if fmt.Sprint(got) != fmt.Sprint(expected) {
						testState.t.Fatalf("%v", fmt.Sprintf("range [%d,%d) reverse=%v got=%v want=%v", a, b, reverse, got, expected))
					}
					for i, k := range held {
						position := i
						if reverse {
							position = len(expected) - 1 - i
						}
						if !bytes.Equal(k, []byte(expected[position])) {
							testState.t.Fatalf("%v", "retained key mutated")
						}
					}
					it.Seek(boundedIteratorsKey(testState, a-1))
					if len(expected) > 0 {
						if !it.Valid() || string(it.Key().UserKey) != expected[0] {
							testState.t.Fatalf("%v", "seek lower clamp")
						}
						if len(expected) > 2 {
							it.Next()
							it.Prev()
							if !it.Valid() || string(it.Key().UserKey) != expected[0] {
								testState.t.Fatalf("%v", "direction change")
							}
						}
					}
					it.Seek(boundedIteratorsKey(testState, a))
					it.Prev()
					if it.Valid() {
						testState.t.Fatalf("%v", "lower predecessor exposed")
					}
					it.Prev()
					if it.Valid() {
						testState.t.Fatalf("%v", "reverse wrapped past lower bound")
					}
					it.Next()
					if len(expected) > 0 && (!it.Valid() || string(it.Key().UserKey) != expected[0]) {
						testState.t.Fatalf("%v", "forward recovery at lower bound")
					}
					it.Seek(boundedIteratorsKey(testState, b))
					if it.Valid() {
						testState.t.Fatalf("%v", "upper bound visible")
					}
					it.Prev()
					if len(expected) > 0 && (!it.Valid() || string(it.Key().UserKey) != expected[len(expected)-1]) {
						testState.t.Fatalf("%v", "Prev after upper seek")
					}
					it.Close()
					boundedIteratorsChecks++
				}
			}
		}
		verify(l.Stats().SeqNum, want)
		verify(snap, old)
		boundedIteratorsMust(testState, l.CompactRange())
		verify(l.Stats().SeqNum, want)
		verify(snap, old)
		l.ReleaseSnapshot(snap)
		// A tombstone's starting key lies outside the bounded range it deletes.
		boundedIteratorsMust(testState, l.Put(boundedIteratorsKey(testState, 30), []byte("live")))
		boundedIteratorsMust(testState, l.DeleteRange(boundedIteratorsKey(testState, 0), boundedIteratorsKey(testState, 100)))
		boundedIteratorsMust(testState, l.Close())
		l, e = testState.openStorage(dir, o)
		boundedIteratorsMust(testState, e)
		it := l.NewIteratorRange(boundedIteratorsKey(testState, 30), boundedIteratorsKey(testState, 31), false)
		if it.Valid() {
			testState.t.Fatalf("%v", "pruned covering tombstone")
		}
		boundedIteratorsMust(testState, storage.IteratorError(it))
		it.Close()
		boundedIteratorsMust(testState, l.Close())
		testState.t.Logf("PASS mode%d bounded snapshots, tombstones, lazy levels, key ownership, direction flips and reopen\n", mode)
	}
	testState.t.Log(fmt.Sprint("PASS", boundedIteratorsChecks, "range comparisons"))
}
