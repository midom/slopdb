// Adapted from standard_current_storage_review.go; see TESTING.md.
package regression

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"sync"
	"testing"

	"slopdb/storage"
)

func storageModelMust(testState *regressionEnv, err error) {
	testState.t.Helper()
	if err != nil {
		testState.t.Fatalf("%v", err)
	}
}
func storageModelVerify(testState *regressionEnv, l *storage.LSMTree, want map[string]string, snap *uint64) {
	testState.t.Helper()
	for k := 0; k < 60; k++ {
		key := fmt.Sprintf("k%03d", k)
		var v []byte
		var ok bool
		var e error
		if snap == nil {
			v, ok, e = l.Get([]byte(key))
		} else {
			v, ok, e = l.GetAt([]byte(key), *snap)
		}
		storageModelMust(testState, e)
		w, exists := want[key]
		if ok != exists || (ok && string(v) != w) {
			testState.t.Fatalf("%v", fmt.Sprintf("get snapshot=%v key=%s got=%q,%t want=%q,%t", snap, key, v, ok, w, exists))
		}
	}
	var it storage.Iterator
	if snap == nil {
		it = l.NewIterator()
	} else {
		it = l.NewIteratorAt(*snap)
	}
	defer it.Close()
	var got []string
	for it.Valid() {
		got = append(got, string(it.Key().UserKey))
		w, ok := want[string(it.Key().UserKey)]
		if !ok || string(it.Value()) != w {
			testState.t.Fatalf("%v", "iterator value mismatch")
		}
		it.Next()
	}
	var keys []string
	for k := range want {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if fmt.Sprint(got) != fmt.Sprint(keys) {
		testState.t.Fatalf("%v", fmt.Sprintf("iterator keys got=%v want=%v", got, keys))
	}
	it.Seek([]byte("z"))
	for i := len(keys) - 1; i >= 0; i-- {
		it.Prev()
		if !it.Valid() || string(it.Key().UserKey) != keys[i] {
			testState.t.Fatalf("%v", fmt.Sprintf("reverse index=%d got=%q want=%q", i, it.Key().UserKey, keys[i]))
		}
	}
	it.Prev()
	if it.Valid() {
		testState.t.Fatalf("%v", "before first valid")
	}
	for _, key := range keys {
		it.Seek([]byte(key))
		if !it.Valid() || string(it.Key().UserKey) != key {
			testState.t.Fatalf("%v", "seek mismatch")
		}
		it.Prev()
		it.Next() /* Next from before first is unspecified; test middle changes below */
	}
	if len(keys) > 2 {
		it.Seek([]byte(keys[1]))
		it.Next()
		it.Prev()
		if !it.Valid() || string(it.Key().UserKey) != keys[1] {
			testState.t.Fatalf("%v", "direction change mismatch")
		}
		it.Next()
		if !it.Valid() || string(it.Key().UserKey) != keys[2] {
			testState.t.Fatalf("%v", "next after prev mismatch")
		}
	}
}
func TestStorageModel(testT *testing.T) {
	testState := newRegressionEnv(testT)

	dir := testState.path("godb_review_random")
	os.RemoveAll(dir)
	opt := storage.DefaultOptions()
	opt.WriteBufferSize = 700
	opt.BlockSize = 128
	opt.MaxOpenFiles = 1
	opt.CompactionL0Trigger = 3
	opt.CompressionType = storage.GzipCompression
	l, e := testState.openStorage(dir, opt)
	storageModelMust(testState, e)
	want := map[string]string{}
	rnd := rand.New(rand.NewSource(8))
	var snapshot uint64
	var old map[string]string
	for i := 0; i < 600; i++ {
		key := fmt.Sprintf("k%03d", rnd.Intn(60))
		switch rnd.Intn(5) {
		case 0:
			storageModelMust(testState, l.Delete([]byte(key)))
			delete(want, key)
		case 1:
			start := rnd.Intn(55)
			end := start + 1 + rnd.Intn(5)
			a, b := fmt.Sprintf("k%03d", start), fmt.Sprintf("k%03d", end)
			storageModelMust(testState, l.DeleteRange([]byte(a), []byte(b)))
			for k := range want {
				if k >= a && k < b {
					delete(want, k)
				}
			}
		default:
			v := fmt.Sprintf("v%d", i)
			storageModelMust(testState, l.Put([]byte(key), []byte(v)))
			want[key] = v
		}
		if i == 150 {
			snapshot = l.Snapshot()
			old = map[string]string{}
			for k, v := range want {
				old[k] = v
			}
		}
		if i%100 == 0 {
			storageModelVerify(testState, l, want, nil)
		}
	}
	storageModelVerify(testState, l, want, nil)
	storageModelVerify(testState, l, old, &snapshot)
	storageModelMust(testState, l.CompactRange())
	storageModelVerify(testState, l, want, nil)
	storageModelVerify(testState, l, old, &snapshot)
	storageModelMust(testState, l.Close())
	l, e = testState.openStorage(dir, opt)
	storageModelMust(testState, e)
	storageModelVerify(testState, l, want, nil)
	storageModelMust(testState, l.Put([]byte("k000"), []byte("after reopen")))
	want["k000"] = "after reopen"
	storageModelVerify(testState, l, want, nil)
	storageModelMust(testState, l.Close())
	testState.t.Log(fmt.Sprint("PASS randomized snapshots, ranges, compaction, cache eviction, reopen, iterators"))
	dir = testState.path("godb_review_concurrent")
	os.RemoveAll(dir)
	l, e = testState.openStorage(dir, opt)
	storageModelMust(testState, e)
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 80; i++ {
				key := fmt.Sprintf("thread%d:%03d", g, i)
				storageModelMust(testState, l.Put([]byte(key), []byte("value")))
				v, ok, e := l.Get([]byte(key))
				storageModelMust(testState, e)
				if !ok || string(v) != "value" {
					testState.t.Fatalf("%v", "concurrent get")
				}
				if i%15 == 0 {
					it := l.NewIterator()
					for it.Valid() {
						it.Next()
					}
					it.Close()
				}
			}
		}(g)
	}
	wg.Wait()
	storageModelMust(testState, l.Close())
	l, e = testState.openStorage(dir, opt)
	storageModelMust(testState, e)
	it := l.NewIterator()
	n := 0
	for it.Valid() {
		n++
		it.Next()
	}
	it.Close()
	if n != 320 {
		testState.t.Fatalf("%v", fmt.Sprintf("concurrent persistence keys=%d", n))
	}
	storageModelMust(testState, l.Close())
	testState.t.Log(fmt.Sprint("PASS concurrent storage"))
	path := testState.path("boundaries.wal")
	w, e := storage.NewWalWriter(path)
	storageModelMust(testState, e)
	var records [][]byte
	for _, n := range []int{0, 1, 32750, 5, 32768, 65536, 2, 32760, 3, 4, 100000} {
		v := bytes.Repeat([]byte{byte(n % 251)}, n)
		records = append(records, v)
		storageModelMust(testState, w.AddRecord(v))
	}
	storageModelMust(testState, w.Close())
	r, e := storage.NewWalReader(path)
	storageModelMust(testState, e)
	for i, v := range records {
		got, e := r.ReadRecord()
		storageModelMust(testState, e)
		if !bytes.Equal(got, v) {
			testState.t.Fatalf("%v", fmt.Sprintf("WAL boundary %d", i))
		}
	}
	_, e = r.ReadRecord()
	if e != io.EOF {
		testState.t.Fatalf("%v", e)
	}
	storageModelMust(testState, r.Close())
	testState.t.Log(fmt.Sprint("PASS WAL boundaries and empty records"))
}
