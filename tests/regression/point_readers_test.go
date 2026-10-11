// Adapted from point_reader_review.go; see TESTING.md.
package regression

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"

	"slopdb/storage"
)

func pointReadersMust(testState *regressionEnv, e error) {
	testState.t.Helper()
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
}
func pointReadersCheck(testState *regressionEnv, b bool, s string) {
	testState.t.Helper()
	if !b {
		testState.t.Fatalf("%v", s)
	}
}
func TestPointReaders(testT *testing.T) {
	testState := newRegressionEnv(testT)

	for mode := 0; mode < 3; mode++ {
		dir := testState.path(fmt.Sprintf("godb_point_reader_%d", mode))
		pointReadersMust(testState, os.RemoveAll(dir))
		o := storage.DefaultOptions()
		o.WriteBufferSize = 4096
		o.BlockSize = 128
		o.MaxOpenFiles = 1
		o.DisableWAL = mode == 1
		o.DisableWALSync = mode == 2
		l, e := testState.openStorage(dir, o)
		pointReadersMust(testState, e)
		keys := make([][]byte, 80)
		initial := make([][]byte, 80)
		for i := range keys {
			keys[i] = []byte(strings.Repeat("very-long-shared-key-prefix", 30) + fmt.Sprintf("%03d", i))
			initial[i] = []byte(strings.Repeat(fmt.Sprintf("value%d", i), 20))
			pointReadersMust(testState, l.Put(keys[i], initial[i]))
		}
		old := l.Snapshot()
		for round := 0; round < 30; round++ {
			for i := 0; i < 8; i++ {
				pointReadersMust(testState, l.Put(keys[i], []byte(fmt.Sprintf("updated%d-%d", round, i))))
			}
		}
		pointReadersMust(testState, l.DeleteRange(keys[10], keys[20]))
		pointReadersMust(testState, l.Put(keys[15], []byte("reinserted")))
		pointReadersMust(testState, l.Delete(keys[25]))
		pointReadersMust(testState, l.CompactRange())
		values, e := l.GetMultiAt(keys, old)
		pointReadersMust(testState, e)
		for i, v := range values {
			pointReadersCheck(testState, v.Found && bytes.Equal(v.Value, initial[i]), "long-key old batch")
			p, ok, e := l.GetAt(keys[i], old)
			pointReadersMust(testState, e)
			pointReadersCheck(testState, ok && bytes.Equal(p, initial[i]), "long-key old point")
		}
		l.ReleaseSnapshot(old)
		verify := func() {
			vs, e := l.GetMulti(keys)
			pointReadersMust(testState, e)
			for i, v := range vs {
				switch {
				case i < 8:
					pointReadersCheck(testState, v.Found && string(v.Value) == fmt.Sprintf("updated29-%d", i), "latest spanning-block versions")
				case i == 15:
					pointReadersCheck(testState, v.Found && string(v.Value) == "reinserted", "range reinsertion")
				case i >= 10 && i < 20 || i == 25:
					pointReadersCheck(testState, !v.Found, "delete/range tombstone")
				default:
					pointReadersCheck(testState, v.Found && bytes.Equal(v.Value, initial[i]), "latest original")
				}
			}
			_, ok, e := l.Get([]byte(strings.Repeat("very-long-shared-key-prefix", 30) + "090"))
			pointReadersMust(testState, e)
			pointReadersCheck(testState, !ok, "absent long key")
		}
		verify()
		pointReadersMust(testState, l.Close())
		l, e = testState.openStorage(dir, o)
		pointReadersMust(testState, e)
		verify()
		pointReadersMust(testState, l.Close())
		testState.t.Logf("PASS mode%d: long keys, versions across blocks, pinned snapshots, negative reads, ranges and reopen\n", mode)
	}
}
