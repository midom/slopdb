// Adapted from /tmp/godb/snapshot_release_review.go; see TESTING.md.
package regression

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"slopdb/storage"
)

func snapshotReleaseMust(testState *regressionEnv, e error) {
	testState.t.Helper()
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
}
func TestSnapshotRelease(testT *testing.T) {
	testState := newRegressionEnv(testT)

	dir := testState.path("godb_review_snapshotrelease")
	os.RemoveAll(dir)
	opt := storage.DefaultOptions()
	opt.WriteBufferSize = 200
	opt.BlockSize = 128
	opt.CompactionL0Trigger = 3
	l, e := testState.openStorage(dir, opt)
	snapshotReleaseMust(testState, e)
	snapshotReleaseMust(testState, l.Put([]byte("k"), []byte("original")))
	a, b := l.Snapshot(), l.Snapshot()
	for i := 0; i < 100; i++ {
		snapshotReleaseMust(testState, l.Put([]byte("k"), []byte(fmt.Sprint(i))))
	}
	l.ReleaseSnapshot(a)
	snapshotReleaseMust(testState, l.CompactRange())
	v, ok, e := l.GetAt([]byte("k"), b)
	snapshotReleaseMust(testState, e)
	if !ok || string(v) != "original" {
		testState.t.Fatalf("%v", "same-sequence snapshot released too soon")
	}
	l.ReleaseSnapshot(b)
	snapshotReleaseMust(testState, l.CompactRange())
	snapshotReleaseMust(testState, l.Close())
	l, e = testState.openStorage(dir, opt)
	snapshotReleaseMust(testState, e)
	snapshotReleaseMust(testState, l.CompactRange())
	v, ok, e = l.Get([]byte("k"))
	snapshotReleaseMust(testState, e)
	if !ok || string(v) != "99" {
		testState.t.Fatalf("%v", "current value after reclamation")
	}
	snapshotReleaseMust(testState, l.Close())
	files, e := os.ReadDir(dir)
	snapshotReleaseMust(testState, e)
	n := 0
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".sst") {
			r, e := storage.OpenSSTable(dir + "/" + f.Name())
			snapshotReleaseMust(testState, e)
			it := r.NewIterator()
			for it.Valid() {
				n++
				it.Next()
			}
			snapshotReleaseMust(testState, it.Err())
			snapshotReleaseMust(testState, r.Close())
		}
	}
	if n != 1 {
		testState.t.Fatalf("%v", fmt.Sprintf("obsolete versions retained=%d", n))
	}
	testState.t.Log(fmt.Sprint("PASS snapshot reference counts and obsolete version reclamation"))
}
