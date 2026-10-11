// Adapted from group_commit_review.go; see TESTING.md.
package regression

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"

	"slopdb/storage"
)

func groupCommitMust(testState *regressionEnv, e error) {
	testState.t.Helper()
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
}
func groupCommitRun(testState *regressionEnv, noWAL, noSync bool) {
	testState.t.Helper()
	dir := testState.path(fmt.Sprintf("godb_group_commit_%v_%v", noWAL, noSync))
	groupCommitMust(testState, os.RemoveAll(dir))
	o := storage.DefaultOptions()
	o.DisableWAL = noWAL
	o.DisableWALSync = noSync
	o.WriteBufferSize = 32768
	o.MaxOpenFiles = 3
	o.BlockSize = 512
	o.CompactionL0Trigger = 3
	l, e := testState.openStorage(dir, o)
	groupCommitMust(testState, e)
	var wg sync.WaitGroup
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
			snapshot := l.Snapshot()
			for w := 0; w < 8; w++ {
				a, pa, e := l.GetAt([]byte(fmt.Sprintf("%d:a", w)), snapshot)
				groupCommitMust(testState, e)
				b, pb, e := l.GetAt([]byte(fmt.Sprintf("%d:b", w)), snapshot)
				groupCommitMust(testState, e)
				if pa != pb || !bytes.Equal(a, b) {
					testState.t.Fatalf("%v", "torn group/batch view")
				}
			}
			l.ReleaseSnapshot(snapshot)
		}
	}()
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for j := 0; j < 300; j++ {
				v := []byte(strconv.Itoa(j))
				groupCommitMust(testState, l.WriteBatch([]storage.Mutation{{Key: []byte(fmt.Sprintf("%d:a", w)), Value: v}, {Key: []byte(fmt.Sprintf("%d:b", w)), Value: v}}))
			}
		}(w)
	}
	wg.Wait()
	close(done)
	<-readDone
	groupCommitMust(testState, l.Close())
	l, e = testState.openStorage(dir, o)
	groupCommitMust(testState, e)
	for w := 0; w < 8; w++ {
		v, found, e := l.Get([]byte(fmt.Sprintf("%d:a", w)))
		groupCommitMust(testState, e)
		if !found || string(v) != "299" {
			testState.t.Fatalf("%v", "group persistence")
		}
	}
	groupCommitMust(testState, l.Close())
	testState.t.Logf("PASS grouped atomic writes, snapshot reads, compaction, persistence: noWAL=%v noSync=%v\n", noWAL, noSync)
}
func TestGroupCommit(testT *testing.T) {
	testState := newRegressionEnv(testT)

	groupCommitRun(testState, false, true)
	groupCommitRun(testState, false, false)
	groupCommitRun(testState, true, false)
}
