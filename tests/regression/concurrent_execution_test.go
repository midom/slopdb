// Adapted from standard_concurrent_execution_review.go; see TESTING.md.
package regression

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"slopdb/db"
	"slopdb/storage"
)

func concurrentExecutionMust(testState *regressionEnv, e error) {
	testState.t.Helper()
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
}
func concurrentExecutionCheck(testState *regressionEnv, b bool, s string) {
	testState.t.Helper()
	if !b {
		testState.t.Fatalf("%v", s)
	}
}
func TestConcurrentExecution(testT *testing.T) {
	testState := newRegressionEnv(testT)

	path := testState.path("godb_concurrent_execution_review")
	concurrentExecutionMust(testState, os.RemoveAll(path))
	o := storage.DefaultOptions()
	o.DisableWALSync = true
	o.WriteBufferSize = 32768
	o.BlockSize = 512
	o.CompactionL0Trigger = 3
	o.MaxOpenFiles = 4
	d, e := testState.openDatabase(path, o)
	concurrentExecutionMust(testState, e)
	_, e = d.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER NOT NULL,w INTEGER); CREATE TABLE counted(id INTEGER PRIMARY KEY,v INTEGER NOT NULL); CREATE TABLE events(v INTEGER)")
	concurrentExecutionMust(testState, e)
	for i := 1; i <= 64; i++ {
		_, e = d.Exec(fmt.Sprintf("INSERT INTO t VALUES(%d,0,0)", i))
		concurrentExecutionMust(testState, e)
	}
	_, e = d.Exec("INSERT INTO counted VALUES(1,0)")
	concurrentExecutionMust(testState, e)
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			s, e := d.NewSession()
			if e != nil {
				errs <- e
				return
			}
			defer s.Close()
			for j := 0; j < 1000; j++ {
				id := (j*7+w)%64 + 1
				r, e := s.Exec(fmt.Sprintf("UPDATE t SET v=v+1,w=v WHERE id=%d", id))
				if e != nil {
					errs <- e
					return
				}
				if r.Changes != 1 {
					errs <- fmt.Errorf("wrong changes")
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		concurrentExecutionMust(testState, e)
	}
	r, e := d.Exec("SELECT SUM(v) AS n FROM t")
	concurrentExecutionMust(testState, e)
	concurrentExecutionCheck(testState, r.Rows[0]["n"].Int == 8000, "lost updates")
	r, e = d.Exec("SELECT COUNT(*) AS n FROM t WHERE w=v-1")
	concurrentExecutionMust(testState, e)
	concurrentExecutionCheck(testState, r.Rows[0]["n"].Int == 64, "SET simultaneous evaluation")
	// Explicit transaction owner must progress while an autocommit waits.
	a, e := d.NewSession()
	concurrentExecutionMust(testState, e)
	b, e := d.NewSession()
	concurrentExecutionMust(testState, e)
	_, e = a.Exec("BEGIN; UPDATE t SET v=v+1 WHERE id=1")
	concurrentExecutionMust(testState, e)
	done := make(chan error, 1)
	go func() { _, e := b.Exec("UPDATE t SET v=v+1 WHERE id=1"); done <- e }()
	time.Sleep(25 * time.Millisecond)
	_, e = a.Exec("COMMIT")
	concurrentExecutionMust(testState, e)
	concurrentExecutionMust(testState, <-done)
	// Canceled/zero-timeout lock attempts cannot leak catalog/row ownership.
	_, e = a.Exec("BEGIN; UPDATE t SET v=v+1 WHERE id=1")
	concurrentExecutionMust(testState, e)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, e = b.ExecContext(ctx, "UPDATE t SET v=v+1 WHERE id=1")
	cancel()
	concurrentExecutionCheck(testState, errors.Is(e, context.DeadlineExceeded), "canceled lock wait")
	zero := time.Duration(0)
	_, e = b.ExecWithOptions("UPDATE t SET v=v+1 WHERE id=1", db.ExecOptions{LockTimeout: &zero})
	concurrentExecutionCheck(testState, errors.Is(e, db.ErrLockTimeout), "zero lock wait")
	_, e = a.Exec("ROLLBACK")
	concurrentExecutionMust(testState, e)
	_, e = b.Exec("UPDATE t SET v=v+1 WHERE id=1")
	concurrentExecutionMust(testState, e)
	// Ineligible index/trigger/check updates retain the complete executor.
	_, e = d.Exec("CREATE INDEX iv ON t(v); CREATE TRIGGER log AFTER UPDATE ON counted BEGIN INSERT INTO events VALUES(NEW.v); END; UPDATE counted SET v=v+1 WHERE id=1; CREATE TABLE ck(id INTEGER PRIMARY KEY,v INTEGER CHECK(v>0)); INSERT INTO ck VALUES(1,1)")
	concurrentExecutionMust(testState, e)
	_, e = b.Exec("UPDATE ck SET v=0 WHERE id=1")
	concurrentExecutionCheck(testState, e != nil, "CHECK fallback")
	r, e = d.Exec("SELECT COUNT(*) AS n FROM events")
	concurrentExecutionMust(testState, e)
	concurrentExecutionCheck(testState, r.Rows[0]["n"].Int == 1, "trigger fallback")
	_, e = b.Exec("UPDATE t SET v=v+1 WHERE id=2")
	concurrentExecutionMust(testState, e)
	r, e = d.Exec("SELECT id FROM t WHERE v=(SELECT v FROM t WHERE id=2)")
	concurrentExecutionMust(testState, e)
	concurrentExecutionCheck(testState, len(r.Rows) > 0, "indexed fallback")
	// Repeated SET targets validate the final row.
	_, e = b.Exec("UPDATE counted SET v=NULL,v=12 WHERE id=1")
	concurrentExecutionMust(testState, e)
	concurrentExecutionMust(testState, a.Close())
	concurrentExecutionMust(testState, b.Close())
	concurrentExecutionMust(testState, d.Close())
	d, e = testState.openDatabase(path, o)
	concurrentExecutionMust(testState, e)
	r, e = d.Exec("SELECT SUM(v) AS n FROM t")
	concurrentExecutionMust(testState, e)
	concurrentExecutionCheck(testState, r.Rows[0]["n"].Int == 8004, "persistence sum")
	concurrentExecutionMust(testState, d.Close())
	testState.t.Log(fmt.Sprint("PASS 8,000 overlapping updates, SET ordering, transaction lock handoff, cancellation, zero timeout, index/trigger/CHECK fallback, and persistence"))
}
