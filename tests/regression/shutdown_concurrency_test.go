// Adapted from shutdown_concurrency_review.go; see TESTING.md.
package regression

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"slopdb/storage"
)

func concurrentShutdownMust(testState *regressionEnv, e error) {
	testState.t.Helper()
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
}
func TestConcurrentShutdown(testT *testing.T) {
	testState := newRegressionEnv(testT)

	for i := 0; i < 30; i++ {
		dir := testState.path(fmt.Sprintf("godb_shutdown_concurrency_%d", i))
		concurrentShutdownMust(testState, os.RemoveAll(dir))
		o := storage.DefaultOptions()
		o.DisableWALSync = true
		d, e := testState.openDatabase(dir, o)
		concurrentShutdownMust(testState, e)
		_, e = d.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY,v INT,INDEX iv(v));INSERT INTO t VALUES(1,0)")
		concurrentShutdownMust(testState, e)
		owner, e := d.Begin()
		concurrentShutdownMust(testState, e)
		_, e = owner.Exec("UPDATE t SET v=1 WHERE id=1")
		concurrentShutdownMust(testState, e)
		waiter, e := d.Begin()
		concurrentShutdownMust(testState, e)
		done := make(chan error, 1)
		go func() { _, e := waiter.Exec("UPDATE t SET v=2 WHERE id=1"); done <- e }()
		time.Sleep(time.Millisecond)
		if i%2 == 0 {
			concurrentShutdownMust(testState, waiter.Rollback())
			e = <-done
			if !errors.Is(e, context.Canceled) {
				testState.t.Fatalf("%v", fmt.Sprintf("rollback active waiter: %v", e))
			}
			concurrentShutdownMust(testState, owner.Rollback())
			concurrentShutdownMust(testState, d.Close())
		} else {
			concurrentShutdownMust(testState, d.Close())
			select {
			case e = <-done:
				if e == nil {
					testState.t.Fatalf("%v", "closed waiter committed")
				}
			case <-time.After(time.Second):
				testState.t.Fatalf("%v", "Close stuck waiting for logical lock")
			}
		}
		concurrentShutdownMust(testState, d.Close())
		os.RemoveAll(dir)
	}
	testState.t.Log(fmt.Sprint("PASS concurrent database Close, repeated Close and Tx.Rollback cancel lock waiters without races/deadlocks"))
}
