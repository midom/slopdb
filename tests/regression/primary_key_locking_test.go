// Adapted from performance_lock_review.go; see TESTING.md.
package regression

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"slopdb/db"
	"slopdb/storage"
)

type primaryKeyLockingExec interface {
	Exec(string) (*db.ExecResult, error)
}

func primaryKeyLockingRun(testState *regressionEnv, x primaryKeyLockingExec, q string) {
	testState.t.Helper()
	if _, e := x.Exec(q); e != nil {
		testState.t.Fatalf("%v", e)
	}
}
func primaryKeyLockingMust(testState *regressionEnv, e error) {
	testState.t.Helper()
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
}
func TestPrimaryKeyLocking(testT *testing.T) {
	testState := newRegressionEnv(testT)

	dir := testState.path("godb_primary_lock_correctness")
	os.RemoveAll(dir)
	o := storage.DefaultOptions()
	o.DisableWALSync = true
	d, e := testState.openDatabase(dir, o)
	primaryKeyLockingMust(testState, e)
	primaryKeyLockingRun(testState, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v INT NOT NULL,KEY iv(v)); INSERT INTO t VALUES(1,1)")
	a, e := d.NewSession()
	primaryKeyLockingMust(testState, e)
	b, e := d.NewSession()
	primaryKeyLockingMust(testState, e)
	zero := time.Duration(0)
	for _, literal := range []string{"2", "2.0", "'2'"} {
		primaryKeyLockingRun(testState, a, "BEGIN; INSERT INTO t VALUES(2,2)")
		_, e = b.ExecWithOptions("INSERT INTO t VALUES("+literal+",3)", db.ExecOptions{LockTimeout: &zero})
		if !errors.Is(e, db.ErrLockTimeout) {
			testState.t.Fatalf("%v", fmt.Sprintf("PK equivalence %s: %v", literal, e))
		}
		primaryKeyLockingRun(testState, a, "ROLLBACK")
		primaryKeyLockingRun(testState, b, "INSERT INTO t VALUES("+literal+",3); DELETE FROM t WHERE id=2")
	}
	primaryKeyLockingRun(testState, a, "BEGIN; DELETE FROM t WHERE id=1")
	_, e = b.ExecWithOptions("INSERT INTO t VALUES(1,9)", db.ExecOptions{LockTimeout: &zero})
	if !errors.Is(e, db.ErrLockTimeout) {
		testState.t.Fatalf("%v", "delete must keep physical PK lock")
	}
	primaryKeyLockingRun(testState, a, "COMMIT")
	primaryKeyLockingRun(testState, b, "INSERT INTO t VALUES(1,9)")
	primaryKeyLockingRun(testState, a, "BEGIN; UPDATE t SET id=2 WHERE id=1")
	for _, id := range []int{1, 2} {
		_, e = b.ExecWithOptions(fmt.Sprintf("INSERT INTO t VALUES(%d,3)", id), db.ExecOptions{LockTimeout: &zero})
		if !errors.Is(e, db.ErrLockTimeout) {
			testState.t.Fatalf("%v", fmt.Sprintf("PK move %d: %v", id, e))
		}
	}
	primaryKeyLockingRun(testState, a, "ROLLBACK")
	primaryKeyLockingRun(testState, a, "BEGIN; SELECT * FROM t WHERE id=1 FOR SHARE")
	if _, e = a.Exec("UPDATE t SET v=NULL WHERE id=1"); e == nil {
		testState.t.Fatalf("%v", "failed statement expected")
	}
	_, e = b.ExecWithOptions("UPDATE t SET v=11 WHERE id=1", db.ExecOptions{LockTimeout: &zero})
	if !errors.Is(e, db.ErrLockTimeout) {
		testState.t.Fatalf("%v", "failed statement dropped earlier shared lock")
	}
	primaryKeyLockingRun(testState, b, "SELECT * FROM t WHERE id=1 FOR SHARE NOWAIT")
	primaryKeyLockingRun(testState, a, "ROLLBACK")
	primaryKeyLockingRun(testState, b, "UPDATE t SET v=12 WHERE id=1")
	primaryKeyLockingMust(testState, a.Close())
	primaryKeyLockingMust(testState, b.Close())
	primaryKeyLockingMust(testState, d.Close())
	testState.t.Log(fmt.Sprint("PASS integer/real/text PK lock equivalence, deletes, PK changes, indexed updates, failed-statement shared-lock downgrade and release"))
}
