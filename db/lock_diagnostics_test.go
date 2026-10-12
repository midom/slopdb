package db

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"slopdb/sql"
)

func waitForReportedLock(t *testing.T, db *Database) LockWaitInfo {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		waits := db.LockWaits()
		if len(waits) == 1 {
			return waits[0]
		}
		runtime.Gosched()
	}
	t.Fatal("lock wait was not reported")
	return LockWaitInfo{}
}

func TestLockWaitsReportsBlockerAndClearsOnCommit(t *testing.T) {
	d, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER); INSERT INTO t VALUES(1, 0)")
	owner, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Rollback() })
	if _, err := owner.Exec("UPDATE t SET v=1 WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	waiter, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = waiter.Close() })
	done := make(chan error, 1)
	go func() { _, err := waiter.Exec("UPDATE t SET v=2 WHERE id=1"); done <- err }()
	wait := waitForReportedLock(t, d)
	ownerID := owner.SessionID()
	waiterID := waiter.SessionID()
	if wait.SessionID != waiterID || wait.ResourceKind != "row" || len(wait.BlockingSessionIDs) != 1 || wait.BlockingSessionIDs[0] != ownerID {
		t.Fatalf("unexpected lock graph: %+v, owner=%d waiter=%d", wait, ownerID, waiterID)
	}
	if wait.WaitDuration < 0 || wait.Resource == "" {
		t.Fatalf("missing resource or elapsed wait: %+v", wait)
	}
	wait.BlockingSessionIDs[0] = 0
	if got := d.LockWaits()[0].BlockingSessionIDs[0]; got != ownerID {
		t.Fatalf("snapshot aliases lock graph: %d", got)
	}
	if err := owner.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if waits := d.LockWaits(); len(waits) != 0 {
		t.Fatalf("completed wait still reported: %+v", waits)
	}
}

func TestLockWaitsCancellationOwnsBlobSnapshots(t *testing.T) {
	d, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB UNIQUE)")
	owner, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Rollback() })
	if _, err := owner.Exec("INSERT INTO t VALUES(1, X'0102')"); err != nil {
		t.Fatal(err)
	}
	waiter, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = waiter.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := waiter.ExecContext(ctx, "INSERT INTO t VALUES(2, X'0102')"); done <- err }()
	wait := waitForReportedLock(t, d)
	if wait.ResourceKind != "unique" || len(wait.Values) != 1 || len(wait.Values[0].Blob) != 2 {
		t.Fatalf("missing BLOB predicate: %+v", wait)
	}
	wait.Values[0].Blob[0] = 255
	if got := d.LockWaits()[0].Values[0].Blob[0]; got != 1 {
		t.Fatalf("caller changed internal predicate: %d", got)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation identity lost: %v", err)
	}
	if waits := d.LockWaits(); len(waits) != 0 {
		t.Fatalf("cancelled wait still reported: %+v", waits)
	}
	if err := owner.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestLockDiagnosticErrorPreservesIdentity(t *testing.T) {
	d, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER); INSERT INTO t VALUES(1, 0)")
	owner, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Rollback() })
	if _, err := owner.Exec("UPDATE t SET v=1 WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	waiter, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = waiter.Close() })
	for _, timeout := range []time.Duration{50 * time.Millisecond, 0} {
		_, err = waiter.ExecWithOptions("UPDATE t SET v=2 WHERE id=1", ExecOptions{LockTimeout: &timeout})
		var lockErr *LockError
		if !errors.As(err, &lockErr) || !errors.Is(err, ErrLockTimeout) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("timeout %v lost error identity or details: %v", timeout, err)
		}
		if lockErr.SessionID != waiter.SessionID() || lockErr.ResourceKind != "row" || lockErr.Resource == "" || !strings.Contains(err.Error(), "LockWaits") {
			t.Fatalf("timeout %v missing actionable details: %+v", timeout, lockErr)
		}
		if waits := d.LockWaits(); len(waits) != 0 {
			t.Fatalf("timed out wait still reported: %+v", waits)
		}
	}
	deadlock := d.lockDiagnosticError(waiter.schema, sql.LockRequest{Resource: "catalog"}, ErrDeadlock)
	if !errors.Is(deadlock, ErrDeadlock) || !strings.Contains(deadlock.Error(), "retry the entire transaction") {
		t.Fatalf("missing deadlock identity or retry advice: %v", deadlock)
	}
}

func TestLockDiagnosticsNilAndClosedHandles(t *testing.T) {
	var d *Database
	var session *Session
	var tx *Tx
	if len(d.LockWaits()) != 0 || d.SessionID() != 0 || session.SessionID() != 0 || tx.SessionID() != 0 {
		t.Fatal("nil handles reported active locks or session IDs")
	}
	if waits := new(Database).LockWaits(); len(waits) != 0 {
		t.Fatalf("uninitialized database reported waits: %+v", waits)
	}
	d, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	session, err = d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	if session.SessionID() == 0 || d.SessionID() == 0 || session.SessionID() == d.SessionID() {
		t.Fatal("open sessions must have distinct nonzero IDs")
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if session.SessionID() != 0 {
		t.Fatal("closed session retained a diagnostic ID")
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if d.SessionID() != 0 || len(d.LockWaits()) != 0 {
		t.Fatal("closed database retained session IDs or waits")
	}
}

func TestLockWaitsReportsUniquePredicate(t *testing.T) {
	d, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER UNIQUE)")
	owner, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Rollback() })
	if _, err := owner.Exec("INSERT INTO t VALUES(1, 9)"); err != nil {
		t.Fatal(err)
	}
	waiter, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = waiter.Close() })
	done := make(chan error, 1)
	go func() { _, err := waiter.Exec("INSERT INTO t VALUES(2, 9)"); done <- err }()
	wait := waitForReportedLock(t, d)
	if wait.ResourceKind != "unique" || len(wait.Values) != 1 || wait.Values[0].Int != 9 ||
		len(wait.BlockingSessionIDs) != 1 || wait.BlockingSessionIDs[0] != d.locks.state(owner.session.schema).id {
		t.Fatalf("unexpected unique wait: %+v", wait)
	}
	if err := owner.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
