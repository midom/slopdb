package db

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"slopdb/sql"
)

func publicationStart(t *testing.T, d *Database, ctx context.Context, query string, options ExecOptions) <-chan error {
	t.Helper()
	s, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { defer s.Close(); _, err := s.ExecContextWithOptions(ctx, query, options); done <- err }()
	return done
}

func publicationOwner(t *testing.T, d *Database) *Tx {
	t.Helper()
	tx, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	if _, err := tx.Exec("UPDATE t SET v='private' WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	return tx
}

func publicationAssertOriginal(t *testing.T, d *Database) {
	t.Helper()
	r := objectExec(t, d, "SELECT v FROM t WHERE id=1")
	if len(r.Rows) != 1 || r.Rows[0]["v"].Text != "original" {
		t.Fatalf("publication committed victim's private value: %#v", r)
	}
	if _, err := d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
}

func publicationWaitSchema(t *testing.T, d *Database, schema *sql.Schema) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		d.locks.graphMu.Lock()
		_, waiting := d.locks.waiting[schema]
		d.locks.graphMu.Unlock()
		if waiting {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("victim statement did not register its row lock wait")
}

func TestIndexPublicationAbortIdleWriterAfterGrace(t *testing.T) {
	d, dir, opt := onlineTestDatabase(t)
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'original')")
	victim := publicationOwner(t, d)
	reached, release := onlineBarrier(t, d, "scanned")
	grace := 150 * time.Millisecond
	done := publicationStart(t, d, context.Background(), "CREATE INDEX iv ON t(v)", ExecOptions{IndexPublicationAbortAfter: &grace})
	onlineReached(t, reached)
	started := time.Now()
	release()
	waitCatalogRequest(t, d, true)
	if err := onlineWait(t, done); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < grace {
		t.Fatalf("publication aborted before grace: %s < %s", elapsed, grace)
	}
	if err := victim.Commit(); !errors.Is(err, ErrTransactionAborted) {
		t.Fatalf("victim Commit: %v", err)
	}
	if _, err := victim.Exec("INSERT INTO t VALUES(2,'late')"); !errors.Is(err, ErrTransactionAborted) && !errors.Is(err, ErrTxDone) {
		t.Fatalf("aborted Tx accepted another statement: %v", err)
	}
	publicationAssertOriginal(t, d)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	publicationAssertOriginal(t, reopened)
	if objectIndexID(t, reopened, "iv") == 0 {
		t.Fatal("published index missing after reopen")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestIndexPublicationGraceAllowsNaturalCommit(t *testing.T) {
	d, _, _ := onlineTestDatabase(t)
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'original')")
	victim := publicationOwner(t, d)
	grace := time.Second
	done := publicationStart(t, d, context.Background(), "CREATE INDEX iv ON t(v)", ExecOptions{IndexPublicationAbortAfter: &grace})
	waitCatalogRequest(t, d, true)
	if err := victim.Commit(); err != nil {
		t.Fatalf("natural commit was killed: %v", err)
	}
	if err := onlineWait(t, done); err != nil {
		t.Fatal(err)
	}
	r := objectExec(t, d, "SELECT id FROM t WHERE v='private'")
	if len(r.Rows) != 1 {
		t.Fatalf("natural commit lost: %#v", r)
	}
}

func TestIndexPublicationDefaultDoesNotAbort(t *testing.T) {
	d, _, _ := onlineTestDatabase(t)
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'original')")
	victim := publicationOwner(t, d)
	done := publicationStart(t, d, context.Background(), "CREATE INDEX iv ON t(v)", ExecOptions{})
	waitCatalogRequest(t, d, true)
	select {
	case err := <-done:
		t.Fatalf("default publication did not wait: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := victim.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := onlineWait(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestIndexPublicationDeadlineDoesNotAbortVictim(t *testing.T) {
	for _, lockTimeout := range []bool{false, true} {
		name := "query-timeout"
		if lockTimeout {
			name = "lock-timeout"
		}
		t.Run(name, func(t *testing.T) {
			d, _, _ := onlineTestDatabase(t)
			objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'original')")
			victim := publicationOwner(t, d)
			grace, timeout := time.Second, 100*time.Millisecond
			options := ExecOptions{IndexPublicationAbortAfter: &grace}
			if lockTimeout {
				options.LockTimeout = &timeout
			} else {
				options.Timeout = &timeout
			}
			done := publicationStart(t, d, context.Background(), "CREATE INDEX iv ON t(v)", options)
			waitCatalogRequest(t, d, true)
			err := onlineWait(t, done)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("publisher deadline: %v", err)
			}
			if lockTimeout && !errors.Is(err, ErrLockTimeout) {
				t.Fatalf("missing lock timeout identity: %v", err)
			}
			if err := victim.Commit(); err != nil {
				t.Fatalf("timed-out publisher killed victim: %v", err)
			}
			if objectIndexID(t, d, "iv") != 0 {
				t.Fatal("timed-out publication exposed index")
			}
		})
	}
}

func TestIndexPublicationSQLAndPreparedPolicies(t *testing.T) {
	cases := []struct {
		name, query            string
		prepare, options, drop bool
	}{
		{name: "create-sql", query: "CREATE INDEX iv ON t(v) KILL TRANSACTIONS AFTER '0s'"},
		{name: "alter-add-sql", query: "ALTER TABLE t ADD INDEX iv(v) KILL TRANSACTIONS AFTER '0s'"},
		{name: "drop-sql", query: "DROP INDEX iv KILL TRANSACTIONS AFTER '0s'", drop: true},
		{name: "alter-drop-sql", query: "ALTER TABLE t DROP INDEX iv KILL TRANSACTIONS AFTER '0s'", drop: true},
		{name: "prepared-sql", query: "CREATE INDEX iv ON t(v) KILL TRANSACTIONS AFTER '0s'", prepare: true},
		{name: "prepared-options", query: "CREATE INDEX iv ON t(v)", prepare: true, options: true},
		{name: "drop-options", query: "DROP INDEX iv", options: true, drop: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, _, _ := onlineTestDatabase(t)
			objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'original')")
			if c.drop {
				objectExec(t, d, "CREATE INDEX iv ON t(v)")
			}
			victim := publicationOwner(t, d)
			zero := time.Duration(0)
			var options ExecOptions
			if c.options {
				options.IndexPublicationAbortAfter = &zero
			}
			publisher, err := d.NewSession()
			if err != nil {
				t.Fatal(err)
			}
			defer publisher.Close()
			if c.prepare {
				stmt, err := publisher.Prepare(c.query)
				if err != nil {
					t.Fatal(err)
				}
				defer stmt.Close()
				_, err = stmt.ExecWithOptions(options)
				if err != nil {
					t.Fatal(err)
				}
			} else if _, err := publisher.ExecWithOptions(c.query, options); err != nil {
				t.Fatal(err)
			}
			if err := victim.Commit(); !errors.Is(err, ErrTransactionAborted) {
				t.Fatalf("victim Commit: %v", err)
			}
			if got := objectIndexID(t, d, "iv"); (got == 0) != c.drop {
				t.Fatalf("index visibility after publication: id=%d drop=%v", got, c.drop)
			}
			publicationAssertOriginal(t, d)
		})
	}
}

func TestIndexPublicationAbortsReusableSessionAndDefaultSession(t *testing.T) {
	for _, defaultSession := range []bool{false, true} {
		name := "session"
		if defaultSession {
			name = "database-default"
		}
		t.Run(name, func(t *testing.T) {
			d, _, _ := onlineTestDatabase(t)
			objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'original')")
			exec := d.Exec
			if !defaultSession {
				s, err := d.NewSession()
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				exec = s.Exec
			}
			if _, err := exec("BEGIN; UPDATE t SET v='private' WHERE id=1"); err != nil {
				t.Fatal(err)
			}
			done := publicationStart(t, d, context.Background(), "CREATE INDEX iv ON t(v) KILL TRANSACTIONS AFTER '0s'", ExecOptions{})
			if err := onlineWait(t, done); err != nil {
				t.Fatal(err)
			}
			if _, err := exec("COMMIT"); !errors.Is(err, ErrTransactionAborted) {
				t.Fatalf("missing transaction abort: %v", err)
			}
			if _, err := exec("BEGIN; UPDATE t SET v='fresh' WHERE id=1; COMMIT"); err != nil {
				t.Fatalf("session unusable after acknowledging abort: %v", err)
			}
			if r := objectExec(t, d, "SELECT v FROM t WHERE id=1"); r.Rows[0]["v"].Text != "fresh" {
				t.Fatal(r)
			}
		})
	}
}

func TestIndexPublicationAbortsActiveLockWaiters(t *testing.T) {
	d, _, _ := onlineTestDatabase(t)
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'original'),(2,'second')")
	owner := publicationOwner(t, d)
	waiter, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer waiter.Rollback()
	if _, err := waiter.Exec("UPDATE t SET v='earlier-private' WHERE id=2"); err != nil {
		t.Fatal(err)
	}
	victimDone := make(chan error, 1)
	go func() { _, err := waiter.Exec("UPDATE t SET v='waiter' WHERE id=1"); victimDone <- err }()
	publicationWaitSchema(t, d, waiter.session.schema)
	done := publicationStart(t, d, context.Background(), "CREATE INDEX iv ON t(v) KILL TRANSACTIONS AFTER '0s'", ExecOptions{})
	if err := onlineWait(t, done); err != nil {
		t.Fatal(err)
	}
	if err := onlineWait(t, victimDone); !errors.Is(err, ErrTransactionAborted) {
		t.Fatalf("active victim did not report publication abort: %v", err)
	}
	if err := owner.Commit(); !errors.Is(err, ErrTransactionAborted) {
		t.Fatalf("idle owner did not report publication abort: %v", err)
	}
	publicationAssertOriginal(t, d)
	r := objectExec(t, d, "SELECT v FROM t WHERE id=2")
	if r.Rows[0]["v"].Text != "second" {
		t.Fatalf("victim earlier statement committed: %#v", r)
	}
}

func TestIndexPublicationAbortsStreamingWriterPreservesReadonly(t *testing.T) {
	d, _, _ := onlineTestDatabase(t)
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'original'),(2,'other')")
	writer := publicationOwner(t, d)
	rows, err := writer.Query("SELECT id,v FROM t ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	reader, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Rollback()
	pinned, err := reader.Query("SELECT id,v FROM t ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Close()
	done := publicationStart(t, d, context.Background(), "CREATE INDEX iv ON t(v) KILL TRANSACTIONS AFTER '0s'", ExecOptions{})
	if err := onlineWait(t, done); err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Fatal("aborted writer cursor returned another row")
	}
	if err := rows.Err(); !errors.Is(err, ErrTransactionAborted) {
		t.Fatalf("writer cursor abort identity: %v", err)
	}
	count := 0
	for pinned.Next() {
		count++
	}
	if err := pinned.Err(); err != nil {
		t.Fatalf("readonly cursor was killed: %v", err)
	}
	if count != 2 {
		t.Fatal(count)
	}
	if err := reader.Commit(); err != nil {
		t.Fatalf("readonly transaction was killed: %v", err)
	}
	if err := writer.Commit(); !errors.Is(err, ErrTransactionAborted) {
		t.Fatalf("writer Commit: %v", err)
	}
	publicationAssertOriginal(t, d)
}

func TestIndexPublicationCancellationAndShutdownBeforeGrace(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		name := "cancel"
		if shutdown {
			name = "shutdown"
		}
		t.Run(name, func(t *testing.T) {
			d, dir, opt := onlineTestDatabase(t)
			objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'original')")
			victim := publicationOwner(t, d)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			grace := time.Second
			done := publicationStart(t, d, ctx, "CREATE INDEX iv ON t(v)", ExecOptions{IndexPublicationAbortAfter: &grace})
			waitCatalogRequest(t, d, true)
			if shutdown {
				if err := d.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				cancel()
			}
			if err := onlineWait(t, done); err == nil {
				t.Fatal("interrupted publisher succeeded")
			}
			if !shutdown {
				if err := victim.Commit(); err != nil {
					t.Fatalf("cancelled publisher killed victim: %v", err)
				}
				if objectIndexID(t, d, "iv") != 0 {
					t.Fatal("cancelled index published")
				}
				if err := d.Close(); err != nil {
					t.Fatal(err)
				}
			}
			reopened, err := Open(dir, opt)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if _, err := reopened.CheckIntegrity(); err != nil {
				t.Fatal(err)
			}
			if objectIndexID(t, reopened, "iv") != 0 {
				t.Fatal("interrupted publication survived reopen")
			}
			if shutdown {
				publicationAssertOriginal(t, reopened)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestIndexPublicationRejectsNegativeGrace(t *testing.T) {
	d, _, _ := onlineTestDatabase(t)
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT)")
	negative := -time.Millisecond
	if _, err := d.ExecWithOptions("CREATE INDEX iv ON t(v)", ExecOptions{IndexPublicationAbortAfter: &negative}); err == nil {
		t.Fatal("negative publication grace accepted")
	}
	if objectIndexID(t, d, "iv") != 0 {
		t.Fatal("invalid grace published an index")
	}
}

func TestIndexPublicationPreservesActiveAutocommitWaiter(t *testing.T) {
	for _, defaultSession := range []bool{false, true} {
		name := "session"
		if defaultSession {
			name = "database-default"
		}
		t.Run(name, func(t *testing.T) {
			d, _, _ := onlineTestDatabase(t)
			objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'original')")
			owner := publicationOwner(t, d)
			exec := d.Exec
			schema := d.schema
			var session *Session
			if !defaultSession {
				var err error
				session, err = d.NewSession()
				if err != nil {
					t.Fatal(err)
				}
				defer session.Close()
				exec = session.Exec
				schema = session.schema
			}
			victimDone := make(chan error, 1)
			go func() { _, err := exec("UPDATE t SET v='waiter' WHERE id=1"); victimDone <- err }()
			publicationWaitSchema(t, d, schema)
			done := publicationStart(t, d, context.Background(), "CREATE INDEX iv ON t(v) KILL TRANSACTIONS AFTER '0s'", ExecOptions{})
			if err := onlineWait(t, done); err != nil {
				t.Fatal(err)
			}
			if err := onlineWait(t, victimDone); err != nil {
				t.Fatalf("autocommit statement was interrupted: %v", err)
			}
			if err := owner.Commit(); !errors.Is(err, ErrTransactionAborted) {
				t.Fatalf("owner Commit: %v", err)
			}
			r := objectExec(t, d, "SELECT id FROM t WHERE v='waiter'")
			if len(r.Rows) != 1 {
				t.Fatalf("publication missed surviving autocommit write: %#v", r)
			}
			if _, err := exec("UPDATE t SET v='fresh' WHERE id=1"); err != nil {
				t.Fatalf("autocommit session unusable after publication: %v", err)
			}
			if _, err := d.CheckIntegrity(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestIndexPublicationAbortSelectionDoesNotCrossTransactionGeneration(t *testing.T) {
	for _, defaultSession := range []bool{false, true} {
		name := "session"
		if defaultSession {
			name = "database-default"
		}
		t.Run(name, func(t *testing.T) {
			d, _, _ := onlineTestDatabase(t)
			objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'original')")
			exec := d.Exec
			if !defaultSession {
				session, err := d.NewSession()
				if err != nil {
					t.Fatal(err)
				}
				defer session.Close()
				exec = session.Exec
			}
			if _, err := exec("BEGIN; UPDATE t SET v='committed' WHERE id=1"); err != nil {
				t.Fatal(err)
			}
			selected, release := onlineBarrier(t, d, "abort-victims-selected")
			done := publicationStart(t, d, context.Background(), "CREATE INDEX iv ON t(v) KILL TRANSACTIONS AFTER '0s'", ExecOptions{})
			onlineReached(t, selected)
			// Complete the selected generation and start another transaction before its
			// delayed abort signal. The new read-only generation owns no catalog lock.
			if _, err := exec("COMMIT; BEGIN; SELECT v FROM t WHERE id=1"); err != nil {
				t.Fatal(err)
			}
			release()
			if err := onlineWait(t, done); err != nil {
				t.Fatal(err)
			}
			if _, err := exec("COMMIT"); err != nil {
				t.Fatalf("stale publication victim selection killed a new transaction: %v", err)
			}
			r := objectExec(t, d, "SELECT id FROM t WHERE v='committed'")
			if len(r.Rows) != 1 {
				t.Fatalf("completed selected generation was lost: %#v", r)
			}
			if _, err := d.CheckIntegrity(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestIndexPublicationCancellationBeforeVictimSignal(t *testing.T) {
	d, _, _ := onlineTestDatabase(t)
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'original')")
	victim := publicationOwner(t, d)
	selected, release := onlineBarrier(t, d, "abort-victims-selected")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := publicationStart(t, d, ctx, "CREATE INDEX iv ON t(v) KILL TRANSACTIONS AFTER '0s'", ExecOptions{})
	onlineReached(t, selected)
	cancel()
	release()
	if err := onlineWait(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("publisher cancel: %v", err)
	}
	if err := victim.Commit(); err != nil {
		t.Fatalf("cancelled publisher signaled selected victim: %v", err)
	}
	if objectIndexID(t, d, "iv") != 0 {
		t.Fatal("cancelled publication exposed index")
	}
	if _, err := d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
}

func TestIndexPublicationCancellationAfterVictimSignalCompletesRollback(t *testing.T) {
	d, _, _ := onlineTestDatabase(t)
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'original')")
	victim, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer victim.Close()
	if _, err := victim.Exec("BEGIN; UPDATE t SET v='private' WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	// Keep the idle rollback worker waiting after its request has been signaled.
	// Publisher cancellation must not abandon that request or retain its locks.
	victim.gate.Lock()
	var unlockOnce sync.Once
	unlock := func() { unlockOnce.Do(victim.gate.Unlock) }
	t.Cleanup(unlock)
	defer unlock()
	signaled, release := onlineBarrier(t, d, "abort-victims-signalled")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := publicationStart(t, d, ctx, "CREATE INDEX iv ON t(v) KILL TRANSACTIONS AFTER '0s'", ExecOptions{})
	onlineReached(t, signaled)
	cancel()
	release()
	publisherErr := onlineWait(t, done)
	unlock()
	if !errors.Is(publisherErr, context.Canceled) {
		t.Fatalf("publisher cancel: %v", publisherErr)
	}
	if _, err := victim.Exec("COMMIT"); !errors.Is(err, ErrTransactionAborted) {
		t.Fatalf("signaled victim did not complete rollback: %v", err)
	}
	publicationAssertOriginal(t, d)
	if objectIndexID(t, d, "iv") != 0 {
		t.Fatal("cancelled publication exposed index")
	}
	if _, err := victim.Exec("BEGIN; UPDATE t SET v='fresh' WHERE id=1; COMMIT"); err != nil {
		t.Fatalf("finished rollback left stale abort request: %v", err)
	}
	if _, err := d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
}
