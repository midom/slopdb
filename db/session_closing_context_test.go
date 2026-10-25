package db

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"slopdb/storage"
)

func closingContextDatabase(t *testing.T) *Database {
	t.Helper()
	options := storage.DefaultOptions()
	options.DisableWALSync = true
	database, err := Open(t.TempDir(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	return database
}

func requireClosingContextLive(t *testing.T, ctx context.Context) {
	t.Helper()
	if ctx == nil {
		t.Fatal("missing session closing context")
	}
	select {
	case <-ctx.Done():
		t.Fatalf("session closing context canceled early: %v", ctx.Err())
	default:
	}
}

func awaitClosingContext(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatalf("closing cause: %v", ctx.Err())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("session closing context was not canceled")
	}
}

func TestSessionClosingContextSurvivesStatementAndRowsCompletion(t *testing.T) {
	d := closingContextDatabase(t)
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES(1),(2)")
	creation, cancelCreation := context.WithCancel(context.Background())
	s, err := d.NewInspectionSession(creation)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	ctx := s.ClosingContext()
	cancelCreation()
	requireClosingContextLive(t, ctx)
	if s.ClosingContext() != ctx {
		t.Fatal("closing context changed identity")
	}
	if _, err := s.Exec("SELECT id FROM t WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	requireClosingContextLive(t, ctx)
	for _, exhaust := range []bool{false, true} {
		rows, err := s.Query("SELECT id FROM t")
		if err != nil {
			t.Fatal(err)
		}
		if exhaust {
			for rows.Next() {
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		requireClosingContextLive(t, ctx)
	}
	query, cancelQuery := context.WithCancel(context.Background())
	rows, err := s.QueryContext(query, "SELECT id FROM t")
	if err != nil {
		t.Fatal(err)
	}
	cancelQuery()
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	requireClosingContextLive(t, ctx)
	// Replication fencing cancels active work without closing the session.
	active, cancelActive := context.WithCancel(context.Background())
	if err := s.registerActive(cancelActive); err != nil {
		t.Fatal(err)
	}
	s.cancelActive()
	awaitClosingContext(t, active)
	s.clearActive()
	requireClosingContextLive(t, ctx)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	awaitClosingContext(t, ctx)
}

func TestSessionClosingContextCancelsBeforeSessionGateDrains(t *testing.T) {
	d := closingContextDatabase(t)
	s, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	s.gate.Lock()
	held := true
	defer func() {
		if held {
			s.gate.Unlock()
		}
	}()
	done := make(chan error, 1)
	go func() { done <- s.Close() }()
	// ClosingContext itself must remain observable without taking this gate.
	awaitClosingContext(t, s.ClosingContext())
	select {
	case err := <-done:
		t.Fatalf("Close passed a held session gate: %v", err)
	default:
	}
	s.gate.Unlock()
	held = false
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestSessionClosingContextCancelsBeforeDatabaseExecutionDrains(t *testing.T) {
	d := closingContextDatabase(t)
	a, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	b, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	if err := d.execution.RLockContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	held := true
	defer func() {
		if held {
			d.execution.RUnlock()
		}
	}()
	done := make(chan error, 1)
	go func() { done <- d.Close() }()
	awaitClosingContext(t, a.ClosingContext())
	awaitClosingContext(t, b.ClosingContext())
	select {
	case err := <-done:
		t.Fatalf("Close passed a held execution view: %v", err)
	default:
	}
	d.execution.RUnlock()
	held = false
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestSessionClosingContextConcurrentCreationAndDatabaseClose(t *testing.T) {
	for round := 0; round < 10; round++ {
		d := closingContextDatabase(t)
		var workers sync.WaitGroup
		start := make(chan struct{})
		sessions := make(chan *Session, 16)
		for i := 0; i < cap(sessions); i++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				<-start
				if s, err := d.NewSession(); err == nil {
					sessions <- s
				}
			}()
		}
		close(start)
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		workers.Wait()
		close(sessions)
		for s := range sessions {
			awaitClosingContext(t, s.ClosingContext())
		}
	}
}

func TestSessionClosingContextTransactionHandleLifecycle(t *testing.T) {
	d := closingContextDatabase(t)
	for _, commit := range []bool{false, true} {
		tx, err := d.Begin()
		if err != nil {
			t.Fatal(err)
		}
		ctx := tx.session.ClosingContext()
		requireClosingContextLive(t, ctx)
		if commit {
			err = tx.Commit()
		} else {
			err = tx.Rollback()
		}
		if err != nil {
			t.Fatal(err)
		}
		awaitClosingContext(t, ctx)
	}
	// Committing SQL in a reusable Session is different from completing a Tx
	// handle: the Session remains open for subsequent work.
	s, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, err := s.Exec("BEGIN; COMMIT"); err != nil {
		t.Fatal(err)
	}
	requireClosingContextLive(t, s.ClosingContext())
}
