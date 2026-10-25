package db

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slopdb/storage"
	"testing"
)

func TestDurableConfirmationFailureDoesNotRollbackCommit(t *testing.T) {
	database, e := Open(filepath.Join(t.TempDir(), "db"), nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, e = database.Exec("CREATE TABLE items(id INTEGER PRIMARY KEY)"); e != nil {
		t.Fatal(e)
	}
	database.SetReplicationGuard(&ReplicationGuard{Check: func(context.Context) error { return nil }, Durable: func(context.Context) error { return context.DeadlineExceeded }})
	transaction, e := database.Begin()
	if e != nil {
		t.Fatal(e)
	}
	if _, e = transaction.Exec("INSERT INTO items VALUES(1)"); e != nil {
		t.Fatal(e)
	}
	e = transaction.CommitDurable()
	if !errors.Is(e, ErrDurabilityUnconfirmed) || !errors.Is(e, context.DeadlineExceeded) {
		t.Fatalf("commit outcome was not preserved: %v", e)
	}
	if e = transaction.Rollback(); !errors.Is(e, ErrTxDone) {
		t.Fatalf("committed transaction could be rolled back: %v", e)
	}
	rows, e := database.Exec("SELECT id FROM items")
	if e != nil || len(rows.Rows) != 1 || rows.Rows[0]["id"].Int != 1 {
		t.Fatalf("commit was lost: %+v %v", rows, e)
	}
	if e = database.Close(); e != nil {
		t.Fatal(e)
	}
	reopened, e := Open(database.dir, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	rows, e = reopened.Exec("SELECT id FROM items")
	if e != nil || len(rows.Rows) != 1 {
		t.Fatalf("committed result not persistent: %+v %v", rows, e)
	}
}

func TestStandaloneDurableCommitRejectsDisabledWALSync(t *testing.T) {
	for _, wal := range []bool{false, true} {
		t.Run(fmt.Sprint(wal), func(t *testing.T) {
			options := storage.DefaultOptions()
			options.DisableWAL = wal
			options.DisableWALSync = !wal
			database, e := Open(filepath.Join(t.TempDir(), "db"), options)
			if e != nil {
				t.Fatal(e)
			}
			defer database.Close()
			if _, e = database.Exec("CREATE TABLE items(id INTEGER PRIMARY KEY); BEGIN; INSERT INTO items VALUES(1)"); e != nil {
				t.Fatal(e)
			}
			if _, e = database.Exec("COMMIT DURABLE"); e == nil {
				t.Fatal("durable commit accepted disabled WAL synchronization")
			}
			if _, e = database.Exec("ROLLBACK"); e != nil {
				t.Fatal(e)
			}
			result, e := database.Exec("SELECT COUNT(*) AS n FROM items")
			if e != nil || result.Rows[0]["n"].Int != 0 {
				t.Fatalf("rejected durable commit published changes: %+v %v", result, e)
			}
		})
	}
}

func TestTransactionControlStillGuardsDataAccess(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, err = database.Exec("CREATE TABLE items(id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	noQuorum := errors.New("test quorum unavailable")
	database.SetReplicationGuard(&ReplicationGuard{Check: func(context.Context) error { return noQuorum }, CheckControl: func(context.Context) error { return nil }})
	// These operations manipulate private session state; the combined call also
	// covers ROLLBACK TO's ReleaseStmt representation.
	if _, err = database.Exec("USE main; SET search_path TO main; BEGIN; SAVEPOINT one; ROLLBACK TO one; RELEASE one; COMMIT"); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"SELECT 1", "INSERT INTO items VALUES(1)", "BEGIN; SELECT 1", "PRAGMA page_size", "EXPLAIN SELECT 1", "CREATE SCHEMA tenant", "DROP SCHEMA tenant CASCADE", "USE main; CREATE SCHEMA tenant"} {
		if _, err = database.Exec(query); !errors.Is(err, noQuorum) {
			t.Fatalf("data access bypassed quorum guard for %q: %v", query, err)
		}
	}
	prepared, err := database.Prepare("SELECT id FROM items")
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if _, err = prepared.Exec(); !errors.Is(err, noQuorum) {
		t.Fatalf("prepared read bypassed quorum guard: %v", err)
	}
	if _, err = database.Query("SELECT id FROM items"); !errors.Is(err, noQuorum) {
		t.Fatalf("streaming read bypassed quorum guard: %v", err)
	}
	// Existing replication owners retain full admission on control statements
	// unless they explicitly provide the specialized control callback.
	database.SetReplicationGuard(&ReplicationGuard{Check: func(context.Context) error { return noQuorum }})
	if _, err = database.Exec("BEGIN"); !errors.Is(err, noQuorum) {
		t.Fatalf("optional control callback changed fallback admission: %v", err)
	}
}
