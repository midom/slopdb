package db

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestIndexPublicationSQLThresholdOverridesOptions(t *testing.T) {
	d, _, _ := onlineTestDatabase(t)
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'original')")
	victim := publicationOwner(t, d)
	immediate := time.Duration(0)
	done := publicationStart(t, d, context.Background(), "CREATE INDEX iv ON t(v) KILL TRANSACTIONS AFTER '1s'", ExecOptions{IndexPublicationAbortAfter: &immediate})
	waitCatalogRequest(t, d, true)
	if err := victim.Commit(); err != nil {
		t.Fatalf("API threshold overrode the SQL grace: %v", err)
	}
	if err := onlineWait(t, done); err != nil {
		t.Fatal(err)
	}
	result := objectExec(t, d, "SELECT id FROM t WHERE v='private'")
	if len(result.Rows) != 1 {
		t.Fatalf("natural commit lost: %v", result.Rows)
	}
}

func TestIndexPublicationRejectsUnsupportedExecutionShapes(t *testing.T) {
	d, _, _ := onlineTestDatabase(t)
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'original'); CREATE INDEX iv ON t(v)")
	tx := publicationOwner(t, d)
	for _, query := range []string{
		"CREATE INDEX second ON t(v) KILL TRANSACTIONS AFTER '0s'",
		"DROP INDEX iv KILL TRANSACTIONS AFTER '0s'",
		"ALTER TABLE t DROP INDEX iv KILL TRANSACTIONS AFTER '0s'",
	} {
		if _, err := tx.Exec(query); err == nil {
			t.Fatalf("accepted policy inside a transaction: %s", query)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("rejecting an unsupported policy ended the transaction: %v", err)
	}
	immediate := time.Duration(0)
	for _, query := range []string{
		"SELECT * FROM t",
		"ALTER TABLE t ADD COLUMN extra TEXT, ADD INDEX second(v)",
	} {
		if _, err := d.ExecWithOptions(query, ExecOptions{IndexPublicationAbortAfter: &immediate}); err == nil {
			t.Fatalf("accepted unsupported API policy: %s", query)
		}
	}
	if d.GetIndexSQL("second") != "" || d.GetIndexSQL("iv") == "" {
		t.Fatal("rejected policies changed indexes")
	}
}

func TestAbortedTransactionOperationsPreserveErrorIdentity(t *testing.T) {
	d, _, _ := onlineTestDatabase(t)
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'original')")
	tx := publicationOwner(t, d)
	if err := onlineWait(t, publicationStart(t, d, context.Background(), "CREATE INDEX iv ON t(v) KILL TRANSACTIONS AFTER '0s'", ExecOptions{})); err != nil {
		t.Fatal(err)
	}
	check := func(operation string, err error) {
		t.Helper()
		if !errors.Is(err, ErrTransactionAborted) {
			t.Fatalf("%s lost abort identity: %v", operation, err)
		}
		var aborted *TransactionAbortedError
		if !errors.As(err, &aborted) || aborted.SessionID == 0 || aborted.PublisherSessionID == 0 {
			t.Fatalf("%s lost victim/publisher context: %v", operation, err)
		}
	}
	check("Commit", tx.Commit())
	check("Rollback", tx.Rollback())
	_, err := tx.Prepare("SELECT * FROM t")
	check("Prepare", err)
	_, err = tx.Query("SELECT * FROM t")
	check("Query", err)
	_, err = tx.Exec("SELECT * FROM t")
	check("Exec", err)
}
