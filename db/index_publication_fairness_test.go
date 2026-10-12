package db

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestIndexPublicationExpiredPolicyAbortsEarlierQueuedTransaction(t *testing.T) {
	d, _, _ := onlineTestDatabase(t)
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'original'); CREATE INDEX iv ON t(v)")
	before := d.GetCreateTableSQL("t")
	owner := publicationOwner(t, d)
	predecessor, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = predecessor.Rollback() })
	predecessorDone := make(chan error, 1)
	go func() {
		_, err := predecessor.Exec("ALTER TABLE t ALTER COLUMN v SET DEFAULT 'uncommitted-default'")
		predecessorDone <- err
	}()
	// This exclusive waiter precedes the publisher. After the current owner's
	// rollback, catalog fairness grants it ownership before publication.
	publicationWaitSchema(t, d, predecessor.session.schema)
	var selections atomic.Int32
	secondSelection := make(chan struct{})
	d.onlineIndexHook = func(phase string) {
		if phase == "abort-victims-selected" && selections.Add(1) == 2 {
			close(secondSelection)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	grace := 30 * time.Millisecond
	publicationDone := publicationStart(t, d, ctx, "DROP INDEX iv", ExecOptions{IndexPublicationAbortAfter: &grace})
	if err := onlineWait(t, publicationDone); err != nil {
		t.Fatalf("publication did not abort the predecessor that acquired ownership after the threshold: %v", err)
	}
	select {
	case <-secondSelection:
	default:
		t.Fatalf("expected separate selections before and after catalog ownership changed, got %d", selections.Load())
	}
	// Cancellation may reach the predecessor during its DDL or after the DDL
	// returns. Both paths must roll back the whole explicit transaction.
	if err := onlineWait(t, predecessorDone); err != nil && !errors.Is(err, ErrTransactionAborted) {
		t.Fatalf("predecessor DDL: %v", err)
	}
	for label, tx := range map[string]*Tx{"initial owner": owner, "queued predecessor": predecessor} {
		if err := tx.Commit(); !errors.Is(err, ErrTransactionAborted) {
			t.Fatalf("%s transaction survived publication: %v", label, err)
		}
	}
	if d.GetIndexSQL("iv") != "" {
		t.Fatal("publication did not drop the index")
	}
	if after := d.GetCreateTableSQL("t"); after != before {
		t.Fatalf("aborted predecessor published its ALTER: before=%s after=%s", before, after)
	}
	publicationAssertOriginal(t, d)
}
