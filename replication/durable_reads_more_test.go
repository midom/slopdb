package replication

import (
	"context"
	"errors"
	"testing"
	"time"

	"slopdb/db"
)

func TestDurableReadStreamRetainsOldBarrier(t *testing.T) {
	c := asyncConfig(t, "streams", "tcp")
	c.Bootstrap = true
	source := openTestNode(t, c)
	ready(t, source)
	exec(t, source, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO items VALUES(1,'one'),(2,'two'); BEGIN; COMMIT DURABLE")
	ctx := db.WithReadBarrier(context.Background(), db.ReadBarrierPrevious)
	rows, err := source.Database().QueryContext(ctx, "SELECT id,v FROM items ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("empty old barrier: %v", rows.Err())
	}
	exec(t, source, "DELETE FROM items WHERE id=1; UPDATE items SET v='new two' WHERE id=2; INSERT INTO items VALUES(3,'three'); BEGIN; COMMIT DURABLE; VACUUM")
	var id int64
	var value string
	if err = rows.Scan(&id, &value); err != nil || id != 1 || value != "one" {
		t.Fatalf("first row crossed barrier: %d %q %v", id, value, err)
	}
	if !rows.Next() {
		t.Fatalf("missing historical second row: %v", rows.Err())
	}
	if err = rows.Scan(&id, &value); err != nil || id != 2 || value != "two" {
		t.Fatalf("old versions reclaimed: %d %q %v", id, value, err)
	}
	if rows.Next() || rows.Err() != nil {
		t.Fatalf("historical stream included later row: %v", rows.Err())
	}
	if err = rows.Close(); err != nil {
		t.Fatal(err)
	}
	result, err := source.Database().ExecWithOptions("SELECT v FROM items WHERE id=2", db.ExecOptions{ReadBarrier: db.ReadBarrierPrevious})
	if err != nil || result.Rows[0]["v"].Text != "new two" {
		t.Fatalf("new previous barrier not adopted: %+v %v", result, err)
	}
	// Switching back to a normal prepared UPDATE must refresh the live catalog,
	// rather than retaining read-only state from a historical snapshot.
	prepared, err := source.Database().Prepare("UPDATE items SET v=? WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if _, err = prepared.Exec("updated", int64(2)); err != nil {
		t.Fatalf("historical read poisoned live update: %v", err)
	}
	result, err = source.Database().Exec("SELECT v FROM items WHERE id=2")
	if err != nil || result.Rows[0]["v"].Text != "updated" {
		t.Fatalf("live update missed: %+v %v", result, err)
	}
}

func TestDurableReadCandidateRetentionAndNoFallback(t *testing.T) {
	node := batchTestNode(t, DurabilityAsync)
	for i := uint64(1); i <= 30; i++ {
		if result := node.Apply(batchTestLog(t, i, command{Version: 1, Kind: "fence"})); result != nil {
			t.Fatal(result)
		}
	}
	node.durableReads.mu.Lock()
	if len(node.durableReads.pending) > 8 || node.durableReads.latest != nil {
		t.Fatal("candidate cache unbounded or uncertified candidate published")
	}
	node.durableReads.mu.Unlock()
	node.advanceDurable(&durableCertificate{Body: durableBody{Index: 30, Term: 1}})
	node.durableReads.mu.Lock()
	if node.durableReads.latest == nil || node.durableReads.latest.index != 30 || len(node.durableReads.pending) != 0 {
		t.Fatal("certificate did not publish/retire candidates")
	}
	node.durableReads.mu.Unlock()
	c := asyncConfig(t, "missing", "tcp")
	c.Bootstrap = true
	source := openTestNode(t, c)
	ready(t, source)
	exec(t, source, "CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1); BEGIN; COMMIT DURABLE")
	source.clearDurableViews()
	if _, err := source.Database().ExecWithOptions("SELECT * FROM items", db.ExecOptions{ReadBarrier: db.ReadBarrierPrevious}); !errors.Is(err, ErrDurableReadUnavailable) {
		t.Fatalf("missing view fell back to live data: %v", err)
	}
	if _, err := source.Database().ExecWithOptions("SELECT * FROM items", db.ExecOptions{ReadBarrier: db.ReadBarrierNext}); err != nil {
		t.Fatalf("NEXT could not establish a fresh retained barrier: %v", err)
	}
}

func TestDurableReadContextEnablesOwnedStream(t *testing.T) {
	source, replica := readReplicaPair(t, DurabilityAsync, "tcp", 0)
	if err := source.FlushDurable(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitDurableReadView(t, replica, source.durableIndex())
	if _, err := replica.Database().Query("SELECT * FROM items"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("ordinary SQL admitted without replica policy: %v", err)
	}
	rows, err := replica.Database().QueryContext(db.WithReadBarrier(context.Background(), db.ReadBarrierPrevious), "SELECT * FROM items")
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatalf("explicit durable context didn't admit stream: %v", rows.Err())
	}
	if err = rows.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDurableReadRejectsDirtyTransactions(t *testing.T) {
	c := asyncConfig(t, "dirty", "tcp")
	c.Bootstrap = true
	node := openTestNode(t, c)
	ready(t, node)
	exec(t, node, "CREATE TABLE items(id INTEGER PRIMARY KEY); BEGIN; COMMIT DURABLE")
	tx, err := node.Database().Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec("INSERT INTO items VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecWithOptions("SELECT * FROM items", db.ExecOptions{ReadBarrier: db.ReadBarrierPrevious}); err == nil {
		t.Fatal("historical read merged private writes")
	}
	result, err := node.Database().Exec("SELECT * FROM items")
	if err != nil || len(result.Rows) != 0 {
		t.Fatalf("dirty transaction leaked changes: %+v %v", result, err)
	}
}

func TestDurableReadSQLProtocols(t *testing.T) {
	for _, protocol := range []string{"mysql", "postgres"} {
		t.Run(protocol, func(t *testing.T) {
			source, replica := readReplicaPair(t, DurabilityAsync, "tcp", time.Second)
			if err := source.FlushDurable(context.Background()); err != nil {
				t.Fatal(err)
			}
			waitDurableReadView(t, replica, source.durableIndex())
			client, ctx := openPositionSQLClient(t, replica, protocol)
			if _, err := client.ExecContext(ctx, "SET READ BARRIER PREVIOUS"); err != nil {
				t.Fatal(err)
			}
			query := "SELECT v FROM items WHERE id=?"
			if protocol == "postgres" {
				query = "SELECT v FROM items WHERE id=$1"
			}
			prepared, err := client.PrepareContext(ctx, query)
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Close()
			exec(t, source, "UPDATE items SET v='not yet stable' WHERE id=1")
			waitApplied(t, replica, source.applied.Load())
			var value string
			if err = prepared.QueryRowContext(ctx, int64(1)).Scan(&value); err != nil || value != "one" {
				t.Fatalf("wire PREVIOUS read live suffix: %q %v", value, err)
			}
			if _, err = client.ExecContext(ctx, "SET READ BARRIER NEXT"); err != nil {
				t.Fatal(err)
			}
			before := source.durableIndex()
			if err = prepared.QueryRowContext(ctx, int64(1)).Scan(&value); err != nil || value != "not yet stable" || source.durableIndex() <= before {
				t.Fatalf("wire NEXT did not wait: %q %v", value, err)
			}
			if _, err = client.ExecContext(ctx, "SET READ BARRIER COMMITTED"); err != nil {
				t.Fatal(err)
			}
			exec(t, source, "UPDATE items SET v='live again' WHERE id=1")
			waitApplied(t, replica, source.applied.Load())
			if err = prepared.QueryRowContext(ctx, int64(1)).Scan(&value); err != nil || value != "live again" {
				t.Fatalf("wire policy override: %q %v", value, err)
			}
		})
	}
}

func TestDurableReadSyncNextCancelsAfterEnqueue(t *testing.T) {
	c := testConfig(t, "cancel-next", "tcp")
	c.Bootstrap = true
	node := openTestNode(t, c)
	ready(t, node)
	reader, err := node.Database().NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	node.fsmMu.Lock()
	held := true
	defer func() {
		if held {
			node.fsmMu.Unlock()
		}
	}()
	before := node.raftNode().CommitIndex()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := reader.ExecContextWithOptions(ctx, "SELECT 1", db.ExecOptions{ReadBarrier: db.ReadBarrierNext})
		done <- err
	}()
	deadline := time.Now().Add(3 * time.Second)
	for node.raftNode().CommitIndex() <= before {
		if time.Now().After(deadline) {
			t.Fatal("read fence was not committed")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("committed fence wait ignored cancellation: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled read waited for held FSM application")
	}
	node.fsmMu.Unlock()
	held = false
}
