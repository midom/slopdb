package replication

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"slopdb/db"
)

func TestTransactionCommitStillRequiresQuorum(t *testing.T) {
	for _, mode := range []Durability{DurabilitySync, DurabilityAsync} {
		t.Run(string(mode), func(t *testing.T) {
			config := asyncConfig(t, "source", "tcp")
			config.Durability = mode
			config.Bootstrap = true
			source := openTestNode(t, config)
			ready(t, source)
			exec(t, source, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO items VALUES(1,'committed')")
			other := asyncConfig(t, "target", "tcp")
			other.Durability = mode
			other.Join = source.ControlAddress()
			target := openTestNode(t, other)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := source.Promote(ctx, other.ID); err != nil {
				t.Fatal(err)
			}
			session, err := source.Database().NewSession()
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			if _, err = session.Exec("BEGIN; INSERT INTO items VALUES(2,'must not be acknowledged')"); err != nil {
				t.Fatal(err)
			}
			if err = target.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err = session.ExecContext(ctx, "COMMIT"); err == nil {
				t.Fatal("commit acknowledged without a voting quorum")
			}
			if _, err = source.Database().ExecContext(ctx, "SELECT id FROM items"); err == nil {
				t.Fatal("read succeeded without a voting quorum")
			}
			// Examine an owned copy, without bypassing the live node's SQL guard.
			// Private transaction state and uncommitted Raft entries must not have
			// entered the native committed database.
			destination := filepath.Join(t.TempDir(), "copy")
			if err = source.Database().ReplicationStorage().Backup(ctx, destination); err != nil {
				t.Fatal(err)
			}
			copy, err := db.Open(destination, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = copy.Close() })
			rows, err := copy.Exec("SELECT id,v FROM items ORDER BY id")
			if err != nil || len(rows.Rows) != 1 || rows.Rows[0]["id"].Int != 1 || rows.Rows[0]["v"].Text != "committed" {
				t.Fatalf("failed commit reached native state: %+v %v", rows, err)
			}
			if err = copy.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
