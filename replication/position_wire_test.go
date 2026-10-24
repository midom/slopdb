package replication

import (
	"testing"
	"time"
)

func TestPositionSQLProtocols(t *testing.T) {
	for _, protocol := range []string{"mysql", "postgres"} {
		t.Run(protocol, func(t *testing.T) {
			source, replica := readReplicaPair(t, DurabilityAsync, "tcp", 2*time.Second)
			master, ctx := openPositionSQLClient(t, source, protocol)
			reader, readCtx := openPositionSQLClient(t, replica, protocol)
			tx, err := master.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = tx.ExecContext(ctx, "INSERT INTO items VALUES(3,'causal write')"); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			var position string
			if err = master.QueryRowContext(ctx, "SHOW LAST COMMIT POSITION").Scan(&position); err != nil {
				t.Fatal(err)
			}
			if _, err = source.parsePosition(position); err != nil {
				t.Fatalf("wire commit position invalid: %q %v", position, err)
			}
			// Master position queries have fixed metadata and can be prepared.
			show, err := master.PrepareContext(ctx, "SHOW REPLICATION POSITION")
			if err != nil {
				t.Fatal(err)
			}
			defer show.Close()
			var current string
			if err = show.QueryRowContext(ctx).Scan(&current); err != nil {
				t.Fatal(err)
			}
			target, err := source.parsePosition(position)
			if err != nil {
				t.Fatal(err)
			}
			latest, err := source.parsePosition(current)
			if err != nil || latest.Index < target.Index {
				t.Fatalf("wire master behind commit: %+v %v", latest, err)
			}
			query := "WAIT FOR POSITION ? TIMEOUT ?"
			if protocol == "postgres" {
				query = "WAIT FOR POSITION $1 TIMEOUT $2"
			}
			wait, err := reader.PrepareContext(readCtx, query)
			if err != nil {
				t.Fatal(err)
			}
			defer wait.Close()
			before := source.raftNode().LastIndex()
			if _, err = wait.ExecContext(readCtx, position, "2s"); err != nil {
				t.Fatal(err)
			}
			var value string
			if err = reader.QueryRowContext(readCtx, "SELECT v FROM items WHERE id=3").Scan(&value); err != nil || value != "causal write" {
				t.Fatalf("wire wait didn't make commit visible: %q %v", value, err)
			}
			if source.raftNode().LastIndex() != before {
				t.Fatal("wire position wait appended log entries")
			}
			if _, err = wait.ExecContext(readCtx, position+"invalid", "1s"); err == nil {
				t.Fatal("wire wait accepted invalid token")
			}
		})
	}
}
