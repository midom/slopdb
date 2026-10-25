package mysqlserver

import (
	"context"
	"errors"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
)

func TestMySQLClosedNativeSessionDisconnectsPoolConnection(t *testing.T) {
	server := startTestServer(t, "tcp", Config{})
	pool := server.open(t, "slopdb", "", "slopdb")
	pool.SetMaxOpenConns(1)
	pool.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, q := range []string{"CREATE TABLE session_survivor(id BIGINT PRIMARY KEY)", "INSERT INTO session_survivor VALUES(1)"} {
		if _, err := pool.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	var first, next uint64
	if err := pool.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&first); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.ExecContext(ctx, "SELECT 1/0"); err == nil {
		t.Fatal("ordinary statement error missing")
	}
	if err := pool.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&next); err != nil || first != next {
		t.Fatal("ordinary error disconnected session", first, next, err)
	}
	// The error packet can reach the driver just before TCP FIN. Retry only
	// these read-only observations until the pool discards that old socket.
	nextConnection := func() (uint64, error) {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			var id uint64
			err := pool.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&id)
			if err == nil {
				return id, nil
			}
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-ticker.C:
			}
		}
	}
	checkFatal := func(err error) {
		t.Helper()
		var protocol *driver.MySQLError
		if !errors.As(err, &protocol) || protocol.Number != 1053 || string(protocol.SQLState[:]) != "08S01" {
			t.Fatalf("closed session must be a connection error: %v", err)
		}
	}
	if err := server.database.WithReplicaStopped(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	_, err := pool.ExecContext(ctx, "INSERT INTO session_survivor VALUES(2)")
	checkFatal(err)
	if next, err = nextConnection(); err != nil || next == first {
		t.Fatal("pool retained closed session", next, err)
	}
	rows, err := pool.QueryContext(ctx, "SHOW RAFT STATUS")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var name, value string
		if err = rows.Scan(&name, &value); err != nil {
			t.Fatal(err)
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	transaction, err := pool.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = transaction.ExecContext(ctx, "INSERT INTO session_survivor VALUES(3)"); err != nil {
		t.Fatal(err)
	}
	if err = server.database.WithReplicaStopped(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	_, err = transaction.ExecContext(ctx, "INSERT INTO session_survivor VALUES(4)")
	checkFatal(err)
	_ = transaction.Rollback()
	if _, err = nextConnection(); err != nil {
		t.Fatal("pool did not reconnect after lost transaction", err)
	}
	var count int64
	if err = pool.QueryRowContext(ctx, "SELECT count(*) FROM session_survivor").Scan(&count); err != nil || count != 1 {
		t.Fatal("lost transaction or rejected write was replayed", count, err)
	}
}
