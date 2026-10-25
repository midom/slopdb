package postgresserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestPostgreSQLClosedNativeSessionDisconnectsPoolConnection(t *testing.T) {
	server := startTestServer(t, "tcp", Config{})
	config, err := pgx.ParseConfig(server.dsn("slopdb", "", "slopdb"))
	if err != nil {
		t.Fatal(err)
	}
	pool := stdlib.OpenDB(*config)
	defer pool.Close()
	pool.SetMaxOpenConns(1)
	pool.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err = pool.ExecContext(ctx, "CREATE TABLE session_survivor(id BIGINT PRIMARY KEY);INSERT INTO session_survivor VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	var first, next int64
	if err = pool.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&first); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.ExecContext(ctx, "SELECT 1/0"); err == nil {
		t.Fatal("ordinary statement error missing")
	}
	if err = pool.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&next); err != nil || first != next {
		t.Fatal("ordinary error disconnected session", first, next, err)
	}
	checkFatal := func(err error) {
		t.Helper()
		var protocol *pgconn.PgError
		if !errors.As(err, &protocol) || protocol.Code != "57P01" || protocol.Severity != "FATAL" {
			t.Fatalf("closed session must be fatal: %v", err)
		}
	}
	if err = server.database.WithReplicaStopped(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	_, err = pool.ExecContext(ctx, "INSERT INTO session_survivor VALUES(2)")
	checkFatal(err)
	if err = pool.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&next); err != nil || next == first {
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
	var count int64
	if err = pool.QueryRowContext(ctx, "SELECT count(*) FROM session_survivor").Scan(&count); err != nil || count != 1 {
		t.Fatal("lost transaction or rejected write was replayed", count, err)
	}
}
