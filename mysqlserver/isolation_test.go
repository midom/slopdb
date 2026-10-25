package mysqlserver

import (
	"context"
	"testing"
)

func TestMySQLRepeatableReadAndCurrentReadProtocol(t *testing.T) {
	server := startTestServer(t, "tcp", Config{})
	pool := server.open(t, "slopdb", "", "slopdb")
	ctx := context.Background()
	a, e := pool.Conn(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	b, e := pool.Conn(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	exec := func(query string) {
		t.Helper()
		if _, e = a.ExecContext(ctx, query); e != nil {
			t.Fatalf("%s: %v", query, e)
		}
	}
	read := func(query string, want int) {
		t.Helper()
		var got int
		if e = a.QueryRowContext(ctx, query).Scan(&got); e != nil || got != want {
			t.Fatalf("%s: got%d want%d %v", query, got, want, e)
		}
	}
	exec("CREATE TABLE records(id INT PRIMARY KEY,v INT)")
	exec("INSERT INTO records VALUES(1,10)")
	var level string
	if e = a.QueryRowContext(ctx, "SELECT @@transaction_isolation").Scan(&level); e != nil || level != "REPEATABLE-READ" {
		t.Fatal(level, e)
	}
	exec("BEGIN")
	read("SELECT v FROM records", 10)
	if _, e = b.ExecContext(ctx, "UPDATE records SET v=20"); e != nil {
		t.Fatal(e)
	}
	read("SELECT v FROM records", 10)
	read("SELECT v FROM records FOR UPDATE", 20)
	read("SELECT v FROM records", 10)
	exec("ROLLBACK")
	exec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
	exec("BEGIN")
	read("SELECT v FROM records", 20)
	if _, e = b.ExecContext(ctx, "UPDATE records SET v=21"); e != nil {
		t.Fatal(e)
	}
	read("SELECT v FROM records", 21)
	exec("COMMIT")
	exec("SET transaction_isolation='REPEATABLE-READ'")
	exec("START TRANSACTION WITH CONSISTENT SNAPSHOT")
	if _, e = b.ExecContext(ctx, "UPDATE records SET v=22"); e != nil {
		t.Fatal(e)
	}
	read("SELECT v FROM records", 21)
	exec("COMMIT")
}
