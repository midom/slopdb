package mysqlserver

import (
	"context"
	gosql "database/sql"
	"testing"
)

func TestMySQLUserVariablesPreparedAndScalarSubqueries(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	ctx := context.Background()
	c, e := pool.Conn(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	run := func(q string) {
		t.Helper()
		if _, e := c.ExecContext(ctx, q); e != nil {
			t.Fatalf("%s: %v", q, e)
		}
	}
	run("CREATE TABLE var_source(id INTEGER PRIMARY KEY,n INTEGER)")
	run("INSERT INTO var_source VALUES(1,10),(2,20)")
	run("SET @a=(SELECT max(n) FROM var_source), @b=@a+2")
	var a, b int64
	if e = c.QueryRowContext(ctx, "SELECT @a,@b").Scan(&a, &b); e != nil || a != 20 || b != 22 {
		t.Fatal(a, b, e)
	}
	p, e := c.PrepareContext(ctx, "SET @a=?")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = p.ExecContext(ctx, 42); e != nil {
		t.Fatal(e)
	}
	p.Close()
	get, e := c.PrepareContext(ctx, "SELECT @a")
	if e != nil {
		t.Fatal(e)
	}
	defer get.Close()
	if e = get.QueryRowContext(ctx).Scan(&a); e != nil || a != 42 {
		t.Fatal(a, e)
	}
	if _, e = c.ExecContext(ctx, "SET @a=(SELECT n FROM var_source)"); e == nil {
		t.Fatal("scalar cardinality accepted")
	}
	if e = get.QueryRowContext(ctx).Scan(&a); e != nil || a != 42 {
		t.Fatal(a, e)
	}
	var format string
	if e = c.QueryRowContext(ctx, "SELECT @a,@@binlog_format").Scan(&a, &format); e != nil || a != 42 || format != "ROW" {
		t.Fatal(a, format, e)
	}
	run("SET @readonly=(SELECT @@read_only)")
	if e = c.QueryRowContext(ctx, "SELECT @readonly").Scan(&a); e != nil || a != 0 {
		t.Fatal(a, e)
	}
	sys, e := c.PrepareContext(ctx, "SELECT @@global.binlog_format,@@session.binlog_row_image")
	if e != nil {
		t.Fatal(e)
	}
	var image string
	if e = sys.QueryRowContext(ctx).Scan(&format, &image); e != nil || format != "ROW" || image != "FULL" {
		t.Fatal(format, image, e)
	}
	sys.Close()
	// Another live connection must not see the first connection's variables.
	other, e := pool.Conn(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer other.Close()
	var absent gosql.NullInt64
	if e = other.QueryRowContext(ctx, "SELECT @a").Scan(&absent); e != nil || absent.Valid {
		t.Fatal(absent, e)
	}
	run("SET @a=0")
	rows, e := c.QueryContext(ctx, "SELECT @a:=@a+1 AS n FROM var_source ORDER BY id")
	if e != nil {
		t.Fatal(e)
	}
	defer rows.Close()
	for want := int64(1); rows.Next(); want++ {
		if e = rows.Scan(&a); e != nil || a != want {
			t.Fatal(a, want, e)
		}
	}
	if e = rows.Err(); e != nil {
		t.Fatal(e)
	}
}
func TestMySQLResetClearsUserVariables(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	c := connectRaw(t, s)
	p := append([]byte{3}, []byte("SET @value=7")...)
	result := c.command(t, p)
	if result[0] != 0 {
		t.Fatal(result)
	}
	result = c.command(t, []byte{31})
	if result[0] != 0 {
		t.Fatal(result)
	}
	// Inspect the same reset connection's SQL NULL row marker.
	first := c.command(t, append([]byte{3}, []byte("SELECT @value")...))
	if len(first) != 1 || first[0] != 1 {
		t.Fatal(first)
	}
	sequence := byte(2)
	for i := 0; i < 4; i++ {
		packet, next, e := readPacket(c.conn, sequence, 1<<20)
		if e != nil {
			t.Fatal(e)
		}
		sequence = next
		if i == 2 && (len(packet) != 1 || packet[0] != 0xfb) {
			t.Fatalf("reset retained variable: %x", packet)
		}
	}
}
