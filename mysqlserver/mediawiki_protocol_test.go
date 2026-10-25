package mysqlserver

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestMySQLNamedLocksAcrossConnections(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	pool.SetMaxIdleConns(0)
	ctx := context.Background()
	owner, err := pool.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	other, err := pool.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	scalar := func(c *sql.Conn, q string, want int) {
		t.Helper()
		var got int
		if err := c.QueryRowContext(ctx, q).Scan(&got); err != nil || got != want {
			t.Fatalf("%s = %d want%d: %v", q, got, want, err)
		}
	}
	scalar(owner, "SELECT GET_LOCK('wiki',0)", 1)
	scalar(owner, "SELECT GET_LOCK('wiki',0)", 1)
	scalar(other, "SELECT GET_LOCK('wiki',0)", 0)
	scalar(owner, "SELECT RELEASE_LOCK('wiki')", 1)
	scalar(other, "SELECT IS_FREE_LOCK('wiki')", 0)
	scalar(other, "SELECT GET_LOCK('wiki',0.01)", 0)
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	// A bounded real wait also covers asynchronous disconnect cleanup.
	scalar(other, "SELECT GET_LOCK('wiki',2)", 1)
	scalar(other, "SELECT RELEASE_LOCK('wiki')", 1)
	scalar(other, "SELECT IS_FREE_LOCK('wiki')", 1)
}

func TestMySQLNamedLockWaitCancellation(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	owner, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	var got int
	if err = owner.QueryRowContext(context.Background(), "SELECT GET_LOCK('cancel',0)").Scan(&got); err != nil || got != 1 {
		t.Fatal(got, err)
	}
	deadline, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err = pool.QueryRowContext(deadline, "SELECT GET_LOCK('cancel',30)").Scan(&got); err == nil {
		t.Fatal("lock wait ignored cancellation")
	}
	// Server shutdown cancels the disconnected wait and must finish promptly.
	done := make(chan error, 1)
	go func() { done <- s.server.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lock waiter blocked shutdown")
	}
}

func TestMySQLStorageEngineIdentity(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	rows, err := pool.Query("SHOW ENGINES")
	if err != nil {
		t.Fatal(err)
	}
	var engine, support, comment, transactions, xa, savepoints string
	if !rows.Next() {
		t.Fatal("missing LSM engine")
	}
	if err = rows.Scan(&engine, &support, &comment, &transactions, &xa, &savepoints); err != nil {
		t.Fatal(err)
	}
	if engine != "LSM" || support != "DEFAULT" || transactions != "YES" || savepoints != "YES" {
		t.Fatal(engine, support, transactions, savepoints)
	}
	if rows.Next() {
		t.Fatal("advertised an unimplemented second engine")
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	// ENGINE clauses are accepted for portable schemas; all rows use native LSM.
	for _, name := range []string{"InnoDB", "MyISAM", "other_engine"} {
		mustExec(t, pool, fmt.Sprintf("CREATE TABLE engine_%s(id INT PRIMARY KEY) ENGINE=%s", name, name))
		var got string
		if err = pool.QueryRow("SELECT engine FROM information_schema.tables WHERE table_name=?", "engine_"+strings.ToLower(name)).Scan(&got); err != nil || got != "LSM" {
			t.Fatal(name, got, err)
		}
	}
}

func TestMySQLPreparedColumnMetadataAndBinaryEncoding(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	mustExec(t, pool, "CREATE TABLE typed(a TINYINT,b SMALLINT UNSIGNED,c INT,d BIGINT,e FLOAT,f DOUBLE,g DATE,h DATETIME,i TIMESTAMP,j BINARY(3),k ENUM('a','b'))")
	mustExec(t, pool, "INSERT INTO typed VALUES(-128,65535,-2147483648,9223372036854775807,1.25,2.5,'2026-10-25','2026-10-25 01:02:03','2026-10-25 04:05:06',X'00ff','b')")
	stmt, err := pool.Prepare("SELECT * FROM typed")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"TINYINT", "UNSIGNED SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "DATE", "DATETIME", "TIMESTAMP", "BINARY", "ENUM"}
	got := make([]string, len(columns))
	for i, c := range columns {
		got[i] = c.DatabaseTypeName()
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("column types %v want %v", got, want)
	}
	if !rows.Next() {
		t.Fatal("missing typed row", rows.Err())
	}
	var a int8
	var b uint16
	var c int32
	var d int64
	var e float32
	var f float64
	var g, h, i string
	var j []byte
	var k string
	if err = rows.Scan(&a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k); err != nil {
		t.Fatal(err)
	}
	if a != -128 || b != 65535 || c != -2147483648 || d != 9223372036854775807 || e != 1.25 || f != 2.5 || g != "2026-10-25" || h != "2026-10-25 01:02:03" || i != "2026-10-25 04:05:06" || !reflect.DeepEqual(j, []byte{0, 255, 0}) || k != "b" {
		t.Fatal(a, b, c, d, e, f, g, h, i, j, k)
	}
	if rows.Next() || rows.Err() != nil {
		t.Fatal("unexpected rows", rows.Err())
	}
}
