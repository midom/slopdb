package mysqlserver

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"
)

func TestMySQLStreamsTypedBaseColumnsAndPreservesFallback(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	ctx := context.Background()
	c, e := pool.Conn(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	for _, q := range []string{"CREATE TABLE streamed(id BIGINT PRIMARY KEY,u BIGINT UNSIGNED,d DECIMAL(30,5),v TEXT,b BLOB)", "INSERT INTO streamed VALUES(1,18446744073709551615,12345678901234567890.12345,'🪐',X'00FF'),(2,2,2.00000,'text',X'')"} {
		if _, e = c.ExecContext(ctx, q); e != nil {
			t.Fatal(e)
		}
	}
	rows, e := c.QueryContext(ctx, "SELECT id,u,d,v,b,v AS v FROM streamed ORDER BY id")
	if e != nil {
		t.Fatal(e)
	}
	labels, e := rows.Columns()
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(labels, []string{"id", "u", "d", "v", "b", "v"}) {
		t.Fatal(labels)
	}
	if !rows.Next() {
		t.Fatal(rows.Err())
	}
	var id int64
	var u uint64
	var decimal, text, duplicate string
	var blob []byte
	if e = rows.Scan(&id, &u, &decimal, &text, &blob, &duplicate); e != nil {
		t.Fatal(e)
	}
	if id != 1 || u != ^uint64(0) || decimal != "12345678901234567890.12345" || text != "🪐" || duplicate != text || !bytes.Equal(blob, []byte{0, 255}) {
		t.Fatalf("%d %d %s %s %x", id, u, decimal, text, blob)
	}
	rows.Close()
	rows, e = c.QueryContext(ctx, "SELECT v FROM streamed WHERE id=99")
	if e != nil {
		t.Fatal(e)
	}
	types, e := rows.ColumnTypes()
	if e != nil {
		t.Fatal(e)
	}
	if len(types) != 1 || types[0].DatabaseTypeName() != "VARCHAR" {
		t.Fatalf("empty text metadata %v", types)
	}
	rows.Close()
	if _, e = c.ExecContext(ctx, "CREATE VIEW mixed_stream AS SELECT CASE WHEN id=1 THEN 9223372036854775807 ELSE '🪐' END AS v FROM streamed"); e != nil {
		t.Fatal(e)
	}
	rows, e = c.QueryContext(ctx, "SELECT v FROM mixed_stream")
	if e != nil {
		t.Fatal(e)
	}
	var values []string
	for rows.Next() {
		var v string
		if e = rows.Scan(&v); e != nil {
			t.Fatal(e)
		}
		values = append(values, v)
	}
	if e = rows.Err(); e != nil {
		t.Fatal(e)
	}
	rows.Close()
	if !reflect.DeepEqual(values, []string{"9223372036854775807", "🪐"}) {
		t.Fatal(values)
	}
	rows, e = c.QueryContext(ctx, "SELECT id FROM streamed WHERE GET_LOCK('stream_once',0)")
	if e != nil {
		t.Fatal(e)
	}
	for rows.Next() {
	}
	if e = rows.Err(); e != nil {
		t.Fatal(e)
	}
	rows.Close()
	for i := 0; i < 2; i++ {
		var released int
		if e = c.QueryRowContext(ctx, "SELECT RELEASE_LOCK('stream_once')").Scan(&released); e != nil || released != 1 {
			t.Fatal(released, e)
		}
	}
	var missing gosql.NullInt64
	if e = c.QueryRowContext(ctx, "SELECT RELEASE_LOCK('stream_once')").Scan(&missing); e != nil || missing.Valid {
		t.Fatalf("materialized query executed twice: %+v %v", missing, e)
	}
}

type blockedWriteConn struct {
	net.Conn
	entered chan struct{}
}

func (c *blockedWriteConn) Write(p []byte) (int, error) {
	select {
	case <-c.entered:
	default:
		close(c.entered)
	}
	return c.Conn.Write(p)
}
func TestMySQLStreamDatabaseCloseInterruptsBlockedOutput(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprint(batch), func(t *testing.T) { testMySQLStreamClose(t, batch) })
	}
}

func testMySQLStreamClose(t *testing.T, batch bool) {
	s := startTestServer(t, "tcp", Config{})
	if _, e := s.database.Exec("CREATE TABLE blocked_stream(id INTEGER PRIMARY KEY,v TEXT);INSERT INTO blocked_stream VALUES(1,'value')"); e != nil {
		t.Fatal(e)
	}
	session, e := s.database.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer session.Close()
	if e = session.SetMySQLMode(true); e != nil {
		t.Fatal(e)
	}
	serverConn, reader := net.Pipe()
	defer reader.Close()
	defer serverConn.Close()
	conn := &blockedWriteConn{Conn: serverConn, entered: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &client{server: s.server, conn: conn, writer: packetWriter{conn: conn}, session: session, ctx: ctx, cancel: cancel, autocommit: true, variables: defaultSessionVariables()}
	if batch {
		c.writer.beginBatch()
	}
	done := make(chan error, 1)
	go func() { _, e := c.streamQuery("SELECT v FROM blocked_stream"); done <- e }()
	select {
	case <-conn.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("stream did not write")
	}
	closed := make(chan error, 1)
	go func() { closed <- s.database.Close() }()
	select {
	case e := <-closed:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("database close retained streaming reader")
	}
	select {
	case e := <-done:
		if e == nil {
			t.Fatal("blocked output unexpectedly succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("output cancellation did not interrupt write")
	}
}
