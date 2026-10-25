package postgresserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"slopdb/storage"
)

type rawClient struct {
	conn     net.Conn
	frontend *pgproto3.Frontend
}

func connectRaw(t *testing.T, s *testServer) *rawClient {
	t.Helper()
	conn, err := net.Dial(s.network, s.address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	c := &rawClient{conn, pgproto3.NewFrontend(conn, conn)}
	c.send(t, &pgproto3.StartupMessage{ProtocolVersion: protocolVersion, Parameters: map[string]string{"user": "slopdb", "database": "slopdb"}})
	for {
		message, err := c.frontend.Receive()
		if err != nil {
			t.Fatal(err)
		}
		switch m := message.(type) {
		case *pgproto3.ReadyForQuery:
			if m.TxStatus != 'I' {
				t.Fatal(m)
			}
			return c
		case *pgproto3.ErrorResponse:
			t.Fatal(m)
		}
	}
}
func (c *rawClient) send(t *testing.T, messages ...pgproto3.FrontendMessage) {
	t.Helper()
	for _, message := range messages {
		c.frontend.Send(message)
	}
	if err := c.frontend.Flush(); err != nil {
		t.Fatal(err)
	}
}
func receive[T pgproto3.BackendMessage](t *testing.T, c *rawClient) T {
	t.Helper()
	message, err := c.frontend.Receive()
	if err != nil {
		t.Fatal(err)
	}
	value, ok := message.(T)
	if !ok {
		var expected T
		t.Fatalf("got %T %+v, want %T", message, message, expected)
	}
	return value
}
func (c *rawClient) query(t *testing.T, q string) []pgproto3.BackendMessage {
	t.Helper()
	c.send(t, &pgproto3.Query{String: q})
	var messages []pgproto3.BackendMessage
	for {
		message, err := c.frontend.Receive()
		if err != nil {
			t.Fatal(err)
		}
		switch m := message.(type) {
		case *pgproto3.ErrorResponse:
			t.Fatalf("%s: %s %s", q, m.Code, m.Message)
		case *pgproto3.ReadyForQuery:
			return messages
		default:
			messages = append(messages, message)
		}
	}
}
func TestPortalsAndPreparedLifetime(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	c := connectRaw(t, s)
	c.query(t, "CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES(1),(2),(3)")
	c.query(t, "BEGIN")
	c.send(t, &pgproto3.Parse{Name: "read", Query: "SELECT id FROM t ORDER BY id"}, &pgproto3.Describe{ObjectType: 'S', Name: "read"})
	receive[*pgproto3.ParseComplete](t, c)
	receive[*pgproto3.ParameterDescription](t, c)
	fields := receive[*pgproto3.RowDescription](t, c)
	if len(fields.Fields) != 1 || fields.Fields[0].DataTypeOID != oidInt4 {
		t.Fatal(fields)
	}
	c.send(t, &pgproto3.Bind{DestinationPortal: "cursor", PreparedStatement: "read", ResultFormatCodes: []int16{1}}, &pgproto3.Describe{ObjectType: 'P', Name: "cursor"}, &pgproto3.Execute{Portal: "cursor", MaxRows: 2}, &pgproto3.Flush{})
	receive[*pgproto3.BindComplete](t, c)
	if desc := receive[*pgproto3.RowDescription](t, c); desc.Fields[0].Format != 1 {
		t.Fatal(desc)
	}
	for _, expected := range []uint64{1, 2} {
		row := receive[*pgproto3.DataRow](t, c)
		if len(row.Values) != 1 || len(row.Values[0]) != 4 || uint64(binary.BigEndian.Uint32(row.Values[0])) != expected {
			t.Fatalf("binary row %+v", row)
		}
	}
	receive[*pgproto3.PortalSuspended](t, c)
	c.send(t, &pgproto3.Execute{Portal: "cursor", MaxRows: 2}, &pgproto3.Sync{})
	row := receive[*pgproto3.DataRow](t, c)
	if binary.BigEndian.Uint32(row.Values[0]) != 3 {
		t.Fatal(row)
	}
	if tag := receive[*pgproto3.CommandComplete](t, c); string(tag.CommandTag) != "SELECT 3" {
		t.Fatal(tag)
	}
	if ready := receive[*pgproto3.ReadyForQuery](t, c); ready.TxStatus != 'T' {
		t.Fatal(ready)
	}
	// Execute the same completed write portal twice: INSERT must run just once.
	c.send(t, &pgproto3.Parse{Name: "insert", Query: "INSERT INTO t VALUES($1)", ParameterOIDs: []uint32{oidInt8}}, &pgproto3.Bind{DestinationPortal: "writer", PreparedStatement: "insert", ParameterFormatCodes: []int16{1}, Parameters: [][]byte{binary.BigEndian.AppendUint64(nil, 4)}}, &pgproto3.Execute{Portal: "writer"}, &pgproto3.Execute{Portal: "writer"}, &pgproto3.Sync{})
	receive[*pgproto3.ParseComplete](t, c)
	receive[*pgproto3.BindComplete](t, c)
	receive[*pgproto3.CommandComplete](t, c)
	receive[*pgproto3.CommandComplete](t, c)
	receive[*pgproto3.ReadyForQuery](t, c)
	// Closing a statement leaves already-bound portals usable.
	c.send(t, &pgproto3.Bind{DestinationPortal: "retained", PreparedStatement: "read"}, &pgproto3.Close{ObjectType: 'S', Name: "read"}, &pgproto3.Execute{Portal: "retained"}, &pgproto3.Sync{})
	receive[*pgproto3.BindComplete](t, c)
	receive[*pgproto3.CloseComplete](t, c)
	for i := 0; i < 4; i++ {
		receive[*pgproto3.DataRow](t, c)
	}
	receive[*pgproto3.CommandComplete](t, c)
	receive[*pgproto3.ReadyForQuery](t, c)
	c.query(t, "COMMIT")
	result, err := s.database.Exec("SELECT COUNT(*) AS n FROM t")
	if err != nil || result.Rows[0]["n"].Int != 4 {
		t.Fatalf("portal writes %v %v", result, err)
	}
}
func TestExtendedSyncRollbackAndRecovery(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	c := connectRaw(t, s)
	c.query(t, "CREATE TABLE t(id INTEGER PRIMARY KEY)")
	c.send(t, &pgproto3.Parse{Name: "insert", Query: "INSERT INTO t VALUES($1)"}, &pgproto3.Bind{PreparedStatement: "insert", Parameters: [][]byte{[]byte("1")}}, &pgproto3.Execute{}, &pgproto3.Bind{PreparedStatement: "insert", Parameters: [][]byte{[]byte("1")}}, &pgproto3.Execute{}, &pgproto3.Execute{Portal: "ignored"}, &pgproto3.Sync{})
	receive[*pgproto3.ParseComplete](t, c)
	receive[*pgproto3.BindComplete](t, c)
	receive[*pgproto3.CommandComplete](t, c)
	receive[*pgproto3.BindComplete](t, c)
	if e := receive[*pgproto3.ErrorResponse](t, c); e.Code != "23505" {
		t.Fatal(e)
	}
	if ready := receive[*pgproto3.ReadyForQuery](t, c); ready.TxStatus != 'I' {
		t.Fatal(ready)
	}
	result, err := s.database.Exec("SELECT COUNT(*) AS n FROM t")
	if err != nil || result.Rows[0]["n"].Int != 0 {
		t.Fatalf("implicit cycle did not roll back: %v %v", result, err)
	}
	c.send(t, &pgproto3.Parse{Query: "SELECT ("}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if e := receive[*pgproto3.ErrorResponse](t, c); e.Code != "42601" {
		t.Fatal(e)
	}
	receive[*pgproto3.ReadyForQuery](t, c)
	c.query(t, "SELECT 1")
	// A malformed negative parameter length is rejected, not sliced or allocated.
	body := []byte{0, 0, 0, 0, 0, 1, 255, 255, 255, 254, 0, 0}
	packet := append([]byte{'B'}, binary.BigEndian.AppendUint32(nil, uint32(len(body)+4))...)
	packet = append(packet, body...)
	if err := writeAll(c.conn, packet); err != nil {
		t.Fatal(err)
	}
	c.send(t, &pgproto3.Sync{})
	if e := receive[*pgproto3.ErrorResponse](t, c); e.Code != "08P01" {
		t.Fatal(e)
	}
	receive[*pgproto3.ReadyForQuery](t, c)
	c.query(t, "SELECT 2")
}
func TestCancellationAndReusableConnection(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	client := s.open(t, "")
	mustExec(t, client, "CREATE TABLE t(id INTEGER PRIMARY KEY,n INTEGER)")
	mustExec(t, client, "INSERT INTO t VALUES(1,0)")
	tx, err := client.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	mustExec(t, tx, "UPDATE t SET n=1 WHERE id=1")
	conn, err := pgx.Connect(context.Background(), s.dsn("slopdb", "", "slopdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := conn.Exec(context.Background(), "UPDATE t SET n=2 WHERE id=$1", int64(1))
		done <- err
	}()
	waitForLock(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := conn.PgConn().CancelRequest(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "57014" {
			t.Fatalf("cancel error %v", err)
		}
	case <-ctx.Done():
		t.Fatal("cancel did not stop SQL")
	}
	var value int64
	if err := conn.QueryRow(context.Background(), "SELECT 7").Scan(&value); err != nil || value != 7 {
		t.Fatalf("connection unusable after cancel: %d %v", value, err)
	}
}
func waitForLock(t *testing.T, s *testServer) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for len(s.database.LockWaits()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("SQL did not register a lock wait")
		}
		time.Sleep(time.Millisecond)
	}
}
func TestDisconnectAndShutdownRollback(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	client := s.open(t, "")
	mustExec(t, client, "CREATE TABLE t(id INTEGER PRIMARY KEY,n INTEGER)")
	mustExec(t, client, "INSERT INTO t VALUES(1,0),(2,0)")
	tx, err := client.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	mustExec(t, tx, "UPDATE t SET n=1 WHERE id=1")
	raw := connectRaw(t, s)
	raw.query(t, "BEGIN")
	raw.query(t, "UPDATE t SET n=2 WHERE id=2")
	raw.send(t, &pgproto3.Query{String: "UPDATE t SET n=2 WHERE id=1"})
	waitForLock(t, s)
	if err := raw.conn.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := client.ExecContext(ctx, "UPDATE t SET n=10 WHERE id=2"); err != nil {
		t.Fatal(err)
	}
	idle, err := net.Dial("tcp", s.address)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	closed := make(chan struct{})
	go func() { _ = s.server.Close(); close(closed) }()
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("shutdown blocked")
	}
	_ = tx.Rollback()
	result, err := s.database.Exec("SELECT n FROM t ORDER BY id")
	if err != nil || len(result.Rows) != 2 || result.Rows[0]["n"].Int != 0 || result.Rows[1]["n"].Int != 10 {
		t.Fatalf("rollback %v %v", result, err)
	}
}
func TestFramingAndLimits(t *testing.T) {
	for _, data := range [][]byte{{0, 0, 0, 3}, {255, 255, 255, 255}, {0, 0, 0, 8, 0}} {
		if _, err := readBody(bytes.NewReader(data), 1024); err == nil {
			t.Fatal("invalid frame accepted")
		}
	}
	s := startTestServer(t, "tcp", Config{MaxPreparedStatements: 1, MaxPortals: 1})
	c := connectRaw(t, s)
	c.send(t, &pgproto3.Parse{Name: "one", Query: "SELECT 1"}, &pgproto3.Parse{Name: "two", Query: "SELECT 2"}, &pgproto3.Sync{})
	receive[*pgproto3.ParseComplete](t, c)
	if e := receive[*pgproto3.ErrorResponse](t, c); e.Code != "54000" {
		t.Fatal(e)
	}
	receive[*pgproto3.ReadyForQuery](t, c)
	c.send(t, &pgproto3.Bind{DestinationPortal: "a", PreparedStatement: "one"}, &pgproto3.Bind{DestinationPortal: "b", PreparedStatement: "one"}, &pgproto3.Sync{})
	receive[*pgproto3.BindComplete](t, c)
	if e := receive[*pgproto3.ErrorResponse](t, c); e.Code != "54000" {
		t.Fatal(e)
	}
	receive[*pgproto3.ReadyForQuery](t, c)
}
func TestSSLHandshakeTimeoutAndClose(t *testing.T) {
	f := newTLSFixture(t)
	for _, shutdown := range []bool{false, true} {
		s := startTestServer(t, "tcp", Config{TLSConfig: f.serverConfig(false), HandshakeTimeout: 100 * time.Millisecond})
		conn, err := net.Dial("tcp", s.address)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		request := binary.BigEndian.AppendUint32(nil, 8)
		request = binary.BigEndian.AppendUint32(request, sslRequest)
		if err := writeAll(conn, request); err != nil {
			t.Fatal(err)
		}
		var response [1]byte
		if _, err := io.ReadFull(conn, response[:]); err != nil || response[0] != 'S' {
			t.Fatalf("SSL negotiation %q %v", response, err)
		}
		if shutdown {
			if err := s.server.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := conn.Read(response[:]); !errors.Is(err, io.EOF) {
			t.Fatalf("stalled TLS not closed: %v", err)
		}
	}
}
func FuzzDecodeMessage(f *testing.F) {
	for _, data := range [][]byte{{'Q', 0}, {'B', 0, 0, 0, 0, 0, 1, 255, 255, 255, 254, 0, 0}, {'P', 0, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1', 0, 0, 0}, {'D', 'S', 0}, {'E', 0, 0, 0, 0, 0}, {'S'}} {
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 {
			return
		}
		_, _ = decodeMessage(data[0], data[1:])
		_, _ = readBody(bytes.NewReader(data), 4096)
	})
}

func TestFailedTransactionBlocksSuspendedPortal(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	c := connectRaw(t, s)
	c.query(t, "CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES(1),(2)")
	c.query(t, "BEGIN")
	c.send(t, &pgproto3.Parse{Name: "read", Query: "SELECT id FROM t ORDER BY id"}, &pgproto3.Bind{DestinationPortal: "cursor", PreparedStatement: "read"}, &pgproto3.Execute{Portal: "cursor", MaxRows: 1}, &pgproto3.Sync{})
	receive[*pgproto3.ParseComplete](t, c)
	receive[*pgproto3.BindComplete](t, c)
	receive[*pgproto3.DataRow](t, c)
	receive[*pgproto3.PortalSuspended](t, c)
	receive[*pgproto3.ReadyForQuery](t, c)
	c.send(t, &pgproto3.Query{String: "SELECT 1/0"})
	receive[*pgproto3.ErrorResponse](t, c)
	if ready := receive[*pgproto3.ReadyForQuery](t, c); ready.TxStatus != 'E' {
		t.Fatal(ready)
	}
	c.send(t, &pgproto3.Execute{Portal: "cursor"}, &pgproto3.Sync{})
	if e := receive[*pgproto3.ErrorResponse](t, c); e.Code != "25P02" {
		t.Fatal(e)
	}
	receive[*pgproto3.ReadyForQuery](t, c)
	c.query(t, "ROLLBACK")
	c.send(t, &pgproto3.Execute{Portal: "cursor"}, &pgproto3.Sync{})
	if e := receive[*pgproto3.ErrorResponse](t, c); e.Code != "34000" {
		t.Fatal(e)
	}
	receive[*pgproto3.ReadyForQuery](t, c)
}

func TestDeadlockKeepsPostgresTransactionAborted(t *testing.T) {
	options := storage.DefaultOptions()
	options.DisableWALSync = true
	options.DeadlockDetection = true
	s := startTestServerOptions(t, "tcp", Config{}, options)
	setup := s.open(t, "")
	mustExec(t, setup, "CREATE TABLE t(id INTEGER PRIMARY KEY,n INTEGER)")
	mustExec(t, setup, "INSERT INTO t VALUES(1,0),(2,0)")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := pgx.Connect(ctx, s.dsn("slopdb", "", "slopdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close(context.Background())
	second, err := pgx.Connect(ctx, s.dsn("slopdb", "", "slopdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close(context.Background())
	for _, conn := range []*pgx.Conn{first, second} {
		if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := first.Exec(ctx, "UPDATE t SET n=1 WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Exec(ctx, "UPDATE t SET n=2 WHERE id=2"); err != nil {
		t.Fatal(err)
	}
	waiting := make(chan error, 1)
	go func() { _, err := first.Exec(ctx, "UPDATE t SET n=1 WHERE id=2"); waiting <- err }()
	waitForLock(t, s)
	_, err = second.Exec(ctx, "UPDATE t SET n=2 WHERE id=1")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "40P01" {
		t.Fatalf("deadlock error %v", err)
	}
	if second.PgConn().TxStatus() != 'E' {
		t.Fatalf("deadlock victim status %c", second.PgConn().TxStatus())
	}
	_, err = second.Exec(ctx, "UPDATE t SET n=99 WHERE id=2")
	pgErr = nil
	if !errors.As(err, &pgErr) || pgErr.Code != "25P02" {
		t.Fatalf("work after automatic rollback was accepted: %v", err)
	}
	select {
	case err := <-waiting:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("deadlock survivor did not resume")
	}
	if _, err := first.Exec(ctx, "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Exec(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("reset aborted transaction: %v", err)
	}
	if second.PgConn().TxStatus() != 'I' {
		t.Fatal("rollback did not reset victim")
	}
	var sum int64
	if err := setup.QueryRow("SELECT SUM(n) FROM t").Scan(&sum); err != nil || sum != 0 {
		t.Fatalf("aborted transaction leaked writes: %d %v", sum, err)
	}
	if _, err := second.Exec(ctx, "UPDATE t SET n=7 WHERE id=2"); err != nil {
		t.Fatalf("victim not reusable: %v", err)
	}
}

func TestFloat4BindingFormatsAgree(t *testing.T) {
	const value = 1.23456789012345
	text, err := decodeValue([]byte("1.23456789012345"), oidFloat4, false)
	if err != nil {
		t.Fatal(err)
	}
	packet := binary.BigEndian.AppendUint32(nil, math.Float32bits(float32(value)))
	binaryValue, err := decodeValue(packet, oidFloat4, true)
	if err != nil {
		t.Fatal(err)
	}
	if text != binaryValue {
		t.Fatalf("text/binary float4 differ: %.17g versus %.17g", text, binaryValue)
	}
}

func TestSyntaxErrorPositions(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	c := connectRaw(t, s)
	for _, extended := range []bool{false, true} {
		if extended {
			c.send(t, &pgproto3.Parse{Query: "SELECT '🙂' +"}, &pgproto3.Sync{})
		} else {
			c.send(t, &pgproto3.Query{String: "SELECT '🙂' +"})
		}
		e := receive[*pgproto3.ErrorResponse](t, c)
		if e.Code != "42601" || e.Position != 13 {
			t.Fatalf("Unicode error position: %+v", e)
		}
		receive[*pgproto3.ReadyForQuery](t, c)
	}
}
