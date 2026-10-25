package mysqlserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net"
	"strconv"
	"testing"
	"time"
)

type rawClient struct {
	conn   net.Conn
	writer packetWriter
}

func connectRaw(t *testing.T, s *testServer) *rawClient {
	return connectRawWithFlags(t, s, 0)
}

func connectRawWithFlags(t *testing.T, s *testServer, extraFlags uint32) *rawClient {
	t.Helper()
	conn, err := net.Dial(s.network, s.address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readPacket(conn, 0, 1<<20); err != nil {
		t.Fatal(err)
	}
	c := &rawClient{conn: conn, writer: packetWriter{conn: conn, sequence: 1}}
	flags := clientProtocol41 | clientSecureConnection | clientPluginAuth | extraFlags
	p := append32(nil, flags)
	p = append32(p, 1<<20)
	p = append(p, 45)
	p = append(p, make([]byte, 23)...)
	p = append(p, []byte("slopdb\x00")...)
	p = append(p, 0)
	p = append(p, authPlugin...)
	p = append(p, 0)
	if err := c.writer.write(p); err != nil {
		t.Fatal(err)
	}
	result, _, err := readPacket(conn, 2, 1<<20)
	if err != nil || len(result) == 0 || result[0] != 0 {
		t.Fatalf("login %x %v", result, err)
	}
	return c
}
func (c *rawClient) send(t *testing.T, p []byte) {
	t.Helper()
	c.writer.sequence = 0
	if err := c.writer.write(p); err != nil {
		t.Fatal(err)
	}
}
func (c *rawClient) command(t *testing.T, p []byte) []byte {
	t.Helper()
	c.send(t, p)
	result, _, err := readPacket(c.conn, 1, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func expectOK(t *testing.T, p []byte, status uint16) {
	t.Helper()
	if len(p) == 0 || p[0] != 0 {
		t.Fatalf("expected OK: %x", p)
	}
	d := decoder{data: p[1:]}
	d.length()
	d.length()
	got := d.u16()
	if d.err != nil || got != status {
		t.Fatalf("status %x, want %x: %v", got, status, d.err)
	}
}
func expectCode(t *testing.T, p []byte, code uint16) {
	t.Helper()
	if len(p) < 9 || p[0] != 0xff || binary.LittleEndian.Uint16(p[1:]) != code {
		t.Fatalf("expected error %d: %q", code, p)
	}
}
func TestProtocolTransactionStatusAndReset(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	c := connectRaw(t, s)
	for _, tc := range []struct {
		q      string
		status uint16
	}{
		{"CREATE TABLE t(id INTEGER PRIMARY KEY)", 2}, {"BEGIN", 3}, {"INSERT INTO t VALUES(1)", 3}, {"SAVEPOINT a", 3}, {"ROLLBACK TO a", 3}, {"ROLLBACK", 2},
		{"SAVEPOINT a", 3}, {"RELEASE a", 2}, {"SET autocommit=0", 0}, {"INSERT INTO t VALUES(2)", 1}, {"COMMIT", 0}, {"INSERT INTO t VALUES(3)", 1},
	} {
		expectOK(t, c.command(t, append([]byte{3}, tc.q...)), tc.status)
	}
	expectOK(t, c.command(t, []byte{31}), 2)
	result, err := s.database.Exec(`SELECT id FROM t ORDER BY id`)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["id"].Int != 2 {
		t.Fatalf("reset rollback %v %v", result, err)
	}
	expectOK(t, c.command(t, []byte{14}), 2)
	expectCode(t, c.command(t, []byte{4}), 1047)
	expectCode(t, c.command(t, []byte{23, 1}), 1105)
	expectCode(t, c.command(t, append([]byte{2}, "missing"...)), 1049)
	expectOK(t, c.command(t, append([]byte{2}, "slopdb"...)), 2)
}
func rawPrepare(t *testing.T, c *rawClient, q string) uint32 {
	t.Helper()
	p := c.command(t, append([]byte{22}, q...))
	if len(p) != 12 || p[0] != 0 {
		t.Fatalf("prepare %q: %x", q, p)
	}
	n := binary.LittleEndian.Uint16(p[7:])
	seq := byte(2)
	for i := 0; i < int(n); i++ {
		_, next, err := readPacket(c.conn, seq, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		seq = next
	}
	if n > 0 {
		eof, next, err := readPacket(c.conn, seq, 1<<20)
		seq = next
		if err != nil || eof[0] != 0xfe {
			t.Fatalf("prepare EOF %x %v", eof, err)
		}
	}
	columns := binary.LittleEndian.Uint16(p[5:])
	for i := 0; i < int(columns); i++ {
		_, next, err := readPacket(c.conn, seq, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		seq = next
	}
	if columns > 0 {
		eof, _, err := readPacket(c.conn, seq, 1<<20)
		if err != nil || len(eof) == 0 || eof[0] != 0xfe {
			t.Fatalf("prepare columns EOF %x %v", eof, err)
		}
	}
	return binary.LittleEndian.Uint32(p[1:])
}
func TestProtocolPreparedBindingsLongDataAndErrors(t *testing.T) {
	s := startTestServer(t, "tcp", Config{MaxPacketBytes: 1024, MaxPreparedStatements: 2})
	c := connectRaw(t, s)
	expectOK(t, c.command(t, append([]byte{3}, "CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB, label TEXT)"...)), 2)
	id := rawPrepare(t, c, "INSERT INTO t VALUES(?,?,?)")
	// Three long-data chunks, including an empty text value.
	for _, chunk := range []struct {
		param uint16
		value []byte
	}{{1, []byte{0, 255}}, {1, []byte{1, 2}}, {2, []byte{}}} {
		p := append32([]byte{24}, id)
		p = append16(p, chunk.param)
		p = append(p, chunk.value...)
		c.send(t, p)
	}
	p := append32([]byte{23}, id)
	p = append(p, 0)
	p = append32(p, 1)
	p = append(p, 0, 1)
	p = append16(p, 8)
	p = append16(p, 252)
	p = append16(p, 253)
	p = append64(p, 1)
	expectOK(t, c.command(t, p), 2)
	r, err := s.database.Exec("SELECT b,label FROM t WHERE id=1")
	if err != nil || len(r.Rows) != 1 || !bytes.Equal(r.Rows[0]["b"].Blob, []byte{0, 255, 1, 2}) || r.Rows[0]["label"].Null || r.Rows[0]["label"].Text != "" {
		t.Fatalf("long data %v %v", r, err)
	}
	// Reuse cached parameter types, now using normal values and NULL bitmap.
	p = append32([]byte{23}, id)
	p = append(p, 0)
	p = append32(p, 1)
	p = append(p, 4, 0)
	p = append64(p, 2)
	p = appendString(p, []byte{})
	expectOK(t, c.command(t, p), 2)
	// Unknown IDs and malformed executions return errors without dropping client.
	unknown := append32([]byte{23}, 999)
	unknown = append(unknown, 0, 1, 0, 0, 0)
	expectCode(t, c.command(t, unknown), 1243)
	for _, bad := range [][]byte{nil, {0}, {0, 1}, {0, 1, 8}, {0, 0}} {
		p = append32([]byte{23}, id)
		p = append(p, 0, 1, 0, 0, 0)
		p = append(p, bad...)
		expectCode(t, c.command(t, p), 1105)
	}
	// Buffered data is bounded across packets; RESET discards deferred errors.
	for i := 0; i < 2; i++ {
		p = append32([]byte{24}, id)
		p = append16(p, 1)
		p = append(p, make([]byte, 700)...)
		c.send(t, p)
	}
	p = append32([]byte{23}, id)
	p = append(p, 0, 1, 0, 0, 0)
	expectCode(t, c.command(t, p), 1105)
	p = append32([]byte{26}, id)
	expectOK(t, c.command(t, p), 2)
	_ = rawPrepare(t, c, "SELECT 1")
	expectCode(t, c.command(t, append([]byte{22}, "SELECT 2"...)), 1461)
	c.send(t, append32([]byte{25}, id))
	expectOK(t, c.command(t, []byte{14}), 2)
	_ = rawPrepare(t, c, "SELECT 3")
}
func TestDisconnectCancelsBlockedSQL(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	client := s.open(t, "slopdb", "", "slopdb")
	mustExec(t, client, "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)")
	mustExec(t, client, "INSERT INTO t VALUES(1,0),(2,0)")
	tx, err := client.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	mustExec(t, tx, "UPDATE t SET n=1 WHERE id=1")
	c := connectRaw(t, s)
	expectOK(t, c.command(t, append([]byte{3}, "BEGIN"...)), 3)
	expectOK(t, c.command(t, append([]byte{3}, "UPDATE t SET n=2 WHERE id=2"...)), 3)
	c.send(t, append([]byte{3}, "UPDATE t SET n=2 WHERE id=1"...))
	deadline := time.Now().Add(3 * time.Second)
	for len(s.database.LockWaits()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no registered wait")
		}
		time.Sleep(time.Millisecond)
	}
	if err := c.conn.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := client.ExecContext(ctx, "UPDATE t SET n=n+10 WHERE id=2"); err != nil {
		t.Fatalf("disconnected client retained locks: %v", err)
	}
	var n int
	if err := client.QueryRow("SELECT n FROM t WHERE id=2").Scan(&n); err != nil || n != 10 {
		t.Fatalf("disconnect rollback %d %v", n, err)
	}
}
func TestPacketFramingAndLimits(t *testing.T) {
	for _, size := range []int{0, 250, 65536, maxFrame, maxFrame + 17} {
		t.Run(fmtSize(size), func(t *testing.T) {
			a, b := net.Pipe()
			defer a.Close()
			defer b.Close()
			_ = a.SetDeadline(time.Now().Add(5 * time.Second))
			_ = b.SetDeadline(time.Now().Add(5 * time.Second))
			data := bytes.Repeat([]byte{'x'}, size)
			done := make(chan error, 1)
			go func() { w := packetWriter{conn: a}; done <- w.write(data) }()
			got, next, err := readPacket(b, 0, size)
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("packet %d: %v", size, err)
			}
			expected := byte(1)
			if size >= maxFrame {
				expected = 2
			}
			if next != expected {
				t.Fatalf("sequence %d", next)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, data := range [][]byte{{1, 0, 0, 0}, {0, 0, 0, 1}, {0xff, 0xff, 0xff, 0}} {
		if _, _, err := readPacket(bytes.NewReader(data), 0, 1024); err == nil {
			t.Fatal("invalid packet accepted")
		}
	}
	var encoded []byte
	for _, value := range []uint64{0, 250, 251, 65535, 65536, 1<<24 - 1, 1 << 24, math.MaxUint64} {
		encoded = appendLen(encoded, value)
	}
	d := decoder{data: encoded}
	for _, value := range []uint64{0, 250, 251, 65535, 65536, 1<<24 - 1, 1 << 24, math.MaxUint64} {
		if got := d.length(); got != value || d.err != nil {
			t.Fatalf("length %d %v", got, d.err)
		}
	}
	// Failed reads must remain failed, including subsequent NUL strings.
	d = decoder{data: []byte{0, 0}}
	d.take(3)
	_ = d.nul()
	if !errors.Is(d.err, errMalformed) {
		t.Fatal("lost decoder error")
	}
	if err := writeAll(shortWriter{}, []byte{1}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(err)
	}
}
func fmtSize(n int) string {
	if n == 0 {
		return "empty"
	}
	if n == maxFrame {
		return "exact_frame"
	}
	if n > maxFrame {
		return "multiple_frames"
	}
	return "partial_" + strconv.Itoa(n)
}

type shortWriter struct{}

func (shortWriter) Write([]byte) (int, error) { return 0, nil }
func FuzzDecodeValue(f *testing.F) {
	for _, data := range [][]byte{{8, 0, 1, 0, 0, 0, 0, 0, 0, 0}, {253, 0, 3, 'a', 'b', 'c'}, {12, 0, 4, 0xe8, 7, 1, 1}, {11, 0, 0}, {252, 0, 0xfe, 255, 255, 255, 255, 255, 255, 255, 255}} {
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		d := decoder{data: data}
		typ := d.u16()
		_, _ = decodeValue(&d, typ)
		// These operations after any truncated field must also remain safe.
		_ = d.nul()
		_ = d.str()
	})
}

func TestMalformedHandshakesAndAuthSwitch(t *testing.T) {
	s := startTestServer(t, "tcp", Config{HandshakeTimeout: 50 * time.Millisecond})
	header := func(flags uint32, charset byte) []byte {
		p := append32(nil, flags)
		p = append32(p, 1<<20)
		p = append(p, charset)
		return append(p, make([]byte, 23)...)
	}
	for _, tc := range []struct {
		p    []byte
		code uint16
	}{
		{nil, 1043}, {header(0, 45), 1251}, {header(clientProtocol41|clientSSL, 45), 1251}, {header(clientProtocol41, 8), 1115}, {header(clientProtocol41|clientSecureConnection, 45), 1043},
	} {
		conn, err := net.Dial(s.network, s.address)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		if _, _, err := readPacket(conn, 0, 1<<20); err != nil {
			t.Fatal(err)
		}
		writer := packetWriter{conn: conn, sequence: 1}
		if err := writer.write(tc.p); err != nil {
			t.Fatal(err)
		}
		p, _, err := readPacket(conn, 2, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		expectCode(t, p, tc.code)
		_ = conn.Close()
	}
	conn, err := net.Dial(s.network, s.address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := readPacket(conn, 0, 1<<20); err != nil {
		t.Fatal(err)
	}
	writer := packetWriter{conn: conn, sequence: 1}
	p := header(clientProtocol41|clientSecureConnection|clientPluginAuth, 45)
	p = append(p, []byte("slopdb\x00\x00caching_sha2_password\x00")...)
	if err := writer.write(p); err != nil {
		t.Fatal(err)
	}
	p, _, err = readPacket(conn, 2, 1<<20)
	if err != nil || len(p) < 2 || p[0] != 0xfe {
		t.Fatalf("auth switch %x %v", p, err)
	}
	writer.sequence = 3
	if err := writer.write(nil); err != nil {
		t.Fatal(err)
	}
	p, _, err = readPacket(conn, 4, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	expectOK(t, p, 2)
	// A peer that never completes authentication is disconnected on its deadline.
	idle, err := net.Dial(s.network, s.address)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	_ = idle.SetDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := readPacket(idle, 0, 1<<20); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if _, err := idle.Read(one[:]); !errors.Is(err, io.EOF) {
		t.Fatalf("handshake timeout did not disconnect: %v", err)
	}
}
