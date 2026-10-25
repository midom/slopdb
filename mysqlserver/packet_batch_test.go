package mysqlserver

import (
	"bytes"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type batchRecordingConn struct {
	net.Conn
	data   bytes.Buffer
	writes int
	sizes  []int
	fail   error
}

func (c *batchRecordingConn) Read(p []byte) (int, error) { return c.data.Read(p) }
func (c *batchRecordingConn) Write(p []byte) (int, error) {
	c.writes++
	c.sizes = append(c.sizes, len(p))
	if c.fail != nil {
		n := len(p) / 2
		_, _ = c.data.Write(p[:n])
		return n, c.fail
	}
	return c.data.Write(p)
}

func TestPacketResponseBatchCoalescesAndOwnsBytes(t *testing.T) {
	conn := &batchRecordingConn{}
	w := packetWriter{conn: conn, sequence: 254}
	w.beginBatch()
	want := [][]byte{{3}, []byte("id"), []byte("v"), []byte("payload"), {0xfe}, []byte("one row"), {0xfe}}
	for _, data := range want {
		packet := append([]byte(nil), data...)
		if err := w.write(packet); err != nil {
			t.Fatal(err)
		}
		clear(packet) // Buffered packets must not retain mutable encoder scratch.
	}
	if conn.writes != 0 {
		t.Fatalf("response written before its boundary: %d writes", conn.writes)
	}
	if err := w.endBatch(); err != nil {
		t.Fatal(err)
	}
	if conn.writes != 1 || len(w.pending) != 0 || cap(w.pending) != responseBatchBytes || w.batched {
		t.Fatalf("batch not coalesced/bounded: writes=%d len=%d cap=%d batched=%v", conn.writes, len(w.pending), cap(w.pending), w.batched)
	}
	sequence := byte(254)
	for _, expected := range want {
		data, next, err := readPacket(&conn.data, sequence, 1024)
		if err != nil || !bytes.Equal(data, expected) {
			t.Fatalf("packet %d: %q, want %q: %v", sequence, data, expected, err)
		}
		sequence = next
	}
	// Standalone users and handshake traffic keep immediate semantics.
	before := conn.writes
	if err := w.write([]byte("immediate")); err != nil || conn.writes != before+2 {
		t.Fatalf("immediate packet delayed: writes=%d: %v", conn.writes, err)
	}
}

func TestPacketResponseBatchFlushesAtBound(t *testing.T) {
	conn := &batchRecordingConn{}
	w := packetWriter{conn: conn}
	w.beginBatch()
	data := bytes.Repeat([]byte{'x'}, 20<<10)
	for i := 0; i < 9; i++ {
		if err := w.write(data); err != nil {
			t.Fatal(err)
		}
		if len(w.pending) > responseBatchBytes || cap(w.pending) > responseBatchBytes {
			t.Fatalf("unbounded pending response: len=%d cap=%d", len(w.pending), cap(w.pending))
		}
	}
	if conn.writes < 2 {
		t.Fatal("large response buffered until completion")
	}
	if err := w.endBatch(); err != nil {
		t.Fatal(err)
	}
	for _, size := range conn.sizes {
		if size > responseBatchBytes {
			t.Fatalf("small packets exceeded output bound: %d", size)
		}
	}
	for sequence := byte(0); sequence < 9; sequence++ {
		got, _, err := readPacket(&conn.data, sequence, len(data))
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("packet %d changed: %v", sequence, err)
		}
	}
}

func TestPacketResponseBatchPreservesLargeFrames(t *testing.T) {
	for _, size := range []int{responseBatchBytes - 4, responseBatchBytes, maxFrame, maxFrame + 17} {
		t.Run(fmtSize(size), func(t *testing.T) {
			conn := &batchRecordingConn{}
			w := packetWriter{conn: conn}
			w.beginBatch()
			data := bytes.Repeat([]byte{'x'}, size)
			if err := w.write([]byte("before")); err != nil {
				t.Fatal(err)
			}
			if err := w.write(data); err != nil {
				t.Fatal(err)
			}
			if err := w.write([]byte("after")); err != nil {
				t.Fatal(err)
			}
			if err := w.endBatch(); err != nil {
				t.Fatal(err)
			}
			if cap(w.pending) > responseBatchBytes {
				t.Fatalf("large frame retained: %d bytes", cap(w.pending))
			}
			sequence := byte(0)
			for _, expected := range [][]byte{[]byte("before"), data, []byte("after")} {
				got, next, err := readPacket(&conn.data, sequence, size)
				if err != nil || !bytes.Equal(got, expected) {
					t.Fatalf("frame %d: %v", sequence, err)
				}
				sequence = next
			}
			if size >= maxFrame && sequence != 4 {
				t.Fatalf("missing continuation/empty terminator: next=%d", sequence)
			}
		})
	}
}

func TestPacketResponseBatchFailureNeverReplaysPartialOutput(t *testing.T) {
	failed := errors.New("partial network failure")
	conn := &batchRecordingConn{fail: failed}
	w := packetWriter{conn: conn}
	w.beginBatch()
	if err := w.write([]byte("not fully sent")); err != nil {
		t.Fatal(err)
	}
	if err := w.flush(); !errors.Is(err, failed) {
		t.Fatal(err)
	}
	writes, sent := conn.writes, conn.data.Len()
	for _, call := range []func() error{w.flush, w.endBatch, func() error { return w.write([]byte("error response")) }} {
		if err := call(); !errors.Is(err, failed) {
			t.Fatal(err)
		}
	}
	if conn.writes != writes || conn.data.Len() != sent {
		t.Fatal("partially sent response replayed")
	}
}

func TestPacketResponseBatchBoundedFlushKeepsBackpressureAndCancellation(t *testing.T) {
	server, peer := net.Pipe()
	defer server.Close()
	defer peer.Close()
	blocked := &blockedWriteConn{Conn: server, entered: make(chan struct{})}
	transport := newSessionIOConn(blocked, defaultSessionVariables())
	w := packetWriter{conn: transport}
	w.beginBatch()
	done := make(chan error, 1)
	go func() {
		// Filling the buffer must perform network I/O before write returns.
		done <- w.write(make([]byte, responseBatchBytes-4))
	}()
	select {
	case <-blocked.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("bounded response never reached the transport")
	}
	select {
	case err := <-done:
		t.Fatalf("write ignored a peer that consumes no bytes: %v", err)
	default:
	}
	// This is the same interruption used by a streaming query's cancellation
	// callback. Buffered output must not escape the connection's deadline.
	if err := transport.SetWriteDeadline(time.Now()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
			t.Fatalf("blocked flush did not retain cancellation: %v", err)
		}
		if err := w.endBatch(); err == nil {
			t.Fatal("cancelled flush lost its sticky transport error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled response flush remained blocked")
	}
}

func TestPacketResponseBatchCompressionSequence(t *testing.T) {
	conn := &batchRecordingConn{}
	compressed := newCompressedConn(conn)
	compressed.writeSequence = 5
	w := packetWriter{conn: compressed, sequence: 9}
	w.beginBatch()
	packets := [][]byte{[]byte("metadata"), bytes.Repeat([]byte("compressible row "), 50), {0xfe}}
	for _, packet := range packets {
		if err := w.write(packet); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.endBatch(); err != nil {
		t.Fatal(err)
	}
	if compressed.writeSequence != 6 {
		t.Fatalf("response not one compressed frame: %d", compressed.writeSequence)
	}
	decoded := newCompressedConn(conn)
	decoded.readSequence.Store(5)
	sequence := byte(9)
	for _, expected := range packets {
		got, next, err := readPacket(decoded, sequence, 4096)
		if err != nil || !bytes.Equal(got, expected) {
			t.Fatalf("inner sequence %d: %v", sequence, err)
		}
		sequence = next
	}
}

// Count writes at the accepted connection, below the actual command loop and
// packet writer. Reads and protocol correctness still use a real TCP client.
type batchCountingListener struct {
	net.Listener
	writes atomic.Uint64
}

func (l *batchCountingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &batchCountingConn{Conn: conn, writes: &l.writes}, nil
}

type batchCountingConn struct {
	net.Conn
	writes *atomic.Uint64
}

func (c *batchCountingConn) Write(p []byte) (int, error) {
	c.writes.Add(1)
	return c.Conn.Write(p)
}

func TestMySQLCommandResponseCoalescesSmallResultsAndErrors(t *testing.T) {
	// Reuse the normal temporary-database helper, but serve an additional owned
	// listener whose accepted connections expose transport write counts.
	s := startTestServer(t, "tcp", Config{})
	if _, err := s.database.Exec("CREATE TABLE batched(id INTEGER PRIMARY KEY,v INTEGER,b BLOB);INSERT INTO batched VALUES(1,17,X'616263')"); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	counted := &batchCountingListener{Listener: listener}
	done := make(chan error, 1)
	go func() { done <- s.server.Serve(counted) }()
	t.Cleanup(func() {
		_ = counted.Close()
		if err := <-done; err != nil && !errors.Is(err, net.ErrClosed) {
			t.Error(err)
		}
	})
	clientServer := *s
	clientServer.address = listener.Addr().String()
	c := connectRaw(t, &clientServer)
	before := counted.writes.Load()
	c.send(t, append([]byte{3}, "SELECT id,v,b FROM batched WHERE id=1"...))
	sequence := byte(1)
	for packet := 0; packet < 7; packet++ {
		data, next, err := readPacket(c.conn, sequence, 1024)
		if err != nil || len(data) == 0 {
			t.Fatalf("result packet %d: %x %v", packet, data, err)
		}
		sequence = next
		if packet == 5 && !bytes.Equal(data, []byte{1, '1', 2, '1', '7', 3, 'a', 'b', 'c'}) {
			t.Fatalf("result row %x", data)
		}
	}
	if got := counted.writes.Load() - before; got != 1 {
		t.Fatalf("small command response used %d writes, want 1", got)
	}
	// Malformed commands must flush their error before waiting for another
	// request, and must not leave stale bytes in the following response.
	for _, command := range [][]byte{nil, {4}, []byte("\x03SELECT missing FROM batched")} {
		before := counted.writes.Load()
		response := c.command(t, command)
		if len(response) == 0 || response[0] != 0xff {
			t.Fatalf("missing command error: %x", response)
		}
		if got := counted.writes.Load() - before; got != 1 {
			t.Fatalf("error response used %d writes", got)
		}
		expectOK(t, c.command(t, []byte{14}), 2)
	}
}
