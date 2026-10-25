package mysqlserver

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestMySQLNetworkTimeoutVariables(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	connection, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err = connection.ExecContext(context.Background(), "SET net_read_timeout=900, net_write_timeout=900"); err != nil {
		t.Fatal(err)
	}
	var read, write, global int
	if err = connection.QueryRowContext(context.Background(), "SELECT @@session.net_read_timeout,@@net_write_timeout,@@global.net_read_timeout").Scan(&read, &write, &global); err != nil || read != 900 || write != 900 || global != 30 {
		t.Fatal(read, write, global, err)
	}
	if _, err = connection.ExecContext(context.Background(), "SET net_read_timeout=901,net_write_timeout=0"); err == nil {
		t.Fatal("invalid timeout accepted")
	}
	if err = connection.QueryRowContext(context.Background(), "SELECT @@net_read_timeout").Scan(&read); err != nil || read != 900 {
		t.Fatal("non-atomic settings", read, err)
	}
}
func TestMySQLNetworkTimeoutTransportIdleAndPartialPacket(t *testing.T) {
	server, peer := net.Pipe()
	defer server.Close()
	defer peer.Close()
	conn := newSessionIOConn(server, defaultSessionVariables())
	conn.readTimeout.Store(int64(20 * time.Millisecond))
	conn.beginCommand()
	done := make(chan error, 1)
	go func() { _, _, err := readPacket(conn, 0, 1024); done <- err }()
	// An idle connection remains available past net_read_timeout. Once its first
	// byte arrives, an incomplete header must time out without polling.
	select {
	case err := <-done:
		t.Fatalf("idle session timed out: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if _, err := peer.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("partial packet did not time out")
	}
}
func TestMySQLNetworkWriteTimeoutTransport(t *testing.T) {
	server, peer := net.Pipe()
	defer server.Close()
	defer peer.Close()
	conn := newSessionIOConn(server, defaultSessionVariables())
	conn.writeTimeout.Store(int64(20 * time.Millisecond))
	if _, err := conn.Write([]byte("peer does not consume this block")); err == nil {
		t.Fatal("stalled write succeeded")
	} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatal(err)
	}
}
