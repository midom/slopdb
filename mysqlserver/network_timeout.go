package mysqlserver

import (
	"net"
	"sync/atomic"
	"time"
)

// Network timeouts cover stalled packet transfers. Idle sessions wait for the
// first byte without a read deadline; an application timeout is not an idle
// connection timeout. Read state belongs only to the command reader, while SET
// publishes independent read/write settings atomically.
type sessionIOConn struct {
	net.Conn
	readTimeout, writeTimeout atomic.Int64
	firstRead                 bool
}

func newSessionIOConn(conn net.Conn, variables sessionVariables) *sessionIOConn {
	c := &sessionIOConn{Conn: conn}
	c.configure(variables)
	return c
}
func (c *sessionIOConn) configure(variables sessionVariables) {
	c.readTimeout.Store(int64(time.Duration(variables.netReadTimeout) * time.Second))
	c.writeTimeout.Store(int64(time.Duration(variables.netWriteTimeout) * time.Second))
}
func (c *sessionIOConn) beginCommand() { c.firstRead = true; _ = c.Conn.SetReadDeadline(time.Time{}) }
func (c *sessionIOConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if c.firstRead {
		n, err := c.Conn.Read(p[:1])
		if n > 0 {
			c.firstRead = false
			_ = c.Conn.SetReadDeadline(time.Now().Add(time.Duration(c.readTimeout.Load())))
		}
		return n, err
	}
	_ = c.Conn.SetReadDeadline(time.Now().Add(time.Duration(c.readTimeout.Load())))
	return c.Conn.Read(p)
}
func (c *sessionIOConn) Write(p []byte) (int, error) {
	_ = c.Conn.SetWriteDeadline(time.Now().Add(time.Duration(c.writeTimeout.Load())))
	return c.Conn.Write(p)
}
func (c *client) installNetworkTimeouts() {
	if compressed, ok := c.conn.(*compressedConn); ok {
		c.transport = newSessionIOConn(compressed.Conn, c.variables)
		compressed.Conn = c.transport
	} else {
		c.transport = newSessionIOConn(c.conn, c.variables)
		c.conn = c.transport
	}
	c.writer.conn = c.conn
}
