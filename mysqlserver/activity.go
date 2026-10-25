package mysqlserver

import (
	"net"
	"slopdb/sql"
	"strconv"
)

func (c *client) syncActivityIdentity() {
	if c.session == nil {
		return
	}
	identity := sql.ActivityIdentity{StartedAt: c.startedAt, BackendID: c.id, Dialect: sql.DialectMySQL, User: c.username, Database: c.selected, Host: "localhost", ClientPort: -1}
	if host, port, err := net.SplitHostPort(c.conn.RemoteAddr().String()); err == nil {
		identity.Host = c.conn.RemoteAddr().String()
		identity.ClientAddress = host
		identity.ClientPort, _ = strconv.Atoi(port)
	}
	c.session.SetActivityIdentity(identity)
}
