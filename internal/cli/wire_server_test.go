package cli

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"slopdb/db"
	"slopdb/mysqlserver"
	"slopdb/postgresserver"
)

type observedWireServer struct {
	protocolServer
	address chan string
}

func (s *observedWireServer) Serve(l net.Listener) error {
	s.address <- l.Addr().String()
	return s.protocolServer.Serve(l)
}

func TestDualWireServerLifecycleAndSharedDatabase(t *testing.T) {
	path := t.TempDir()
	d, e := db.Open(path, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	m, e := mysqlserver.New(d, mysqlserver.Config{})
	if e != nil {
		t.Fatal(e)
	}
	p, e := postgresserver.New(d, postgresserver.Config{})
	if e != nil {
		t.Fatal(e)
	}
	my := &observedWireServer{m, make(chan string, 1)}
	pg := &observedWireServer{p, make(chan string, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- serveWireServers(ctx, []wireEndpoint{{server: my, protocol: "MySQL", tcp: "127.0.0.1:0"}, {server: pg, protocol: "PostgreSQL", tcp: "127.0.0.1:0"}})
	}()
	deadline, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	address := func(c <-chan string) string {
		t.Helper()
		select {
		case a := <-c:
			return a
		case <-deadline.Done():
			t.Fatal(deadline.Err())
			return ""
		}
	}
	myAddr, pgAddr := address(my.address), address(pg.address)
	c, e := gosql.Open("mysql", "slopdb@tcp("+myAddr+")/slopdb?timeout=3s&readTimeout=3s")
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	host, port, _ := net.SplitHostPort(pgAddr)
	c2, e := gosql.Open("postgres", fmt.Sprintf("host=%s port=%s user=slopdb dbname=slopdb sslmode=disable connect_timeout=3", host, port))
	if e != nil {
		t.Fatal(e)
	}
	defer c2.Close()
	if _, e = c.ExecContext(deadline, "CREATE TABLE shared_wire(id INTEGER PRIMARY KEY,value TEXT)"); e != nil {
		t.Fatal(e)
	}
	if _, e = c.ExecContext(deadline, `INSERT INTO shared_wire VALUES(1,"MySQL\nline")`); e != nil {
		t.Fatal(e)
	}
	var value string
	if e = c2.QueryRowContext(deadline, `SELECT "value" FROM shared_wire WHERE id=$1`, 1).Scan(&value); e != nil || value != "MySQL\nline" {
		t.Fatal(value, e)
	}
	if _, e = c2.ExecContext(deadline, `UPDATE shared_wire SET value=$1 WHERE id=$2`, "PG's value", 1); e != nil {
		t.Fatal(e)
	}
	if e = c.QueryRowContext(deadline, "SELECT value FROM shared_wire WHERE id=1").Scan(&value); e != nil || value != "PG's value" {
		t.Fatal(value, e)
	}
	c.Close()
	c2.Close()
	cancel()
	select {
	case e = <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-deadline.Done():
		t.Fatal("dual listener shutdown hung")
	}
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
	d, e = db.Open(path, nil)
	if e != nil {
		t.Fatal(e)
	}
	r, e := d.Exec("SELECT value FROM shared_wire")
	if e != nil || r.Rows[0]["value"].Text != "PG's value" {
		t.Fatal(r, e)
	}
}

func TestDualWireServerStartupFailureClosesOtherEndpoint(t *testing.T) {
	d, e := db.Open(t.TempDir(), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	m, e := mysqlserver.New(d, mysqlserver.Config{})
	if e != nil {
		t.Fatal(e)
	}
	p, e := postgresserver.New(d, postgresserver.Config{})
	if e != nil {
		t.Fatal(e)
	}
	my := &observedWireServer{m, make(chan string, 1)}
	if e = serveWireServers(context.Background(), []wireEndpoint{{server: my, protocol: "MySQL", tcp: "127.0.0.1:0"}, {server: p, protocol: "PostgreSQL", tcp: "invalid listener address"}}); e == nil {
		t.Fatal("bad second listener accepted")
	}
	select {
	case address := <-my.address:
		c, e := net.DialTimeout("tcp", address, time.Second)
		if e == nil {
			c.Close()
			t.Fatal("first listener remains open")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("first serve did not finish")
	}
}
