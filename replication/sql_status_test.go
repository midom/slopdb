package replication

import (
	"context"
	gosql "database/sql"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"slopdb/mysqlserver"
	"slopdb/postgresserver"
)

func statusProtocolClient(t *testing.T, n *Node, protocol string) *gosql.DB {
	t.Helper()
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	var server interface {
		Serve(net.Listener) error
		Close() error
	}
	if protocol == "mysql" {
		server, e = mysqlserver.New(n.Database(), mysqlserver.Config{})
	} else {
		server, e = postgresserver.New(n.Database(), postgresserver.Config{})
	}
	if e != nil {
		listener.Close()
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		if e := server.Close(); e != nil {
			t.Error(e)
		}
		if e := <-done; e != nil && !errors.Is(e, net.ErrClosed) {
			t.Error(e)
		}
	})
	var c *gosql.DB
	if protocol == "mysql" {
		cfg := mysql.NewConfig()
		cfg.User = "slopdb"
		cfg.DBName = "slopdb"
		cfg.Net = "tcp"
		cfg.Addr = listener.Addr().String()
		cfg.Timeout = 3 * time.Second
		cfg.ReadTimeout = 5 * time.Second
		cfg.WriteTimeout = 5 * time.Second
		c, e = gosql.Open("mysql", cfg.FormatDSN())
	} else {
		host, port, _ := net.SplitHostPort(listener.Addr().String())
		c, e = gosql.Open("postgres", fmt.Sprintf("host=%s port=%s user=slopdb dbname=slopdb sslmode=disable connect_timeout=3", host, port))
	}
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func TestRaftSQLInspectionOnLeaderAndFollower(t *testing.T) {
	cfg := testConfig(t, "status-leader", "tcp")
	cfg.Bootstrap = true
	n1 := openTestNode(t, cfg)
	ready(t, n1)
	exec(t, n1, "CREATE TABLE items(id INTEGER PRIMARY KEY)")
	follower := testConfig(t, "status-follower", "tcp")
	follower.Join = n1.ControlAddress()
	n2 := openTestNode(t, follower)
	waitApplied(t, n2, n1.applied.Load())
	for _, n := range []*Node{n1, n2} {
		ro, e := n.Database().ReadOnly(context.Background())
		if e != nil || ro != (n == n2) {
			t.Fatalf("role read_only on %s: %v %v", n.config.ID, ro, e)
		}
		before := n.raftNode().LastIndex()
		for _, protocol := range []string{"mysql", "postgres"} {
			c := statusProtocolClient(t, n, protocol)
			rows, e := c.Query("SHOW RAFT STATUS")
			if e != nil {
				t.Fatalf("%s status on %s: %v", protocol, n.config.ID, e)
			}
			state := map[string]string{}
			for rows.Next() {
				var name, value string
				if e = rows.Scan(&name, &value); e != nil {
					t.Fatal(e)
				}
				state[name] = value
			}
			if e = rows.Err(); e != nil {
				t.Fatal(e)
			}
			rows.Close()
			var metric, engine string
			if e = c.QueryRow("SHOW LSM STATUS LIKE 'engine'").Scan(&metric, &engine); e != nil || metric != "engine" || engine != "LSM" {
				t.Fatalf("%s LSM status on %s: %s %s %v", protocol, n.config.ID, metric, engine, e)
			}
			want := "Leader"
			if n == n2 {
				want = "Follower"
			}
			if state["node_id"] != n.config.ID || state["role"] != want || state["enabled"] != "ON" {
				t.Fatalf("%s status %+v", protocol, state)
			}
			if protocol == "mysql" {
				var readonly int64
				if e = c.QueryRow("SELECT @@read_only").Scan(&readonly); e != nil || readonly != boolInt(n == n2) {
					t.Fatalf("@@read_only %d %v", readonly, e)
				}
			} else {
				var name, value string
				if e = c.QueryRow("SHOW GLOBAL VARIABLES LIKE 'read_only'").Scan(&name, &value); e != nil || value != map[bool]string{true: "ON", false: "OFF"}[n == n2] {
					t.Fatalf("PG read_only %s %s %v", name, value, e)
				}
			}
			if n == n2 {
				if _, e = c.Exec("INSERT INTO items VALUES(99)"); e == nil {
					t.Fatal("inspection admitted follower write")
				}
				if _, e = c.Query("SELECT * FROM items"); e == nil {
					t.Fatal("inspection admitted uncertified follower data read")
				}
			}
		}
		if after := n.raftNode().LastIndex(); after != before {
			t.Fatalf("inspection appended log entries: %d -> %d", before, after)
		}
		if n.Database().ReplicationStorage().ChangeLogEnabled() {
			t.Fatal("inspection activated journal")
		}
	}
}
func boolInt(v bool) int64 {
	if v {
		return 1
	}
	return 0
}
