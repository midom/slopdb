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
	"github.com/lib/pq"
	"slopdb/db"
	"slopdb/mysqlserver"
	"slopdb/postgresserver"
)

func TestAsyncDurabilityThroughSQLProtocols(t *testing.T) {
	for _, protocol := range []string{"mysql", "postgres"} {
		t.Run(protocol, func(t *testing.T) {
			config := asyncConfig(t, protocol, "tcp")
			config.Bootstrap = true
			node := openTestNode(t, config)
			ready(t, node)
			listener, e := net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() { _ = listener.Close() })
			var server interface {
				Serve(net.Listener) error
				Close() error
			}
			if protocol == "mysql" {
				server, e = mysqlserver.New(node.Database(), mysqlserver.Config{})
			} else {
				server, e = postgresserver.New(node.Database(), postgresserver.Config{})
			}
			if e != nil {
				t.Fatal(e)
			}
			served := make(chan error, 1)
			go func() { served <- server.Serve(listener) }()
			t.Cleanup(func() {
				if e := server.Close(); e != nil {
					t.Error(e)
				}
				if e := <-served; e != nil {
					t.Error(e)
				}
			})
			var client *gosql.DB
			if protocol == "mysql" {
				options := mysql.NewConfig()
				options.User = "slopdb"
				options.DBName = "slopdb"
				options.Net = "tcp"
				options.Addr = listener.Addr().String()
				connector, e := mysql.NewConnector(options)
				if e != nil {
					t.Fatal(e)
				}
				client = gosql.OpenDB(connector)
			} else {
				host, port, _ := net.SplitHostPort(listener.Addr().String())
				client, e = gosql.Open("postgres", fmt.Sprintf("host=%s port=%s user=slopdb dbname=slopdb sslmode=disable", host, port))
				if e != nil {
					t.Fatal(e)
				}
			}
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			connection, e := client.Conn(ctx)
			if e != nil {
				t.Fatal(e)
			}
			defer connection.Close()
			run := func(query string) {
				t.Helper()
				if _, e := connection.ExecContext(ctx, query); e != nil {
					t.Fatalf("%s: %v", query, e)
				}
			}
			run("CREATE TABLE items(id INTEGER PRIMARY KEY)")
			run("BEGIN")
			run("INSERT INTO items VALUES(1)")
			run("COMMIT ASYNC")
			if node.CurrentToken().Index <= node.durableIndex() {
				t.Fatal("async commit waited for a durable seal")
			}
			run("BEGIN")
			run("INSERT INTO items VALUES(2)")
			prepared, e := connection.PrepareContext(ctx, "COMMIT DURABLE")
			if e != nil {
				t.Fatal(e)
			}
			if _, e = prepared.ExecContext(ctx); e != nil {
				t.Fatal(e)
			}
			if e = prepared.Close(); e != nil {
				t.Fatal(e)
			}
			if node.CurrentToken().Index != node.durableIndex() {
				t.Fatal("durable SQL response preceded persistence")
			}
			node.Database().SetReplicationGuard(&db.ReplicationGuard{Check: node.checkSQL, Durable: func(context.Context) error { return context.DeadlineExceeded }})
			run("BEGIN")
			run("INSERT INTO items VALUES(3)")
			_, e = connection.ExecContext(ctx, "COMMIT DURABLE")
			if protocol == "mysql" {
				var driverError *mysql.MySQLError
				if !errors.As(e, &driverError) || string(driverError.SQLState[:]) != "40003" {
					t.Fatalf("lost commit outcome classification: %v", e)
				}
			} else {
				var driverError *pq.Error
				if !errors.As(e, &driverError) || string(driverError.Code) != "40003" {
					t.Fatalf("lost commit outcome classification: %v", e)
				}
				run("ROLLBACK")
			}
			var count int
			if e = connection.QueryRowContext(ctx, "SELECT COUNT(*) FROM items").Scan(&count); e != nil || count != 3 {
				t.Fatalf("uncertain confirmation rolled back SQL: count=%d error=%v", count, e)
			}
		})
	}
}
