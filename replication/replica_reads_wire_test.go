package replication

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"slopdb/mysqlserver"
	"slopdb/postgresserver"
)

func TestReplicaReadsThroughSQLProtocols(t *testing.T) {
	for _, protocol := range []string{"mysql", "postgres"} {
		t.Run(protocol, func(t *testing.T) {
			source, replica := readReplicaPair(t, DurabilitySync, "tcp", 2*time.Second)
			connection, ctx := openPositionSQLClient(t, replica, protocol)
			query := "SELECT v FROM items WHERE id=?"
			if protocol == "postgres" {
				query = "SELECT v FROM items WHERE id=$1"
			}
			prepared, err := connection.PrepareContext(ctx, query)
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Close()
			before := source.raftNode().LastIndex()
			var value string
			if err = prepared.QueryRowContext(ctx, int64(1)).Scan(&value); err != nil || value != "one" {
				t.Fatalf("wire replica read: %q %v", value, err)
			}
			tx, err := connection.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err = tx.QueryRowContext(ctx, "SELECT v FROM items WHERE id=2").Scan(&value); err != nil || value != "two" {
				t.Fatalf("wire read transaction: %q %v", value, err)
			}
			if _, err = tx.ExecContext(ctx, "INSERT INTO items VALUES(3,'bad')"); err == nil {
				t.Fatal("wire replica accepted a write")
			}
			if err = tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if source.raftNode().LastIndex() != before {
				t.Fatal("wire replica reads/rejections appended entries")
			}
			exec(t, source, "ALTER TABLE items ADD COLUMN extra TEXT; UPDATE items SET v='updated' WHERE id=1")
			waitApplied(t, replica, source.applied.Load())
			if err = prepared.QueryRowContext(ctx, int64(1)).Scan(&value); err != nil || value != "updated" {
				t.Fatalf("wire prepared read after schema update: %q %v", value, err)
			}
			if err = source.httpServer.Close(); err != nil {
				t.Fatal(err)
			}
			replica.replicaReads.mu.Lock()
			replica.replicaReads.proof.since = time.Now().Add(-3 * time.Second)
			replica.replicaReads.mu.Unlock()
			err = prepared.QueryRowContext(ctx, int64(1)).Scan(&value)
			if err == nil || !strings.Contains(err.Error(), ErrReplicaTooStale.Error()) {
				t.Fatalf("wire read ignored expired proof: %v", err)
			}
		})
	}
}

func openPositionSQLClient(t *testing.T, node *Node, protocol string) (*gosql.Conn, context.Context) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	var server interface {
		Serve(net.Listener) error
		Close() error
	}
	if protocol == "mysql" {
		server, err = mysqlserver.New(node.Database(), mysqlserver.Config{})
	} else {
		server, err = postgresserver.New(node.Database(), postgresserver.Config{})
	}
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Error(err)
		}
		if err := <-served; err != nil {
			t.Error(err)
		}
	})
	var client *gosql.DB
	if protocol == "mysql" {
		options := mysql.NewConfig()
		options.User, options.DBName, options.Net, options.Addr = "slopdb", "slopdb", "tcp", listener.Addr().String()
		connector, err := mysql.NewConnector(options)
		if err != nil {
			t.Fatal(err)
		}
		client = gosql.OpenDB(connector)
	} else {
		host, port, _ := net.SplitHostPort(listener.Addr().String())
		client, err = gosql.Open("postgres", fmt.Sprintf("host=%s port=%s user=slopdb dbname=slopdb sslmode=disable", host, port))
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	connection, err := client.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection, ctx
}
