package mysqlserver

import (
	"context"
	"testing"
)

func TestMySQLAdditionalSessionFunctions(t *testing.T) {
	server := startTestServer(t, "tcp", Config{})
	pool := server.open(t, "slopdb", "", "slopdb")
	c, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var identity, alias1, alias2, db string
	if err := c.QueryRowContext(context.Background(), "SELECT USER(),SESSION_USER(),SYSTEM_USER(),SCHEMA()").Scan(&identity, &alias1, &alias2, &db); err != nil || identity != alias1 || identity != alias2 || db != "slopdb" {
		t.Fatal(identity, alias1, alias2, db, err)
	}
	var free, got, used, released int64
	if err := c.QueryRowContext(context.Background(), "SELECT GET_LOCK('all_a',0),GET_LOCK('all_a',0),GET_LOCK('all_b',0),RELEASE_ALL_LOCKS()").Scan(&got, &used, &free, &released); err != nil || got != 1 || used != 1 || free != 1 || released != 3 {
		t.Fatal(got, used, free, released, err)
	}
	var owner, connection uint64
	if _, err := c.ExecContext(context.Background(), "SELECT GET_LOCK('all_a',0)"); err != nil {
		t.Fatal(err)
	}
	if err := c.QueryRowContext(context.Background(), "SELECT IS_USED_LOCK('all_a'),CONNECTION_ID()").Scan(&owner, &connection); err != nil || owner != connection {
		t.Fatal(owner, connection, err)
	}
	if err := c.QueryRowContext(context.Background(), "SELECT RELEASE_ALL_LOCKS()").Scan(&released); err != nil || released != 1 {
		t.Fatal(released, err)
	}
}
