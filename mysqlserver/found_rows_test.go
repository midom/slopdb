package mysqlserver

import (
	"context"
	"testing"
)

func TestMySQLCalculatedFoundRowsProtocolAndPrepared(t *testing.T) {
	server := startTestServer(t, "tcp", Config{})
	pool := server.open(t, "slopdb", "", "slopdb")
	ctx := context.Background()
	c, e := pool.Conn(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	for _, q := range []string{"CREATE TABLE calc_rows(id INTEGER PRIMARY KEY,n INTEGER)", "INSERT INTO calc_rows VALUES(1,10),(2,20),(3,30)"} {
		if _, e = c.ExecContext(ctx, q); e != nil {
			t.Fatal(e)
		}
	}
	found := func(want int64) {
		t.Helper()
		var count int64
		if e = c.QueryRowContext(ctx, "SELECT FOUND_ROWS()").Scan(&count); e != nil || count != want {
			t.Fatal("protocol count", count, want, e)
		}
	}
	var id int64
	// This base-column shape normally uses mysqlserver.streamQuery.
	if e = c.QueryRowContext(ctx, "SELECT SQL_CALC_FOUND_ROWS id FROM calc_rows ORDER BY id LIMIT 1").Scan(&id); e != nil || id != 1 {
		t.Fatal(id, e)
	}
	found(3)
	p, e := c.PrepareContext(ctx, "SELECT SQL_CALC_FOUND_ROWS id FROM calc_rows WHERE n>=? ORDER BY id LIMIT ?")
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	if e = p.QueryRowContext(ctx, 20, 1).Scan(&id); e != nil || id != 2 {
		t.Fatal(id, e)
	}
	found(2)
	point, e := c.PrepareContext(ctx, "SELECT SQL_CALC_FOUND_ROWS id FROM calc_rows WHERE id=? LIMIT ?")
	if e != nil {
		t.Fatal(e)
	}
	defer point.Close()
	rows, e := point.QueryContext(ctx, 1, 0)
	if e != nil {
		t.Fatal(e)
	}
	if rows.Next() {
		t.Fatal("LIMIT0 returned row")
	}
	if e = rows.Close(); e != nil {
		t.Fatal(e)
	}
	found(1)
	if e = c.QueryRowContext(ctx, "SELECT id FROM calc_rows LIMIT 1").Scan(&id); e != nil {
		t.Fatal(e)
	}
	found(1)
}
