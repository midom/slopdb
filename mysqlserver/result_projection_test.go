package mysqlserver

import (
	"reflect"
	"testing"
)

func TestMySQLDuplicateResultLabels(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	c := s.open(t, "slopdb", "", "slopdb")
	check := func(prepared bool, q string) {
		t.Helper()
		var labels []string
		var a, b int64
		if prepared {
			p, e := c.Prepare(q)
			if e != nil {
				t.Fatal(e)
			}
			defer p.Close()
			rows, e := p.Query()
			if e != nil {
				t.Fatal(e)
			}
			defer rows.Close()
			labels, e = rows.Columns()
			if e != nil {
				t.Fatal(e)
			}
			if !rows.Next() {
				t.Fatal(rows.Err())
			}
			if e = rows.Scan(&a, &b); e != nil {
				t.Fatal(e)
			}
		} else {
			rows, e := c.Query(q)
			if e != nil {
				t.Fatal(e)
			}
			defer rows.Close()
			labels, e = rows.Columns()
			if e != nil {
				t.Fatal(e)
			}
			if !rows.Next() {
				t.Fatal(rows.Err())
			}
			if e = rows.Scan(&a, &b); e != nil {
				t.Fatal(e)
			}
		}
		if !reflect.DeepEqual(labels, []string{"x", "x"}) || a != 1 || b != 2 {
			t.Fatalf("prepared=%v labels=%v values=%d,%d", prepared, labels, a, b)
		}
	}
	for _, prepared := range []bool{false, true} {
		check(prepared, "SELECT 1 AS x,2 AS x")
		mustExec(t, c, "CREATE TABLE IF NOT EXISTS projection(id INTEGER PRIMARY KEY,a INTEGER,b INTEGER)")
		mustExec(t, c, "REPLACE INTO projection VALUES(1,1,2)")
		check(prepared, "SELECT a AS x,b AS x FROM projection WHERE id=1")
	}
}
