package db

import (
	"slopdb/sql"
	"testing"
)

func TestFunctionArgumentPoliciesKeepPostgresStrictAndMySQLPermissive(t *testing.T) {
	d, e := Open(t.TempDir(), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	s, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.SetDialect(sql.DialectPostgreSQL); e != nil {
		t.Fatal(e)
	}
	for _, q := range []string{"SELECT chr('65junk')", "SELECT repeat('a','2junk')", "SELECT left('abcd','2junk')", "SELECT lpad('a','2junk')", "SELECT get_byte(decode('00','hex'),'0junk')", "SELECT set_byte(decode('00','hex'),0,'1junk')", "SELECT format('%*s','3junk','x')", "SELECT chr(2147483648)"} {
		if _, e = s.Exec(q); e == nil {
			t.Fatalf("accepted permissive PG conversion %s", q)
		}
		p, e := s.Prepare(q)
		if e == nil {
			_, e = p.Exec()
			p.Close()
		}
		if e == nil {
			t.Fatalf("prepared accepted permissive PG conversion %s", q)
		}
	}
	for _, q := range []string{"SELECT chr('65') AS v", "SELECT repeat('a','2') AS v", "SELECT left('abcd','2') AS v", "SELECT lpad('a','2') AS v"} {
		if _, e = s.Exec(q); e != nil {
			t.Fatalf("%s: %v", q, e)
		}
	}
	if e = s.SetDialect(sql.DialectMySQL); e != nil {
		t.Fatal(e)
	}
	r, e := s.Exec("SELECT REPEAT('x','2junk') AS v,LEFT('abcd','2junk') AS p")
	if e != nil || r.Rows[0]["v"].Text != "xx" || r.Rows[0]["p"].Text != "ab" {
		t.Fatal(r, e)
	}
	// Lazy branches must not evaluate an error solely to build generic arguments.
	r, e = s.Exec("SELECT IF(1,7,1/0) AS v,COALESCE(3,1/0) AS p")
	if e != nil || r.Rows[0]["v"].Int != 7 || r.Rows[0]["p"].Int != 3 {
		t.Fatal(r, e)
	}
}
