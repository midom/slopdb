package db

import (
	"slopdb/sql"
	"testing"
)

func TestPostgresTemporaryDefinitionsRollbackSavepointsAndShadow(t *testing.T) {
	d := projectionDatabase(t)
	if _, e := d.Exec("CREATE TABLE shared(id INTEGER PRIMARY KEY,v TEXT);INSERT INTO shared VALUES(1,'durable')"); e != nil {
		t.Fatal(e)
	}
	s, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	s.SetPostgresMode(true)
	exec := func(q string) *ExecResult {
		t.Helper()
		r, e := s.Exec(q)
		if e != nil {
			t.Fatalf("%s: %v", q, e)
		}
		return r
	}
	exec("BEGIN;CREATE TEMPORARY TABLE shared(id INTEGER PRIMARY KEY,v TEXT);INSERT INTO shared VALUES(2,'private');ROLLBACK")
	r := exec("SELECT v FROM shared")
	if len(r.Rows) != 1 || r.Rows[0]["v"].Text != "durable" {
		t.Fatal("rollback did not restore persistent shadow", r.Rows)
	}
	exec("CREATE TEMPORARY TABLE scratch(id INTEGER PRIMARY KEY,v TEXT);CREATE INDEX scratch_v ON scratch(v);INSERT INTO scratch VALUES(1,'old')")
	exec("BEGIN;SAVEPOINT keep;DROP TABLE scratch")
	if _, e = s.Exec("SELECT * FROM scratch"); e == nil {
		t.Fatal("dropped definition visible")
	}
	exec("ROLLBACK TO keep")
	r = exec("SELECT v FROM scratch WHERE v='old'")
	if len(r.Rows) != 1 {
		t.Fatal("drop/savepoint removed committed rows", r.Rows)
	}
	exec("ALTER TABLE scratch ADD COLUMN extra INTEGER DEFAULT 7")
	r = exec("SELECT extra FROM scratch")
	if r.Rows[0]["extra"].Int != 7 {
		t.Fatal(r.Rows)
	}
	exec("ROLLBACK TO keep")
	if _, e = s.Exec("SELECT extra FROM scratch"); e == nil {
		t.Fatal("ALTER metadata survived rollback")
	}
	exec("UPDATE scratch SET v='new';ROLLBACK")
	r = exec("SELECT v FROM scratch WHERE v='old'")
	if len(r.Rows) != 1 {
		t.Fatal("DML rollback lost rows/index", r.Rows)
	}
	exec("BEGIN;DROP TABLE scratch;COMMIT")
	if _, e = s.Exec("SELECT * FROM scratch"); e == nil {
		t.Fatal("committed drop visible")
	}
	if got := temporaryLSMCount(t, d); got != 0 {
		t.Fatalf("committed drop left private entries %d", got)
	}
	// Catalog rollback must be policy-dependent, not a storage implementation fork.
	for _, dialect := range []sql.Dialect{sql.DialectNative, sql.DialectMySQL} {
		other, e := d.NewSession()
		if e != nil {
			t.Fatal(e)
		}
		other.SetDialect(dialect)
		if _, e = other.Exec("BEGIN;CREATE TEMPORARY TABLE retained(id INTEGER);ROLLBACK"); e != nil {
			t.Fatal(e)
		}
		if _, e = other.Exec("SELECT * FROM retained"); e != nil {
			t.Fatalf("%s definitions unexpectedly transactional: %v", dialect, e)
		}
		other.Close()
	}
}
