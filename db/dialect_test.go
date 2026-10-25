package db

import (
	"errors"
	"slopdb/sql"
	"testing"
)

func TestSessionDialectSelectionAndPlanInvalidation(t *testing.T) {
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
	if s.Dialect() != sql.DialectNative || s.IsolationLevel() != sql.ReadCommitted {
		t.Fatal("native default")
	}
	if e = s.SetDialect(sql.DialectMySQL); e != nil {
		t.Fatal(e)
	}
	if s.IsolationLevel() != sql.RepeatableRead {
		t.Fatal("MySQL default")
	}
	if e = s.SetPostgresMode(true); e != nil {
		t.Fatal(e)
	}
	if s.Dialect() != sql.DialectPostgreSQL || s.IsolationLevel() != sql.ReadCommitted {
		t.Fatal("exclusive PostgreSQL default")
	}
	if e = s.SetMySQLMode(false); e != nil || s.Dialect() != sql.DialectPostgreSQL {
		t.Fatal("disabling inactive dialect changed selection", e)
	}
	if e = s.SetDialect(sql.Dialect(255)); e == nil || s.Dialect() != sql.DialectPostgreSQL {
		t.Fatal("invalid dialect accepted")
	}
	if e = s.SetIsolationLevel(sql.RepeatableRead); e != nil {
		t.Fatal(e)
	}
	if e = s.SetDialect(sql.DialectNative); e != nil {
		t.Fatal(e)
	}
	if s.IsolationLevel() != sql.RepeatableRead {
		t.Fatal("explicit isolation lost")
	}
	if _, e = s.Exec("CREATE TABLE modes(id INTEGER PRIMARY KEY,label TEXT); INSERT INTO modes VALUES(1,'value')"); e != nil {
		t.Fatal(e)
	}
	p, e := s.Prepare("SELECT label FROM modes WHERE id=?")
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	for i := 0; i < 8; i++ {
		r, e := p.Exec(1)
		if e != nil || len(r.Rows) != 1 || r.Rows[0]["label"].Text != "value" {
			t.Fatal(r, e)
		}
	}
	before := p.PlanStats()
	if before.Hits == 0 {
		t.Fatal("prepared plan was not reused")
	}
	if e = s.SetDialect(sql.DialectMySQL); e != nil {
		t.Fatal(e)
	}
	r, e := p.Exec(1)
	if e != nil || r.Rows[0]["label"].Text != "value" {
		t.Fatal(r, e)
	}
	if p.PlanStats().Builds <= before.Builds {
		t.Fatal("dialect switch reused an incompatible plan")
	}
	if _, e = s.Exec("BEGIN"); e != nil {
		t.Fatal(e)
	}
	if e = s.SetDialect(sql.DialectPostgreSQL); e == nil {
		t.Fatal("changed active transaction dialect")
	}
	if e = s.SetDialect(sql.DialectMySQL); e != nil {
		t.Fatal("idempotent transaction selection", e)
	}
	if _, e = s.Exec("ROLLBACK"); e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	if e = s.SetDialect(sql.DialectNative); !errors.Is(e, ErrSessionClosed) {
		t.Fatal(e)
	}
}
func TestPreparedFunctionsFollowCurrentDialect(t *testing.T) {
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
	p, e := s.Prepare("SELECT CONCAT('a',NULL,'b') AS value,LENGTH('世界') AS n")
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	for _, dialect := range []sql.Dialect{sql.DialectMySQL, sql.DialectPostgreSQL, sql.DialectMySQL, sql.DialectPostgreSQL} {
		if e = s.SetDialect(dialect); e != nil {
			t.Fatal(e)
		}
		r, e := p.Exec()
		if e != nil {
			t.Fatal(e)
		}
		if dialect == sql.DialectMySQL {
			if !r.Rows[0]["value"].Null || r.Rows[0]["n"].Int != 6 {
				t.Fatalf("MySQL %+v", r.Rows)
			}
		} else if r.Rows[0]["value"].Text != "ab" || r.Rows[0]["n"].Int != 2 {
			t.Fatalf("PostgreSQL %+v", r.Rows)
		}
	}
}
