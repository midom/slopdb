package db

import (
	"bytes"
	"os"
	"path/filepath"
	"slopdb/sql"
	"strings"
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

func TestPostgresTemporaryCommitActions(t *testing.T) {
	d := projectionDatabase(t)
	s, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.SetDialect(sql.DialectPostgreSQL); err != nil {
		t.Fatal(err)
	}
	exec := func(q string) *ExecResult {
		t.Helper()
		r, e := s.Exec(q)
		if e != nil {
			t.Fatalf("%s: %v", q, e)
		}
		return r
	}
	count := func(table string, want int64) {
		t.Helper()
		r := exec("SELECT COUNT(*) AS n FROM " + table)
		if r.Rows[0]["n"].Int != want {
			t.Fatalf("%s count: %v", table, r.Rows)
		}
	}
	exec("CREATE TABLE shadow(id INTEGER PRIMARY KEY);INSERT INTO shadow VALUES(9)")
	exec("CREATE TEMP TABLE kept(id INTEGER PRIMARY KEY,v TEXT) ON COMMIT PRESERVE ROWS;INSERT INTO kept VALUES(1,'keep')")
	exec("CREATE TEMP TABLE cleared(id INTEGER PRIMARY KEY,v TEXT,KEY cleared_v(v)) ON COMMIT DELETE ROWS")
	exec("BEGIN;INSERT INTO cleared VALUES(1,'before-commit');SAVEPOINT inner_save;INSERT INTO kept VALUES(2,'keep-too');RELEASE inner_save")
	count("cleared", 1)
	exec("COMMIT")
	count("cleared", 0)
	count("kept", 2)
	if r := exec("SELECT * FROM cleared WHERE v='before-commit'"); len(r.Rows) != 0 {
		t.Fatal("stale secondary index", r.Rows)
	}
	exec("BEGIN;INSERT INTO cleared VALUES(2,'rollback');ROLLBACK")
	count("cleared", 0)
	exec("BEGIN;CREATE TEMP TABLE shadow(id INTEGER PRIMARY KEY) ON COMMIT DROP;INSERT INTO shadow VALUES(1)")
	count("shadow", 1)
	exec("COMMIT")
	if r := exec("SELECT id FROM shadow"); r.Rows[0]["id"].Int != 9 {
		t.Fatal("commit did not reveal persistent shadow", r.Rows)
	}
	exec("CREATE TEMP TABLE instant(id INTEGER) ON COMMIT DROP")
	if _, err = s.Exec("SELECT * FROM instant"); err == nil {
		t.Fatal("autocommit ON COMMIT DROP retained definition")
	}
	exec("BEGIN;SAVEPOINT before_create;CREATE TEMP TABLE rolled_back(id INTEGER) ON COMMIT DROP;ROLLBACK TO before_create;COMMIT")
	if _, err = s.Exec("SELECT * FROM rolled_back"); err == nil {
		t.Fatal("savepoint retained new temporary definition")
	}
	exec("BEGIN;DROP TABLE cleared;ROLLBACK;INSERT INTO cleared VALUES(3,'autocommit-clears')")
	count("cleared", 0)
	// Empty commits still apply the delete action, and later inserts do not inherit
	// a stale cleanup marker from the previous commit.
	exec("BEGIN;COMMIT;BEGIN;INSERT INTO cleared VALUES(4,'visible-in-tx')")
	count("cleared", 1)
	exec("COMMIT")
	count("cleared", 0)
	for _, dialect := range []sql.Dialect{sql.DialectNative, sql.DialectMySQL} {
		other, e := d.NewSession()
		if e != nil {
			t.Fatal(e)
		}
		other.SetDialect(dialect)
		if _, e = other.Exec("CREATE TEMPORARY TABLE unsupported(id INTEGER) ON COMMIT DELETE ROWS"); e == nil {
			t.Fatalf("%s accepted PG commit action", dialect)
		}
		other.Close()
	}
	if _, err = s.Exec("CREATE TABLE unsupported(id INTEGER) ON COMMIT DROP"); err == nil {
		t.Fatal("permanent table accepted ON COMMIT")
	}
}

func TestPostgresTemporaryCommitActionsRemainWALless(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	s, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.SetDialect(sql.DialectPostgreSQL)
	run := func(q string) {
		t.Helper()
		if _, e := s.Exec(q); e != nil {
			t.Fatalf("%s: %v", q, e)
		}
	}
	run("CREATE TABLE durable(id INTEGER PRIMARY KEY,v TEXT)")
	run("CREATE TEMP TABLE ephemeral(id INTEGER PRIMARY KEY,v TEXT,KEY ev(v)) ON COMMIT DELETE ROWS")
	run("BEGIN;INSERT INTO durable VALUES(1,'durable-commit-policy-marker');INSERT INTO ephemeral VALUES(1,'private-commit-policy-marker');COMMIT")
	if n := temporaryLSMCount(t, d); n != 0 {
		t.Fatalf("private rows/indexes after commit: %d", n)
	}
	entries, e := os.ReadDir(dir)
	if e != nil {
		t.Fatal(e)
	}
	durable := false
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "WAL-") {
			continue
		}
		b, e := os.ReadFile(filepath.Join(dir, entry.Name()))
		if e != nil {
			t.Fatal(e)
		}
		if bytes.Contains(b, []byte("private-commit-policy-marker")) {
			t.Fatal("temporary mutation leaked into WAL")
		}
		durable = durable || bytes.Contains(b, []byte("durable-commit-policy-marker"))
	}
	if !durable {
		t.Fatal("mixed transaction did not publish durable mutation")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := d.Exec("SELECT v FROM durable")
	if err != nil || len(r.Rows) != 1 || r.Rows[0]["v"].Text != "durable-commit-policy-marker" {
		t.Fatalf("reopen: %v %v", r, err)
	}
	if _, err = d.Exec("SELECT * FROM ephemeral"); err == nil {
		t.Fatal("temporary definition persisted")
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}
