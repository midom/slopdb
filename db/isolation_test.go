package db

import (
	"context"
	"slopdb/sql"
	"slopdb/storage"
	"testing"
)

func isolationSession(t *testing.T, d *Database) *Session {
	t.Helper()
	s, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
func isolationExec(t *testing.T, s *Session, q string) *ExecResult {
	t.Helper()
	r, e := s.Exec(q)
	if e != nil {
		t.Fatalf("%s: %v", q, e)
	}
	return r
}
func isolationValue(t *testing.T, s *Session, q string, want int64) {
	t.Helper()
	r := isolationExec(t, s, q)
	if len(r.Rows) != 1 || r.Rows[0]["v"].Int != want {
		t.Fatalf("%s: %+v want %d", q, r.Rows, want)
	}
}
func TestRepeatableReadConsistentAndCurrentReads(t *testing.T) {
	opt := storage.DefaultOptions()
	opt.DisableWALSync = true
	d, err := Open(t.TempDir(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	a, b := isolationSession(t, d), isolationSession(t, d)
	isolationExec(t, b, "CREATE TABLE records(id INTEGER PRIMARY KEY,v INTEGER,tag TEXT,KEY tag_idx(tag))")
	isolationExec(t, b, "INSERT INTO records VALUES(1,10,'a')")
	isolationExec(t, a, "BEGIN ISOLATION LEVEL REPEATABLE READ")
	isolationValue(t, a, "SELECT v FROM records WHERE id=1", 10)
	isolationExec(t, b, "UPDATE records SET v=20,tag='b' WHERE id=1;INSERT INTO records VALUES(2,30,'b')")
	for _, q := range []string{"SELECT v FROM records WHERE id=1", "SELECT v FROM records WHERE tag='a'", "SELECT SUM(v) AS v FROM records", "SELECT (SELECT v FROM records WHERE id=1) AS v"} {
		isolationValue(t, a, q, 10)
	}
	isolationValue(t, a, "SELECT v FROM records WHERE id=1 FOR UPDATE", 20)
	isolationValue(t, a, "SELECT v FROM records WHERE id=1", 10)
	if r := isolationExec(t, a, "UPDATE records SET v=v+1 WHERE v=20"); r.Changes != 1 {
		t.Fatal(r)
	}
	isolationValue(t, a, "SELECT v FROM records WHERE id=1", 21)
	isolationValue(t, a, "SELECT COUNT(*) AS v FROM records", 1)
	if r := isolationExec(t, a, "UPDATE records SET v=31 WHERE id=2"); r.Changes != 1 {
		t.Fatal(r)
	}
	isolationValue(t, a, "SELECT COUNT(*) AS v FROM records", 2)
	isolationExec(t, a, "COMMIT")
	isolationValue(t, b, "SELECT SUM(v) AS v FROM records", 52)
}
func TestRepeatableReadLazySnapshotExplicitSnapshotAndScope(t *testing.T) {
	d, e := Open(t.TempDir(), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	a, b := isolationSession(t, d), isolationSession(t, d)
	isolationExec(t, b, "CREATE TABLE records(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO records VALUES(1,10)")
	isolationExec(t, a, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;BEGIN;SELECT 1")
	isolationExec(t, b, "UPDATE records SET v=20")
	isolationValue(t, a, "SELECT v FROM records FOR UPDATE", 20)
	isolationExec(t, b, "INSERT INTO records VALUES(2,30)")
	isolationValue(t, a, "SELECT COUNT(*) AS v FROM records", 2)
	isolationExec(t, b, "INSERT INTO records VALUES(3,40)")
	isolationValue(t, a, "SELECT COUNT(*) AS v FROM records", 2)
	isolationExec(t, a, "ROLLBACK;START TRANSACTION WITH CONSISTENT SNAPSHOT")
	isolationExec(t, b, "UPDATE records SET v=21 WHERE id=1")
	isolationValue(t, a, "SELECT v FROM records WHERE id=1", 20)
	isolationExec(t, a, "COMMIT;SET TRANSACTION ISOLATION LEVEL READ COMMITTED;BEGIN")
	isolationValue(t, a, "SELECT v FROM records WHERE id=1", 21)
	isolationExec(t, b, "UPDATE records SET v=22 WHERE id=1")
	isolationValue(t, a, "SELECT v FROM records WHERE id=1", 22)
	if _, e = a.Exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"); e == nil {
		t.Fatal("changed active transaction characteristics")
	}
	isolationExec(t, a, "COMMIT;BEGIN")
	isolationValue(t, a, "SELECT v FROM records WHERE id=1", 22)
	isolationExec(t, b, "UPDATE records SET v=23 WHERE id=1")
	isolationValue(t, a, "SELECT v FROM records WHERE id=1", 22)
	isolationExec(t, a, "ROLLBACK")
	if _, e = a.Exec("BEGIN ISOLATION LEVEL SERIALIZABLE"); e == nil {
		t.Fatal("accepted unsupported isolation")
	}
	if e = a.SetIsolationLevel(sql.ReadCommitted); e != nil {
		t.Fatal(e)
	}
}
func TestRepeatableReadPreparedStreamingSavepointsAndCompaction(t *testing.T) {
	opt := storage.DefaultOptions()
	opt.DisableWALSync = true
	d, e := Open(t.TempDir(), opt)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	a, b := isolationSession(t, d), isolationSession(t, d)
	isolationExec(t, b, "CREATE TABLE records(id INTEGER PRIMARY KEY,v INTEGER,KEY value_idx(v));INSERT INTO records VALUES(1,10)")
	p, e := a.Prepare("SELECT v FROM records WHERE id=?")
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	isolationExec(t, a, "BEGIN ISOLATION LEVEL REPEATABLE READ")
	if _, e = p.Exec(1); e != nil {
		t.Fatal(e)
	}
	isolationExec(t, b, "UPDATE records SET v=20")
	if e = d.lsm.CompactRange(); e != nil {
		t.Fatal(e)
	}
	r, e := p.Exec(1)
	if e != nil || len(r.Rows) != 1 || r.Rows[0]["v"].Int != 10 {
		t.Fatal(r, e)
	}
	isolationExec(t, a, "SAVEPOINT before;UPDATE records SET v=21 WHERE id=1")
	isolationValue(t, a, "SELECT v FROM records", 21)
	isolationExec(t, a, "ROLLBACK TO before")
	isolationValue(t, a, "SELECT v FROM records", 10)
	rows, e := a.QueryContext(context.Background(), "SELECT v FROM records")
	if e != nil {
		t.Fatal(e)
	}
	if !rows.Next() {
		t.Fatal("missing stream row")
	}
	got, err := rows.Values()
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Int != 10 {
		t.Fatal(got)
	}
	if e = rows.Close(); e != nil {
		t.Fatal(e)
	}
	isolationExec(t, a, "COMMIT")
	r, e = p.Exec(1)
	if e != nil || r.Rows[0]["v"].Int != 20 {
		t.Fatal(r, e)
	}
}

func TestRepeatableReadNestedLockingReads(t *testing.T) {
	d, e := Open(t.TempDir(), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	a, b := isolationSession(t, d), isolationSession(t, d)
	isolationExec(t, b, "CREATE TABLE records(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO records VALUES(1,10)")
	isolationExec(t, a, "BEGIN ISOLATION LEVEL REPEATABLE READ")
	isolationValue(t, a, "SELECT v FROM records", 10)
	isolationExec(t, b, "UPDATE records SET v=20")
	r := isolationExec(t, a, "SELECT v,(SELECT v FROM records WHERE id=1 FOR UPDATE) AS latest FROM records")
	if len(r.Rows) != 1 || r.Rows[0]["v"].Int != 10 || r.Rows[0]["latest"].Int != 20 {
		t.Fatal(r.Rows)
	}
	r = isolationExec(t, a, "SELECT v,(SELECT v FROM records WHERE id=1) AS earlier,EXISTS(SELECT 1 FROM records WHERE v=10) AS matched FROM records FOR UPDATE")
	if len(r.Rows) != 1 || r.Rows[0]["v"].Int != 20 || r.Rows[0]["earlier"].Int != 10 || r.Rows[0]["matched"].Int != 1 {
		t.Fatal(r.Rows)
	}
	isolationExec(t, a, "ROLLBACK")
}
