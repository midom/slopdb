package db

import (
	"errors"
	"testing"

	"slopdb/sql"
)

func TestPostgresRepeatableReadSnapshotForConstantsAndMutations(t *testing.T) {
	for _, query := range []string{"UPDATE records SET v=v+1 WHERE id=1", "DELETE FROM records WHERE id=1", "SELECT v FROM records WHERE id=1 FOR UPDATE", "SELECT v FROM records WHERE id=1 FOR SHARE"} {
		t.Run(query, func(t *testing.T) {
			d, e := Open(t.TempDir(), nil)
			if e != nil {
				t.Fatal(e)
			}
			defer d.Close()
			a, b := isolationSession(t, d), isolationSession(t, d)
			if e = a.SetDialect(sql.DialectPostgreSQL); e != nil {
				t.Fatal(e)
			}
			isolationExec(t, b, "CREATE TABLE records(id INTEGER PRIMARY KEY,v INTEGER)")
			isolationExec(t, b, "INSERT INTO records VALUES(1,10)")
			isolationExec(t, a, "BEGIN ISOLATION LEVEL REPEATABLE READ")
			isolationExec(t, a, "SELECT 1") // PostgreSQL constants establish the snapshot.
			isolationExec(t, b, "UPDATE records SET v=20 WHERE id=1; INSERT INTO records VALUES(2,30)")
			isolationValue(t, a, "SELECT v FROM records WHERE id=1", 10)
			if r := isolationExec(t, a, "UPDATE records SET v=40 WHERE id=2"); r.Changes != 0 {
				t.Fatal("updated a row outside snapshot", r)
			}
			p, e := a.Prepare(query)
			if e != nil {
				t.Fatal(e)
			}
			defer p.Close()
			if _, e = p.Exec(); !errors.Is(e, ErrConflict) {
				t.Fatalf("snapshot conflict %v", e)
			}
			isolationExec(t, a, "ROLLBACK")
			isolationValue(t, b, "SELECT v FROM records WHERE id=1", 20)
		})
	}
}

func TestPostgresRepeatableReadOwnWritesAndNestedLocking(t *testing.T) {
	d, e := Open(t.TempDir(), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	a, b := isolationSession(t, d), isolationSession(t, d)
	if e = a.SetDialect(sql.DialectPostgreSQL); e != nil {
		t.Fatal(e)
	}
	isolationExec(t, b, "CREATE TABLE records(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO records VALUES(1,10)")
	isolationExec(t, a, "BEGIN ISOLATION LEVEL REPEATABLE READ;SAVEPOINT before_read")
	isolationExec(t, b, "UPDATE records SET v=15 WHERE id=1")
	isolationValue(t, a, "SELECT v FROM records WHERE id=1 FOR UPDATE", 15)
	isolationExec(t, b, "INSERT INTO records VALUES(2,25)")
	isolationExec(t, a, "UPDATE records SET v=16 WHERE id=1;INSERT INTO records VALUES(3,30)")
	isolationValue(t, a, "SELECT sum(v) AS v FROM records", 46)
	isolationValue(t, a, "SELECT (SELECT v FROM records WHERE id=1 FOR SHARE) AS v", 16)
	isolationExec(t, a, "ROLLBACK TO before_read")
	isolationValue(t, a, "SELECT sum(v) AS v FROM records", 15)
	isolationExec(t, a, "COMMIT")
	isolationValue(t, b, "SELECT sum(v) AS v FROM records", 40)
}
