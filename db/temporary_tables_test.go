package db_test

import (
	"testing"

	"slopdb/db"
	"slopdb/storage"
)

func TestMySQLTemporaryTableIsolationShadowAndReopen(t *testing.T) {
	path := t.TempDir()
	d, err := db.Open(path, storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { d.Close() }()
	a, b := mysqlTypeSession(t, d), mysqlTypeSession(t, d)
	mysqlTypeExec(t, a, "CREATE TABLE data(id INTEGER PRIMARY KEY AUTO_INCREMENT,v VARCHAR(30),KEY value_idx(v))")
	mysqlTypeExec(t, a, "INSERT INTO data(v) VALUES('permanent')")
	mysqlTypeExec(t, a, "CREATE TEMPORARY TABLE data(id INTEGER PRIMARY KEY AUTO_INCREMENT,v VARCHAR(30),KEY value_idx(v))")
	mysqlTypeExec(t, a, "INSERT INTO data(v) VALUES('private'),('second')")
	for _, q := range []string{"SELECT v FROM data WHERE id=1", "SELECT v FROM data WHERE v='private'", "SELECT v FROM data WHERE id IN (1,99)"} {
		r := mysqlTypeExec(t, a, q)
		if len(r.Rows) != 1 || r.Rows[0]["v"].Text != "private" {
			t.Fatalf("temporary query %s: %+v", q, r.Rows)
		}
	}
	r := mysqlTypeExec(t, b, "SELECT id,v FROM data")
	if len(r.Rows) != 1 || r.Rows[0]["v"].Text != "permanent" {
		t.Fatalf("temporary leaked: %+v", r.Rows)
	}
	mysqlTypeExec(t, b, "CREATE TABLE other(id INTEGER PRIMARY KEY)")
	mysqlTypeExec(t, b, "INSERT INTO other VALUES(1)")
	r = mysqlTypeExec(t, a, "SELECT data.v FROM data JOIN other ON data.id=other.id")
	if len(r.Rows) != 1 || r.Rows[0]["data.v"].Text != "private" {
		t.Fatalf("temporary join: %+v", r.Rows)
	}
	mysqlTypeExec(t, a, "CREATE INDEX temp_extra ON data(v,id)")
	mysqlTypeExec(t, a, "INSERT INTO other VALUES(2)")
	mysqlTypeExec(t, a, "DROP TEMPORARY TABLE data")
	r = mysqlTypeExec(t, a, "SELECT v FROM data WHERE id=1")
	if len(r.Rows) != 1 || r.Rows[0]["v"].Text != "permanent" {
		t.Fatalf("shadow did not restore: %+v", r.Rows)
	}
	mysqlTypeExec(t, a, "CREATE TEMPORARY TABLE only_temp(id INTEGER PRIMARY KEY,v BINARY(3))")
	mysqlTypeExec(t, a, "INSERT INTO only_temp VALUES(1,'x')")
	mysqlTypeExec(t, a, "CREATE TABLE persistent_after(id INTEGER PRIMARY KEY)")
	mysqlTypeExec(t, a, "INSERT INTO persistent_after VALUES(7)")
	if _, err = b.Exec("SELECT * FROM only_temp"); err == nil {
		t.Fatal("another session saw private table")
	}
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = db.Open(path, storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	a = mysqlTypeSession(t, d)
	if _, err = a.Exec("SELECT * FROM only_temp"); err == nil {
		t.Fatal("temporary table persisted")
	}
	r = mysqlTypeExec(t, a, "SELECT id,v FROM data")
	if len(r.Rows) != 1 || r.Rows[0]["v"].Text != "permanent" {
		t.Fatalf("persistent shadow corrupted: %+v", r.Rows)
	}
	r = mysqlTypeExec(t, a, "SELECT id FROM persistent_after")
	if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 7 {
		t.Fatalf("persistent write lost: %+v", r.Rows)
	}
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLTemporaryDefinitionsSurviveRollbackRowsDoNot(t *testing.T) {
	d, err := db.Open(t.TempDir(), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s := mysqlTypeSession(t, d)
	mysqlTypeExec(t, s, "BEGIN")
	mysqlTypeExec(t, s, "CREATE TEMPORARY TABLE scratch(id INTEGER PRIMARY KEY AUTO_INCREMENT,v INTEGER UNIQUE)")
	mysqlTypeExec(t, s, "INSERT INTO scratch(v) VALUES(10)")
	mysqlTypeExec(t, s, "ROLLBACK")
	if r := mysqlTypeExec(t, s, "SELECT * FROM scratch"); len(r.Rows) != 0 {
		t.Fatalf("rows survived rollback: %+v", r.Rows)
	}
	mysqlTypeExec(t, s, "INSERT INTO scratch(v) VALUES(20)")
	mysqlTypeExec(t, s, "BEGIN")
	mysqlTypeExec(t, s, "UPDATE scratch SET v=30")
	mysqlTypeExec(t, s, "SAVEPOINT keep")
	mysqlTypeExec(t, s, "INSERT INTO scratch(v) VALUES(40)")
	mysqlTypeExec(t, s, "ROLLBACK TO keep")
	mysqlTypeExec(t, s, "COMMIT")
	r := mysqlTypeExec(t, s, "SELECT id,v FROM scratch")
	if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 2 || r.Rows[0]["v"].Int != 30 {
		t.Fatalf("temporary transactional rows: %+v", r.Rows)
	}
	mysqlTypeExec(t, s, "BEGIN")
	mysqlTypeExec(t, s, "DROP TEMPORARY TABLE scratch")
	mysqlTypeExec(t, s, "ROLLBACK")
	if _, err = s.Exec("SELECT * FROM scratch"); err == nil {
		t.Fatal("DROP temporary definition rolled back")
	}
	mysqlTypeExec(t, s, "CREATE TEMPORARY TABLE scratch(id INTEGER PRIMARY KEY AUTO_INCREMENT,v INTEGER)")
	mysqlTypeExec(t, s, "INSERT INTO scratch(v) VALUES(99)")
	r = mysqlTypeExec(t, s, "SELECT id FROM scratch")
	if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 1 {
		t.Fatalf("recreated temporary counter: %+v", r.Rows)
	}
}
