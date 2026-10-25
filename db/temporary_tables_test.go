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
	mysqlTypeExec(t, s, "INSERT INTO scratch(id,v) VALUES(100,100)")
	mysqlTypeExec(t, s, "DELETE FROM scratch WHERE id=100")
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
	mysqlTypeExec(t, s, "INSERT INTO scratch(v) VALUES(50)")
	r = mysqlTypeExec(t, s, "SELECT id FROM scratch WHERE v=50")
	if len(r.Rows) != 1 || r.Rows[0]["id"].Int <= 100 {
		t.Fatalf("temporary explicit ID floor: %+v", r.Rows)
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

func TestMySQLTemporaryLikeRenamePreparedAndIndexLifecycle(t *testing.T) {
	d, err := db.Open(t.TempDir(), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	a, b := mysqlTypeSession(t, d), mysqlTypeSession(t, d)
	mysqlTypeExec(t, a, "CREATE TABLE original(id INTEGER PRIMARY KEY AUTO_INCREMENT,b BINARY(3),e ENUM('z','a'),KEY named(b(2)))")
	mysqlTypeExec(t, a, "INSERT INTO original(b,e) VALUES('old','z')")
	prepared, err := a.Prepare("SELECT b,e+0 AS ordinal FROM original WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if _, err = prepared.Exec(1); err != nil {
		t.Fatal(err)
	}
	mysqlTypeExec(t, a, "CREATE TEMPORARY TABLE IF NOT EXISTS original LIKE original")
	mysqlTypeExec(t, a, "INSERT INTO original(b,e) VALUES('new',2)")
	r, err := prepared.Exec(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || string(r.Rows[0]["b"].Blob) != "new" || r.Rows[0]["ordinal"].Int != 2 {
		t.Fatalf("prepared shadow read: %+v", r.Rows)
	}
	mysqlTypeExec(t, a, "CREATE TEMPORARY TABLE IF NOT EXISTS original LIKE missing_source")
	mysqlTypeExec(t, a, "DROP INDEX named ON original")
	mysqlTypeExec(t, a, "CREATE INDEX named ON original(e)")
	mysqlTypeExec(t, a, "ALTER TABLE original ADD COLUMN n INTEGER DEFAULT 5")
	mysqlTypeExec(t, a, "ALTER TABLE original RENAME TO renamed")
	r = mysqlTypeExec(t, a, "SELECT b,n FROM renamed WHERE e='a'")
	if len(r.Rows) != 1 || string(r.Rows[0]["b"].Blob) != "new" || r.Rows[0]["n"].Int != 5 {
		t.Fatalf("renamed temporary data: %+v", r.Rows)
	}
	r, err = prepared.Exec(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || string(r.Rows[0]["b"].Blob) != "old" {
		t.Fatalf("prepared restored permanent: %+v", r.Rows)
	}
	r = mysqlTypeExec(t, b, "SELECT b FROM original")
	if len(r.Rows) != 1 || string(r.Rows[0]["b"].Blob) != "old" {
		t.Fatalf("permanent source changed: %+v", r.Rows)
	}
	if _, err = b.Exec("SELECT * FROM renamed"); err == nil {
		t.Fatal("renamed temporary table leaked")
	}
	mysqlTypeExec(t, a, "CREATE TABLE permanent_after_index_drop(id INTEGER PRIMARY KEY)")
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
	mysqlTypeExec(t, a, "DROP TEMPORARY TABLE renamed")
}

func TestMySQLTemporaryForeignKeysRejectedAndDDLFailureAtomic(t *testing.T) {
	d, err := db.Open(t.TempDir(), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	a, b := mysqlTypeSession(t, d), mysqlTypeSession(t, d)
	mysqlTypeExec(t, a, "CREATE TABLE parent(id INTEGER PRIMARY KEY)")
	mysqlTypeExec(t, a, "CREATE TABLE child(id INTEGER PRIMARY KEY,parent_id INTEGER REFERENCES parent(id))")
	if _, err = a.Exec("CREATE TEMPORARY TABLE parent(id INTEGER PRIMARY KEY)"); err == nil {
		t.Fatal("temporary table shadowed FK parent")
	}
	mysqlTypeExec(t, a, "CREATE TEMPORARY TABLE scratch(id INTEGER PRIMARY KEY,v INTEGER)")
	if _, err = a.Exec("CREATE TABLE bad_child(id INTEGER PRIMARY KEY,parent_id INTEGER REFERENCES scratch(id))"); err == nil {
		t.Fatal("persistent FK referenced temporary table")
	}
	mysqlTypeExec(t, a, "INSERT INTO scratch VALUES(1,10),(2,10)")
	if _, err = a.Exec("ALTER TABLE scratch ADD UNIQUE INDEX bad_unique(v)"); err == nil {
		t.Fatal("duplicate unique index succeeded")
	}
	mysqlTypeExec(t, a, "INSERT INTO scratch VALUES(3,10)")
	mysqlTypeExec(t, b, "CREATE TABLE independent(id INTEGER PRIMARY KEY)")
	if r := mysqlTypeExec(t, a, "SELECT id FROM scratch ORDER BY id"); len(r.Rows) != 3 {
		t.Fatalf("failed DDL changed temporary data: %+v", r.Rows)
	}
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLTemporaryAutoCountersNeverEnterDurableGroupCommit(t *testing.T) {
	path := t.TempDir()
	options := storage.DefaultOptions()
	options.DisableWAL, options.DisableWALSync = false, false
	d, err := db.Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { d.Close() }()
	a, b := mysqlTypeSession(t, d), mysqlTypeSession(t, d)
	mysqlTypeExec(t, a, "CREATE TABLE shadowed(id INTEGER PRIMARY KEY AUTO_INCREMENT,v INTEGER)")
	mysqlTypeExec(t, a, "INSERT INTO shadowed(v) VALUES(10)")
	mysqlTypeExec(t, a, "CREATE TABLE durable(id INTEGER PRIMARY KEY AUTO_INCREMENT)")
	mysqlTypeExec(t, a, "CREATE TEMPORARY TABLE shadowed(id INTEGER PRIMARY KEY AUTO_INCREMENT,v INTEGER)")
	mysqlTypeExec(t, a, "INSERT INTO shadowed VALUES(1000,1000)")
	mysqlTypeExec(t, a, "BEGIN")
	mysqlTypeExec(t, a, "INSERT INTO shadowed(v) VALUES(1001)")
	mysqlTypeExec(t, a, "INSERT INTO durable DEFAULT VALUES")
	mysqlTypeExec(t, a, "COMMIT")
	mysqlTypeExec(t, a, "INSERT INTO durable DEFAULT VALUES")
	r := mysqlTypeExec(t, b, "INSERT INTO shadowed(v) VALUES(20)")
	if r.LastID != 2 {
		t.Fatalf("temporary counter reached permanent table: %+v", r)
	}
	mysqlTypeExec(t, b, "DELETE FROM shadowed WHERE id=2")
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = db.Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	a = mysqlTypeSession(t, d)
	r = mysqlTypeExec(t, a, "INSERT INTO shadowed(v) VALUES(30)")
	if r.LastID != 3 {
		t.Fatalf("temporary counter persisted or durable floor lost: %+v", r)
	}
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
}
