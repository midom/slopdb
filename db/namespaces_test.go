package db_test

import (
	"slopdb/db"
	"slopdb/storage"
	"testing"
)

func TestVirtualSchemasSamePhysicalDatabase(t *testing.T) {
	path := t.TempDir()
	d, err := db.Open(path, storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	exec := func(q string) *db.ExecResult {
		t.Helper()
		r, e := d.Exec(q)
		if e != nil {
			t.Fatalf("%s: %v", q, e)
		}
		return r
	}
	exec("CREATE SCHEMA a; CREATE SCHEMA b; CREATE TABLE a.items(id INTEGER PRIMARY KEY,v TEXT);CREATE TABLE b.items(id INTEGER PRIMARY KEY,v TEXT)")
	exec("INSERT INTO a.items VALUES(1,'a');INSERT INTO b.items VALUES(1,'b')")
	exec("USE a")
	if r := exec("SELECT v FROM items"); len(r.Rows) != 1 || r.Rows[0]["v"].Text != "a" {
		t.Fatal(r)
	}
	stream, err := d.Query("SELECT v FROM items")
	if err != nil {
		t.Fatal(err)
	}
	if !stream.Next() {
		t.Fatal(stream.Err())
	}
	var streamed string
	if err = stream.Scan(&streamed); err != nil || streamed != "a" {
		t.Fatal(streamed, err)
	}
	if err = stream.Close(); err != nil {
		t.Fatal(err)
	}

	exec("SET search_path TO b,a")
	if r := exec("SELECT v FROM items"); len(r.Rows) != 1 || r.Rows[0]["v"].Text != "b" {
		t.Fatal(r)
	}
	r := exec("SELECT a.v AS av,b.v AS bv FROM a.items a JOIN b.items b ON a.id=b.id")
	if len(r.Rows) != 1 || r.Rows[0]["av"].Text != "a" || r.Rows[0]["bv"].Text != "b" {
		t.Fatal(r)
	}
	exec("BEGIN; UPDATE a.items SET v='changed';USE b;UPDATE items SET v='changed';ROLLBACK")
	if r := exec("SELECT v FROM a.items"); r.Rows[0]["v"].Text != "a" {
		t.Fatal(r)
	}
	if r := exec("SELECT v FROM b.items"); r.Rows[0]["v"].Text != "b" {
		t.Fatal(r)
	}
	exec("CREATE INDEX a.same_name ON a.items(v)")
	exec("CREATE INDEX b.same_name ON b.items(v)")
	exec("ALTER TABLE a.items ADD INDEX added(v)")
	exec("ALTER TABLE b.items ADD INDEX added(v)")
	exec("ALTER TABLE a.items DROP INDEX added")
	if r := exec("SELECT a.items.v AS av FROM a.items"); len(r.Rows) != 1 || r.Rows[0]["av"].Text != "a" {
		t.Fatal(r)
	}
	if r := exec("SHOW TABLES FROM b"); len(r.Rows) != 1 {
		t.Fatal(r)
	}

	exec("CREATE VIEW a.names AS SELECT v FROM a.items")
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = db.Open(path, storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	exec("USE a")
	if r := exec("SELECT v FROM names"); len(r.Rows) != 1 || r.Rows[0]["v"].Text != "a" {
		t.Fatal(r)
	}
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}
