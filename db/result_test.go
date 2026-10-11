package db_test

import (
	"path/filepath"
	"testing"

	"slopdb/db"
)

// Adapted from /tmp/godb/covering_ownership_review.go.
func TestResultOwnership(t *testing.T) {
	d, err := db.Open(filepath.Join(t.TempDir(), "database"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	run := func(query string) *db.ExecResult {
		t.Helper()
		r, err := d.Exec(query)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return r
	}
	run("CREATE TABLE t(id INTEGER PRIMARY KEY,b BLOB,hidden TEXT,INDEX ix(b)); INSERT INTO t VALUES(1,X'0001','x'),(2,X'0002','y')")
	first := run("SELECT id,b FROM t WHERE b=X'0001'")
	first.Rows[0]["b"].Blob[0] = 9
	delete(first.Rows[0], "id")
	second := run("SELECT b,id FROM t WHERE b=X'0001'")
	if len(second.Rows) != 1 || second.Rows[0]["id"].Int != 1 || string(second.Rows[0]["b"].Blob) != "\x00\x01" {
		t.Fatal("mutating a result changed the index or subsequent result")
	}
	run("UPDATE t SET b=X'0003' WHERE id=1")
	if string(second.Rows[0]["b"].Blob) != "\x00\x01" {
		t.Fatal("a later write reused an earlier result's blob")
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if second.Rows[0]["id"].Int != 1 || string(second.Rows[0]["b"].Blob) != "\x00\x01" {
		t.Fatal("closing the database invalidated caller-owned results")
	}
}
