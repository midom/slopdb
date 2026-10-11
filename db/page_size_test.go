package db_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"slopdb/db"
	"slopdb/storage"
)

func TestDatabasePageSizePragmaAPIAndReopen(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "database")
	options := storage.DefaultOptions()
	options.PageSize = 16 << 10
	d, err := db.Open(directory, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	check := func(want int64) {
		t.Helper()
		if d.PageSize() != want {
			t.Fatal(d.PageSize(), want)
		}
		result, e := d.Exec("PRAGMA page_size")
		if e != nil || len(result.Rows) != 1 || result.Rows[0]["page_size"].Int != want {
			t.Fatal(e, result)
		}
	}
	check(16 << 10)
	session, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if _, err = d.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT);INSERT INTO t VALUES(1,'before');VACUUM"); err != nil {
		t.Fatal(err)
	}
	if _, err = d.Exec("PRAGMA page_size(65536)"); err != nil {
		t.Fatal(err)
	}
	check(64 << 10)
	result, err := session.Exec("PRAGMA page_size")
	if err != nil || result.Rows[0]["page_size"].Int != 64<<10 {
		t.Fatal("session kept stale page size", err, result)
	}
	if err = d.SetPageSize(1 << 20); err != nil {
		t.Fatal(err)
	}
	check(1 << 20)
	if _, err = d.Exec("INSERT INTO t VALUES(2,'after');VACUUM"); err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(t.TempDir(), "backup")
	if err = d.Backup(backup); err != nil {
		t.Fatal(err)
	}
	restored, err := db.Open(backup, nil)
	if err != nil {
		t.Fatal(err)
	}
	if restored.PageSize() != 1<<20 {
		t.Fatal("backup lost current target", restored.PageSize())
	}
	if err = restored.Close(); err != nil {
		t.Fatal(err)
	}
	if err = session.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = db.Open(directory, nil)
	if err != nil {
		t.Fatal(err)
	}
	check(1 << 20)
	result, err = d.Exec("SELECT id,v FROM t ORDER BY id")
	if err != nil || len(result.Rows) != 2 || result.Rows[0]["v"].Text != "before" || result.Rows[1]["v"].Text != "after" {
		t.Fatal(err, result)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	options = storage.DefaultOptions()
	options.PageSize = 16 << 10
	d, err = db.Open(directory, options)
	if err != nil {
		t.Fatal(err)
	}
	check(16 << 10)
}
func TestDatabasePageSizeRejectsTransactionsAndInvalidValues(t *testing.T) {
	d := operationalDB(t)
	for _, statement := range []string{"PRAGMA page_size=-1", "PRAGMA page_size=0", "PRAGMA page_size=513", "PRAGMA page_size=4294967296", "PRAGMA page_size=9223372036854775807", "PRAGMA page_size=65536.0", "PRAGMA page_size='65536'", "PRAGMA page_size=NULL", "PRAGMA page_size(16384,65536)"} {
		if _, err := d.Exec(statement); err == nil {
			t.Fatal("invalid page target accepted", statement)
		}
		if d.PageSize() != 4096 {
			t.Fatal("invalid setter changed target", statement, d.PageSize())
		}
	}
	tx, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec("PRAGMA page_size=16384"); err == nil {
		t.Fatal("transaction changed physical configuration")
	}
	if _, err = tx.Exec("PRAGMA page_size(16384)"); err == nil {
		t.Fatal("transaction changed physical configuration with parentheses")
	}
	if _, err = tx.Exec("PRAGMA page_size"); err != nil {
		t.Fatal("transaction cannot read page size", err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err = d.Exec("BEGIN"); err != nil {
		t.Fatal(err)
	}
	if err = d.SetPageSize(16384); err == nil {
		t.Fatal("API changed physical configuration inside default transaction")
	}
	if _, err = d.Exec("ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = d.SetPageSizeContext(ctx, 16384); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if d.PageSize() != 4096 {
		t.Fatal("cancelled setter changed configuration")
	}
}
