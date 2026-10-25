package db

import (
	"slopdb/sql"
	"slopdb/storage"
	"testing"
)

func TestMySQLTableLocalIndexes(t *testing.T) {
	path := t.TempDir()
	for pass := 0; pass < 2; pass++ {
		opt := storage.DefaultOptions()
		opt.DisableWALSync = true
		d, err := Open(path, opt)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { d.Close() })
		s, err := d.NewSession()
		if err != nil {
			t.Fatal(err)
		}
		if err = s.SetDialect(sql.DialectMySQL); err != nil {
			t.Fatal(err)
		}
		exec := func(q string) {
			t.Helper()
			if _, err := s.Exec(q); err != nil {
				t.Fatal(q, err)
			}
		}
		if pass == 0 {
			for _, q := range []string{
				"CREATE TABLE wp_postmeta (ID bigint PRIMARY KEY, meta_key VARCHAR(255), KEY meta_key(meta_key(191)))",
				"CREATE TABLE wp_usermeta (ID bigint PRIMARY KEY, meta_key VARCHAR(255), KEY meta_key(meta_key(191)))",
				"INSERT INTO wp_postmeta VALUES(1,'hello')", "INSERT INTO wp_usermeta VALUES(2,'world')",
				"CREATE INDEX same ON wp_postmeta(meta_key)", "CREATE INDEX same ON wp_usermeta(meta_key)",
				"ALTER TABLE wp_postmeta ADD INDEX added(meta_key)", "ALTER TABLE wp_usermeta ADD INDEX added(meta_key)",
				"EXPLAIN ALTER TABLE wp_postmeta RENAME INDEX same TO renamed",
				"ALTER TABLE wp_postmeta RENAME INDEX same TO renamed", "DROP INDEX renamed ON wp_postmeta",
				"ALTER TABLE wp_postmeta RENAME TO wp_moved",
				"CREATE TABLE copied LIKE wp_usermeta",
				"CREATE TABLE wp_postmeta(ID bigint, meta_key TEXT, KEY meta_key(meta_key))",
				"CREATE DATABASE scoped",
				"CREATE TABLE scoped.first(ID bigint, meta_key TEXT, KEY meta_key(meta_key))",
				"CREATE TABLE scoped.second(ID bigint, meta_key TEXT, KEY meta_key(meta_key))",
			} {
				exec(q)
			}
			for _, q := range []string{"CREATE INDEX same ON wp_usermeta(ID)", "ALTER TABLE wp_usermeta RENAME INDEX same TO added", "ALTER TABLE wp_usermeta ADD INDEX meta_key(ID)", "CREATE TABLE bad(ID INT, KEY dup(ID), KEY dup(ID))"} {
				if _, err := s.Exec(q); err == nil {
					t.Fatal("accepted duplicate", q)
				}
			}
		}
		for _, table := range []string{"wp_moved", "wp_usermeta", "copied"} {
			r, err := s.Exec("SHOW INDEX FROM " + table)
			if err != nil {
				t.Fatal(err)
			}
			names := map[string]bool{}
			for _, row := range r.Rows {
				names[row["Key_name"].Text] = true
			}
			if !names["meta_key"] || !names["added"] {
				t.Fatal(table, r.Rows)
			}
		}
		r, err := s.Exec("SELECT ID FROM wp_usermeta WHERE meta_key='world'")
		if err != nil || len(r.Rows) != 1 || r.Rows[0]["id"].Int != 2 {
			t.Fatal(r, err)
		}
		if pass == 1 {
			exec("DROP INDEX same ON wp_usermeta")
			exec("ALTER TABLE wp_moved DROP INDEX meta_key")
			exec("CREATE INDEX meta_key ON wp_moved(meta_key)")
		}
		s.Close()
		if err = d.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
