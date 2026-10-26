package mysqlserver

import (
	"reflect"
	"testing"
)

func TestMySQLDeclaredColumnSpelling(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	for _, q := range []string{"CREATE TABLE entries (ID bigint PRIMARY KEY, title TEXT)", "INSERT INTO entries VALUES(1,'title')"} {
		if _, err := pool.Exec(q); err != nil {
			t.Fatal(q, err)
		}
	}
	check := func(q string, want []string) {
		t.Helper()
		rows, err := pool.Query(q)
		if err != nil {
			t.Fatal(q, err)
		}
		defer rows.Close()
		got, err := rows.Columns()
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: columns %q want %q: %v", q, got, want, err)
		}
	}
	check("SELECT * FROM entries", []string{"ID", "title"})
	check("SELECT ID FROM entries", []string{"ID"})
	check("SELECT ID AS renamed FROM entries", []string{"renamed"})
	check("SELECT ID,ID FROM entries", []string{"ID", "ID"})
	stmt, err := pool.Prepare("SELECT * FROM entries WHERE ID=?")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := stmt.Query(1)
	if err != nil {
		t.Fatal(err)
	}
	cols, err := rows.Columns()
	rows.Close()
	stmt.Close()
	if err != nil || !reflect.DeepEqual(cols, []string{"ID", "title"}) {
		t.Fatal(cols, err)
	}
	for _, q := range []string{"ALTER TABLE entries RENAME COLUMN ID TO PostID", "ALTER TABLE entries ADD ExtraValue TEXT", "ALTER TABLE entries CHANGE ExtraValue FinalValue TEXT"} {
		if _, err := pool.Exec(q); err != nil {
			t.Fatal(q, err)
		}
	}
	check("SELECT * FROM entries", []string{"PostID", "title", "FinalValue"})
}
