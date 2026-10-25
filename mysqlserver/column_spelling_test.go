package mysqlserver

import (
	"reflect"
	"testing"
)

func TestMySQLDeclaredColumnSpelling(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	for _, q := range []string{"CREATE TABLE wp_posts (ID bigint PRIMARY KEY, post_title TEXT)", "INSERT INTO wp_posts VALUES(1,'title')"} {
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
	check("SELECT * FROM wp_posts", []string{"ID", "post_title"})
	check("SELECT ID FROM wp_posts", []string{"ID"})
	check("SELECT ID AS renamed FROM wp_posts", []string{"renamed"})
	check("SELECT ID,ID FROM wp_posts", []string{"ID", "ID"})
	stmt, err := pool.Prepare("SELECT * FROM wp_posts WHERE ID=?")
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
	if err != nil || !reflect.DeepEqual(cols, []string{"ID", "post_title"}) {
		t.Fatal(cols, err)
	}
	for _, q := range []string{"ALTER TABLE wp_posts RENAME COLUMN ID TO PostID", "ALTER TABLE wp_posts ADD ExtraValue TEXT", "ALTER TABLE wp_posts CHANGE ExtraValue FinalValue TEXT"} {
		if _, err := pool.Exec(q); err != nil {
			t.Fatal(q, err)
		}
	}
	check("SELECT * FROM wp_posts", []string{"PostID", "post_title", "FinalValue"})
}
