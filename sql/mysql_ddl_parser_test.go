package sql

import "testing"

func TestMySQLTemporaryAndLikeParser(t *testing.T) {
	for _, text := range []string{"CREATE TEMPORARY TABLE tmp LIKE source", "CREATE TEMPORARY TABLE IF NOT EXISTS tmp (LIKE source)", "CREATE TABLE tmp LIKE source"} {
		parsed, err := Parse(text)
		if err != nil {
			t.Fatalf("%s: %v", text, err)
		}
		stmt := parsed[0].(*CreateTableStmt)
		if stmt.Table != "tmp" || stmt.Like != "source" {
			t.Fatalf("%s: %#v", text, stmt)
		}
		if stmt.Temporary != (text != "CREATE TABLE tmp LIKE source") {
			t.Fatalf("temporary flag: %s", text)
		}
	}
	parsed, err := Parse("DROP TEMPORARY TABLE IF EXISTS tmp")
	if err != nil {
		t.Fatal(err)
	}
	drop := parsed[0].(*DropTableStmt)
	if !drop.Temporary || !drop.IfExists || drop.Table != "tmp" {
		t.Fatalf("%#v", drop)
	}
	for _, text := range []string{"CREATE TEMPORARY INDEX x ON y(z)", "CREATE TABLE x LIKE", "CREATE TABLE x (LIKE y", "DROP TEMPORARY VIEW x"} {
		if _, err := Parse(text); err == nil {
			t.Errorf("accepted %s", text)
		}
	}
}
