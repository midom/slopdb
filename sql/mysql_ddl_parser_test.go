package sql

import (
	"strings"
	"testing"
)

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

func TestMySQLJoinedMutationParser(t *testing.T) {
	for _, text := range []string{
		"UPDATE a AS x JOIN b AS y ON x.id=y.id SET x.v=y.v",
		"DELETE x,y FROM a AS x JOIN b AS y USING(id)",
		"DELETE FROM x.*,y USING a AS x JOIN b AS y USING(id)",
	} {
		parsed, err := Parse(text)
		if err != nil {
			t.Fatalf("%s: %v", text, err)
		}
		formatted, err := formatDMLStatement(parsed[0])
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Parse(formatted); err != nil {
			t.Fatalf("round trip %s: %v", formatted, err)
		}
	}
	for _, text := range []string{"UPDATE a JOIN SET a.v=1", "UPDATE a SET a.=1", "DELETE x, FROM a x", "DELETE FROM x,y a JOIN b", "DELETE x FROM (SELECT 1) AS x"} {
		if _, err := Parse(text); err == nil {
			t.Errorf("accepted %s", text)
		}
	}
}

func TestMySQLJoinedMutationRename(t *testing.T) {
	for _, text := range []string{"UPDATE a AS x JOIN b AS y ON x.id=y.id SET x.v=y.v", "DELETE y FROM a AS x JOIN b AS y ON x.v=y.v"} {
		parsed, err := Parse(text)
		if err != nil {
			t.Fatal(err)
		}
		if err := renameDependencyStatement(parsed[0], "b", "b", "v", "renamed", false); err != nil {
			t.Fatal(err)
		}
		if err := renameDependencyStatement(parsed[0], "b", "new_b", "", "", false); err != nil {
			t.Fatal(err)
		}
		formatted, err := formatDMLStatement(parsed[0])
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(formatted, `"new_b"`) || !strings.Contains(formatted, `"y"."renamed"`) {
			t.Fatal(formatted)
		}
		if _, err := Parse(formatted); err != nil {
			t.Fatal(formatted, err)
		}
	}
}
