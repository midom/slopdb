package db

import (
	"context"
	"slopdb/sql"
	"testing"
)

func TestCrossDialectNamespacePolicy(t *testing.T) {
	d := projectionDatabase(t)
	session := func(dialect sql.Dialect) *Session {
		t.Helper()
		s, err := d.NewSession()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Close() })
		if err = s.SetDialect(dialect); err != nil {
			t.Fatal(err)
		}
		return s
	}
	mysql, pg, native := session(sql.DialectMySQL), session(sql.DialectPostgreSQL), session(sql.DialectNative)
	exec := func(s *Session, q string) {
		t.Helper()
		if _, err := s.Exec(q); err != nil {
			t.Fatal(q, err)
		}
	}
	value := func(s *Session, q string, want int64) {
		t.Helper()
		r, err := s.Exec(q)
		if err != nil {
			t.Fatal(q, err)
		}
		if len(r.Rows) != 1 || r.Rows[0][r.Columns[0].Name].Int != want {
			t.Fatal(q, r.Rows, want)
		}
	}
	exec(mysql, "CREATE TABLE marker(ID bigint)")
	exec(mysql, "INSERT INTO marker VALUES(1)")
	exec(mysql, "CREATE DATABASE public")
	exec(mysql, "CREATE TABLE public.marker(ID bigint)")
	exec(mysql, "INSERT INTO public.marker VALUES(2)")
	value(mysql, "SELECT ID FROM public.marker", 2)
	value(pg, "SELECT ID FROM public.marker", 1)
	value(native, "SELECT ID FROM public.marker", 1)
	p, err := mysql.Prepare("SELECT ID FROM marker WHERE ID>?")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	exec(mysql, "USE public")
	r, err := p.Exec(0)
	if err != nil || len(r.Rows) != 1 || r.Rows[0]["id"].Int != 2 {
		t.Fatal(r, err)
	}
	exec(mysql, "USE main")
	r, err = p.Exec(0)
	if err != nil || len(r.Rows) != 1 || r.Rows[0]["id"].Int != 1 {
		t.Fatal(r, err)
	}
	if err = mysql.RegisterRootSchemaAlias(context.Background(), "public"); err != nil {
		t.Fatal(err)
	}
	value(mysql, "SELECT ID FROM public.marker", 1)
	exec(pg, "CREATE SCHEMA shared")
	exec(pg, "CREATE TABLE shared.values_table (id bigint)")
	exec(pg, "INSERT INTO shared.values_table VALUES(3)")
	value(mysql, "SELECT id FROM shared.values_table", 3)
}
