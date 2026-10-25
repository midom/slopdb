package db_test

import (
	"context"
	"slopdb/db"
	"slopdb/sql"
	"testing"
)

func TestPostgresNativeCatalogs(t *testing.T) {
	path := t.TempDir()
	d, e := db.Open(path, nil)
	if e != nil {
		t.Fatal(e)
	}
	run := func(reopened bool) {
		s, e := d.NewSession()
		if e != nil {
			t.Fatal(e)
		}
		defer s.Close()
		if e = s.SetPostgresMode(true); e != nil {
			t.Fatal(e)
		}
		ctx := sql.WithPostgresSession(context.Background(), sql.PostgresSessionOptions{Database: "catalog_test", User: "tester"})
		exec := func(q string) *db.ExecResult {
			t.Helper()
			r, e := s.ExecContext(ctx, q)
			if e != nil {
				t.Fatalf("%s: %v", q, e)
			}
			return r
		}
		if !reopened {
			for _, q := range []string{"CREATE SCHEMA tenant", "CREATE TABLE tenant.parent (id BIGINT PRIMARY KEY, label VARCHAR(20) NOT NULL DEFAULT 'x', price DECIMAL(9,2))", "CREATE UNIQUE INDEX parent_label ON tenant.parent(label)", "CREATE TABLE tenant.child (id BIGINT PRIMARY KEY, parent_id BIGINT, CONSTRAINT child_parent FOREIGN KEY(parent_id) REFERENCES tenant.parent(id) ON DELETE CASCADE)"} {
				exec(q)
			}
		}
		r := exec("SELECT c.oid AS oid, n.nspname AS nspname, c.relname AS relname, c.relkind AS relkind, c.relam AS relam FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='tenant' AND c.relname='parent'")
		if len(r.Rows) != 1 || r.Rows[0]["relkind"].Text != "r" || r.Rows[0]["relam"].Int != 8000 {
			t.Fatalf("class: %+v", r.Rows)
		}
		oid := r.Rows[0]["oid"].Int
		r = exec("SELECT column_name, data_type, is_nullable, character_maximum_length, numeric_precision, numeric_scale, udt_name FROM information_schema.columns WHERE table_schema='tenant' AND table_name='parent' ORDER BY ordinal_position")
		if len(r.Rows) != 3 || r.Rows[0]["udt_name"].Text != "int8" || r.Rows[1]["character_maximum_length"].Int != 20 || r.Rows[2]["numeric_precision"].Int != 9 || r.Rows[2]["numeric_scale"].Int != 2 {
			t.Fatalf("columns: %+v", r.Rows)
		}
		r = exec("SELECT to_regclass('tenant.parent') AS id, format_type(1700, 589830) AS typ, to_regclass('missing') AS absent")
		if r.Rows[0]["id"].Int != oid || r.Rows[0]["typ"].Text != "numeric(9,2)" || !r.Rows[0]["absent"].Null {
			t.Fatalf("helpers: %+v", r.Rows)
		}
		r = exec("SELECT constraint_name, delete_rule FROM information_schema.referential_constraints WHERE constraint_name='child_parent'")
		if len(r.Rows) != 1 || r.Rows[0]["delete_rule"].Text != "CASCADE" {
			t.Fatalf("foreign keys: %+v", r.Rows)
		}
		exec("SET search_path TO tenant, public")
		r = exec("SELECT pg_table_is_visible(to_regclass('tenant.parent')) AS visible")
		if r.Rows[0]["visible"].Int != 1 {
			t.Fatalf("visible: %+v", r.Rows)
		}
		exec("BEGIN")
		exec("CREATE TABLE tenant.uncommitted (id BIGINT)")
		r = exec("SELECT relname FROM pg_class WHERE relname='uncommitted'")
		if len(r.Rows) != 1 {
			t.Fatal("own catalog writes absent")
		}
		exec("ROLLBACK")
		r = exec("SELECT relname FROM pg_class WHERE relname='uncommitted'")
		if len(r.Rows) != 0 {
			t.Fatal("rolled back catalog leaked")
		}
		for _, name := range []string{"pg_namespace", "pg_class", "pg_attribute", "pg_type", "pg_index", "pg_constraint", "pg_attrdef", "pg_tables", "pg_indexes", "pg_views", "pg_database", "pg_am", "information_schema.schemata", "information_schema.tables", "information_schema.columns", "information_schema.views", "information_schema.table_constraints", "information_schema.key_column_usage", "information_schema.referential_constraints"} {
			if p, e := s.PrepareContext(ctx, "SELECT * FROM "+name+" LIMIT 0"); e != nil {
				t.Fatalf("prepare %s: %v", name, e)
			} else {
				p.Close()
			}
			exec("SELECT * FROM " + name + " LIMIT 0")
		}
	}
	run(false)
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
	d, e = db.Open(path, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	run(true)
}
