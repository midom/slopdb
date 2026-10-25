package postgresserver

import (
	"context"
	"github.com/jackc/pgx/v5"
	"testing"
)

func TestPostgreSQLSystemCatalogAndFunctionMetadata(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	ctx := context.Background()
	c, e := pgx.Connect(ctx, s.dsn("slopdb", "", "slopdb"))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close(ctx)
	for _, q := range []string{"CREATE SCHEMA tenant", "CREATE TABLE tenant.account (id BIGINT PRIMARY KEY, email VARCHAR(80) NOT NULL, amount NUMERIC(9,2) DEFAULT 1.25)", "CREATE UNIQUE INDEX account_email ON tenant.account(email)"} {
		if _, e = c.Exec(ctx, q); e != nil {
			t.Fatal(e)
		}
	}
	q := "SELECT c.oid AS oid, n.nspname AS schema_name, c.relname AS table_name FROM pg_catalog.pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='tenant' AND c.relkind='r'"
	p, e := c.Prepare(ctx, "catalog", q)
	if e != nil {
		t.Fatal(e)
	}
	if len(p.Fields) != 3 {
		t.Fatalf("fields: %+v", p.Fields)
	}
	var oid int64
	var ns, name string
	if e = c.QueryRow(ctx, q).Scan(&oid, &ns, &name); e != nil {
		t.Fatal(e)
	}
	if oid == 0 || ns != "tenant" || name != "account" {
		t.Fatalf("%d %s %s", oid, ns, name)
	}
	var visible bool
	if e = c.QueryRow(ctx, "SELECT pg_table_is_visible(to_regclass('tenant.account'))").Scan(&visible); e != nil {
		t.Fatal(e)
	}
	if visible {
		t.Fatal("tenant table unexpectedly in public search path")
	}
	if _, e = c.Exec(ctx, "SET search_path TO tenant, public"); e != nil {
		t.Fatal(e)
	}
	if e = c.QueryRow(ctx, "SELECT pg_table_is_visible(to_regclass('tenant.account'))").Scan(&visible); e != nil || !visible {
		t.Fatalf("visible %v %v", visible, e)
	}
	var typ string
	if e = c.QueryRow(ctx, "SELECT format_type(1700,589830)").Scan(&typ); e != nil || typ != "numeric(9,2)" {
		t.Fatalf("type %s %v", typ, e)
	}
	p, e = c.Prepare(ctx, "types", "SELECT quote_nullable(NULL), jsonb_build_object('id',1), format_type(23,-1), pg_table_is_visible(to_regclass('tenant.account'))")
	if e != nil {
		t.Fatal(e)
	}
	want := []uint32{25, 3802, 25, 16}
	for i, f := range p.Fields {
		if f.DataTypeOID != want[i] {
			t.Fatalf("field %d oid %d want %d", i, f.DataTypeOID, want[i])
		}
	}
	var found int64
	if e = c.QueryRow(ctx, "SELECT count(*) FROM information_schema.columns WHERE table_schema='tenant' AND table_name='account'").Scan(&found); e != nil || found != 3 {
		t.Fatalf("columns %d %v", found, e)
	}
}
