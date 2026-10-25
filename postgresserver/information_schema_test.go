package postgresserver

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestPostgreSQLCanonicalInformationSchemaWire(t *testing.T) {
	server := startTestServer(t, "tcp", Config{})
	ctx := context.Background()
	c, e := pgx.Connect(ctx, server.dsn("slopdb", "", "slopdb"))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close(ctx)
	for _, q := range []string{"CREATE TABLE catalog_parent(id BIGINT PRIMARY KEY)", "CREATE TABLE catalog_child(id BIGINT PRIMARY KEY,parent_id BIGINT,CONSTRAINT catalog_fk FOREIGN KEY(parent_id) REFERENCES catalog_parent(id),CONSTRAINT positive_id CHECK(id>0))", "CREATE INDEX catalog_child_parent ON catalog_child(parent_id)", "CREATE VIEW catalog_view AS SELECT parent_id FROM catalog_child"} {
		if _, e = c.Exec(ctx, q); e != nil {
			t.Fatal(e)
		}
	}
	q := "SELECT ordinal_position,column_name FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2 ORDER BY ordinal_position"
	description, e := c.Prepare(ctx, "canonical_columns", q)
	if e != nil {
		t.Fatal(e)
	}
	if len(description.Fields) != 2 || description.Fields[0].DataTypeOID != 20 {
		t.Fatal(description)
	}
	rows, e := c.Query(ctx, "canonical_columns", "public", "catalog_child")
	if e != nil {
		t.Fatal(e)
	}
	count := 0
	for rows.Next() {
		var ordinal int64
		var name string
		if e = rows.Scan(&ordinal, &name); e != nil {
			t.Fatal(e)
		}
		count++
		if ordinal != int64(count) {
			t.Fatal(ordinal)
		}
	}
	rows.Close()
	if rows.Err() != nil || count != 2 {
		t.Fatal(count, rows.Err())
	}
	var target, column string
	if e = c.QueryRow(ctx, "SELECT referenced_table_name,referenced_column_name FROM information_schema.key_column_usage WHERE constraint_name='catalog_fk'").Scan(&target, &column); e != nil || target != "catalog_parent" || column != "id" {
		t.Fatal(target, column, e)
	}
	var clause string
	if e = c.QueryRow(ctx, "SELECT check_clause FROM information_schema.check_constraints WHERE constraint_name='positive_id'").Scan(&clause); e != nil || clause == "" {
		t.Fatal(clause, e)
	}
	var name string
	if e = c.QueryRow(ctx, "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='catalog_view'").Scan(&name); e != nil || name != "parent_id" {
		t.Fatal(name, e)
	}
}
