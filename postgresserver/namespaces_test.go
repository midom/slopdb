package postgresserver

import (
	"context"
	"github.com/jackc/pgx/v5"
	"testing"
)

func TestPostgreSQLVirtualSchemasAndSearchPath(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	ctx := context.Background()
	c, err := pgx.Connect(ctx, s.dsn("slopdb", "", "slopdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	exec := func(q string) {
		t.Helper()
		if _, err := c.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec("CREATE SCHEMA alpha")
	exec("CREATE SCHEMA beta")
	exec("CREATE TABLE alpha.items(id INTEGER PRIMARY KEY,label TEXT)")
	exec("CREATE TABLE beta.items(id INTEGER PRIMARY KEY,label TEXT)")
	exec("INSERT INTO alpha.items VALUES(1,'alpha')")
	exec("INSERT INTO beta.items VALUES(1,'beta')")
	exec("SET search_path TO alpha, public")
	var label string
	if err = c.QueryRow(ctx, "SELECT label FROM items").Scan(&label); err != nil || label != "alpha" {
		t.Fatal(label, err)
	}
	if err = c.QueryRow(ctx, "SELECT current_schema()").Scan(&label); err != nil || label != "alpha" {
		t.Fatal(label, err)
	}
	exec("SET SESSION search_path TO beta,alpha,public")
	if err = c.QueryRow(ctx, "SELECT label FROM items").Scan(&label); err != nil || label != "beta" {
		t.Fatal(label, err)
	}
	if err = c.QueryRow(ctx, "SHOW search_path").Scan(&label); err != nil || label != "beta, alpha, public" {
		t.Fatal(label, err)
	}
	var a, b string
	if err = c.QueryRow(ctx, "SELECT a.label,b.label FROM alpha.items a JOIN beta.items b ON a.id=b.id").Scan(&a, &b); err != nil || a != "alpha" || b != "beta" {
		t.Fatal(a, b, err)
	}
}
