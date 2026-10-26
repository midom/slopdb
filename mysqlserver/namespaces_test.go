package mysqlserver

import (
	"context"
	"os"
	"testing"
)

func TestMySQLVirtualDatabasesShareStorageAndTransactions(t *testing.T) {
	path := t.TempDir()
	s := accountServer(t, path)
	pool := s.open(t, "owner", "owner-password", "compat")
	ctx := context.Background()
	c, err := pool.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	exec := func(q string) {
		t.Helper()
		if _, err := c.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec("CREATE TABLE items(id INT PRIMARY KEY,v TEXT)")
	exec("INSERT INTO items VALUES(1,'root')")
	exec("CREATE DATABASE extra")
	exec("CREATE DATABASE second")
	exec("CREATE TABLE extra.items(id INT PRIMARY KEY,v TEXT)")
	exec("INSERT INTO extra.items VALUES(1,'extra')")
	exec("CREATE TABLE second.items(id INT PRIMARY KEY,v TEXT)")
	exec("INSERT INTO second.items VALUES(1,'second')")
	prepared, err := c.PrepareContext(ctx, "SELECT v FROM items WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	exec("USE extra")
	var value string
	if err = c.QueryRowContext(ctx, "SELECT v FROM items").Scan(&value); err != nil || value != "extra" {
		t.Fatal(value, err)
	}
	if err = prepared.QueryRowContext(ctx, 1).Scan(&value); err != nil || value != "root" {
		t.Fatal("prepared changed default", value, err)
	}
	other, err := c.PrepareContext(ctx, "SELECT v FROM items WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	exec("USE second")
	if err = other.QueryRowContext(ctx, 1).Scan(&value); err != nil || value != "extra" {
		t.Fatal("prepared namespace changed", value, err)
	}
	exec("BEGIN")
	exec("UPDATE items SET v='changed'")
	exec("USE extra")
	exec("UPDATE items SET v='changed'")
	exec("ROLLBACK")
	if err = c.QueryRowContext(ctx, "SELECT v FROM extra.items").Scan(&value); err != nil || value != "extra" {
		t.Fatal(value, err)
	}
	if err = c.QueryRowContext(ctx, "SELECT v FROM second.items").Scan(&value); err != nil || value != "second" {
		t.Fatal(value, err)
	}
	var a, b string
	if err = c.QueryRowContext(ctx, "SELECT a.v,b.v FROM extra.items a JOIN second.items b ON a.id=b.id").Scan(&a, &b); err != nil || a != "extra" || b != "second" {
		t.Fatal(a, b, err)
	}
	var n int
	if err = c.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='extra' AND table_name='items'").Scan(&n); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() == "mysql-databases" {
			t.Fatal("virtual database allocated another physical database")
		}
	}
}
