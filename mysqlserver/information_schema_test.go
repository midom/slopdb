package mysqlserver

import (
	"context"
	"testing"
)

func TestMySQLCanonicalCatalogPreparedWire(t *testing.T) {
	server := startTestServer(t, "tcp", Config{})
	pool := server.open(t, "slopdb", "", "slopdb")
	ctx := context.Background()
	c, e := pool.Conn(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	for _, q := range []string{"CREATE TABLE catalog_parent(id BIGINT PRIMARY KEY)", "CREATE TABLE catalog_child(id BIGINT PRIMARY KEY,parent_id BIGINT,CONSTRAINT catalog_fk FOREIGN KEY(parent_id) REFERENCES catalog_parent(id))", "CREATE INDEX catalog_child_parent ON catalog_child(parent_id)"} {
		if _, e = c.ExecContext(ctx, q); e != nil {
			t.Fatal(e)
		}
	}
	prepared, e := c.PrepareContext(ctx, "SELECT ordinal_position,column_name FROM information_schema.columns WHERE table_schema=? AND table_name=? ORDER BY ordinal_position")
	if e != nil {
		t.Fatal(e)
	}
	defer prepared.Close()
	rows, e := prepared.QueryContext(ctx, "slopdb", "catalog_child")
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
	if e = rows.Err(); e != nil {
		t.Fatal(e)
	}
	rows.Close()
	if count != 2 {
		t.Fatal(count)
	}
	var target, column string
	if e = c.QueryRowContext(ctx, "SELECT referenced_table_name,referenced_column_name FROM information_schema.key_column_usage WHERE constraint_name='catalog_fk'").Scan(&target, &column); e != nil || target != "catalog_parent" || column != "id" {
		t.Fatal(target, column, e)
	}
	var engine string
	if e = c.QueryRowContext(ctx, "SELECT engine FROM information_schema.engines WHERE support='DEFAULT'").Scan(&engine); e != nil || engine != "LSM" {
		t.Fatal(engine, e)
	}
	preparedShow, e := c.PrepareContext(ctx, "SHOW FULL COLUMNS FROM catalog_child")
	if e != nil {
		t.Fatal(e)
	}
	defer preparedShow.Close()
	showRows, e := preparedShow.QueryContext(ctx)
	if e != nil {
		t.Fatal(e)
	}
	labels, e := showRows.Columns()
	if e != nil || len(labels) != 9 || labels[2] != "Collation" || labels[7] != "Privileges" {
		t.Fatal(labels, e)
	}
	showRows.Close()
	preparedIndex, e := c.PrepareContext(ctx, "SHOW INDEX FROM catalog_child")
	if e != nil {
		t.Fatal(e)
	}
	defer preparedIndex.Close()
	showRows, e = preparedIndex.QueryContext(ctx)
	if e != nil {
		t.Fatal(e)
	}
	metadata, e := showRows.ColumnTypes()
	if e != nil || len(metadata) != 15 || metadata[1].DatabaseTypeName() != "BIGINT" || metadata[3].DatabaseTypeName() != "BIGINT" {
		t.Fatal("wrong prepared SHOW numeric fields", e)
	}
	showRows.Close()

}
