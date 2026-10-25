package db

import (
	"context"
	"strings"
	"testing"

	"slopdb/sql"
)

func TestCDCMySQLDefinitionsAndRowPreservingAlter(t *testing.T) {
	d, e := Open(t.TempDir(), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	ctx := context.Background()
	if _, e = d.EnableCDC(ctx); e != nil {
		t.Fatal(e)
	}
	s := isolationSession(t, d)
	isolationExec(t, s, "CREATE SCHEMA tenant;CREATE TABLE tenant.parent(id BIGINT UNSIGNED PRIMARY KEY);CREATE TABLE tenant.child(id INTEGER PRIMARY KEY AUTOINCREMENT,p BIGINT UNSIGNED,v VARCHAR(20) DEFAULT 'a', UNIQUE KEY u(v(4)), CONSTRAINT fk FOREIGN KEY(p) REFERENCES tenant.parent(id) ON UPDATE CASCADE ON DELETE SET NULL)")
	reader, e := d.NewCDCReader(ctx, 0)
	if e != nil {
		t.Fatal(e)
	}
	rows, e := reader.Read(ctx, 100)
	if e != nil {
		t.Fatal(e)
	}
	if len(rows[0].CreatedNamespaces) != 1 || rows[0].CreatedNamespaces[0] != "tenant" {
		t.Fatal(rows)
	}
	table := rows[len(rows)-1].SchemaChanges[0].After
	ddl := CDCMySQLCreate(*table, "root")
	for _, want := range []string{"BIGINT", "AUTO_INCREMENT", "UNIQUE KEY `u` (`v`(4))", "REFERENCES `tenant`.`parent` (`id`)", "ON DELETE SET NULL", "ON UPDATE CASCADE"} {
		if !strings.Contains(ddl, want) {
			t.Fatalf("missing%s: %s", want, ddl)
		}
	}
	isolationExec(t, s, "ALTER TABLE tenant.child RENAME COLUMN v TO title")
	rows, e = reader.Read(ctx, 100)
	if e != nil || len(rows) != 1 {
		t.Fatal(rows, e)
	}
	queries, e := CDCMySQLAlter(*rows[0].SchemaChanges[0].Before, *rows[0].SchemaChanges[0].After, "root")
	if e != nil {
		t.Fatal(e)
	}
	combined := strings.Join(queries, ";")
	if !strings.Contains(combined, "CHANGE COLUMN `v` `title`") || strings.Contains(combined, "DROP TABLE") || strings.Contains(combined, "CREATE TABLE") {
		t.Fatal(queries)
	}
	isolationExec(t, s, "DROP SCHEMA tenant CASCADE")
	rows, e = reader.Read(ctx, 100)
	if e != nil || len(rows) != 1 || len(rows[0].DroppedNamespaces) != 1 || rows[0].DroppedNamespaces[0] != "tenant" {
		t.Fatal(rows, e)
	}
}

func TestCDCMySQLUnboundedTypesAndAmbiguousAlter(t *testing.T) {
	before := CDCTable{ID: 1, Schema: "main", Name: "values", Columns: []sql.ColumnDef{
		{Name: "a", ColType: sql.ColText}, {Name: "b", ColType: sql.ColBlob},
	}}
	definition := CDCMySQLCreate(before, "source")
	for _, expected := range []string{"`source`.`values`", "`a` LONGTEXT", "`b` LONGBLOB", "COLLATE utf8mb4_bin"} {
		if !strings.Contains(definition, expected) {
			t.Fatalf("missing %q: %s", expected, definition)
		}
	}
	after := before
	after.Columns = []sql.ColumnDef{{Name: "new_a", ColType: sql.ColInteger}, before.Columns[1]}
	if statements, err := CDCMySQLAlter(before, after, "source"); err == nil {
		t.Fatalf("ambiguous rename/type change emitted potentially destructive SQL: %v", statements)
	}
}
