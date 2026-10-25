package sql

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestCatalogSnapshotOwnershipAndEvolution(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	if err := s.ExecuteStatement(func() error {
		_, e := s.ExecuteNamespace(&NamespaceStmt{Operation: "create", Names: []string{"tenant"}})
		return e
	}); err != nil {
		t.Fatal(err)
	}
	statisticsSQL(t, s, "CREATE TABLE tenant.items(id INTEGER PRIMARY KEY, label TEXT DEFAULT 'x')")
	statisticsSQL(t, s, "CREATE UNIQUE INDEX tenant.items_label ON tenant.items(label)")
	statisticsSQL(t, s, "CREATE VIEW tenant.labels AS SELECT label FROM tenant.items")
	before := s.CatalogSnapshot()
	r, ok := before.Relation("tenant.items")
	if !ok || r.Kind != "table" || r.Schema != "tenant" || len(r.Columns) != 2 || len(r.Indexes) == 0 {
		t.Fatalf("relation %+v", r)
	}
	r.Columns[0].Name = "changed"
	r.Definition.Columns[0].Name = "changed"
	r.Indexes[0].Column = "changed"
	again, _ := before.Relation("tenant.items")
	if again.Columns[0].Name != "id" || again.Definition.Columns[0].Name != "id" || again.Indexes[0].Column == "changed" {
		t.Fatal("owned catalog facts alias caller changes")
	}
	statisticsSQL(t, s, "ALTER TABLE tenant.items ADD COLUMN extra INTEGER")
	old, _ := before.Relation("tenant.items")
	after, _ := s.CatalogSnapshot().Relation("tenant.items")
	if len(old.Columns) != 2 || len(after.Columns) != 3 {
		t.Fatalf("snapshot evolved: old=%d new=%d", len(old.Columns), len(after.Columns))
	}
	v, ok := before.Relation("tenant.labels")
	if !ok || v.Kind != "view" || !strings.Contains(v.ViewSQL, "label") {
		t.Fatalf("view %+v", v)
	}
}
func TestCatalogMetadataDecodeAndStaticVirtualDescribe(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	statisticsSQL(t, s, "CREATE TABLE items(id INTEGER PRIMARY KEY)")
	tables, e := json.Marshal(s.tables)
	if e != nil {
		t.Fatal(e)
	}
	triggers, e := json.Marshal(map[string]*TriggerInfo{"audit": {Name: "audit"}})
	if e != nil {
		t.Fatal(e)
	}
	view, e := DecodeCatalogSnapshot(map[string][]byte{schemaPrefix: tables, schemaTrigPrefix: triggers})
	if e != nil {
		t.Fatal(e)
	}
	if len(view.TableNames()) != 1 || len(view.TriggerNames()) != 1 || view.TriggerNames()[0] != "audit" {
		t.Fatalf("decoded catalog %+v", view)
	}
	if _, e = DecodeCatalogSnapshot(map[string][]byte{schemaPrefix: []byte("{")}); e == nil {
		t.Fatal("malformed catalog ignored")
	}
	if e = s.SetDialect(DialectPostgreSQL); e != nil {
		t.Fatal(e)
	}
	// Data projection refuses an OID that cannot be represented. Describe only
	// needs the virtual relation's declared columns and must not build its rows.
	s.tables["items"].ID = 1 << 63
	st, e := Parse("SELECT relname FROM pg_catalog.pg_class")
	if e != nil {
		t.Fatal(e)
	}
	cols, e := s.Describe(st[0])
	if e != nil || len(cols) != 1 || cols[0].Name != "relname" {
		t.Fatalf("virtual Describe read catalog rows: %+v %v", cols, e)
	}
	if _, e = s.PostgresCatalogTable("pg_class"); e == nil {
		t.Fatal("unrepresentable OID unexpectedly built")
	}
}
func TestVirtualCatalogDialectRouting(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	statisticsSQL(t, s, "CREATE TABLE items(id INTEGER PRIMARY KEY)")
	s.SetContext(WithMySQLSession(context.Background(), MySQLSessionOptions{Database: "app"}))
	for _, dialect := range []Dialect{DialectNative, DialectMySQL, DialectPostgreSQL} {
		if e := s.SetDialect(dialect); e != nil {
			t.Fatal(e)
		}
		if dialect == DialectPostgreSQL {
			s.SetContext(WithPostgresSession(context.Background(), PostgresSessionOptions{Database: "app"}))
		} else {
			s.SetContext(WithMySQLSession(context.Background(), MySQLSessionOptions{Database: "app"}))
		}
		stmt, e := Parse("SELECT table_name FROM information_schema.tables WHERE table_name='items'")
		if e != nil {
			t.Fatal(e)
		}
		r, e := s.Select(stmt[0].(*SelectStmt))
		if e != nil || len(r.Rows) != 1 || r.Rows[0]["table_name"].Text != "items" {
			t.Fatalf("dialect %v catalog %+v %v", dialect, r, e)
		}
		source := stmt[0].(*SelectStmt)
		if source.From != "information_schema.tables" || source.FromExpr != nil {
			t.Fatal("virtual rewrite mutated prepared AST")
		}
	}
}
