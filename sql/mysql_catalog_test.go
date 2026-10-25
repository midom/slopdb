package sql

import (
	"context"
	"testing"
)

func TestMySQLVirtualCatalogUsesCurrentSchema(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	statisticsSQL(t, s, "CREATE TABLE actor(id INTEGER PRIMARY KEY,name TEXT)")
	s.SetContext(WithMySQLSession(context.Background(), MySQLSessionOptions{Database: "wiki", Schemas: []string{"wiki"}}))
	stmt := &SelectStmt{From: "information_schema.tables", Columns: []SelectColumn{{Expr: &ColumnRefExpr{Name: "table_name"}}}, Where: &BinaryExpr{Op: "=", Left: &ColumnRefExpr{Name: "table_name"}, Right: &LiteralExpr{Value: NewTextValue("actor")}}, Limit: 1}
	r, err := s.Select(stmt)
	if err != nil || len(r.Rows) != 1 || r.Rows[0]["table_name"].Text != "actor" {
		t.Fatalf("catalog: %+v %v", r, err)
	}
	if err = s.DropTable(&DropTableStmt{Table: "actor"}); err != nil {
		t.Fatal(err)
	}
	r, err = s.Select(stmt)
	if err != nil || len(r.Rows) != 0 {
		t.Fatalf("stale catalog: %+v %v", r, err)
	}
	// Query-local rewriting must not overwrite a reusable prepared AST.
	if stmt.From != "information_schema.tables" || stmt.FromExpr != nil {
		t.Fatal("catalog query AST mutated")
	}
}

func TestMySQLCatalogRespectsConfiguredName(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	statisticsSQL(t, s, "CREATE TABLE actor(id INTEGER)")
	s.SetContext(WithMySQLSession(context.Background(), MySQLSessionOptions{Database: "wiki"}))
	r, err := s.Select(&SelectStmt{From: "wiki.actor", SelectAll: true, Columns: []SelectColumn{{Star: true}}, Limit: -1})
	if err != nil || len(r.Columns) != 1 {
		t.Fatalf("qualified table: %+v %v", r, err)
	}
	if _, err = s.Select(&SelectStmt{From: "other.actor", SelectAll: true, Limit: -1}); err == nil {
		t.Fatal("unknown schema accepted")
	}
}

func TestInformationSchemaDoesNotEvaluateDefaults(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	statisticsSQL(t, s, "CREATE TABLE catalog_default(id INTEGER DEFAULT (1 / 0))")
	s.SetContext(WithMySQLSession(context.Background(), MySQLSessionOptions{Database: "wiki"}))
	r, e := s.informationSchemaTable("columns", catalogFilter{Schema: "wiki", Table: "catalog_default"})
	if e != nil || len(r.Rows) != 1 || r.Rows[0]["column_default"].Null {
		t.Fatalf("default SQL introspection: %+v %v", r, e)
	}
	statement, e := Parse("SELECT ordinal_position FROM information_schema.columns WHERE 0")
	if e != nil {
		t.Fatal(e)
	}
	columns, e := s.Describe(statement[0])
	if e != nil || len(columns) != 1 || columns[0].ColType != ColInteger {
		t.Fatalf("typed metadata %+v %v", columns, e)
	}
}

func TestInformationSchemaPointQueryAvoidsUnrelatedViewDescription(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	statisticsSQL(t, s, "CREATE TABLE selected_table(id INTEGER)")
	// A damaged/unresolvable view must not poison a point lookup for another
	// relation. No query execution is needed to discover a catalog's schema.
	s.views["broken_view"] = &ViewInfo{Name: "broken_view", SQL: "SELECT absent FROM missing_table"}
	s.SetContext(WithMySQLSession(context.Background(), MySQLSessionOptions{Database: "wiki"}))
	statements, e := Parse("SELECT column_name FROM information_schema.columns WHERE table_schema='wiki' AND table_name='selected_table'")
	if e != nil {
		t.Fatal(e)
	}
	r, e := s.Select(statements[0].(*SelectStmt))
	if e != nil || len(r.Rows) != 1 || r.Rows[0]["column_name"].Text != "id" {
		t.Fatalf("point catalog: %+v %v", r, e)
	}
}
