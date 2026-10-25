package sql

import "testing"

func TestCatalogPhysicalRelationsKeepStatement(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	for _, query := range []string{"SELECT id FROM ordinary WHERE id=1", "SELECT a.id FROM ordinary a JOIN other b ON a.id=b.id", "SELECT 1"} {
		statements, e := Parse(query)
		if e != nil {
			t.Fatal(e)
		}
		stmt := statements[0].(*SelectStmt)
		resolved, e := s.resolveCatalogTables(stmt)
		if e != nil {
			t.Fatal(e)
		}
		if resolved != stmt {
			t.Fatalf("physical relation cloned: %s", query)
		}
	}
}
func TestCatalogRelationSubstitutionDoesNotMutateStatement(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	for _, query := range []string{"SELECT * FROM information_schema.tables", "SELECT a.id FROM ordinary a JOIN information_schema.tables b ON a.id=b.table_name", "SELECT * FROM main.ordinary"} {
		statements, e := Parse(query)
		if e != nil {
			t.Fatal(e)
		}
		stmt := statements[0].(*SelectStmt)
		from := stmt.From
		var join string
		if len(stmt.Joins) > 0 {
			join = stmt.Joins[0].Table
		}
		resolved, e := s.resolveCatalogTables(stmt)
		if e != nil {
			t.Fatal(e)
		}
		if resolved == stmt {
			t.Fatalf("relation substitution reused mutable tree: %s", query)
		}
		if stmt.From != from || stmt.FromExpr != nil || len(stmt.Joins) > 0 && (stmt.Joins[0].Table != join || stmt.Joins[0].Subquery != nil) {
			t.Fatalf("source changed: %s", query)
		}
	}
}
