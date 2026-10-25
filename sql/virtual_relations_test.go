package sql

import (
	"context"
	"testing"
)

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

func TestPreparedCatalogNamesKeepLiveProjection(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	for _, tc := range []struct {
		dialect       Dialect
		query, column string
	}{
		{DialectMySQL, "SELECT id FROM information_schema.processlist LIMIT ?", "id"},
		{DialectPostgreSQL, "SELECT pid FROM pg_stat_activity LIMIT ?", "pid"},
	} {
		if err := s.SetDialect(tc.dialect); err != nil {
			t.Fatal(err)
		}
		var calls uint64
		s.SetActivitySnapshot(func(context.Context) ([]SessionActivity, error) {
			calls++
			return []SessionActivity{{ID: 100 + calls, SessionID: 1, State: ActivityIdle}}, nil
		})
		_, bound := namespaceBoundForTest(t, tc.query, NewIntValue(1))
		stmt := bound.statements[0].(*SelectStmt)
		for repeat := uint64(1); repeat <= 3; repeat++ {
			result, err := s.Select(stmt)
			if err != nil || len(result.Rows) != 1 {
				t.Fatal(result, err)
			}
			if got := result.Rows[0][tc.column]; got.Int != int64(100+repeat) && got.Uint != 100+repeat {
				t.Fatalf("stale live catalog projection: %+v", got)
			}
			if calls != repeat {
				t.Fatalf("provider called %d times", calls)
			}
		}
	}
}
