package sql

import (
	"context"
	"testing"
)

func namespaceBoundForTest(t testing.TB, query string, args ...Value) (*PreparedQuery, *BoundQuery) {
	t.Helper()
	p, e := Prepare(query)
	if e != nil {
		t.Fatal(e)
	}
	b, e := p.Bind(args)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(b.Release)
	return p, b
}
func TestPreparedNamespaceRootReadRetainsPrivatePlan(t *testing.T) {
	for _, dialect := range []Dialect{DialectNative, DialectMySQL, DialectPostgreSQL} {
		for _, ctxPath := range [][]string{nil, {"main"}} {
			t.Run(dialect.String(), func(t *testing.T) {
				_, bound := namespaceBoundForTest(t, "SELECT v FROM records WHERE id=?", NewIntValue(7))
				stmt := bound.statements[0]
				plan := stmt.(*SelectStmt).prepared
				schema := &Schema{dialect: dialect, namespaces: map[string]bool{"main": true, "tenant": true}, searchPath: []string{"main"}, ctx: context.Background()}
				if ctxPath != nil {
					schema.ctx = WithNamespaceSearchPath(schema.ctx, ctxPath)
					schema.searchPath = []string{"tenant"}
				}
				resolved, e := schema.ResolveNamespaces(stmt)
				if e != nil || resolved != stmt || stmt.(*SelectStmt).prepared != plan {
					t.Fatalf("lost private root plan: %T %v", resolved, e)
				}
				if allocations := testing.AllocsPerRun(100, func() { resolved, e = schema.ResolveNamespaces(stmt) }); allocations != 0 {
					t.Fatalf("root namespace resolution allocated %g times", allocations)
				}
				if e != nil || resolved != stmt {
					t.Fatal(e)
				}
			})
		}
	}
}
func TestPreparedNamespaceRequirementsFollowCurrentContext(t *testing.T) {
	_, bound := namespaceBoundForTest(t, "SELECT v FROM records WHERE id=?", NewIntValue(7))
	stmt := bound.statements[0]
	schema := &Schema{namespaces: map[string]bool{"main": true, "tenant": true}, ctx: context.Background()}
	schema.searchPath = []string{"tenant"}
	resolved, e := schema.ResolveNamespaces(stmt)
	if e != nil || resolved == stmt || resolved.(*SelectStmt).From != "tenant.records" {
		t.Fatalf("tenant path %v %v", resolved, e)
	}
	schema.ctx = WithNamespaceSearchPath(schema.ctx, []string{"main"})
	resolved, e = schema.ResolveNamespaces(stmt)
	if e != nil || resolved != stmt {
		t.Fatalf("context root override %v %v", resolved, e)
	}
	schema.ctx = WithNamespaceSearchPath(context.Background(), []string{"tenant", "main"})
	resolved, e = schema.ResolveNamespaces(stmt)
	if e != nil || resolved == stmt || resolved.(*SelectStmt).From != "tenant.records" {
		t.Fatalf("context tenant override %v %v", resolved, e)
	}
	if stmt.(*SelectStmt).From != "records" || stmt.(*SelectStmt).prepared == nil {
		t.Fatal("rewrote bound tree used by later executions")
	}
}
func TestPreparedNamespaceBareKeywordsRemainDialectSensitive(t *testing.T) {
	q, bound := namespaceBoundForTest(t, "SELECT user AS u,current_schema AS s FROM records WHERE id=?", NewIntValue(7))
	stmt := bound.statements[0]
	if !q.namespaces.nativeBare {
		t.Fatal("bare keywords absent from compiled requirements")
	}
	schema := &Schema{namespaces: map[string]bool{"main": true}, ctx: context.Background()}
	for _, dialect := range []Dialect{DialectNative, DialectPostgreSQL, DialectMySQL, DialectPostgreSQL} {
		schema.dialect = dialect
		resolved, e := schema.ResolveNamespaces(stmt)
		if e != nil {
			t.Fatal(e)
		}
		expression := resolved.(*SelectStmt).Columns[0].Expr
		if dialect == DialectPostgreSQL {
			if resolved != stmt {
				t.Fatal("copied PostgreSQL keyword tree")
			}
			if _, ok := expression.(*FuncCallExpr); !ok {
				t.Fatalf("PostgreSQL keyword became %T", expression)
			}
		} else {
			if resolved == stmt {
				t.Fatal("native bare keyword tree not isolated")
			}
			if ref, ok := expression.(*ColumnRefExpr); !ok || ref.Name != "user" {
				t.Fatalf("native keyword became %T %v", expression, expression)
			}
		}
	}
}
func TestPreparedNamespaceQualifiedAndNestedFallback(t *testing.T) {
	for _, query := range []string{"SELECT v FROM public.records WHERE id=?", "SELECT (SELECT v FROM tenant.records WHERE id=?) AS v FROM records", "WITH x AS(SELECT v FROM tenant.records WHERE id=?) SELECT v FROM x"} {
		t.Run(query, func(t *testing.T) {
			q, b := namespaceBoundForTest(t, query, NewIntValue(7))
			if !q.namespaces.qualified {
				t.Fatal("missed qualified relation")
			}
			schema := &Schema{namespaces: map[string]bool{"main": true, "tenant": true}, ctx: context.Background()}
			resolved, e := schema.ResolveNamespaces(b.statements[0])
			if e != nil || resolved == b.statements[0] {
				t.Fatalf("qualified fallback %v %v", resolved, e)
			}
		})
	}
}
func BenchmarkPreparedNamespaceResolution(b *testing.B) {
	for _, many := range []bool{false, true} {
		name := "root"
		if many {
			name = "additional_schema"
		}
		b.Run(name, func(b *testing.B) {
			_, bound := namespaceBoundForTest(b, "SELECT v FROM records WHERE id=?", NewIntValue(7))
			stmt := bound.statements[0]
			schema := &Schema{namespaces: map[string]bool{"main": true}, searchPath: []string{"main"}, ctx: context.Background()}
			if many {
				schema.namespaces["tenant"] = true
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, e := schema.ResolveNamespaces(stmt)
				if e != nil || result != stmt {
					b.Fatal(e)
				}
			}
		})
	}
}
