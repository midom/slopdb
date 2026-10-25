package sql

import (
	"bytes"
	"encoding/hex"
	"reflect"
	"testing"
)

func TestNamespaceTraversalNestedRelationsAndProjectionValues(t *testing.T) {
	cases := []struct {
		query                               string
		qualified, multipart, bare, catalog bool
	}{
		{"SELECT a.items.* FROM a.items JOIN b.items ON a.items.id=b.items.id", true, true, false, false},
		{"SELECT (SELECT current_schema FROM b.items WHERE id=?) AS nested FROM a.items WHERE EXISTS(SELECT 1 FROM c.items)", true, false, true, false},
		{"WITH source AS(SELECT * FROM a.items) INSERT INTO b.items SELECT * FROM source", true, false, false, false},
		{"UPDATE a.items SET b=(SELECT b FROM b.items WHERE id=?) WHERE EXISTS(SELECT 1 FROM c.items)", true, false, false, false},
		{"SELECT (SELECT table_name FROM information_schema.tables LIMIT 1) AS n FROM items", true, false, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			q, e := Prepare(tc.query)
			if e != nil {
				t.Fatal(e)
			}
			got := q.namespaces
			if got.qualified != tc.qualified || got.multiPartReference != tc.multipart || got.nativeBare != tc.bare || got.catalogReference != tc.catalog {
				t.Fatalf("namespace requirements=%+v", got)
			}
		})
	}
}

func TestPreparedMetadataCloneOwnsNestedLiteralAndBoundPagination(t *testing.T) {
	payload := bytes.Repeat([]byte{0, 1, 127, 255}, 2048)
	query := "SELECT (SELECT X'" + hex.EncodeToString(payload) + "' FROM tenant.items WHERE id=? LIMIT ?) AS nested FROM items LIMIT ? OFFSET ?"
	q, e := Prepare(query)
	if e != nil {
		t.Fatal(e)
	}
	first := q.MetadataStatement().(*SelectStmt)
	sub := first.Columns[0].Expr.(*SubqueryExpr).Stmt
	literal := sub.Columns[0].Expr.(*LiteralExpr)
	literal.Value.Blob[0] = 99
	sub.From = "changed"
	sub.LimitParam = 0
	first.LimitParam = 0
	first.OffsetParam = 0
	second := q.MetadataStatement().(*SelectStmt)
	nested := second.Columns[0].Expr.(*SubqueryExpr).Stmt
	if nested.From != "tenant.items" || !bytes.Equal(nested.Columns[0].Expr.(*LiteralExpr).Value.Blob, payload) || nested.LimitParam != 2 || second.LimitParam != 3 || second.OffsetParam != 4 {
		t.Fatal("metadata clone leaked mutation into immutable template")
	}
	binding, e := q.Bind([]Value{NewIntValue(7), NewIntValue(2), NewIntValue(3), NewIntValue(4)})
	if e != nil {
		t.Fatal(e)
	}
	defer binding.Release()
	bound := binding.Statements()[0].(*SelectStmt)
	boundSub := bound.Columns[0].Expr.(*SubqueryExpr).Stmt
	if bound.Limit != 3 || bound.Offset != 4 || boundSub.Limit != 2 || boundSub.Where.(*BinaryExpr).Right.(*LiteralExpr).Value.Int != 7 || !bytes.Equal(boundSub.Columns[0].Expr.(*LiteralExpr).Value.Blob, payload) {
		t.Fatal("nested parameter/pagination binding changed")
	}
	boundSub.Columns[0].Expr.(*LiteralExpr).Value.Blob[0] = 33
	if !bytes.Equal(nested.Columns[0].Expr.(*LiteralExpr).Value.Blob, payload) {
		t.Fatal("bound AST aliases owned metadata AST")
	}
}

func BenchmarkNamespaceTraversal(b *testing.B) {
	cases := []string{"UPDATE stock SET quantity=?,ytd=ytd+?,order_count=order_count+1,remote_count=remote_count+? WHERE w_id=? AND i_id=?", "SELECT (SELECT v FROM tenant.items WHERE id=?) FROM items WHERE EXISTS(SELECT 1 FROM other.items)"}
	for i, query := range cases {
		b.Run([]string{"mutation", "nested"}[i], func(b *testing.B) {
			q, e := Prepare(query)
			if e != nil {
				b.Fatal(e)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				requirements := compileNamespaceRequirements(q.template)
				if i == 1 && !requirements.qualified {
					b.Fatal("lost namespace")
				}
			}
		})
	}
}

// The public metadata visitor must discover parameters in nested statements and
// LIMIT/OFFSET while ignoring arbitrary BLOB bytes as expression-tree content.
func TestNamespaceTraversalFindsNestedPreparedParameters(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	statisticsSQL(t, s, "CREATE TABLE items(id INTEGER PRIMARY KEY,b BLOB)")
	q, e := Prepare("SELECT (SELECT b FROM items WHERE id=? LIMIT ?) FROM items WHERE b=? LIMIT ? OFFSET ?")
	if e != nil {
		t.Fatal(e)
	}
	metadata := q.MetadataStatement()
	description, e := s.DescribePrepared(metadata, DescribeOptions{})
	if e != nil {
		t.Fatal(e)
	}
	if len(description.Parameters) != 5 {
		t.Fatalf("parameter metadata=%+v", description.Parameters)
	}
	if !reflect.DeepEqual(q.MetadataStatement(), metadata) {
		t.Fatal("describing a private metadata AST changed its template")
	}
}
