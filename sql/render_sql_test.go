package sql

import "testing"

func TestDialectRenderSQLPreservesLiteralsAndIdentifiers(t *testing.T) {
	source := "CREATE TABLE \"t`x\"(\"q\" TEXT DEFAULT 'a\nb\\c''d')"
	rendered := DialectMySQL.RenderSQL(source)
	parsed, e := ParseWithOptions(rendered, ParseOptions{MySQLQuoting: true})
	if e != nil {
		t.Fatal(rendered, e)
	}
	stmt := parsed[0].(*CreateTableStmt)
	if stmt.Table != "t`x" || stmt.Columns[0].Name != "q" || stmt.Columns[0].Default.(*LiteralExpr).Value.Text != "a\nb\\c'd" {
		t.Fatalf("literal changed: %q %+v", rendered, stmt)
	}
	for _, dialect := range []Dialect{DialectNative, DialectPostgreSQL} {
		if dialect.RenderSQL(source) != source {
			t.Fatal("canonical rendering changed", dialect)
		}
	}
}
