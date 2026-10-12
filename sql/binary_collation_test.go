package sql

import "testing"

func parseTestExpression(t *testing.T, text string) Expression {
	t.Helper()
	stmts, err := Parse("SELECT " + text)
	if err != nil {
		t.Fatal(err)
	}
	return stmts[0].(*SelectStmt).Columns[0].Expr
}

func TestBinaryExpressionOverridesUnicodeCollation(t *testing.T) {
	s := &Schema{collation: "NFC_CI"}
	for _, tc := range []struct {
		expr string
		want int64
	}{
		{"'É' = 'e\u0301'", 1},
		{"BINARY 'É' = 'e\u0301'", 0},
		{"'É' COLLATE BINARY = 'e\u0301'", 0},
		{"'É' = BINARY 'e\u0301'", 0},
		{"'É' LIKE 'e\u0301'", 1},
		{"BINARY 'É' LIKE 'e\u0301'", 0},
	} {
		v, err := s.evalExpr(parseTestExpression(t, tc.expr), nil)
		if err != nil || v.Int != tc.want {
			t.Errorf("%s: got %v, %v; want %d", tc.expr, v, err, tc.want)
		}
	}
}

func TestBinaryColumnDeclarationPersistsInAST(t *testing.T) {
	stmts, err := Parse("CREATE TABLE t(a TEXT COLLATE BINARY, b TEXT BINARY, c TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	cols := stmts[0].(*CreateTableStmt).Columns
	if !cols[0].Binary || !cols[1].Binary || cols[2].Binary {
		t.Fatalf("unexpected binary flags: %#v", cols)
	}
	for _, query := range []string{"CREATE TABLE t(a INTEGER COLLATE BINARY)", "CREATE TABLE t(a TEXT COLLATE NOCASE)"} {
		if _, err := Parse(query); err == nil {
			t.Errorf("expected rejection for %s", query)
		}
	}
}
