package sql

import "testing"

func TestVaryingColumnDeclarations(t *testing.T) {
	statements, err := Parse("CREATE TABLE t(a VARCHAR(7) COLLATE BINARY, b VARBINARY(12), c TEXT, d BLOB)")
	if err != nil {
		t.Fatal(err)
	}
	columns := statements[0].(*CreateTableStmt).Columns
	for i, want := range []string{"VARCHAR(7)", "VARBINARY(12)", "TEXT", "BLOB"} {
		if got := columns[i].DeclaredType(); got != want {
			t.Fatalf("column %d type = %s, want %s", i, got, want)
		}
	}
	if !columns[0].Binary || columns[1].Binary {
		t.Fatalf("unexpected BINARY modifiers: %#v", columns)
	}
	for _, query := range []string{
		"CREATE TABLE t(a VARCHAR)",
		"CREATE TABLE t(a VARBINARY)",
		"CREATE TABLE t(a VARCHAR())",
		"CREATE TABLE t(a VARCHAR(0))",
		"CREATE TABLE t(a VARCHAR(-1))",
		"CREATE TABLE t(a VARCHAR(1.5))",
		"CREATE TABLE t(a VARCHAR(1,2))",
		"CREATE TABLE t(a VARBINARY(9223372036854775808))",
		"CREATE TABLE t(a VARBINARY(2) COLLATE BINARY)",
	} {
		if _, err := Parse(query); err == nil {
			t.Errorf("accepted invalid declaration %s", query)
		}
	}
}

func TestVaryingCasts(t *testing.T) {
	s := &Schema{collation: BinaryCollation}
	for _, test := range []struct {
		expression string
		kind       ColumnType
	}{
		{"CAST('é' AS VARCHAR(1))", ColText},
		{"CAST('é' AS VARBINARY(2))", ColBlob},
		{"CAST(X'00FF' AS VARBINARY(2))", ColBlob},
	} {
		value, err := s.evalExpr(parseTestExpression(t, test.expression), nil)
		if err != nil || value.Type != test.kind {
			t.Errorf("%s: value=%v error=%v", test.expression, value, err)
		}
	}
	for _, expression := range []string{
		"CAST('é' AS VARCHAR(0))",
		"CAST('é' AS VARBINARY(-1))",
	} {
		if _, err := Parse("SELECT " + expression); err == nil {
			t.Errorf("accepted %s", expression)
		}
	}
	for _, expression := range []string{
		"CAST('éé' AS VARCHAR(1))",
		"CAST('é' AS VARBINARY(1))",
	} {
		if _, err := s.evalExpr(parseTestExpression(t, expression), nil); err == nil {
			t.Errorf("accepted overlength cast %s", expression)
		}
	}
}
