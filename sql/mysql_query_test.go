package sql

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestMySQLScalarExpressions(t *testing.T) {
	s := &Schema{}
	for _, tc := range []struct{ sql, want string }{
		{"IF(1,7,1/0)", "7"}, {"IF(NULL,1,2)", "2"},
		{"GREATEST(1,17,3)", "17"}, {"LEAST('z','a','b')", "a"},
		{"CONCAT('a',17,'b')", "a17b"}, {"SUBSTRING('café',-2,2)", "fé"},
		{"SUBSTRING('abc',0,2)", ""}, {"SUBSTRING('abc',2,-1)", ""},
		{"CAST('17' AS SIGNED)", "17"}, {"CAST(17 AS CHARACTER)", "17"},
		{"HEX(CAST('a' AS BINARY(3)))", "610000"},
		{"CONVERT(X'636166c3a9' USING utf8mb4)", "café"},
		{"UNIX_TIMESTAMP('1970-01-01 00:00:01')", "1"},
	} {
		value, err := s.evalExpr(parseTestExpression(t, tc.sql), nil)
		if err != nil || value.String() != tc.want {
			t.Errorf("%s: got %v (%v), want %q", tc.sql, value, err, tc.want)
		}
	}
	for _, expr := range []string{"CONCAT('a',NULL)", "GREATEST(1,NULL)", "SUBSTRING(NULL,1)"} {
		v, err := s.evalExpr(parseTestExpression(t, expr), nil)
		if err != nil || !v.Null {
			t.Errorf("%s: %v %v", expr, v, err)
		}
	}
	for _, expr := range []string{"IF(1,2)", "SYSDATE(7)", "CONVERT(X'ff' USING utf8mb4)"} {
		if _, err := s.evalExpr(parseTestExpression(t, expr), nil); err == nil {
			t.Errorf("accepted invalid expression %s", expr)
		}
	}
}

func TestMySQLClockAndSessionFunctionsRemainVolatile(t *testing.T) {
	s := &Schema{statementTime: time.Date(2026, 10, 25, 1, 2, 3, 123456000, time.UTC)}
	for _, expr := range []string{"NOW(6)", "CURRENT_TIMESTAMP(6)"} {
		v, err := s.evalExpr(parseTestExpression(t, expr), nil)
		if err != nil || v.Text != "2026-10-25 01:02:03.123456" {
			t.Fatalf("%s: %v %v", expr, v, err)
		}
	}
	s.ctx = WithMySQLSession(context.Background(), MySQLSessionOptions{Function: func(ctx context.Context, name string, args []Value) (Value, bool, error) {
		return NewIntValue(23), name == "GET_LOCK", nil
	}})
	v, err := s.evalExpr(parseTestExpression(t, "GET_LOCK('resource',0)"), nil)
	if err != nil || v.Int != 23 {
		t.Fatal(v, err)
	}
	for _, name := range []string{"GET_LOCK", "RELEASE_LOCK", "IS_FREE_LOCK", "SYSDATE", "NOW", "CURRENT_TIMESTAMP", "UNIX_TIMESTAMP", "CURRENT_USER", "CONNECTION_ID"} {
		if deterministicFunction(name) {
			t.Fatalf("%s incorrectly classified as deterministic", name)
		}
	}
}

func TestMySQLNamedCollationWeightsAndLike(t *testing.T) {
	s := &Schema{}
	for _, tc := range []struct {
		expr string
		want int64
	}{
		{"'É' COLLATE utf8mb4_general_ci = 'e'", 1},
		{"'ß' COLLATE utf8mb4_general_ci = 's'", 1},
		{"'ß' COLLATE utf8mb4_general_ci = 'ss'", 0},
		{"'é' COLLATE utf8mb4_general_ci = 'é'", 0},
		{"'a ' COLLATE utf8mb4_general_ci = 'a'", 1},
		{"'a ' COLLATE utf8mb4_general_ci LIKE 'a'", 0},
		{"'École' COLLATE utf8mb4_general_ci LIKE 'e%'", 1},
		{"'A' COLLATE utf8mb4_bin LIKE 'a'", 0},
		{"'😀' COLLATE utf8mb4_general_ci = '😁'", 1},
	} {
		v, err := s.evalExpr(parseTestExpression(t, tc.expr), nil)
		if err != nil || v.Int != tc.want {
			t.Errorf("%s: %v %v want %d", tc.expr, v, err, tc.want)
		}
	}
	for _, c := range []string{"utf8mb4_general_ci", "UTF8_GENERAL_CI", "utf8mb3_bin"} {
		if _, err := ParseCollation(c); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMySQLQuerySyntaxAndCanonicalRoundTrip(t *testing.T) {
	for _, query := range []string{
		"SELECT id FROM wiki.actor FORCE INDEX FOR JOIN (actor_name) WHERE id=1",
		"SELECT a.id FROM a STRAIGHT_JOIN b ON a.id=b.id",
		"SELECT a.id FROM a LEFT JOIN (b JOIN c ON b.id=c.id) ON a.id=b.id",
		"SELECT CONVERT(v USING utf8mb4) COLLATE utf8mb4_general_ci FROM t",
		"SELECT GROUP_CONCAT(DISTINCT v ORDER BY v DESC SEPARATOR '|') FROM t",
	} {
		stmts, err := Parse(query)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		canonical := formatSelectSQL(stmts[0].(*SelectStmt))
		if _, err = Parse(canonical); err != nil {
			t.Fatalf("round trip %s: %v", canonical, err)
		}
	}
	for _, query := range []string{"SELECT 1 FROM t FORCE INDEX (", "SELECT 1 FROM t USE INDEX FOR BOGUS (a)", "SELECT 1 FROM a LEFT JOIN (b JOIN) ON 1=1", "SELECT CONVERT('a' USING unknown)", "SELECT v COLLATE NOCASE FROM t"} {
		if _, err := Parse(query); err == nil {
			t.Errorf("accepted invalid query %s", query)
		}
	}
	stmt, err := Parse("SELECT 1 FROM `information_schema`.`tables`")
	if err != nil || stmt[0].(*SelectStmt).From != "information_schema.tables" {
		t.Fatal(stmt, err)
	}
}

func TestMySQLGroupConcatValues(t *testing.T) {
	s := &Schema{ctx: WithMySQLSession(context.Background(), MySQLSessionOptions{GroupConcatMaxLen: 6})}
	rows := []map[string]Value{{"a": NewTextValue("ab"), "b": NewTextValue("c"), "n": NewIntValue(2)}, {"a": NewTextValue("a"), "b": NewTextValue("bc"), "n": NewIntValue(1)}, {"a": NewTextValue("ab"), "b": NewTextValue("c"), "n": NewIntValue(3)}, {"a": NewNullValue(), "b": NewTextValue("x"), "n": NewIntValue(0)}}
	f := parseTestExpression(t, "GROUP_CONCAT(DISTINCT a,b ORDER BY n SEPARATOR '|')").(*FuncCallExpr)
	v, err := s.evalAggregate(f, rows)
	if err != nil || v.Text != "abc|ab" {
		t.Fatal(v, err)
	}
	s.ctx = context.Background()
	v, err = s.evalAggregate(f, rows)
	if err != nil || v.Text != "abc|abc" {
		t.Fatal(v, err)
	}
	if !strings.Contains(formatExpr(f), "SEPARATOR") {
		t.Fatal("lost separator metadata")
	}
}
