package sql

import (
	"context"
	"testing"
)

func TestSharedSessionContextAndDialectExtensions(t *testing.T) {
	calls := 0
	callback := func(_ context.Context, name string, args []Value) (Value, bool, error) {
		calls++
		return NewTextValue(name), true, nil
	}
	ctx := WithSessionContext(nil, SessionContext{Dialect: DialectPostgreSQL, Database: "shared", User: "reader", BackendID: 42, Function: callback})
	schema := &Schema{dialect: DialectPostgreSQL, ctx: ctx}
	for _, tc := range []struct{ name, want string }{{"CURRENT_DATABASE", "shared"}, {"CURRENT_USER", "reader"}, {"PG_BACKEND_PID", "42"}} {
		v, e := schema.evalResolvedFunction(&FuncCallExpr{Name: tc.name}, nil)
		if e != nil || v.String() != tc.want {
			t.Fatal(tc, v, e)
		}
	}
	if _, _, e := FunctionMetadata(DialectPostgreSQL, "CURRENT_SETTING", []ColumnDef{{ColType: ColText}}); e != nil {
		t.Fatal(e)
	}
	if calls != 0 {
		t.Fatal("metadata evaluated callback")
	}
	v, e := schema.evalResolvedFunction(&FuncCallExpr{Name: "CURRENT_SETTING", Args: []Expression{&LiteralExpr{Value: NewTextValue("custom")}}}, nil)
	if e != nil || v.Text != "CURRENT_SETTING" || calls != 1 {
		t.Fatal(v, e, calls)
	}
	pg := WithPostgresSession(ctx, PostgresSessionOptions{Database: "pg", User: "postgres", BackendPID: 12, Function: callback})
	base, ok := SessionContextFrom(pg)
	if !ok || base.Dialect != DialectPostgreSQL || base.Database != "pg" || base.User != "postgres" || base.BackendID != 12 {
		t.Fatal(base, ok)
	}
	mysql := WithMySQLSession(pg, MySQLSessionOptions{Database: "mysql", User: "writer", BackendID: 7, Function: callback, GroupConcatMaxLen: 4096, SQLMode: "STRICT_ALL_TABLES"})
	base, ok = SessionContextFrom(mysql)
	extension, ex := MySQLSession(mysql)
	if !ok || !ex || base.Dialect != DialectMySQL || base.Database != "mysql" || base.User != "writer" || base.BackendID != 7 || extension.SQLMode != "STRICT_ALL_TABLES" || MySQLGroupConcatMaxLen(mysql) != 4096 {
		t.Fatal(base, extension)
	}
	v, handled, e := MySQLFunction(mysql, "identity", nil)
	if e != nil || !handled || v.Text != "identity" {
		t.Fatal(v, handled, e)
	}
}

func TestSessionTypedStateOwnershipBudgetAndReset(t *testing.T) {
	var state SessionState
	blob := NewBlobValue([]byte{1, 2})
	if e := state.SetVariable("a", blob, 1024); e != nil {
		t.Fatal(e)
	}
	blob.Blob[0] = 9
	got, ok := state.Variable("a")
	if !ok || got.Blob[0] != 1 {
		t.Fatal(got, ok)
	}
	got.Blob[0] = 8
	got, _ = state.Variable("a")
	if got.Blob[0] != 1 {
		t.Fatal("aliased read")
	}
	if e := state.SetVariable("a", NewBlobValue(make([]byte, 2048)), 1024); e == nil {
		t.Fatal("budget accepted")
	}
	got, _ = state.Variable("a")
	if got.Blob[0] != 1 {
		t.Fatal("failed write changed value")
	}
	state.Reset()
	if _, ok = state.Variable("a"); ok || state.bytes != 0 {
		t.Fatal("reset retained state")
	}
}

func TestSessionVariableInspectionClassification(t *testing.T) {
	for _, tc := range []struct {
		q     string
		local bool
	}{{"SET @a=1+2", true}, {"SET @a=(SELECT @@read_only)", true}, {"SET @a=(SELECT @b+1)", true}, {"SET @a=(SELECT n FROM table_name)", false}, {"SET @a=(SELECT n FROM table_name)+(SELECT 1)", false}, {"SET @a=(WITH x AS (SELECT n FROM table_name) SELECT n FROM x)", false}, {"SET @a=(SELECT ROW_NUMBER() OVER(ORDER BY 1))", false}} {
		statements, e := Parse(tc.q)
		if e != nil {
			t.Fatal(tc.q, e)
		}
		statement := statements[0].(*SessionVariableStmt)
		if statement.LocalOnly() != tc.local {
			t.Fatalf("%s local=%v want%v", tc.q, statement.LocalOnly(), tc.local)
		}
	}
}
