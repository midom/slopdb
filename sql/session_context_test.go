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
