package sql

import (
	"context"
	"testing"
)

func TestAdditionalFunctionDescriptorsAndBudgets(t *testing.T) {
	for name, want := range map[string]TypeID{"STARTS_WITH": TypeBoolean, "IS_NORMALIZED": TypeBoolean, "SHA256": TypeBinary, "BIT_COUNT": TypeInt64, "GEN_RANDOM_UUID": TypeUUID, "NUM_NULLS": TypeInt32} {
		spec, ok := LookupFunction(DialectPostgreSQL, name)
		if !ok {
			t.Fatal(name)
		}
		metadata, _, err := FunctionMetadata(DialectPostgreSQL, name, functionUnknownColumns(spec.Min))
		if err != nil || metadata.LogicalType().Kind != want {
			t.Fatal(name, metadata, err)
		}
	}
	if FunctionDeterministic(DialectPostgreSQL, "GEN_RANDOM_UUID") {
		t.Fatal("UUID expression cached as deterministic")
	}
	if _, _, e := FunctionArgumentTypes(DialectPostgreSQL, "TO_HEX", functionUnknownColumns(1)); e == nil {
		t.Fatal("ambiguous TO_HEX signature guessed")
	}
	s := &Schema{dialect: DialectPostgreSQL}
	id1, e := s.evalExpr(parseTestExpression(t, "gen_random_uuid()"), nil)
	if e != nil {
		t.Fatal(e)
	}
	id2, e := s.evalExpr(parseTestExpression(t, "gen_random_uuid()"), nil)
	if e != nil || id1.Text == id2.Text || id1.LogicalType().Kind != TypeUUID {
		t.Fatal(id1, id2, e)
	}
	s.ctx = WithQueryMemory(context.Background(), 2)
	if _, e = s.evalExpr(parseTestExpression(t, "normalize('é',NFD)"), nil); e == nil {
		t.Fatal("normalization exceeded output budget")
	}
	s.dialect = DialectMySQL
	if _, e = s.evalExpr(parseTestExpression(t, `JSON_SEARCH('["x"]','all','x')`), nil); e == nil {
		t.Fatal("JSON search exceeded output budget")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.ctx = ctx
	if _, e = s.evalExpr(parseTestExpression(t, `JSON_SEARCH('["x"]','all','x')`), nil); e == nil {
		t.Fatal("JSON search ignored cancellation")
	}
}
