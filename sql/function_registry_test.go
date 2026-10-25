package sql

import "testing"

func TestResolvedFunctionsUseOneDialectSignature(t *testing.T) {
	for _, d := range []Dialect{DialectNative, DialectMySQL, DialectPostgreSQL} {
		spec, ok := LookupFunction(d, "length")
		if !ok || !spec.valid(1) || spec.valid(0) {
			t.Fatal(d, spec)
		}
		col, ok, e := FunctionMetadata(d, "length", []ColumnDef{{ColType: ColText}})
		if e != nil || !ok || col.ColType != ColInteger {
			t.Fatal(d, col, ok, e)
		}
		if _, _, e = FunctionMetadata(d, "length", nil); e == nil {
			t.Fatal("arity differs between lookup and metadata", d)
		}
		if FunctionDeterministic(d, "NOW") || FunctionDeterministic(d, "RAND") {
			t.Fatal("volatile function treated deterministic", d)
		}
	}
	if _, known := LookupFunction(DialectMySQL, "jsonb_build_object"); known {
		t.Fatal("PG-only name leaked into MySQL")
	}
	if _, _, e := FunctionMetadata(DialectPostgreSQL, "JSONB_BUILD_OBJECT", []ColumnDef{{ColType: ColText}}); e == nil {
		t.Fatal("odd key/value signature accepted")
	}
	// The returned descriptor is a copy; callers cannot change the registry.
	spec, _ := LookupFunction(DialectPostgreSQL, "LENGTH")
	spec.Min = 100
	if _, _, e := FunctionMetadata(DialectPostgreSQL, "LENGTH", []ColumnDef{{ColType: ColText}}); e != nil {
		t.Fatal(e)
	}
}
func TestSharedArgumentInferencePreservesPolymorphicPositions(t *testing.T) {
	unknown := ColumnDef{ColType: ColUnknown}
	args, known, e := FunctionArgumentTypes(DialectPostgreSQL, "GET_BIT", []ColumnDef{unknown, unknown})
	if e != nil || !known || len(args) != 2 || args[0].LogicalType().Kind != TypeBinary || args[1].LogicalType().Kind != TypeInt64 {
		t.Fatal(args, known, e)
	}
	args, known, e = FunctionArgumentTypes(DialectPostgreSQL, "LPAD", []ColumnDef{unknown, unknown, unknown})
	if e != nil || !known || args[0].LogicalType().Kind != TypeText || args[1].LogicalType().Kind != TypeInt32 || args[2].LogicalType().Kind != TypeText {
		t.Fatal(args, known, e)
	}
	args, _, e = FunctionArgumentTypes(DialectPostgreSQL, "CONCAT_WS", []ColumnDef{unknown, unknown, unknown})
	if e != nil || args[0].ColType != ColText || args[1].ColType != ColUnknown || args[2].ColType != ColUnknown {
		t.Fatal(args, e)
	}
	args, _, e = FunctionArgumentTypes(DialectPostgreSQL, "SUBSTRING", []ColumnDef{{ColType: ColBlob}, unknown})
	if e != nil || args[0].LogicalType().Kind != TypeBinary || args[1].LogicalType().Kind != TypeInt32 {
		t.Fatal(args, e)
	}
}

// Captured independently with PostgreSQL 16.15 PREPARE without declared input
// types and pg_prepared_statements.parameter_types; see the reference docs.
func TestPostgresPreferredUnknownFunctionSignatures(t *testing.T) {
	cases := []struct {
		name string
		n    int
		want []string
	}{{"SUBSTRING", 2, []string{"TEXT", "TEXT"}}, {"SUBSTR", 2, []string{"TEXT", "INTEGER"}}, {"REGEXP_REPLACE", 4, []string{"TEXT", "TEXT", "TEXT", "TEXT"}}, {"ABS", 1, []string{"DOUBLE PRECISION"}}, {"ROUND", 1, []string{"DOUBLE PRECISION"}}, {"ROUND", 2, []string{"NUMERIC", "INTEGER"}}, {"CEIL", 1, []string{"DOUBLE PRECISION"}}}
	for _, tc := range cases {
		args, known, e := FunctionArgumentTypes(DialectPostgreSQL, tc.name, functionUnknownColumns(tc.n))
		if e != nil || !known {
			t.Fatal(tc, e)
		}
		for i, want := range tc.want {
			if args[i].LogicalType().Kind != LogicalTypeFromDeclaration(want).Kind {
				t.Fatalf("%s argument%d=%v want%s", tc.name, i, args[i].LogicalType(), want)
			}
		}
	}
	if _, _, e := FunctionArgumentTypes(DialectPostgreSQL, "GCD", functionUnknownColumns(2)); e == nil {
		t.Fatal("ambiguous PG overload guessed")
	}
}
