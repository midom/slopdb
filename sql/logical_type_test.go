package sql

import (
	"encoding/json"
	"testing"
)

func TestLogicalTypesPreserveNativeAndDeclaredWidths(t *testing.T) {
	cases := []struct {
		c    ColumnDef
		kind TypeID
		bits int
	}{
		{ColumnDef{ColType: ColInteger, Declared: "INTEGER"}, TypeInt64, 64},
		{ColumnDef{ColType: ColInteger, Declared: "INTEGER", Postgres: true}, TypeInt32, 32},
		{ColumnDef{ColType: ColInteger, Declared: "SMALLINT", Postgres: true}, TypeInt16, 16},
		{ColumnDef{ColType: ColInteger, Declared: "INT", MySQL: true, IntegerBits: 32, Unsigned: true}, TypeUint32, 32},
		{ColumnDef{ColType: ColUnsigned}, TypeUint64, 64},
		{ColumnDef{ColType: ColReal, Declared: "REAL"}, TypeFloat64, 0},
		{ColumnDef{ColType: ColReal, Declared: "REAL", Postgres: true}, TypeFloat32, 0},
		{ColumnDef{ColType: ColReal, Declared: "FLOAT", MySQL: true}, TypeFloat32, 0},
	}
	for _, tc := range cases {
		got := tc.c.LogicalType()
		if got.Kind != tc.kind || got.IntegerBits != tc.bits {
			t.Fatalf("%+v => %+v", tc.c, got)
		}
		if copy := got.Column().LogicalType(); copy.Kind != got.Kind {
			t.Fatalf("roundtrip %+v => %+v", got, copy)
		}
	}
}
func TestLogicalTypesKeepModifiersAndProtocolNeutralValues(t *testing.T) {
	for _, name := range []string{"NUMERIC(20,4)", "VARCHAR(128)", "TIMESTAMP(3) WITH TIME ZONE", "UUID", "JSONB", "BOOLEAN"} {
		typ := LogicalTypeFromDeclaration(name)
		if !typ.Known() {
			t.Fatal(name)
		}
		if got := typ.Column().LogicalType(); got != typ {
			t.Fatalf("%s %+v => %+v", name, typ, got)
		}
	}
	v := NewIntValue(1).WithSemanticType(TypeBoolean)
	if v.PGType != "" || v.LogicalType().Kind != TypeBoolean {
		t.Fatal(v)
	}
	legacy := Value{Type: ColInteger, Int: 1, PGType: "BOOLEAN"}
	if legacy.LogicalType().Kind != TypeBoolean {
		t.Fatal("legacy compatibility")
	}
	data, e := json.Marshal(v)
	if e != nil || string(data) != "1" {
		t.Fatalf("row JSON changed %s %v", data, e)
	}
	c := LogicalTypeFromDeclaration("TIMESTAMPTZ").Column()
	data, e = json.Marshal(c)
	if e != nil {
		t.Fatal(e)
	}
	var restored ColumnDef
	if e = json.Unmarshal(data, &restored); e != nil {
		t.Fatal(e)
	}
	if restored.SemanticType != TypeUnknown {
		t.Fatal("transient ID persisted")
	}
}
