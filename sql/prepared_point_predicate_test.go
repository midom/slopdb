package sql

import "testing"

func TestPreparedPointPredicateProofShapeAndTypes(t *testing.T) {
	cases := []struct {
		where   string
		columns []ColumnDef
		keys    []string
		want    bool
	}{
		{"id=?", []ColumnDef{{Name: "id", ColType: ColInteger}}, []string{"id"}, true},
		{"?=id", []ColumnDef{{Name: "id", ColType: ColInteger}}, []string{"id"}, true},
		{"a=? AND b=?", []ColumnDef{{Name: "a", ColType: ColInteger}, {Name: "b", ColType: ColInteger}}, []string{"a", "b"}, true},
		{"id=? AND id=?", []ColumnDef{{Name: "id", ColType: ColInteger}}, []string{"id"}, false},
		{"id=? AND v=1", []ColumnDef{{Name: "id", ColType: ColInteger}, {Name: "v", ColType: ColInteger}}, []string{"id"}, false},
		{"id=? AND random()>0", []ColumnDef{{Name: "id", ColType: ColInteger}}, []string{"id"}, false},
		{"id=? OR id=2", []ColumnDef{{Name: "id", ColType: ColInteger}}, []string{"id"}, false},
		{"t.id=?", []ColumnDef{{Name: "id", ColType: ColInteger}}, []string{"id"}, false},
		{"id=BINARY ?", []ColumnDef{{Name: "id", ColType: ColInteger}}, []string{"id"}, false},
		{"id=?", []ColumnDef{{Name: "id", ColType: ColInteger, Unsigned: true}}, []string{"id"}, false},
		{"id=?", []ColumnDef{{Name: "id", ColType: ColInteger, Declared: "BOOLEAN"}}, []string{"id"}, false},
		{"id=?", []ColumnDef{{Name: "id", ColType: ColReal}}, []string{"id"}, false},
		{"id=?", []ColumnDef{{Name: "id", ColType: ColText}}, []string{"id"}, false},
		{"id=?", []ColumnDef{{Name: "id", ColType: ColDecimal}}, []string{"id"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.where+tc.columns[0].Declared, func(t *testing.T) {
			q, e := Prepare("SELECT * FROM t WHERE " + tc.where)
			if e != nil {
				t.Fatal(e)
			}
			args := make([]Value, q.NumInput())
			for i := range args {
				args[i] = NewIntValue(int64(i + 1))
			}
			bound, e := q.Bind(args)
			if e != nil {
				t.Fatal(e)
			}
			defer bound.Release()
			statement := bound.statements[0].(*SelectStmt)
			plan := statement.prepared
			plan.info = &TableInfo{Name: "t", ID: 1, Columns: tc.columns, PKColumns: tc.keys}
			plan.compilePointBindings()
			if plan.pointPredicateExact != tc.want {
				t.Fatalf("proof=%v want%v", plan.pointPredicateExact, tc.want)
			}
			_, ok := plan.primaryKey("t", plan.info)
			if plan.pointPredicateSatisfied != (tc.want && ok) {
				t.Fatalf("bound proof %v key%v", plan.pointPredicateSatisfied, ok)
			}
		})
	}
}
func TestPreparedPointPredicateProofChecksEachBinding(t *testing.T) {
	q, e := Prepare("SELECT * FROM t WHERE id=?")
	if e != nil {
		t.Fatal(e)
	}
	bound, e := q.Bind([]Value{NewIntValue(1)})
	if e != nil {
		t.Fatal(e)
	}
	defer bound.Release()
	plan := bound.statements[0].(*SelectStmt).prepared
	plan.info = &TableInfo{Name: "t", ID: 1, Columns: []ColumnDef{{Name: "id", ColType: ColInteger}}, PKColumns: []string{"id"}}
	plan.compilePointBindings()
	if !plan.pointPredicateExact {
		t.Fatal("plain signed equality not proven")
	}
	plain := NewIntValue(1)
	binary := plain
	binary.Binary = true
	numeric := plain
	numeric.NumericText = "2"
	rational := plain
	rational.NumericExact = "2"
	enum := plain
	enum.EnumValue = true
	enum.EnumOrdinal = 2
	collated := plain
	collated.Collation = BinaryCollation
	for _, value := range []Value{plain, NewIntValue(-1), NewNullValue(), NewTextValue("1"), NewRealValue(1), NewUnsignedValue(1), binary, numeric, rational, enum, collated, plain.WithSemanticType(TypeBoolean), plain} {
		plan.equalities[0].literal.Value = value
		plan.pointKeyReady = false
		plan.pointPredicateSatisfied = false
		_, ok := plan.primaryKey("t", plan.info)
		want := preparedPlainSignedInteger(value) && ok
		if plan.pointPredicateSatisfied != want {
			t.Fatalf("value%+v key%v proof%v want%v", value, ok, plan.pointPredicateSatisfied, want)
		}
	}
}
func TestPreparedPointPredicateScratchBoundUsesBlobCapacity(t *testing.T) {
	p := &preparedExecutionPlan{pointSelectScratch: map[string]Value{"b": NewBlobValue(make([]byte, 1, 128<<10))}}
	p.releaseValues()
	if len(p.pointSelectScratch) != 0 {
		t.Fatal("retained oversized private BLOB allocation")
	}
}
