package sql

import (
	"reflect"
	"strings"
	"testing"
)

func typedTextFixture(t testing.TB, where string) (*typedAggregate, *TableInfo) {
	t.Helper()
	info := &TableInfo{Name: "t", Columns: []ColumnDef{{Name: "shipdate", ColType: ColText}, {Name: "n", ColType: ColInteger}, {Name: "payload", ColType: ColBlob}}}
	argument := typedTestExpr(t, "n")
	aggregate, ok := compileTypedAggregate(info, "t", "t", typedTestExpr(t, where), nil, []*FuncCallExpr{{Name: "SUM", Args: []Expression{argument}}}, nil)
	if !ok {
		t.Fatalf("compile %s", where)
	}
	return aggregate, info
}

func TestTypedRawTextPredicateDifferential(t *testing.T) {
	values := []Value{NewNullValue(), NewTextValue(""), NewTextValue("1994-01-01"), NewTextValue("1994-06-01"), NewTextValue("1995-01-01"), NewTextValue("a\x00b"), NewTextValue(string([]byte{0xff, 0xfe})), NewIntValue(5), NewBlobValue([]byte("1994-06-01"))}
	predicates := []string{
		"shipdate>='1994-01-01' AND shipdate<'1995-01-01' AND n>0",
		"'1994-01-01'<=shipdate AND shipdate<'1995-01-01'",
		"shipdate BETWEEN '1994-01-01' AND '1995-01-01'",
		"shipdate NOT BETWEEN '1994-01-01' AND '1995-01-01'",
		"NOT (shipdate='1994-06-01' OR shipdate='')",
		"shipdate<'1995-01-01' OR n/0>1",
		"shipdate<'1995-01-01' AND n/0>1",
		"0 AND (shipdate<'1995-01-01' AND n/0>1)",
		"shipdate>0 AND n>0",
	}
	for _, dialect := range []Dialect{DialectNative, DialectMySQL, DialectPostgreSQL} {
		s := &Schema{dialect: dialect, collation: BinaryCollation}
		for _, where := range predicates {
			aggregate, info := typedTextFixture(t, where)
			aggregate.setDialect(s)
			expectedExpression := typedTestExpr(t, where)
			raw := aggregate.rawScan(s)
			layout := []int{aggregate.slots["shipdate"], aggregate.slots["n"], -1}
			columns := []scanColumn{{name: "shipdate", needed: true, definition: info.Columns[0]}, {name: "n", needed: true, definition: info.Columns[1]}, {name: "payload"}}
			for _, date := range values {
				for _, number := range []int64{0, 1} {
					data := encodeRow(map[string]Value{"shipdate": date, "n": NewIntValue(number), "payload": NewBlobValue([]byte{1, 2, 3})}, info)
					expectedRow := make(map[string]Value)
					decodeScanRow(data, columns, expectedRow)
					want, wantErr := s.evalExpr(expectedExpression, expectedRow)
					row := make([]Value, aggregate.width)
					program := aggregate.where
					if raw != nil && raw.decodeRow(data, layout, row) {
						program = aggregate.rawWhere
					} else if raw == nil {
						decodeTypedRow(data, layout, row)
					}
					aggregate.decorateRow(row)
					got, err := program.eval(row)
					if (err == nil) != (wantErr == nil) || err != nil && err.Error() != wantErr.Error() || err == nil && !reflect.DeepEqual(got, want) {
						t.Fatalf("dialect %v where %s date %#v n %d: got %#v/%v want %#v/%v", dialect, where, date, number, got, err, want, wantErr)
					}
					if raw != nil && program == aggregate.rawWhere && !raw.decode[layout[0]] && row[layout[0]].Text != "" {
						t.Fatal("predicate-only text was materialized")
					}
				}
			}
			aggregate.clearRegisters()
		}
	}
}

func TestTypedRawTextBindingProofRechecked(t *testing.T) {
	aggregate, _ := typedTextFixture(t, "shipdate='1994-06-01'")
	s := &Schema{dialect: DialectMySQL, collation: BinaryCollation}
	aggregate.setDialect(s)
	if aggregate.rawScan(s) == nil {
		t.Fatal("plain binary TEXT proof rejected")
	}
	literal := aggregate.rawText[0].node.literal
	for _, value := range []Value{NewIntValue(1994), NewNullValue(), func() Value { v := NewTextValue("a"); v.Collation = "NFC_CI"; return v }()} {
		literal.Value = value
		aggregate.clearRegisters()
		aggregate.setDialect(s)
		if aggregate.rawScan(s) != nil {
			t.Fatalf("unsafe rebound literal %#v used byte predicate", value)
		}
	}
	literal.Value = NewTextValue("1994-06-01")
	aggregate.setDialect(s)
	if aggregate.rawScan(s) == nil {
		t.Fatal("compatible rebound TEXT did not reenable proof")
	}
	s.collation = "NFC_CI"
	if aggregate.rawScan(s) != nil {
		t.Fatal("nonbinary database collation used byte predicate")
	}
}

func BenchmarkTypedRawDatePredicate(b *testing.B) {
	aggregate, info := typedTextFixture(b, "shipdate>='1994-01-01' AND shipdate<'1995-01-01' AND n>0")
	s := &Schema{collation: BinaryCollation}
	aggregate.setDialect(s)
	raw := aggregate.rawScan(s)
	if raw == nil {
		b.Fatal("no raw predicate")
	}
	layout := []int{aggregate.slots["shipdate"], aggregate.slots["n"], -1}
	data := encodeRow(map[string]Value{"shipdate": NewTextValue("1994-06-01"), "n": NewIntValue(1), "payload": NewBlobValue([]byte(strings.Repeat("x", 240)))}, info)
	for _, optimized := range []bool{false, true} {
		name := "decoded"
		if optimized {
			name = "encoded"
		}
		b.Run(name, func(b *testing.B) {
			row := make([]Value, aggregate.width)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				program := aggregate.where
				if optimized {
					if !raw.decodeRow(data, layout, row) {
						b.Fatal("fallback")
					}
					program = aggregate.rawWhere
				} else {
					decodeTypedRow(data, layout, row)
				}
				got, err := program.eval(row)
				if err != nil || got.Null || got.Int != 1 {
					b.Fatalf("%#v/%v", got, err)
				}
			}
		})
	}
}
