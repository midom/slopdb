package sql

import (
	"bytes"
	"math"
	"reflect"
	"strings"
	"testing"
)

func typedTestExpr(tb testing.TB, text string) Expression {
	tb.Helper()
	statements, err := Parse("SELECT " + text + " FROM t")
	if err != nil {
		tb.Fatal(err)
	}
	return statements[0].(*SelectStmt).Columns[0].Expr
}

func TestTypedExpressionSemantics(t *testing.T) {
	for _, tc := range []struct {
		text string
		want Value
		err  string
	}{
		{"a+1", NewIntValue(9007199254740994), ""},
		{"a=9007199254740993", NewIntValue(1), ""},
		{"a=9007199254740992", NewIntValue(0), ""},
		{"a='9007199254740993'", NewIntValue(1), ""},
		{"'9007199254740993'=a", NewIntValue(1), ""},
		{"0 AND 1/0", NewIntValue(0), ""},
		{"1 OR 1/0", NewIntValue(1), ""},
		{"(0 AND 1/0) OR (1 AND a>0)", NewIntValue(1), ""},
		{"NULL AND 0", NewIntValue(0), ""},
		{"NULL OR 1", NewIntValue(1), ""},
		{"NULL AND 1", NewNullValue(), ""},
		{"NULL OR 0", NewNullValue(), ""},
		{"a BETWEEN NULL AND 1", NewIntValue(0), ""},
		{"a NOT BETWEEN NULL AND 1", NewIntValue(1), ""},
		{"a BETWEEN 1 AND NULL", NewNullValue(), ""},
		{"NULL IS NULL", NewIntValue(1), ""},
		{"NULL IS NOT NULL", NewIntValue(0), ""},
		{"'a'||'b'", NewTextValue("ab"), ""},
		{"X'0102'", NewBlobValue([]byte{1, 2}), ""},
		{"9223372036854775807+1", Value{}, "integer overflow"},
		{"(-9223372036854775807-1)/-1", Value{}, "integer overflow"},
		{"-(-9223372036854775807-1)", Value{}, "integer overflow"},
		{"a/0", Value{}, "division by zero"},
		{"a+X'01'", Value{}, "arithmetic requires numeric operands"},
		{"a%0.5", Value{}, "modulo requires integer operands"},
		{"1e308*1e308", Value{}, "numeric value out of range"},
	} {
		t.Run(tc.text, func(t *testing.T) {
			p, ok := compileTypedProgram(typedTestExpr(t, tc.text), map[string]int{"a": 0})
			if !ok {
				t.Fatal("supported expression did not compile")
			}
			// Reusing the same registers must preserve branches and NULLs.
			for i := 0; i < 3; i++ {
				v, err := p.eval([]Value{NewIntValue(9007199254740993)})
				if tc.err != "" {
					if err == nil || !strings.Contains(err.Error(), tc.err) {
						t.Fatalf("error %v, want %q", err, tc.err)
					}
				} else if err != nil || !reflect.DeepEqual(v, tc.want) {
					t.Fatalf("value %#v, error %v; want %#v", v, err, tc.want)
				}
			}
		})
	}
}

func TestTypedExpressionDifferential(t *testing.T) {
	values := []Value{NewNullValue(), NewIntValue(0), NewIntValue(-1), NewIntValue(math.MinInt64), NewIntValue(math.MaxInt64), NewIntValue(9007199254740993), NewRealValue(0.5), NewRealValue(9007199254740992), NewTextValue("12"), NewBlobValue([]byte{0, 1})}
	s := &Schema{}
	for _, text := range []string{"a+b", "a-b", "a*b", "a/b", "a%b", "a=b", "a<>b", "a<b", "a<=b", "a>b", "a>=b", "a AND b", "a OR b", "a IS b", "a IS NOT b", "a||b", "NOT a", "-a", "+a", "a BETWEEN b AND 4", "a NOT BETWEEN b AND 4", "(a=0 AND b/0>1) OR a IS NULL"} {
		expr := typedTestExpr(t, text)
		p, ok := compileTypedProgram(expr, map[string]int{"a": 0, "b": 1})
		if !ok {
			t.Fatalf("cannot compile %s", text)
		}
		for _, a := range values {
			for _, b := range values {
				want, wantErr := s.evalExpr(expr, map[string]Value{"a": a, "b": b})
				got, err := p.eval([]Value{a, b})
				if (err == nil) != (wantErr == nil) || err != nil && err.Error() != wantErr.Error() || err == nil && !reflect.DeepEqual(got, want) {
					t.Fatalf("%s a=%#v b=%#v: got %#v / %v, want %#v / %v", text, a, b, got, err, want, wantErr)
				}
			}
		}
	}
	for _, text := range []string{"COALESCE(a,0)", "CASE WHEN a=0 THEN 0 ELSE 1/a END", "a IN(1,2)", "RANDOM()", "missing", "(SELECT 1)"} {
		if _, ok := compileTypedProgram(typedTestExpr(t, text), map[string]int{"a": 0}); ok {
			t.Fatalf("unsupported expression %s must retain general execution", text)
		}
	}
}

func TestTypedRowDecoding(t *testing.T) {
	data := append(encodeValue(NewIntValue(13)), encodeValue(NewTextValue(strings.Repeat("unused", 100)))...)
	data = append(data, encodeValue(NewBlobValue([]byte{0, 1, 2}))...)
	data = append(data, encodeValue(NewTextValue("owned"))...)
	row := make([]Value, 4)
	decodeTypedRow(data, []int{0, -1, 1, 2, 3}, row)
	want := []Value{NewIntValue(13), NewBlobValue([]byte{0, 1, 2}), NewTextValue("owned"), NewNullValue()}
	if !reflect.DeepEqual(row, want) {
		t.Fatalf("got %#v, want %#v", row, want)
	}
	// Iterator buffers may be reused after the callback: decoded BLOB/text
	// values retained for representative groups must own their contents.
	clear(data)
	if !bytes.Equal(row[1].Blob, []byte{0, 1, 2}) || row[2].Text != "owned" {
		t.Fatal("decoded values alias iterator buffers")
	}
}

func BenchmarkTypedQ6Kernel(b *testing.B) {
	info := &TableInfo{Name: "t", Columns: []ColumnDef{{Name: "shipdate", ColType: ColText}, {Name: "discount", ColType: ColReal}, {Name: "quantity", ColType: ColInteger}, {Name: "price", ColType: ColReal}, {Name: "unused", ColType: ColText}}}
	data := encodeRow(map[string]Value{"shipdate": NewTextValue("1994-06-01"), "discount": NewRealValue(0.06), "quantity": NewIntValue(12), "price": NewRealValue(123.45), "unused": NewTextValue(strings.Repeat("x", 200))}, info)
	where := typedTestExpr(b, "shipdate>='1994-01-01' AND shipdate<'1995-01-01' AND discount BETWEEN 0.05 AND 0.07 AND quantity<24")
	arg := typedTestExpr(b, "price*discount")
	f := &FuncCallExpr{Name: "SUM", Args: []Expression{arg}}
	b.Run("map_AST", func(b *testing.B) {
		s := &Schema{}
		columns := []scanColumn{{name: "shipdate", needed: true}, {name: "discount", needed: true}, {name: "quantity", needed: true}, {name: "price", needed: true}, {name: "unused"}}
		row := make(map[string]Value)
		state := aggregateState{}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			decodeScanRow(data, columns, row)
			match, err := s.evalCondition(where, row)
			if err != nil {
				b.Fatal(err)
			}
			if match {
				if err := s.addAggregate(&state, f, row); err != nil {
					b.Fatal(err)
				}
			}
		}
		if state.count != int64(b.N) {
			b.Fatal("unexpected count")
		}
	})
	b.Run("typed_batches", func(b *testing.B) {
		slots := map[string]int{"shipdate": 0, "discount": 1, "quantity": 2, "price": 3}
		filter, _ := compileTypedProgram(where, slots)
		value, _ := compileTypedProgram(arg, slots)
		batch := make([]Value, typedBatchRows*4)
		state := aggregateState{}
		b.ReportAllocs()
		b.ResetTimer()
		for start := 0; start < b.N; start += typedBatchRows {
			n := min(typedBatchRows, b.N-start)
			for i := 0; i < n; i++ {
				decodeTypedRow(data, []int{0, 1, 2, 3, -1}, batch[i*4:(i+1)*4])
			}
			for i := 0; i < n; i++ {
				row := batch[i*4 : (i+1)*4]
				match, err := filter.eval(row)
				if err != nil {
					b.Fatal(err)
				}
				if truthValue(match) {
					v, err := value.eval(row)
					if err != nil {
						b.Fatal(err)
					}
					if err := addAggregateValue(&state, f, v); err != nil {
						b.Fatal(err)
					}
				}
			}
		}
		if state.count != int64(b.N) {
			b.Fatal("unexpected count")
		}
	})
}

func BenchmarkTypedQ1Kernel(b *testing.B) {
	info := &TableInfo{Name: "t", Columns: []ColumnDef{{Name: "flag", ColType: ColText}, {Name: "status", ColType: ColText}, {Name: "quantity", ColType: ColInteger}, {Name: "price", ColType: ColReal}, {Name: "discount", ColType: ColReal}, {Name: "tax", ColType: ColReal}, {Name: "shipdate", ColType: ColText}, {Name: "unused", ColType: ColText}}}
	data := encodeRow(map[string]Value{"flag": NewTextValue("A"), "status": NewTextValue("F"), "quantity": NewIntValue(12), "price": NewRealValue(123.45), "discount": NewRealValue(0.06), "tax": NewRealValue(0.02), "shipdate": NewTextValue("1994-06-01"), "unused": NewTextValue(strings.Repeat("x", 200))}, info)
	where := typedTestExpr(b, "shipdate<='1998-09-02'")
	var functions []*FuncCallExpr
	for _, expr := range []string{"SUM(quantity)", "SUM(price)", "SUM(price*(1-discount))", "SUM(price*(1-discount)*(1+tax))", "AVG(quantity)", "AVG(price)", "AVG(discount)"} {
		functions = append(functions, typedTestExpr(b, expr).(*FuncCallExpr))
	}
	b.Run("map_AST", func(b *testing.B) {
		s := &Schema{}
		var columns []scanColumn
		for _, col := range info.Columns {
			columns = append(columns, scanColumn{name: col.Name, needed: col.Name != "unused"})
		}
		row := make(map[string]Value)
		states := make([]aggregateState, len(functions))
		var keys [128]byte
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			decodeScanRow(data, columns, row)
			match, err := s.evalCondition(where, row)
			if err != nil || !match {
				b.Fatalf("filter %v, %v", match, err)
			}
			key := append(keys[:0], valueIdentity(row["flag"])...)
			key = append(key, valueIdentity(row["status"])...)
			if len(key) != 6 {
				b.Fatal(key)
			}
			for j, f := range functions {
				if err := s.addAggregate(&states[j], f, row); err != nil {
					b.Fatal(err)
				}
			}
		}
		if states[0].count != int64(b.N) {
			b.Fatal("unexpected count")
		}
	})
	b.Run("typed_batches", func(b *testing.B) {
		slots := map[string]int{"flag": 0, "status": 1, "quantity": 2, "price": 3, "discount": 4, "tax": 5, "shipdate": 6}
		filter, _ := compileTypedProgram(where, slots)
		var args []*typedProgram
		for _, f := range functions {
			arg, _ := compileTypedProgram(f.Args[0], slots)
			args = append(args, arg)
		}
		batch := make([]Value, typedBatchRows*7)
		states := make([]aggregateState, len(functions))
		var keys [128]byte
		b.ReportAllocs()
		b.ResetTimer()
		for start := 0; start < b.N; start += typedBatchRows {
			n := min(typedBatchRows, b.N-start)
			for i := 0; i < n; i++ {
				decodeTypedRow(data, []int{0, 1, 2, 3, 4, 5, 6, -1}, batch[i*7:(i+1)*7])
			}
			for i := 0; i < n; i++ {
				row := batch[i*7 : (i+1)*7]
				match, err := filter.eval(row)
				if err != nil || !truthValue(match) {
					b.Fatalf("filter %#v, %v", match, err)
				}
				key := appendValueIdentity(keys[:0], row[0])
				key = appendValueIdentity(key, row[1])
				if len(key) != 6 {
					b.Fatal(key)
				}
				for j, arg := range args {
					v, err := arg.eval(row)
					if err != nil {
						b.Fatal(err)
					}
					if err := addAggregateValue(&states[j], functions[j], v); err != nil {
						b.Fatal(err)
					}
				}
			}
		}
		if states[0].count != int64(b.N) {
			b.Fatal("unexpected count")
		}
	})
}
