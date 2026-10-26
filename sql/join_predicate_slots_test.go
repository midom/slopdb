package sql

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

func joinSlotFixture() *joinPlan {
	return &joinPlan{order: []int{0, 1}, relations: []*joinRelation{{alias: "l", needed: map[string]bool{"a": true, "unused": true}}, {alias: "r", needed: map[string]bool{"b": true, "padding": true}}}, firstOwner: map[string]int{"a": 0, "unused": 0, "b": 1, "padding": 1}}
}

func TestJoinPredicateSlotsCompactAndEquivalent(t *testing.T) {
	expression := typedTestExpr(t, "(l.a*l.a+r.b*r.b>l.a*r.b) AND l.a+r.b<100")
	decimal, err := NewDecimalValue("0.5")
	if err != nil {
		t.Fatal(err)
	}
	values := []Value{NewNullValue(), NewIntValue(-2), NewIntValue(0), NewIntValue(3), NewIntValue(math.MaxInt64), NewRealValue(0.5), NewUnsignedValue(math.MaxUint64), decimal, NewTextValue("3")}
	for _, dialect := range []Dialect{DialectNative, DialectMySQL, DialectPostgreSQL} {
		s := &Schema{dialect: dialect, collation: BinaryCollation}
		vectors := s.compileJoinPredicateSlots(joinSlotFixture(), [][]Expression{nil, {expression}})
		if vectors == nil || len(vectors.values) != 2 || len(vectors.bindings[0]) != 1 || len(vectors.bindings[1]) != 1 {
			t.Fatal("predicate did not compact unused columns")
		}
		for _, a := range values {
			for _, b := range values {
				vectors.bind(0, map[string]Value{"a": a})
				vectors.bind(1, map[string]Value{"b": b})
				want, wantErr := s.evalExpr(expression, map[string]Value{"l.a": a, "r.b": b})
				got, err := vectors.programs[1][0].eval(vectors.values)
				if (err == nil) != (wantErr == nil) || err != nil && err.Error() != wantErr.Error() || err == nil && !reflect.DeepEqual(got, want) {
					t.Fatalf("dialect %v a=%#v b=%#v: got %#v/%v want %#v/%v", dialect, a, b, got, err, want, wantErr)
				}
			}
		}
		vectors.bind(0, map[string]Value{"a": NewIntValue(3)})
		vectors.reset(1)
		got, err := vectors.programs[1][0].eval(vectors.values)
		if err != nil || !got.Null {
			t.Fatalf("unmatched outer row %#v/%v", got, err)
		}
		vectors.close()
		for _, value := range vectors.values {
			if !reflect.DeepEqual(value, Value{}) {
				t.Fatal("vector retained input after close")
			}
		}
		for _, text := range []string{"l.a=r.b", "COALESCE(l.a,r.b)>0", "EXISTS(SELECT 1)"} {
			if s.compileJoinPredicateSlots(joinSlotFixture(), [][]Expression{nil, {typedTestExpr(t, text)}}) != nil {
				t.Fatalf("small/unsupported expression %s lowered", text)
			}
		}
	}
}

func TestJoinPredicateSlotsOuterSQLAnswers(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	statisticsSQL(t, s, "CREATE TABLE jl(id INTEGER PRIMARY KEY,a INTEGER);CREATE TABLE jr(id INTEGER PRIMARY KEY,b INTEGER)")
	var left, right []string
	for i := 0; i < 21; i++ {
		left = append(left, fmt.Sprintf("(%d,%d)", i, i%6))
		if i < 16 {
			value := "NULL"
			if i%5 != 0 {
				value = fmt.Sprint(i % 7)
			}
			right = append(right, fmt.Sprintf("(%d,%s)", i, value))
		}
	}
	statisticsSQL(t, s, "INSERT INTO jl VALUES "+strings.Join(left, ","))
	statisticsSQL(t, s, "INSERT INTO jr VALUES "+strings.Join(right, ","))
	for _, dialect := range []Dialect{DialectNative, DialectMySQL, DialectPostgreSQL} {
		if err := s.SetDialect(dialect); err != nil {
			t.Fatal(err)
		}
		result := statisticsSQL(t, s, "SELECT l.id AS id,r.b AS b FROM jl l LEFT JOIN jr r ON l.id=r.id AND l.a*l.a+r.b*r.b>l.a*r.b WHERE l.a+r.b<100 OR r.b IS NULL ORDER BY l.id")
		if len(result.Rows) != 21 {
			t.Fatalf("dialect %v rows %d", dialect, len(result.Rows))
		}
		for i, row := range result.Rows {
			a, b := i%6, i%7
			matched := i < 16 && i%5 != 0 && a*a+b*b > a*b
			if row["id"].Int != int64(i) || matched && (row["b"].Null || row["b"].Int != int64(b)) || !matched && !row["b"].Null {
				t.Fatalf("dialect %v row %d: %#v", dialect, i, row)
			}
		}
	}
}

func BenchmarkJoinPredicateSlots(b *testing.B) {
	expression := typedTestExpr(b, "(l.a*l.a+r.b*r.b>l.a*r.b) AND l.a+r.b<100")
	s := &Schema{collation: BinaryCollation}
	vectors := s.compileJoinPredicateSlots(joinSlotFixture(), [][]Expression{nil, {expression}})
	if vectors == nil {
		b.Fatal("not compiled")
	}
	defer vectors.close()
	left, right := map[string]Value{"a": NewIntValue(3)}, map[string]Value{"b": NewIntValue(5)}
	row := map[string]Value{"l.a": left["a"], "r.b": right["b"]}
	for _, bound := range []bool{false, true} {
		name := "map_AST"
		if bound {
			name = "compact_slots"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var value Value
				var err error
				if bound {
					vectors.bind(0, left)
					vectors.bind(1, right)
					value, err = vectors.programs[1][0].eval(vectors.values)
				} else {
					value, err = s.evalExpr(expression, row)
				}
				if err != nil || value.Null || value.Int != 1 {
					b.Fatalf("%#v/%v", value, err)
				}
			}
		})
	}
}
