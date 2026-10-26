package sql

import (
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestTypedIntegerNumericInstructions(t *testing.T) {
	values := []int64{math.MinInt64, math.MinInt64 + 1, -9007199254740993, -2, -1, 0, 1, 2, 9007199254740993, math.MaxInt64 - 1, math.MaxInt64}
	seed := uint64(0x7836a94c)
	for i := 0; i < 48; i++ {
		seed = seed*6364136223846793005 + 1442695040888963407
		values = append(values, int64(seed))
	}
	for _, dialect := range []Dialect{DialectNative, DialectMySQL, DialectPostgreSQL} {
		for _, op := range []string{"%", "=", "<>", "<", "<=", ">", ">=", "IS", "IS NOT"} {
			program, ok := compileTypedProgram(typedTestExpr(t, "a "+op+" b"), map[string]int{"a": 0, "b": 1})
			if !ok {
				t.Fatal(op)
			}
			program.setDialect(&Schema{dialect: dialect, collation: BinaryCollation})
			for _, a := range values {
				for _, b := range values {
					got, err := program.eval([]Value{NewIntValue(a), NewIntValue(b)})
					if op == "%" && b == 0 {
						if err == nil || !strings.Contains(err.Error(), "division by zero") {
							t.Fatalf("%d %% 0: %v", a, err)
						}
						continue
					}
					want := NewIntValue(0)
					switch op {
					case "%":
						want.Int = a % b
					case "=", "IS":
						if a == b {
							want.Int = 1
						}
					case "<>", "IS NOT":
						if a != b {
							want.Int = 1
						}
					case "<":
						if a < b {
							want.Int = 1
						}
					case "<=":
						if a <= b {
							want.Int = 1
						}
					case ">":
						if a > b {
							want.Int = 1
						}
					case ">=":
						if a >= b {
							want.Int = 1
						}
					}
					if dialect == DialectPostgreSQL && op != "%" {
						want = want.WithSemanticType(TypeBoolean)
					}
					if err != nil || !reflect.DeepEqual(got, want) {
						t.Fatalf("dialect %v %d %s %d: got %#v/%v want %#v", dialect, a, op, b, got, err, want)
					}
				}
			}
			program.clearRegisters()
		}
	}
}

func TestTypedIntegerFastPathGatesAndOwnership(t *testing.T) {
	left, right := NewIntValue(7), NewIntValue(2)
	dst := NewBlobValue([]byte{1, 2, 3})
	dst.Text = "stale"
	dst.NumericExact = "stale"
	if !typedIntegerCompare(&dst, &left, &right, ">", true) || !reflect.DeepEqual(dst, NewIntValue(1).WithSemanticType(TypeBoolean)) {
		t.Fatalf("destination not reset %#v", dst)
	}
	if !typedIntegerCompare(&left, &left, &right, ">", false) || !reflect.DeepEqual(left, NewIntValue(1)) {
		t.Fatal("aliased destination changed operand before comparison")
	}
	left = NewIntValue(math.MinInt64)
	right = NewIntValue(-1)
	if handled, err := typedIntegerModulo(&left, &left, &right); !handled || err != nil || !reflect.DeepEqual(left, NewIntValue(0)) {
		t.Fatalf("MinInt64 remainder %#v %v", left, err)
	}
	for _, operand := range []Value{NewRealValue(2), NewUnsignedValue(2), NewTextValue("2"), NewBlobValue([]byte{2}), func() Value { v := NewIntValue(2); v.EnumValue = true; v.EnumOrdinal = 1; return v }(), func() Value { v := NewIntValue(2); v.HexLiteral = true; return v }()} {
		right = NewIntValue(2)
		dst = NewTextValue("untouched")
		before := dst
		if typedIntegerCompare(&dst, &operand, &right, "=", false) || !reflect.DeepEqual(dst, before) {
			t.Fatalf("unproven comparison handled %#v", operand)
		}
		if handled, _ := typedIntegerModulo(&dst, &operand, &right); handled || !reflect.DeepEqual(dst, before) {
			t.Fatalf("unproven modulo handled %#v", operand)
		}
	}
	for _, op := range []string{"=", "IS", "IS NOT"} {
		a, b := NewNullValue(), NewIntValue(2)
		dst = Value{Blob: []byte{4}}
		if !typedIntegerCompare(&dst, &a, &b, op, true) {
			t.Fatal("NULL fallback")
		}
		want := NewNullValue().WithSemanticType(TypeBoolean)
		if op == "IS" {
			want = NewIntValue(0).WithSemanticType(TypeBoolean)
		}
		if op == "IS NOT" {
			want = NewIntValue(1).WithSemanticType(TypeBoolean)
		}
		if !reflect.DeepEqual(dst, want) {
			t.Fatalf("NULL %s %#v want %#v", op, dst, want)
		}
	}
}
