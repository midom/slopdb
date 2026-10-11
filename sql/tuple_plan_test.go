package sql

import (
	"bytes"
	"math"
	"testing"
)

func TestTupleBetweenBounds(t *testing.T) {
	info := &TableInfo{Name: "t", PKColumns: []string{"a", "b", "c"}, Columns: []ColumnDef{
		{Name: "a", ColType: ColInteger, NotNull: true},
		{Name: "b", ColType: ColInteger, NotNull: true},
		{Name: "c", ColType: ColInteger, NotNull: true},
	}}
	secondary := &IndexInfo{Name: "bc", Table: "t", Columns: []string{"b", "c"}}
	for _, tc := range []struct {
		name, where string
		index       *IndexInfo
		contains    func(a, b int64) bool
	}{
		{"leading", "a BETWEEN 1 AND 2", nil, func(a, b int64) bool { return a >= 1 && a <= 2 }},
		{"negative", "a BETWEEN -4 AND -2", nil, func(a, b int64) bool { return a >= -4 && a <= -2 }},
		{"signed", "a BETWEEN -2 AND +2", nil, func(a, b int64) bool { return a >= -2 && a <= 2 }},
		{"equality_prefix", "a=3 AND b BETWEEN 1 AND 4", nil, func(a, b int64) bool { return a == 3 && b >= 1 && b <= 4 }},
		{"in_prefixes", "a IN(1,3) AND b BETWEEN 1 AND 4", nil, func(a, b int64) bool { return (a == 1 || a == 3) && b >= 1 && b <= 4 }},
		{"tighter_comparisons", "a=3 AND b BETWEEN 1 AND 4 AND b>1 AND b<4", nil, func(a, b int64) bool { return a == 3 && b > 1 && b < 4 }},
		{"overlapping", "a=3 AND b BETWEEN 1 AND 4 AND b BETWEEN 2 AND 5", nil, func(a, b int64) bool { return a == 3 && b >= 2 && b <= 4 }},
		{"contradictory", "a BETWEEN 4 AND 2", nil, func(a, b int64) bool { return false }},
		{"qualified", "x.a BETWEEN 1 AND 2", nil, func(a, b int64) bool { return a >= 1 && a <= 2 }},
		{"secondary", "b BETWEEN 1 AND 4", secondary, func(a, b int64) bool { return b >= 1 && b <= 4 }},
		{"secondary_prefix", "b=3 AND c BETWEEN 1 AND 4", secondary, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statements, err := Parse("SELECT * FROM t AS x WHERE " + tc.where)
			if err != nil {
				t.Fatal(err)
			}
			stmt := statements[0].(*SelectStmt)
			plan := buildTuplePlan(stmt, info, tc.index, tupleConstraints(stmt.Where, "t", "x", info))
			if plan == nil {
				t.Fatal("BETWEEN fell back to an unbounded scan")
			}
			// Trailing key components must include both endpoints, even at
			// int64 extremes. Compare the actual encoded keys to the bounds.
			for a := int64(-6); a <= 6; a++ {
				for b := int64(-1); b <= 6; b++ {
					for _, c := range []int64{math.MinInt64, 0, 1, 4, 5, math.MaxInt64} {
						row := map[string]Value{"a": NewIntValue(a), "b": NewIntValue(b), "c": NewIntValue(c)}
						key := info.encodePK("t", row)
						if tc.index != nil {
							key = encodeRowIndexKey(tc.index, row, key)
						}
						inside := false
						for _, interval := range plan.intervals {
							inside = inside || bytes.Compare(key, interval.lower) >= 0 && bytes.Compare(key, interval.upper) < 0
						}
						want := b == 3 && c >= 1 && c <= 4
						if tc.contains != nil {
							want = tc.contains(a, b)
						}
						if inside != want {
							t.Fatalf("key (%d,%d,%d): in bounded ranges=%v, want %v", a, b, c, inside, want)
						}
					}
				}
			}
		})
	}
}

func TestTupleBetweenResidual(t *testing.T) {
	info := &TableInfo{Name: "t", PKColumns: []string{"a", "b"}, Columns: []ColumnDef{
		{Name: "a", ColType: ColInteger, NotNull: true},
		{Name: "b", ColType: ColInteger, NotNull: true},
	}}
	// Only the integer lower bound is usable. Removing the whole BETWEEN
	// would lose the real upper bound and return a=3 and a=4 incorrectly.
	statements, err := Parse("SELECT * FROM t WHERE a BETWEEN 1 AND 4 AND a BETWEEN 2 AND 2.5")
	if err != nil {
		t.Fatal(err)
	}
	stmt := statements[0].(*SelectStmt)
	plan := buildTuplePlan(stmt, info, nil, tupleConstraints(stmt.Where, "t", "", info))
	if plan == nil || plan.residual == nil {
		t.Fatal("expected bounded access with a residual BETWEEN predicate")
	}
	schema := &Schema{}
	for a := int64(2); a <= 4; a++ {
		match, err := schema.evalCondition(plan.residual, map[string]Value{"a": NewIntValue(a), "b": NewIntValue(0)})
		if err != nil {
			t.Fatal(err)
		}
		if match != (a == 2) {
			t.Fatalf("residual matched a=%d: %v", a, match)
		}
	}
	for _, where := range []string{"a NOT BETWEEN 1 AND 4", "a BETWEEN 1 AND 4 OR a=6"} {
		statements, err := Parse("SELECT * FROM t WHERE " + where)
		if err != nil {
			t.Fatal(err)
		}
		stmt := statements[0].(*SelectStmt)
		if plan := buildTuplePlan(stmt, info, nil, tupleConstraints(stmt.Where, "t", "", info)); plan != nil {
			t.Fatalf("incorrect conjunctive BETWEEN bounds for %s", where)
		}
	}
}

func TestTupleSignedLiteralOverflow(t *testing.T) {
	minimum := &LiteralExpr{Value: NewIntValue(math.MinInt64)}
	overflow := &UnaryExpr{Op: "-", Right: minimum}
	if _, ok := tupleLiteral(overflow); ok {
		t.Fatal("folded a sign that overflows int64")
	}
	if tupleSafePredicate(overflow) {
		t.Fatal("unsafe LIMIT pushdown can hide integer overflow")
	}
	for _, expr := range []Expression{
		&UnaryExpr{Op: "+", Right: minimum},
		&UnaryExpr{Op: "-", Right: &LiteralExpr{Value: NewIntValue(math.MaxInt64)}},
	} {
		literal, ok := tupleLiteral(expr)
		if !ok {
			t.Fatal("rejected a valid signed integer literal")
		}
		value, err := (&Schema{}).evalExpr(expr, nil)
		if err != nil || value.Type != ColInteger || value.Null || value.Int != literal.Value.Int {
			t.Fatalf("folded value %v differs from SQL evaluation %v: %v", literal.Value, value, err)
		}
	}
}
