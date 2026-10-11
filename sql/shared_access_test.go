package sql

import (
	"bytes"
	"math"
	"testing"
)

func TestSharedScalarAndTypedRangePlans(t *testing.T) {
	for _, tc := range []struct {
		name, where string
		typ         ColumnType
		values      []Value
		match       func(Value) bool
	}{
		{"signed_integer", "x.a BETWEEN -2 AND +2", ColInteger, []Value{NewIntValue(-3), NewIntValue(-2), NewIntValue(0), NewIntValue(2), NewIntValue(3)}, func(v Value) bool { return v.Int >= -2 && v.Int <= 2 }},
		{"constant_bounds", "x.a BETWEEN 1+1 AND 2*2", ColInteger, []Value{NewIntValue(1), NewIntValue(2), NewIntValue(4), NewIntValue(5)}, func(v Value) bool { return v.Int >= 2 && v.Int <= 4 }},
		{"text_prefix", "x.a BETWEEN 'a' AND 'b'", ColText, []Value{NewTextValue(""), NewTextValue("\x00"), NewTextValue("a"), NewTextValue("a\x00"), NewTextValue("a:"), NewTextValue("aa"), NewTextValue("b"), NewTextValue("bb")}, func(v Value) bool { return v.Text >= "a" && v.Text <= "b" }},
		{"text_upper_only", "x.a < 'a'", ColText, []Value{NewTextValue(""), NewTextValue("\x00"), NewTextValue("a"), NewTextValue("b")}, func(v Value) bool { return v.Text < "a" }},
		{"blob", "x.a BETWEEN X'00' AND X'ff'", ColBlob, []Value{NewBlobValue(nil), NewBlobValue([]byte{0}), NewBlobValue([]byte{0, 255}), NewBlobValue([]byte{255}), NewBlobValue([]byte{255, 0})}, func(v Value) bool {
			return bytes.Compare(v.Blob, []byte{0}) >= 0 && bytes.Compare(v.Blob, []byte{255}) <= 0
		}},
		{"real_upper_only", "x.a < 2.5", ColReal, []Value{NewRealValue(math.Inf(-1)), NewRealValue(-2), NewRealValue(math.Copysign(0, -1)), NewRealValue(0), NewRealValue(2.5), NewRealValue(math.Inf(1))}, func(v Value) bool { return v.Real < 2.5 }},
	} {
		for _, composite := range []bool{false, true} {
			t.Run(tc.name+map[bool]string{false: "_scalar", true: "_tuple"}[composite], func(t *testing.T) {
				info := &TableInfo{Name: "t", PKColumns: []string{"a"}, Columns: []ColumnDef{{Name: "a", ColType: tc.typ, NotNull: true}, {Name: "b", ColType: ColInteger, NotNull: true}}}
				if composite {
					info.PKColumns = append(info.PKColumns, "b")
				}
				parsed, err := Parse("SELECT * FROM t AS x WHERE " + tc.where + " ORDER BY b DESC")
				if err != nil {
					t.Fatal(err)
				}
				stmt := parsed[0].(*SelectStmt)
				constraints := tupleConstraints(stmt.Where, "t", "x", info)
				tuple := buildTuplePlan(stmt, info, nil, constraints)
				access := buildAccessPath(info, nil, constraints)
				if tuple == nil || access == nil {
					t.Fatal("typed predicate fell back to a full table scan")
				}
				if tuple.ordered {
					t.Fatal("payload sort incorrectly treated as physical key order")
				}
				for _, v := range tc.values {
					key := info.encodePK("t", map[string]Value{"a": v, "b": NewIntValue(5)})
					for _, intervals := range [][]coveringInterval{tuple.intervals, access.intervals} {
						inside := false
						for _, interval := range intervals {
							inside = inside || bytes.Compare(key, interval.lower) >= 0 && bytes.Compare(key, interval.upper) < 0
						}
						if inside != tc.match(v) {
							t.Fatalf("physical intervals admit %v = %v, expected %v", v, inside, tc.match(v))
						}
					}
				}
			})
		}
	}
}

func TestSharedSecondaryVariableRangeSafety(t *testing.T) {
	info := &TableInfo{Name: "t", PKColumns: []string{"id"}, Columns: []ColumnDef{{Name: "id", ColType: ColInteger, NotNull: true}, {Name: "a", ColType: ColText}, {Name: "b", ColType: ColInteger}}}
	for _, idx := range []*IndexInfo{{Name: "a", Table: "t", Column: "a"}, {Name: "ab", Table: "t", Columns: []string{"a", "b"}}} {
		for _, where := range []string{"a < 'b'", "a > ''", "a BETWEEN 'a' AND 'b'", "a='a:'", "a IN('a','a:','a::')"} {
			parsed, err := Parse("SELECT * FROM t WHERE " + where + " ORDER BY a LIMIT 2")
			if err != nil {
				t.Fatal(err)
			}
			stmt := parsed[0].(*SelectStmt)
			constraints := tupleConstraints(stmt.Where, "t", "", info)
			plan := buildTuplePlan(stmt, info, idx, constraints)
			if plan == nil || plan.residual == nil || plan.ordered {
				t.Fatalf("unsafe secondary variable plan for %s", where)
			}
			for id, value := range []string{"", "\x00", "a", "a\x00", "a!", "a:", "a::", "aa", "b", "b\x00", "bb"} {
				row := map[string]Value{"id": NewIntValue(int64(id)), "a": NewTextValue(value), "b": NewIntValue(1)}
				want, err := (&Schema{}).evalCondition(stmt.Where, row)
				if err != nil {
					t.Fatal(err)
				}
				key := encodeRowIndexKey(idx, row, info.encodePK("t", row))
				inside := false
				for _, interval := range plan.intervals {
					inside = inside || bytes.Compare(key, interval.lower) >= 0 && bytes.Compare(key, interval.upper) < 0
				}
				if want && !inside {
					t.Fatalf("bounded %s loses value %q in %s", idx.Name, value, where)
				}
			}
		}
	}
}

func TestSharedSecondaryRealZeroSafety(t *testing.T) {
	info := &TableInfo{Name: "t", PKColumns: []string{"id"}, Columns: []ColumnDef{{Name: "id", ColType: ColInteger, NotNull: true}, {Name: "a", ColType: ColReal}, {Name: "b", ColType: ColInteger}}}
	for _, idx := range []*IndexInfo{{Name: "a", Table: "t", Column: "a"}, {Name: "ab", Table: "t", Columns: []string{"a", "b"}}} {
		parsed, err := Parse("SELECT * FROM t WHERE a > -1.5 AND a < 1.5 ORDER BY a LIMIT 1")
		if err != nil {
			t.Fatal(err)
		}
		stmt := parsed[0].(*SelectStmt)
		c := tupleConstraints(stmt.Where, "t", "", info)
		plan := buildTuplePlan(stmt, info, idx, c)
		access := buildAccessPath(info, idx, c)
		if plan == nil || access == nil || plan.residual == nil || plan.ordered {
			t.Fatal("secondary signed zero must retain a residual and SQL sorting")
		}
		for id, v := range []float64{math.Inf(-1), -2, -1, math.Copysign(0, -1), 0, 1, 2, math.Inf(1)} {
			row := map[string]Value{"id": NewIntValue(int64(id)), "a": NewRealValue(v), "b": NewIntValue(1)}
			key := encodeRowIndexKey(idx, row, info.encodePK("t", row))
			for _, intervals := range [][]coveringInterval{plan.intervals, access.intervals} {
				inside := false
				for _, interval := range intervals {
					inside = inside || bytes.Compare(key, interval.lower) >= 0 && bytes.Compare(key, interval.upper) < 0
				}
				if v > -1.5 && v < 1.5 && !inside {
					t.Fatalf("secondary range loses %v", v)
				}
			}
		}
	}
}
