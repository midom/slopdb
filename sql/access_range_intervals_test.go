package sql

import (
	"bytes"
	"strings"
	"testing"
)

func TestSecondaryRangeIntervalsPreservePrefixes(t *testing.T) {
	words := []string{"", "0", "1", "9", ":", "a", "b", "\x00", "\xff", "é"}
	var values []string
	values = append(values, words...)
	for _, a := range words {
		for _, b := range words {
			values = append(values, a+b)
		}
	}
	bounds := []string{"", "10", "19", ":0", "a\x00", "a:", "b1", "é1", "\xff9"}
	for _, typ := range []ColumnType{ColText, ColBlob} {
		info := &TableInfo{Name: "t", PKColumns: []string{"id"}, Columns: []ColumnDef{{Name: "id", ColType: ColInteger}, {Name: "lead", ColType: ColInteger}, {Name: "v", ColType: typ}, {Name: "tail", ColType: ColInteger}}}
		for _, columns := range [][]string{{"v"}, {"v", "tail"}, {"lead", "v", "tail"}} {
			idx := &IndexInfo{Name: "v_idx", Table: "t", Columns: columns}
			start := []byte(indexDataPrefix(idx.Name))
			if columns[0] == "lead" {
				start = appendAccessValue(start, NewIntValue(7), idx)
			}
			value := func(s string) Value {
				if typ == ColBlob {
					return NewBlobValue([]byte(s))
				}
				return NewTextValue(s)
			}
			for _, upper := range bounds {
				for _, lower := range bounds {
					for _, strict := range []bool{false, true} {
						lo, hi := value(lower), value(upper)
						c := &tupleConstraint{lower: &lo, upper: &hi, lowerStrict: strict, upperStrict: strict}
						intervals, _ := accessRangeIntervals(start, "v", info, idx, c)
						for i, interval := range intervals {
							if i > 0 && bytes.Compare(intervals[i-1].upper, interval.lower) >= 0 {
								t.Fatal("interval union overlaps or is unsorted")
							}
						}
						for id, text := range values {
							v := value(text)
							row := map[string]Value{"id": NewIntValue(int64(id)), "lead": NewIntValue(7), "v": v, "tail": NewIntValue(3)}
							key := encodeRowIndexKey(idx, row, info.encodePK("t", row))
							inside := false
							for _, interval := range intervals {
								if bytes.Compare(key, interval.lower) >= 0 && bytes.Compare(key, interval.upper) < 0 {
									inside = true
								}
							}
							lc, hc := ValueCompare(v, lo), ValueCompare(v, hi)
							want := (lc > 0 || lc == 0 && !strict) && (hc < 0 || hc == 0 && !strict)
							if want && !inside {
								t.Fatalf("type=%v cols=%v lower=%q upper=%q strict=%v: lost %q", typ, columns, lower, upper, strict, text)
							}
						}
					}
				}
			}
		}
	}
}

func TestSecondaryRangeIntervalsBoundLongPlans(t *testing.T) {
	info := &TableInfo{Name: "t", Columns: []ColumnDef{{Name: "v", ColType: ColText}}}
	idx := &IndexInfo{Name: "v_idx", Table: "t", Columns: []string{"v"}}
	hi := NewTextValue(strings.Repeat("1", 1000))
	c := &tupleConstraint{upper: &hi}
	intervals, _ := accessRangeIntervals([]byte(indexDataPrefix(idx.Name)), "v", info, idx, c)
	if len(intervals) > 1 {
		t.Fatalf("long bound generated %d intervals", len(intervals))
	}
	for _, text := range []string{"", "1", "11", "111:", strings.Repeat("1", 999)} {
		if ValueCompare(NewTextValue(text), hi) > 0 {
			continue
		}
		key := appendAccessValue([]byte(indexDataPrefix(idx.Name)), NewTextValue(text), idx)
		inside := false
		for _, interval := range intervals {
			if bytes.Compare(key, interval.lower) >= 0 && bytes.Compare(key, interval.upper) < 0 {
				inside = true
			}
		}
		if !inside {
			t.Fatalf("long fallback lost prefix %q", text)
		}
	}
}
