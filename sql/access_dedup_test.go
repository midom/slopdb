package sql

import "testing"

func TestAccessPathsNeedDeduplicationOnlyForOverlap(t *testing.T) {
	interval := func(a, b string) coveringInterval { return coveringInterval{[]byte(a), []byte(b)} }
	for _, tc := range []struct {
		name  string
		paths []*accessPath
		want  bool
	}{
		{"full_scan", nil, false},
		{"single_interval", []*accessPath{{intervals: []coveringInterval{interval("a", "z")}}}, false},
		{"disjoint_bands", []*accessPath{{intervals: []coveringInterval{interval("a", "c"), interval("d", "f"), interval("g", "z")}}}, false},
		{"touching_bands", []*accessPath{{intervals: []coveringInterval{interval("a", "c"), interval("c", "f")}}}, false},
		{"overlapping_bands", []*accessPath{{intervals: []coveringInterval{interval("a", "e"), interval("d", "z")}}}, true},
		{"contained_band", []*accessPath{{intervals: []coveringInterval{interval("a", "z"), interval("d", "f")}}}, true},
		{"unordered_bands_conservative", []*accessPath{{intervals: []coveringInterval{interval("d", "f"), interval("a", "c")}}}, true},
		{"or_paths", []*accessPath{{intervals: []coveringInterval{interval("a", "e")}}, {intervals: []coveringInterval{interval("d", "z")}}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := accessPathsMayRepeatRows(tc.paths); got != tc.want {
				t.Fatalf("dedup=%v, want %v", got, tc.want)
			}
		})
	}
}
