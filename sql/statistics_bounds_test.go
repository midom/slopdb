package sql

import (
	"fmt"
	"strings"
	"testing"
)

func TestDistributionCostsConservativeSecondaryRanges(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	statisticsSQL(t, s, "CREATE TABLE dated(id INTEGER PRIMARY KEY,d TEXT,v INTEGER);CREATE INDEX dated_d ON dated(d)")
	var rows []string
	for i := 0; i < 1000; i++ {
		date := "1998-01-01"
		if i < 100 {
			date = "1993-07-01"
		}
		rows = append(rows, fmt.Sprintf("(%d,'%s',%d)", i, date, i))
	}
	// Short proper prefixes force conservative delimiter bands. They must
	// remain visible in the physical cost and in the residual SQL predicate.
	rows = append(rows, "(1000,'199',1000)", "(1001,'1993-07',1001)")
	statisticsSQL(t, s, "INSERT INTO dated VALUES"+strings.Join(rows, ",")+";ANALYZE")
	for _, tc := range []struct {
		where string
		want  int
		scan  bool
	}{
		{"d>='1993-07-01' AND d<'1993-10-01'", 100, false},
		{"d BETWEEN '1993-07-01' AND '1993-10-01'", 100, false},
		{"d='1993-07-01'", 100, false},
	} {
		stmt := statisticsSelect(t, "SELECT * FROM dated WHERE "+tc.where)
		paths := s.accessPaths(stmt.Where, s.tables["dated"])
		if (paths == nil) != tc.scan {
			t.Fatalf("%s: scan=%v expected %v", tc.where, paths == nil, tc.scan)
		}
		plan := s.tuplePlan(stmt, s.tables["dated"])
		if (plan == nil) != tc.scan {
			t.Fatalf("%s: tuple scan=%v expected %v", tc.where, plan == nil, tc.scan)
		}
		if got := len(statisticsSQL(t, s, "SELECT * FROM dated WHERE "+tc.where).Rows); got != tc.want {
			t.Fatalf("%s returned %d want %d", tc.where, got, tc.want)
		}
		if got := statisticsSQL(t, s, "SELECT COUNT(*) AS n FROM dated WHERE "+tc.where).Rows[0]["n"].Int; got != int64(tc.want) {
			t.Fatalf("aggregate %s count=%d want %d", tc.where, got, tc.want)
		}
	}
	constraints := s.accessConstraints(statisticsSelect(t, "SELECT * FROM dated WHERE d>='1993-07-01' AND d<'1993-10-01'").Where, s.tables["dated"])
	rowsEstimate, known := s.accessEstimate(s.tables["dated"], s.indexes["dated_d"], constraints)
	if !known || rowsEstimate < 50 || rowsEstimate > 150 {
		t.Fatalf("estimated physical rows=%g; wanted narrow date span", rowsEstimate)
	}
}

func TestDistributionNoncoveringRangeWorkingSetCosts(t *testing.T) {
	info := &TableInfo{Name: "t", PKColumns: []string{"id"}, Columns: []ColumnDef{{Name: "id", ColType: ColInteger}, {Name: "k", ColType: ColInteger}, {Name: "payload", ColType: ColText}}}
	column := &columnStatistics{Type: ColInteger, DistinctEstimate: 1000000, SampleRows: 2048}
	for i := 0; i <= 100; i++ {
		column.Histogram = append(column.Histogram, statisticValue(NewIntValue(int64(i))))
	}
	s := &Schema{tables: map[string]*TableInfo{"t": info}, indexes: map[string]*IndexInfo{"tk": {Name: "tk", Table: "t", Column: "k"}}, stats: map[string]int64{"t": 1000000}, distributions: map[string]*tableStatistics{"t": {Rows: 1000000, Columns: map[string]*columnStatistics{"k": column}}}}
	for _, tc := range []struct {
		upper int
		scan  bool
	}{{3, false}, {14, true}} {
		stmt := statisticsSelect(t, fmt.Sprintf("SELECT * FROM t WHERE k>=0 AND k<=%d", tc.upper))
		if plan := s.tuplePlan(stmt, info); (plan == nil) != tc.scan {
			t.Fatalf("upper=%d: scan=%v want %v", tc.upper, plan == nil, tc.scan)
		}
		if paths := s.accessPaths(stmt.Where, info); (paths == nil) != tc.scan {
			t.Fatalf("upper=%d: access scan=%v want %v", tc.upper, paths == nil, tc.scan)
		}
	}
}
