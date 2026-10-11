package sql

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"

	"slopdb/storage"
)

func statisticsSchema(t *testing.T, dir string) (*Schema, *storage.LSMTree) {
	t.Helper()
	o := storage.DefaultOptions()
	o.DisableWALSync = true
	lsm, err := storage.NewLSMTree(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lsm.Close() })
	s, err := NewSchema(lsm, 4096)
	if err != nil {
		t.Fatal(err)
	}
	return s, lsm
}
func statisticsSQL(t *testing.T, s *Schema, q string) *ResultSet {
	t.Helper()
	statements, err := Parse(q)
	if err != nil {
		t.Fatal(err)
	}
	var result *ResultSet
	for _, statement := range statements {
		if err := s.StartStatement(); err != nil {
			t.Fatal(err)
		}
		err = s.ExecuteStatement(func() error {
			switch stmt := statement.(type) {
			case *CreateTableStmt:
				return s.CreateTable(stmt)
			case *CreateIndexStmt:
				return s.CreateIndex(stmt)
			case *CreateViewStmt:
				return s.CreateView(stmt)
			case *AlterTableStmt:
				return s.AlterTable(stmt)
			case *InsertStmt:
				_, _, e := s.Insert(stmt)
				return e
			case *UpdateStmt:
				_, _, e := s.Update(stmt)
				return e
			case *DeleteStmt:
				_, _, e := s.Delete(stmt)
				return e
			case *AnalyzeStmt:
				return s.UpdateStats()
			case *SelectStmt:
				var e error
				result, e = s.Select(stmt)
				return e
			case *ExplainStmt:
				var e error
				result, e = s.Explain(stmt)
				return e
			default:
				return fmt.Errorf("unexpected statistics test statement %T", statement)
			}
		})
		s.EndStatement()
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	return result
}
func statisticsSelect(t *testing.T, q string) *SelectStmt {
	t.Helper()
	statements, err := Parse(q)
	if err != nil {
		t.Fatal(err)
	}
	return statements[0].(*SelectStmt)
}

func TestDistributionSkewPlansAndAnswers(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	statisticsSQL(t, s, "CREATE TABLE t(id INTEGER PRIMARY KEY,k INTEGER,v INTEGER,payload TEXT);CREATE INDEX tk ON t(k);CREATE INDEX tv ON t(v)")
	var rows []string
	for i := 0; i < 1000; i++ {
		k := "0"
		if i >= 900 {
			k = fmt.Sprint(i)
		}
		if i >= 950 && i < 999 {
			k = "NULL"
		}
		rows = append(rows, fmt.Sprintf("(%d,%s,%d,'payload')", i, k, i%10))
	}
	statisticsSQL(t, s, "INSERT INTO t VALUES"+strings.Join(rows, ","))
	statisticsSQL(t, s, "ANALYZE")
	c := s.columnDistribution("t", "k")
	if c.NullFraction != 0.049 || c.DistinctEstimate != 52 || c.SampleRows != 1000 {
		t.Fatalf("statistics: %+v", c)
	}
	if c.equality(NewIntValue(0)) != 0.9 {
		t.Fatalf("skew fraction=%g", c.equality(NewIntValue(0)))
	}
	for _, tc := range []struct {
		q, index string
		count    int64
	}{
		{"SELECT * FROM t WHERE k=0", "", 900},
		{"SELECT * FROM t WHERE k=999", "tk", 1},
		{"SELECT * FROM t WHERE k=0 AND v=9", "tv", 90},
		{"SELECT * FROM t WHERE k IS NULL", "", 49},
		{"SELECT * FROM t WHERE k BETWEEN 900 AND 910", "tk", 11},
	} {
		stmt := statisticsSelect(t, tc.q)
		plan := s.tuplePlan(stmt, s.tables["t"])
		if tc.index == "" {
			if plan != nil {
				t.Fatalf("%s: expected scan, got %s", tc.q, plan.description())
			}
		} else if plan == nil || plan.index == nil || plan.index.Name != tc.index {
			t.Fatalf("%s: wrong index %+v", tc.q, plan)
		}
		result := statisticsSQL(t, s, tc.q)
		if int64(len(result.Rows)) != tc.count {
			t.Fatalf("%s: got %d rows want %d", tc.q, len(result.Rows), tc.count)
		}
		if tc.index == "" && strings.Contains(tc.q, "k=0") {
			if !s.distributionScanPreferred(stmt, s.tables["t"]) || s.accessPaths(stmt.Where, s.tables["t"]) != nil {
				t.Fatal("legacy/indexed readers ignored costed scan")
			}
		}
	}
	statisticsSQL(t, s, "CREATE TABLE dim(id INTEGER PRIMARY KEY,n TEXT)")
	// Analyze the new dimension as well as the existing fact table.
	statisticsSQL(t, s, "INSERT INTO dim VALUES(0,'hot'),(999,'rare');ANALYZE")
	for _, tc := range []struct {
		value    int
		estimate float64
		answer   int64
	}{{0, 900, 900}, {999, 1, 1}} {
		q := fmt.Sprintf("SELECT COUNT(*) AS n FROM t JOIN dim ON t.k=dim.id WHERE t.k=%d", tc.value)
		p := s.makeJoinPlan(statisticsSelect(t, q))
		if p == nil {
			t.Fatal("missing join plan")
		}
		if math.Abs(p.relations[0].count-tc.estimate) > 0.01 {
			t.Fatalf("estimated rows %g want %g", p.relations[0].count, tc.estimate)
		}
		if !strings.Contains(p.description(), "estimated rows=") {
			t.Fatal("missing estimated cardinality evidence")
		}
		if got := statisticsSQL(t, s, q).Rows[0]["n"].Int; got != tc.answer {
			t.Fatalf("join count %d want %d", got, tc.answer)
		}
	}
	// NULLs never become equality join matches; outer joins retain all inputs.
	if got := statisticsSQL(t, s, "SELECT COUNT(*) AS n FROM t LEFT JOIN dim ON t.k=dim.id").Rows[0]["n"].Int; got != 1000 {
		t.Fatalf("LEFT result=%d", got)
	}
	// The two dimension probes include a very frequent fact key. Cardinality
	// alone used to retain probes; distributions account for their large fanout.
	p := s.makeJoinPlan(statisticsSelect(t, "SELECT COUNT(*) AS n FROM dim JOIN t ON dim.id=t.k"))
	reader := s.joinReader(p.relations[1], p.pairs(1, 1))
	if fanout := p.fanout(1, 1); fanout < 400 || fanout > 500 {
		t.Fatalf("skew fanout=%g", fanout)
	}
	if !p.preferHashJoin(1, 1, 2, reader) {
		t.Fatal("large-fanout probes should use hash input")
	}
	if got := statisticsSQL(t, s, "SELECT COUNT(*) AS n FROM dim JOIN t ON dim.id=t.k").Rows[0]["n"].Int; got != 901 {
		t.Fatalf("skew join count=%d", got)
	}
}

func TestDistributionPersistenceAndTransactionIsolation(t *testing.T) {
	dir := t.TempDir()
	s, lsm := statisticsSchema(t, dir)
	statisticsSQL(t, s, "CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER,r REAL,b BLOB);INSERT INTO t VALUES(1,9223372036854775807,1.5,X'00FF'),(2,9223372036854775807,1.5,X'00FF');ANALYZE")
	other := s.NewSession()
	if err := s.Begin(); err != nil {
		t.Fatal(err)
	}
	if err := s.Savepoint("before_analysis"); err != nil {
		t.Fatal(err)
	}
	statisticsSQL(t, s, "UPDATE t SET v=0 WHERE id=1;DELETE FROM t WHERE id=2;ANALYZE")
	if s.stats["t"] != 1 || s.distributions["t"].Rows != 1 {
		t.Fatal("ANALYZE double-counted transaction overlay")
	}
	if err := other.RefreshReadCommitted(); err != nil {
		t.Fatal(err)
	}
	if other.stats["t"] != 2 || other.distributions["t"].Rows != 2 {
		t.Fatal("uncommitted analysis leaked to session")
	}
	if err := s.RollbackTo("before_analysis"); err != nil {
		t.Fatal(err)
	}
	if s.distributions["t"].Rows != 2 {
		t.Fatal("savepoint rollback lost statistics")
	}
	if err := s.Rollback(); err != nil {
		t.Fatal(err)
	}
	if s.distributions["t"].Rows != 2 {
		t.Fatal("rollback lost statistics")
	}
	if err := lsm.Close(); err != nil {
		t.Fatal(err)
	}
	s, lsm = statisticsSchema(t, dir)
	for _, tc := range []struct {
		column string
		value  Value
	}{{"v", NewIntValue(math.MaxInt64)}, {"r", NewRealValue(1.5)}, {"b", NewBlobValue([]byte{0, 255})}} {
		c := s.columnDistribution("t", tc.column)
		if c == nil || len(c.Common) != 1 {
			t.Fatalf("missing persisted column %s", tc.column)
		}
		v := Value(c.Common[0].Value)
		if v.Type != tc.value.Type || ValueCompare(v, tc.value) != 0 {
			t.Fatalf("persisted %s value: %+v", tc.column, v)
		}
	}
	if err := s.Begin(); err != nil {
		t.Fatal(err)
	}
	statisticsSQL(t, s, "INSERT INTO t VALUES(3,0,0,X'');ANALYZE")
	if err := s.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := lsm.Close(); err != nil {
		t.Fatal(err)
	}
	s, lsm = statisticsSchema(t, dir)
	if s.distributions["t"].Rows != 3 {
		t.Fatal("committed ANALYZE not persisted")
	}
	statisticsSQL(t, s, "CREATE INDEX tv ON t(v)")
	if s.distributions["t"] == nil {
		t.Fatal("index DDL discarded unchanged column statistics")
	}
	statisticsSQL(t, s, "CREATE VIEW t_view AS SELECT * FROM t")
	if s.distributions["t"] == nil {
		t.Fatal("view DDL discarded unchanged table statistics")
	}
	statisticsSQL(t, s, "ALTER TABLE t RENAME COLUMN v TO renamed_v")
	if s.distributions["t"] != nil {
		t.Fatal("ALTER retained stale column bindings")
	}
	if err := lsm.Close(); err != nil {
		t.Fatal(err)
	}
	s, _ = statisticsSchema(t, dir)
	if len(s.distributions) != 0 {
		t.Fatal("DDL invalidation was not persisted")
	}
}

func TestDistributionBoundedSamplesAndLegacyMetadata(t *testing.T) {
	s, lsm := statisticsSchema(t, t.TempDir())
	statisticsSQL(t, s, "CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER)")
	var rows []string
	for i := 0; i < 10000; i++ {
		rows = append(rows, fmt.Sprintf("(%d,%d)", i, i))
	}
	statisticsSQL(t, s, "INSERT INTO t VALUES"+strings.Join(rows, ",")+";ANALYZE")
	c := s.columnDistribution("t", "v")
	if c.SampleRows != statisticsSampleSize || len(c.Histogram) > 128 || len(c.Common) > 16 {
		t.Fatalf("unbounded sample %+v", c)
	}
	if c.DistinctEstimate < 7000 || c.DistinctEstimate > 13000 {
		t.Fatalf("KMV estimate=%g", c.DistinctEstimate)
	}
	data, _ := json.Marshal(map[string]int64{"t": 10000})
	if err := lsm.Put([]byte(statsPrefix), data); err != nil {
		t.Fatal(err)
	}
	if err := lsm.Delete([]byte(distributionPrefix)); err != nil {
		t.Fatal(err)
	}
	legacy, err := NewSchema(lsm, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if legacy.stats["t"] != 10000 || len(legacy.distributions) != 0 {
		t.Fatal("legacy table counts not retained")
	}
	statisticsSQL(t, legacy, "ANALYZE")
	if legacy.columnDistribution("t", "v") == nil {
		t.Fatal("legacy statistics not upgraded")
	}
}

func TestDistributionEmptyAndOuterFanout(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	statisticsSQL(t, s, "CREATE TABLE a(id INTEGER PRIMARY KEY);CREATE TABLE b(id INTEGER PRIMARY KEY);INSERT INTO a VALUES(1);ANALYZE")
	for _, join := range []string{"JOIN", "LEFT JOIN"} {
		q := "SELECT COUNT(*) AS n FROM a " + join + " b ON a.id=b.id"
		p := s.makeJoinPlan(statisticsSelect(t, q))
		if p.relations[1].total != 0 {
			t.Fatal("empty analyzed table assigned fallback rows")
		}
		want := int64(0)
		if join == "LEFT JOIN" {
			want = 1
		}
		if f := p.fanout(1, 1); f != float64(want) {
			t.Fatalf("%s fanout=%g want %d", join, f, want)
		}
		if got := statisticsSQL(t, s, q).Rows[0]["n"].Int; got != want {
			t.Fatalf("%s count=%d", join, got)
		}
	}
}
