package regression

import (
	"fmt"
	"os"
	"testing"

	"slopdb/db"
	"slopdb/storage"
)

func TestDisjointSecondaryRangesDoNotSpill(t *testing.T) {
	env := newRegressionEnv(t)
	opt := storage.DefaultOptions()
	opt.DisableWALSync = true
	opt.QueryMemoryBytes = 512
	opt.QueryTempDir = env.path("query-temp")
	if err := os.Mkdir(opt.QueryTempDir, 0700); err != nil {
		t.Fatal(err)
	}
	d, err := env.openDatabase(env.path("database"), opt)
	if err != nil {
		t.Fatal(err)
	}
	exec := func(query string) *db.ExecResult {
		t.Helper()
		r, err := d.Exec(query)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return r
	}
	exec("CREATE TABLE dated(id INTEGER PRIMARY KEY,d TEXT NOT NULL,v INTEGER); CREATE INDEX dated_d ON dated(d)")
	// Proper prefixes sort before the SQL upper bound, even though their
	// legacy ':' index delimiters sort after its next byte. The physical
	// plan therefore has several disjoint bands, not overlapping origins.
	dates := []string{"1994-06-01", "1995", "1995-", "1995-0", "1995-01", "1995-01-0", "1995-01-01"}
	for i := 0; i < 350; i++ {
		exec(fmt.Sprintf("INSERT INTO dated VALUES(%d,'%s',%d)", i, dates[i%len(dates)], i))
	}
	query := "SELECT COUNT(*) AS n FROM dated WHERE d>='1994-01-01' AND d<'1995-01-01'"
	verify := func() {
		t.Helper()
		if got := exec(query).Rows[0]["n"].Int; got != 300 {
			t.Fatalf("count=%d, want 300", got)
		}
		p := exec("EXPLAIN ANALYZE " + query).Profile
		if p == nil || p.IndexEntriesExamined != 300 || p.SpillRuns != 0 || p.SpillBytes != 0 {
			t.Fatalf("disjoint index intervals unexpectedly spilled or were bypassed: %+v", p)
		}
		// Separate OR paths can revisit the same origin, so they must retain
		// deduplication and its disk fallback under this tiny memory budget.
		orQuery := "SELECT COUNT(*) AS n FROM dated WHERE (d>='1994-01-01' AND d<'1995-01-01') OR d='1994-06-01'"
		if got := exec(orQuery).Rows[0]["n"].Int; got != 300 {
			t.Fatalf("overlapping OR origins counted %d, want 300", got)
		}
		p = exec("EXPLAIN ANALYZE " + orQuery).Profile
		if p == nil || p.SpillRuns == 0 {
			t.Fatalf("overlapping paths lost bounded deduplication: %+v", p)
		}
		entries, err := os.ReadDir(opt.QueryTempDir)
		if err != nil || len(entries) != 0 {
			t.Fatalf("temporary dedup files leaked: %v, %v", entries, err)
		}
	}
	verify()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = env.openDatabase(env.path("database"), opt)
	if err != nil {
		t.Fatal(err)
	}
	verify()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}
