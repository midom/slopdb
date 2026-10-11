package regression

import (
	"fmt"
	"os"
	"testing"

	"slopdb/db"
	"slopdb/storage"
)

func TestDistributionHashJoinHonorsMemoryBudget(t *testing.T) {
	env := newRegressionEnv(t)
	opts := storage.DefaultOptions()
	opts.DisableWALSync = true
	opts.QueryTempDir = env.path("spills")
	if err := os.Mkdir(opts.QueryTempDir, 0700); err != nil {
		t.Fatal(err)
	}
	d, err := env.openDatabase(env.path("data"), opts)
	if err != nil {
		t.Fatal(err)
	}
	exec := func(query string) *db.ExecResult {
		t.Helper()
		result, err := d.Exec(query)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return result
	}
	exec("CREATE TABLE a(id INTEGER PRIMARY KEY,k INTEGER);CREATE TABLE b(id INTEGER PRIMARY KEY,k INTEGER);CREATE INDEX ak ON a(k);CREATE INDEX bk ON b(k)")
	for i := 0; i < 40; i++ {
		exec(fmt.Sprintf("INSERT INTO a VALUES(%d,1);INSERT INTO b VALUES(%d,1)", i, i))
	}
	exec("INSERT INTO a VALUES(40,NULL);ANALYZE")
	for _, budget := range []int64{512, 1 << 20} {
		query := "SELECT COUNT(*) AS c,SUM(b.id) AS s FROM a LEFT JOIN b ON a.k=b.k"
		result, err := d.ExecWithOptions(query, db.ExecOptions{MemoryBytes: &budget})
		if err != nil {
			t.Fatal(err)
		}
		// Forty rows each match all forty right rows; NULL is unmatched.
		if len(result.Rows) != 1 || result.Rows[0]["c"].Int != 1601 || result.Rows[0]["s"].Int != 31200 {
			t.Fatalf("budget %d: %v", budget, result.Rows)
		}
		profile, err := d.ExecWithOptions("EXPLAIN ANALYZE "+query, db.ExecOptions{MemoryBytes: &budget})
		if err != nil {
			t.Fatal(err)
		}
		wantOperator := "HashJoin"
		if budget == 512 {
			wantOperator = "SpillHashJoin"
			if profile.Profile.SpillBytes == 0 {
				t.Fatal("high-fanout hash join did not spill within its budget")
			}
		}
		found := false
		for _, op := range profile.Profile.Operators {
			if op.Operator == wantOperator {
				found = true
			}
		}
		if !found {
			t.Fatalf("budget %d: missing %s in %+v", budget, wantOperator, profile.Profile)
		}
		entries, err := os.ReadDir(opts.QueryTempDir)
		if err != nil || len(entries) != 0 {
			t.Fatalf("temporary join files leaked: %v, %v", entries, err)
		}
	}
}
