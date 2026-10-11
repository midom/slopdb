package regression

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"slopdb/db"
	"slopdb/storage"
)

func TestOrderedAggregateClusteredPrefix(t *testing.T) {
	env := newRegressionEnv(t)
	opt := storage.DefaultOptions()
	opt.DisableWALSync = true
	opt.QueryMemoryBytes = 512
	opt.QueryTempDir = env.path("spills")
	if err := os.Mkdir(opt.QueryTempDir, 0700); err != nil {
		t.Fatal(err)
	}
	d, err := env.openDatabase(env.path("data"), opt)
	if err != nil {
		t.Fatal(err)
	}
	exec := func(q string) *db.ExecResult {
		t.Helper()
		r, e := d.Exec(q)
		if e != nil {
			t.Fatalf("%s: %v", q, e)
		}
		return r
	}
	exec("CREATE TABLE t(g INTEGER,id INTEGER,n INTEGER,PRIMARY KEY(g,id))")
	insert, err := d.Prepare("INSERT INTO t VALUES(?,?,?)")
	if err != nil {
		t.Fatal(err)
	}
	for g := 0; g < 400; g++ {
		for id := 0; id < 3; id++ {
			var n any = g
			if id == 1 {
				n = nil
			}
			if _, err = insert.Exec(g, id, n); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, q := range []string{
		"SELECT g,COUNT(*) AS c,SUM(n) AS s FROM t GROUP BY g HAVING SUM(n)>=794 ORDER BY g",
		"SELECT g,COUNT(*) AS c,SUM(ABS(n)) AS s FROM t GROUP BY g HAVING SUM(ABS(n))>=794 ORDER BY g",
		"SELECT x.g AS bucket,COUNT(*) AS c,SUM(x.n) AS s FROM t AS x GROUP BY bucket HAVING SUM(x.n)>=794 ORDER BY bucket",
		"SELECT g,COUNT(*) AS c,SUM(n) AS s FROM t GROUP BY 1 HAVING SUM(n)>=794 ORDER BY g",
	} {
		r := exec(q)
		if len(r.Rows) != 3 {
			t.Fatalf("%s: %v", q, r.Rows)
		}
		for i, row := range r.Rows {
			key := "g"
			if _, ok := row["bucket"]; ok {
				key = "bucket"
			}
			if row[key].Int != int64(397+i) || row["c"].Int != 3 || row["s"].Int != int64(2*(397+i)) {
				t.Fatal(row)
			}
		}
		profileQuery := q[:strings.LastIndex(q, " ORDER BY")]
		prof := exec("EXPLAIN ANALYZE " + profileQuery)
		ordered := false
		for _, operator := range prof.Profile.Operators {
			ordered = ordered || operator.Operator == "OrderedAggregate"
			if operator.Operator == "SpillAggregate" {
				t.Fatal("clustered prefix used spilled grouping")
			}
		}
		if !ordered || prof.Profile.SpillBytes != 0 || prof.Profile.RowsExamined != 1200 {
			t.Fatalf("ordered profile: %+v", prof.Profile)
		}
	}
	r := exec("SELECT g,COUNT(DISTINCT n) AS c,SUM(DISTINCT n) AS s,GROUP_CONCAT(n,'|') AS txt FROM t WHERE id<>1 GROUP BY g HAVING g<3 ORDER BY g")
	if len(r.Rows) != 3 {
		t.Fatal(r.Rows)
	}
	for i, row := range r.Rows {
		if row["g"].Int != int64(i) || row["c"].Int != 1 || row["s"].Int != int64(i) || row["txt"].Text != fmt.Sprintf("%d|%d", i, i) {
			t.Fatal(row)
		}
	}
	r = exec("SELECT g,COUNT(*) AS c,SUM(n) AS s FROM t WHERE g BETWEEN 20 AND 22 GROUP BY g ORDER BY g DESC LIMIT 2 OFFSET 1")
	if len(r.Rows) != 2 || r.Rows[0]["g"].Int != 21 || r.Rows[1]["g"].Int != 20 {
		t.Fatal(r.Rows)
	}
	r = exec("SELECT g FROM t WHERE g<0 GROUP BY g")
	if len(r.Rows) != 0 {
		t.Fatal(r.Rows)
	}
	prof := exec("EXPLAIN ANALYZE SELECT id,COUNT(*) AS c FROM t GROUP BY id")
	for _, op := range prof.Profile.Operators {
		if op.Operator == "OrderedAggregate" {
			t.Fatal("non-prefix grouping treated as ordered")
		}
	}
	prepared, err := d.Prepare("SELECT g,SUM(n) AS s FROM t WHERE g>=? GROUP BY g HAVING SUM(n)>? ORDER BY g")
	if err != nil {
		t.Fatal(err)
	}
	for _, lo := range []int{398, 399} {
		r, err = prepared.Exec(lo, 0)
		if err != nil || len(r.Rows) != 400-lo {
			t.Fatalf("prepared: %v %v", r, err)
		}
	}
	exec("BEGIN;INSERT INTO t VALUES(400,0,400);UPDATE t SET n=500 WHERE g=399;DELETE FROM t WHERE g=398")
	r = exec("SELECT g,SUM(n) AS s FROM t WHERE g>=398 GROUP BY g ORDER BY g")
	if len(r.Rows) != 2 || r.Rows[0]["g"].Int != 399 || r.Rows[0]["s"].Int != 1500 || r.Rows[1]["g"].Int != 400 || r.Rows[1]["s"].Int != 400 {
		t.Fatal(r.Rows)
	}
	exec("ROLLBACK")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = d.ExecContext(ctx, "SELECT g,SUM(n) FROM t GROUP BY g"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err = d.Exec("SELECT g,SUM(1/(id-1)) FROM t GROUP BY g"); err == nil {
		t.Fatal("aggregate error swallowed")
	}
	entries, err := os.ReadDir(opt.QueryTempDir)
	if err != nil || len(entries) > 0 {
		t.Fatalf("spills leaked: %v %v", entries, err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = env.openDatabase(env.path("data"), opt)
	if err != nil {
		t.Fatal(err)
	}
	r, err = d.Exec("SELECT g,SUM(n) AS s FROM t WHERE g>=398 GROUP BY g ORDER BY g")
	if err != nil || len(r.Rows) != 2 || r.Rows[0]["s"].Int != 796 || r.Rows[1]["s"].Int != 798 {
		t.Fatalf("reopen: %v %v", r, err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOrderedAggregateBinaryPrefixesAndNullArguments(t *testing.T) {
	env := newRegressionEnv(t)
	opt := storage.DefaultOptions()
	opt.DisableWALSync = true
	opt.QueryMemoryBytes = 256
	d, err := env.openDatabase(env.path("data"), opt)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"CREATE TABLE t(g BLOB,id INTEGER,n INTEGER,PRIMARY KEY(g,id))", "INSERT INTO t VALUES(X'00',0,NULL),(X'00',1,NULL),(X'00FF',0,2),(X'00FF',1,2),(X'FF',0,3)", "CREATE TABLE u(g REAL,id INTEGER,n INTEGER,PRIMARY KEY(g,id))", "INSERT INTO u VALUES(-1.5,0,1),(-1.5,1,2),(0.0,0,3),(0.0,1,4),(1.5,0,5)"} {
		if _, err = d.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	r, err := d.Exec("SELECT HEX(g) AS k,COUNT(*) AS c,COUNT(n) AS cn,SUM(n) AS s FROM t GROUP BY g ORDER BY g")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 || r.Rows[0]["k"].Text != "00" || r.Rows[0]["c"].Int != 2 || r.Rows[0]["cn"].Int != 0 || !r.Rows[0]["s"].Null || r.Rows[1]["k"].Text != "00FF" || r.Rows[1]["s"].Int != 4 || r.Rows[2]["k"].Text != "FF" || r.Rows[2]["s"].Int != 3 {
		t.Fatal(r.Rows)
	}
	r, err = d.Exec("SELECT g,SUM(n) AS s FROM u GROUP BY g ORDER BY g")
	if err != nil || len(r.Rows) != 3 || r.Rows[0]["s"].Int != 3 || r.Rows[1]["s"].Int != 7 || r.Rows[2]["s"].Int != 5 {
		t.Fatalf("real groups: %v %v", r, err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}
