package regression

import (
	"context"
	"strings"
	"testing"
	"time"

	"slopdb/db"
	"slopdb/storage"
)

func TestQueryProfileActualReads(t *testing.T) {
	env := newRegressionEnv(t)
	opts := storage.DefaultOptions()
	opts.DisableWALSync = true
	d, err := env.openDatabase(env.path("profiles"), opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		"CREATE TABLE t(a INTEGER,b INTEGER,v TEXT,PRIMARY KEY(a,b))",
		"INSERT INTO t VALUES(0,0,'x'),(0,1,'x'),(0,2,'x'),(1,0,'x'),(1,1,'x'),(1,2,'x'),(2,0,'x'),(2,1,'x'),(2,2,'x'),(3,0,'x'),(3,1,'x'),(3,2,'x')",
	} {
		if _, err := d.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		query         string
		examined, out int64
	}{
		{"SELECT * FROM t WHERE a BETWEEN 1 AND 2 ORDER BY a,b LIMIT 2", 2, 2},
		{"SELECT * FROM t WHERE a+0=1", 12, 3},
		{"SELECT COUNT(*) FROM t", 12, 1},
		{"SELECT * FROM t WHERE a=1 AND b=2", 1, 1},
		{"SELECT * FROM t WHERE a BETWEEN 2 AND 1", 0, 0},
	} {
		t.Run(tc.query, func(t *testing.T) {
			result, err := d.Exec("EXPLAIN ANALYZE " + tc.query)
			if err != nil {
				t.Fatal(err)
			}
			p := result.Profile
			if p == nil || p.RowsExamined != tc.examined {
				t.Fatalf("profile=%+v, expected %d examined rows", p, tc.examined)
			}
			if len(p.Operators) == 0 || p.Operators[0].RowsReturned != tc.out {
				t.Fatalf("operators=%+v, expected %d output rows", p.Operators, tc.out)
			}
			if p.ElapsedMS <= 0 || p.ProcessAllocations == 0 {
				t.Fatalf("missing elapsed/allocation metrics: %+v", p)
			}
			if tc.examined > 0 && p.BytesRead <= 0 {
				t.Fatal("read byte accounting missing")
			}
			found := false
			for _, row := range result.Rows {
				if row["opcode"].Text == "QueryProfile" {
					found = true
					if row["rows_examined"].Int != tc.examined {
						t.Fatalf("SQL profile differs: %v", row)
					}
				}
			}
			if !found {
				t.Fatal("SQL profile summary missing")
			}
		})
	}
	for _, query := range []string{"SELECT * FROM t", "EXPLAIN SELECT * FROM t"} {
		result, err := d.Exec(query)
		if err != nil || result.Profile != nil {
			t.Fatalf("profiling leaked to %s: %+v, %v", query, result, err)
		}
	}
	for _, query := range []string{"CREATE TABLE u(a INTEGER PRIMARY KEY)", "INSERT INTO u VALUES(0),(1),(2),(3)"} {
		if _, err := d.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	joined, err := d.Exec("EXPLAIN ANALYZE SELECT t.a,t.b FROM t JOIN u ON u.a=t.a WHERE t.a=1 ORDER BY t.b")
	if err != nil {
		t.Fatal(err)
	}
	joinMetrics := false
	for _, op := range joined.Profile.Operators {
		if strings.Contains(op.Operator, "Join") && op.RowsReturned > 0 {
			joinMetrics = true
		}
	}
	if !joinMetrics || joined.Profile.Operators[0].RowsReturned != 3 {
		t.Fatalf("actual join metrics missing: %+v", joined.Profile)
	}
	if _, err := d.Exec("EXPLAIN ANALYZE SELECT 1/0"); err == nil || !strings.Contains(err.Error(), "zero") {
		t.Fatalf("execution error was swallowed: %v", err)
	}
	if _, err := d.Exec("EXPLAIN ANALYZE DELETE FROM t"); err == nil {
		t.Fatal("unsupported ANALYZE mutation silently accepted")
	}
}

func TestQueryProfileLockWaitAndPrepared(t *testing.T) {
	env := newRegressionEnv(t)
	opts := storage.DefaultOptions()
	opts.DisableWALSync = true
	d, err := env.openDatabase(env.path("profile-locks"), opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER)", "INSERT INTO t VALUES(1,10),(2,20)"} {
		if _, err := d.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec("UPDATE t SET v=11 WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	session, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	stmt, err := session.Prepare("EXPLAIN ANALYZE SELECT * FROM t WHERE id=? FOR UPDATE")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	done := make(chan struct {
		result *db.ExecResult
		err    error
	}, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		result, err := stmt.ExecContext(ctx, 1)
		done <- struct {
			result *db.ExecResult
			err    error
		}{result, err}
	}()
	select {
	case result := <-done:
		t.Fatalf("did not wait for the held lock: %+v", result)
	case <-time.After(30 * time.Millisecond):
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	result := <-done
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.result.Profile == nil || result.result.Profile.LockWaitMS <= 0 {
		t.Fatalf("lock wait missing: %+v", result.result.Profile)
	}
	if result.result.Profile.Operators[0].Error == "" {
		t.Fatal("retry work was omitted from the profile")
	}
	second, err := stmt.Exec(2)
	if err != nil || second.Profile == nil || second.Profile.LockWaitMS != 0 {
		t.Fatalf("prepared execution reused stale profile: %+v, %v", second, err)
	}
}
