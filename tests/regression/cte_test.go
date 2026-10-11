package regression

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"slopdb/db"
	"slopdb/sql"
	"slopdb/storage"
)

func cteDatabase(t *testing.T) *db.Database {
	t.Helper()
	env := newRegressionEnv(t)
	opt := storage.DefaultOptions()
	opt.DisableWALSync = true
	d, err := env.openDatabase(env.path("ctes"), opt)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func cteExec(t *testing.T, d *db.Database, query string) *db.ExecResult {
	t.Helper()
	r, err := d.Exec(query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return r
}
func cteInts(t *testing.T, r *db.ExecResult, column string, want []int64) {
	t.Helper()
	got := make([]int64, 0, len(r.Rows))
	for _, row := range r.Rows {
		v := row[column]
		if v.Null || v.Type != sql.ColInteger {
			t.Fatalf("%s value: %+v", column, v)
		}
		got = append(got, v.Int)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: got %v, want %v", column, got, want)
	}
}
func TestCTENonRecursiveScopes(t *testing.T) {
	d := cteDatabase(t)
	cteExec(t, d, "CREATE TABLE x(n INTEGER PRIMARY KEY)")
	cteExec(t, d, "INSERT INTO x VALUES (99)")
	cteInts(t, cteExec(t, d, "WITH x(n) AS (SELECT 1 UNION ALL SELECT 2), y(k) AS (SELECT n+10 FROM x) SELECT k FROM y ORDER BY k"), "k", []int64{11, 12})
	cteInts(t, cteExec(t, d, "WITH x(n) AS (SELECT 7) SELECT (WITH x(n) AS (SELECT 8) SELECT n FROM x) AS inner_n, n FROM x"), "n", []int64{7})
	cteInts(t, cteExec(t, d, "WITH x(n) AS (SELECT 7) SELECT (WITH x(n) AS (SELECT 8) SELECT n FROM x) AS inner_n, n FROM x"), "inner_n", []int64{8})
	cteInts(t, cteExec(t, d, "SELECT n FROM x"), "n", []int64{99})
	cteInts(t, cteExec(t, d, "WITH x(n) AS (SELECT 2 UNION ALL SELECT 3) SELECT a.n+b.n AS n FROM x a JOIN x b ON a.n=b.n ORDER BY n"), "n", []int64{4, 6})
	cteInts(t, cteExec(t, d, "WITH x(n) AS (SELECT 3) SELECT n FROM x UNION ALL SELECT n+1 FROM x ORDER BY n"), "n", []int64{3, 4})
	star := cteExec(t, d, "WITH a(x) AS (SELECT 1), b(y,z) AS (SELECT 2,3) SELECT * FROM a CROSS JOIN b")
	names := make([]string, len(star.Columns))
	for i, c := range star.Columns {
		names[i] = c.Name
	}
	if !reflect.DeepEqual(names, []string{"x", "y", "z"}) {
		t.Fatalf("CTE join star columns: %v", names)
	}
	for name, want := range map[string]int64{"x": 1, "y": 2, "z": 3} {
		cteInts(t, star, name, []int64{want})
	}

	cteInts(t, cteExec(t, d, "WITH x(n) AS (SELECT 3) SELECT (SELECT n FROM x) AS n"), "n", []int64{3})
	cteInts(t, cteExec(t, d, "SELECT n FROM x WHERE n IN (WITH q(n) AS (SELECT 99 UNION ALL SELECT 98) SELECT n FROM q)"), "n", []int64{99})
	cteInts(t, cteExec(t, d, "SELECT n FROM (WITH x(n) AS (SELECT 6) SELECT n FROM x) sub"), "n", []int64{6})
	cteExec(t, d, "WITH x(n) AS MATERIALIZED (SELECT RANDOM()) SELECT a.n AS a,b.n AS b FROM x a CROSS JOIN x b")
	r := cteExec(t, d, "WITH x(n) AS (SELECT RANDOM()) SELECT a.n AS a,b.n AS b FROM x a CROSS JOIN x b")
	if r.Rows[0]["a"].Int != r.Rows[0]["b"].Int {
		t.Fatal("CTE volatile expression evaluated more than once")
	}
	for _, q := range []string{
		"WITH x AS (SELECT 1), x AS (SELECT 2) SELECT * FROM x",
		"WITH x(a,b) AS (SELECT 1) SELECT * FROM x",
		"WITH x AS (SELECT * FROM x) SELECT * FROM x",
		"WITH x AS (SELECT * FROM later), later AS (SELECT 1) SELECT * FROM x",
		"WITH x AS NOT MATERIALIZED (SELECT 1) SELECT * FROM x",
		"WITH x AS (SELECT 1) SELECT missing FROM x",
		"WITH x AS (WITH q(n) AS (SELECT 1) INSERT INTO x SELECT n FROM q) SELECT * FROM x",
		"CREATE VIEW bad AS WITH q(n) AS (SELECT 1) INSERT INTO x SELECT n FROM q",
	} {
		if _, err := d.Exec(q); err == nil {
			t.Fatalf("expected error: %s", q)
		}
	}
	cteInts(t, cteExec(t, d, "SELECT n FROM x"), "n", []int64{99})
	nestedStar := cteExec(t, d, "WITH a(x) AS (SELECT 1), b(y,z) AS (SELECT 2,3) SELECT * FROM a CROSS JOIN (SELECT * FROM b) sub")
	if len(nestedStar.Columns) != 3 {
		t.Fatalf("nested CTE star: %+v", nestedStar.Columns)
	}
	cteExec(t, d, "CREATE VIEW cycle AS WITH q AS (SELECT * FROM cycle) SELECT * FROM q")
	if _, err := d.Exec("SELECT * FROM cycle"); err == nil {
		t.Fatal("cyclic CTE view accepted")
	}
}

func TestCTERecursiveSemantics(t *testing.T) {
	d := cteDatabase(t)
	cteInts(t, cteExec(t, d, "WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM nums WHERE n<5) SELECT n FROM nums ORDER BY n DESC LIMIT 3 OFFSET 1"), "n", []int64{4, 3, 2})
	cteInts(t, cteExec(t, d, "WITH RECURSIVE nums(n) AS (SELECT 1 UNION SELECT n FROM nums) SELECT n FROM nums"), "n", []int64{1})
	r := cteExec(t, d, "WITH RECURSIVE nums(n) AS (SELECT NULL UNION SELECT n FROM nums) SELECT n FROM nums")
	if len(r.Rows) != 1 || !r.Rows[0]["n"].Null {
		t.Fatalf("NULL UNION: %+v", r.Rows)
	}
	cteExec(t, d, "CREATE TABLE edge(parent INTEGER, child INTEGER)")
	cteExec(t, d, "INSERT INTO edge VALUES (1,2),(1,3),(2,4),(3,4),(4,1)")
	cteInts(t, cteExec(t, d, "WITH RECURSIVE walk(n) AS (SELECT 1 UNION SELECT edge.child FROM walk JOIN edge ON walk.n=edge.parent) SELECT n FROM walk ORDER BY n"), "n", []int64{1, 2, 3, 4})
	cteInts(t, cteExec(t, d, "WITH RECURSIVE base(n) AS (SELECT 2), nums(n) AS (SELECT n FROM base UNION ALL SELECT n+2 FROM nums WHERE n<6) SELECT n FROM nums ORDER BY n"), "n", []int64{2, 4, 6})
	for _, q := range []string{
		"WITH RECURSIVE x(n) AS (SELECT n FROM x) SELECT n FROM x",
		"WITH RECURSIVE x(n) AS (SELECT n FROM x UNION ALL SELECT n+1 FROM x) SELECT n FROM x",
		"WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT a.n+b.n FROM x a JOIN x b ON a.n=b.n) SELECT n FROM x",
		"WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT (SELECT n FROM x)) SELECT n FROM x",
		"WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT COUNT(n) FROM x) SELECT n FROM x",
		"WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x LIMIT 2) SELECT n FROM x",
		"WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n,n FROM x) SELECT n FROM x",
	} {
		if _, err := d.Exec(q); err == nil {
			t.Fatalf("expected unsupported recursion error: %s", q)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := d.ExecContext(ctx, "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x) SELECT n FROM x")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancellation: %v", err)
	}
	cteInts(t, cteExec(t, d, "WITH x(n) AS (SELECT 10) SELECT n FROM x"), "n", []int64{10})
}

func TestCTEPreparedDMLViewsAndCursors(t *testing.T) {
	env := newRegressionEnv(t)
	path := env.path("ctes")
	d, err := env.openDatabase(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	cteExec(t, d, "CREATE TABLE dst(n INTEGER PRIMARY KEY)")
	cteExec(t, d, "WITH x(n) AS (SELECT 1 UNION ALL SELECT 2) INSERT INTO dst SELECT n FROM x")
	cteExec(t, d, "WITH x(n) AS (SELECT 2) UPDATE dst SET n=n+10 WHERE n IN (SELECT n FROM x)")
	cteExec(t, d, "WITH x(n) AS (SELECT 1) DELETE FROM dst WHERE n IN (SELECT n FROM x)")
	cteInts(t, cteExec(t, d, "SELECT n FROM dst"), "n", []int64{12})
	if _, err := d.Exec("WITH x(n) AS (SELECT 13 UNION ALL SELECT 12) INSERT INTO dst SELECT n FROM x"); err == nil {
		t.Fatal("expected duplicate key")
	}
	cteInts(t, cteExec(t, d, "SELECT n FROM dst"), "n", []int64{12})
	cteExec(t, d, "CREATE VIEW physical_view AS SELECT n FROM dst")
	cteInts(t, cteExec(t, d, "WITH dst(n) AS (SELECT 77) SELECT n FROM physical_view"), "n", []int64{12})

	cteExec(t, d, "CREATE VIEW saved AS WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<3) SELECT n FROM x")
	cteExec(t, d, "CREATE VIEW saved_union AS WITH x(n) AS (SELECT 7) SELECT n FROM x UNION ALL SELECT n+1 FROM x ORDER BY n LIMIT 2")
	cteExec(t, d, "CREATE VIEW saved_star AS WITH x(n) AS (SELECT 6) SELECT * FROM x")
	cteExec(t, d, "CREATE VIEW table_cte AS WITH q(v) AS (SELECT n FROM dst) SELECT v FROM q")
	cteExec(t, d, "CREATE VIEW table_cte_implicit AS WITH q AS (SELECT n FROM dst) SELECT n FROM q")
	cteExec(t, d, "CREATE VIEW table_cte_star AS WITH q AS (SELECT * FROM dst) SELECT n FROM q")
	cteExec(t, d, "ALTER TABLE dst RENAME COLUMN n TO value")
	cteExec(t, d, "ALTER TABLE dst RENAME TO renamed_dst")
	cteInts(t, cteExec(t, d, "SELECT v FROM table_cte"), "v", []int64{12})
	cteInts(t, cteExec(t, d, "SELECT n FROM table_cte_implicit"), "n", []int64{12})
	cteInts(t, cteExec(t, d, "SELECT n FROM table_cte_star"), "n", []int64{12})
	p, err := d.Prepare("WITH RECURSIVE x(n) AS (SELECT ? UNION ALL SELECT n+? FROM x WHERE n<?) SELECT n FROM x ORDER BY n LIMIT ? OFFSET ?")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for _, start := range []int64{1, 4} {
		r, err := p.Exec(start, int64(1), start+3, int64(2), int64(1))
		if err != nil {
			t.Fatal(err)
		}
		cteInts(t, r, "n", []int64{start + 1, start + 2})
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := p.Exec(int64(i+1), int64(1), int64(i+4), int64(2), int64(0))
			if err != nil {
				t.Error(err)
				return
			}
			if len(r.Rows) != 2 || r.Rows[0]["n"].Int != int64(i+1) {
				t.Errorf("prepared leakage: %+v", r.Rows)
			}
		}(i)
	}
	wg.Wait()
	for _, q := range []string{"WITH x(n) AS (SELECT 1) SELECT n FROM x", "WITH renamed_dst(n) AS (SELECT 7) SELECT n FROM renamed_dst"} {
		rows, err := d.Query(q)
		if err != nil {
			t.Fatal(err)
		}
		if !rows.Next() {
			t.Fatalf("cursor missing row: %s", q)
		}
		if rows.Next() {
			t.Fatal("extra cursor row")
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for _, q := range []string{"EXPLAIN WITH x(n) AS (SELECT 1) SELECT n FROM x", "EXPLAIN ANALYZE WITH x(n) AS (SELECT 1) SELECT n FROM x",
		"EXPLAIN WITH x(n) AS (SELECT 1/0) SELECT n FROM x",
		"EXPLAIN WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x) SELECT n FROM x",
		"EXPLAIN WITH x(n) AS (SELECT 1) SELECT n FROM x UNION ALL SELECT n+1 FROM x",
		"EXPLAIN ANALYZE WITH x(n) AS (SELECT 1) SELECT n FROM x UNION ALL SELECT n+1 FROM x"} {
		cteExec(t, d, q)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = env.openDatabase(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	cteInts(t, cteExec(t, d, "SELECT n FROM saved ORDER BY n"), "n", []int64{1, 2, 3})
	cteInts(t, cteExec(t, d, "SELECT n FROM saved_union"), "n", []int64{7, 8})
	cteInts(t, cteExec(t, d, "SELECT * FROM saved_star"), "n", []int64{6})
	cteInts(t, cteExec(t, d, "SELECT v FROM table_cte"), "v", []int64{12})
	cteInts(t, cteExec(t, d, "SELECT n FROM table_cte_implicit"), "n", []int64{12})
	cteInts(t, cteExec(t, d, "SELECT n FROM table_cte_star"), "n", []int64{12})
	if _, err := d.Exec("SELECT * FROM x"); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("scope escaped: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCTEConfigurableLimits(t *testing.T) {
	for _, test := range []struct {
		name             string
		iterations, rows int64
		query, errorPart string
	}{
		{"iterations", 2, 100, "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<4) SELECT n FROM x", "iteration limit"},
		{"rows", 100, 2, "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<4) SELECT n FROM x", "row limit"},
		{"large", 1 << 40, 1 << 40, "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<4) SELECT n FROM x", ""},
		{"unlimited", 0, 0, "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<4) SELECT n FROM x", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			env := newRegressionEnv(t)
			opt := storage.DefaultOptions()
			opt.MaxCTEIterations = test.iterations
			opt.MaxCTERows = test.rows
			d, err := env.openDatabase(env.path(test.name), opt)
			if err != nil {
				t.Fatal(err)
			}
			r, err := d.Exec(test.query)
			if test.errorPart != "" {
				if err == nil || !strings.Contains(err.Error(), test.errorPart) {
					t.Fatalf("expected %s, got %v", test.errorPart, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			cteInts(t, r, "n", []int64{1, 2, 3, 4})
			session, err := d.NewSession()
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			r, err = session.Exec(test.query)
			if err != nil {
				t.Fatal(err)
			}
			cteInts(t, r, "n", []int64{1, 2, 3, 4})
		})
	}
	env := newRegressionEnv(t)
	opt := storage.DefaultOptions()
	opt.MaxCTERows = -1
	if d, err := db.Open(env.path("invalid"), opt); err == nil {
		d.Close()
		t.Fatal("negative CTE limit accepted")
	}
}
