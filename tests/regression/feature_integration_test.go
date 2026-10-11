package regression

import (
	"strings"
	"testing"
)

func TestCTEWindowUpsertPreparedIntegration(t *testing.T) {
	d := cteDatabase(t)
	cteExec(t, d, "CREATE TABLE totals(id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)")
	cteExec(t, d, "INSERT INTO totals VALUES(1,10)")
	stmt, err := d.Prepare(`WITH input(id,amount) AS (SELECT 1,? UNION ALL SELECT 2,?)
		INSERT INTO totals SELECT id,SUM(amount) OVER(ORDER BY id) FROM input
		ON CONFLICT(id) DO UPDATE SET amount=totals.amount+excluded.amount RETURNING id,amount`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for _, args := range [][2]int64{{3, 4}, {5, 6}} {
		if _, err := stmt.Exec(args[0], args[1]); err != nil {
			t.Fatal(err)
		}
	}
	cteInts(t, cteExec(t, d, "SELECT amount FROM totals ORDER BY id"), "amount", []int64{18, 18})
	rows, err := d.Query(`WITH x AS (SELECT id,amount FROM totals)
		SELECT id,ROW_NUMBER() OVER(ORDER BY amount,id) AS n FROM x ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Materialized() {
		t.Fatal("window cursor must use materialized execution")
	}
	for n := int64(1); n <= 2; n++ {
		if !rows.Next() {
			t.Fatalf("missing row %d: %v", n, rows.Err())
		}
		var id, rank int64
		if err := rows.Scan(&id, &rank); err != nil {
			t.Fatal(err)
		}
		if id != n || rank != n {
			t.Fatalf("row %d: id=%d rank=%d", n, id, rank)
		}
	}
	if rows.Next() || rows.Err() != nil {
		t.Fatalf("unexpected tail: %v", rows.Err())
	}
}

func TestRecursiveCTERejectsWindowStep(t *testing.T) {
	d := cteDatabase(t)
	_, err := d.Exec(`WITH RECURSIVE x(n) AS (
		SELECT 1 UNION ALL SELECT ROW_NUMBER() OVER()+n FROM x WHERE n<3
	) SELECT n FROM x`)
	if err == nil || !strings.Contains(err.Error(), "windows") {
		t.Fatalf("expected recursive window rejection, got %v", err)
	}
}

func TestWindowViewAlterRenameAndReopen(t *testing.T) {
	env := newRegressionEnv(t)
	dir := env.path("window-rename")
	d, err := env.openDatabase(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	cteExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER)")
	cteExec(t, d, "INSERT INTO t VALUES(1,20),(2,10)")
	cteExec(t, d, `CREATE VIEW named AS SELECT id,ROW_NUMBER() OVER w AS n FROM t WINDOW w AS(ORDER BY t.v)`)
	cteExec(t, d, `CREATE VIEW inline AS SELECT id,ROW_NUMBER() OVER(ORDER BY t.v) AS n FROM t`)
	cteExec(t, d, "ALTER TABLE t RENAME COLUMN v TO amount")
	cteExec(t, d, "ALTER TABLE t RENAME TO renamed")
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = env.openDatabase(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"named", "inline"} {
		cteInts(t, cteExec(t, d, "SELECT id FROM "+name+" ORDER BY n"), "id", []int64{2, 1})
	}
}
