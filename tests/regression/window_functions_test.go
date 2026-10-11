package regression

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"testing"

	"slopdb/db"
	"slopdb/sql"
)

func TestWindowFunctions(t *testing.T) {
	e := newRegressionEnv(t)
	dir := e.path("window")
	d, err := e.openDatabase(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	run := func(q string) *db.ExecResult {
		t.Helper()
		r, err := d.Exec(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return r
	}
	run("CREATE TABLE t(id INTEGER PRIMARY KEY,g TEXT,v INTEGER); INSERT INTO t VALUES(1,'a',10),(2,'a',10),(3,'a',20),(4,'b',NULL),(5,'b',7)")
	check := func(q string, want [][]string) {
		t.Helper()
		r := run(q)
		if len(r.Rows) != len(want) {
			t.Fatalf("%s: rows %d want %d", q, len(r.Rows), len(want))
		}
		for i, row := range r.Rows {
			for j, c := range r.Columns {
				got := row[c.Name].String()
				if got != want[i][j] {
					t.Fatalf("%s row%d col%s=%s want%s", q, i, c.Name, got, want[i][j])
				}
			}
		}
	}
	check("SELECT id,ROW_NUMBER() OVER(PARTITION BY g ORDER BY v,id) AS rn,RANK() OVER(PARTITION BY g ORDER BY v) AS r,DENSE_RANK() OVER(PARTITION BY g ORDER BY v) AS dr FROM t ORDER BY id", [][]string{{"1", "1", "1", "1"}, {"2", "2", "1", "1"}, {"3", "3", "3", "2"}, {"4", "1", "1", "1"}, {"5", "2", "2", "2"}})
	check("SELECT id,SUM(v) OVER(PARTITION BY g ORDER BY v) AS s,COUNT(*) OVER(PARTITION BY g) AS n FROM t ORDER BY id", [][]string{{"1", "20", "3"}, {"2", "20", "3"}, {"3", "40", "3"}, {"4", "NULL", "2"}, {"5", "7", "2"}})
	check("SELECT id,SUM(v) OVER(ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s FROM t ORDER BY id", [][]string{{"1", "20"}, {"2", "40"}, {"3", "30"}, {"4", "27"}, {"5", "7"}})
	check("SELECT id,COUNT(*) OVER(PARTITION BY g ORDER BY v GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS n FROM t ORDER BY id", [][]string{{"1", "2"}, {"2", "2"}, {"3", "3"}, {"4", "1"}, {"5", "2"}})
	check("SELECT id,COUNT(*) OVER(ORDER BY v RANGE BETWEEN 5 PRECEDING AND 5 FOLLOWING) AS n FROM t ORDER BY id", [][]string{{"1", "3"}, {"2", "3"}, {"3", "1"}, {"4", "1"}, {"5", "3"}})
	check("SELECT id,LAG(v,1,-1) OVER w AS prev,LEAD(v,1,-1) OVER w AS next,FIRST_VALUE(v) OVER w AS first,LAST_VALUE(v) OVER w AS last,NTH_VALUE(v,2) OVER w AS second FROM t WINDOW w AS(PARTITION BY g ORDER BY id) ORDER BY id", [][]string{{"1", "-1", "10", "10", "10", "NULL"}, {"2", "10", "20", "10", "10", "10"}, {"3", "10", "-1", "10", "20", "10"}, {"4", "-1", "7", "NULL", "NULL", "NULL"}, {"5", "NULL", "-1", "NULL", "7", "7"}})
	check("SELECT g,SUM(v) AS total,SUM(SUM(v)) OVER(ORDER BY g) AS running,RANK() OVER(ORDER BY SUM(v) DESC) AS r FROM t GROUP BY g ORDER BY g", [][]string{{"a", "40", "40", "1"}, {"b", "7", "47", "2"}})
	check("SELECT id,ROW_NUMBER() OVER(ORDER BY id DESC)+10 AS n FROM t ORDER BY n LIMIT 2 OFFSET 1", [][]string{{"4", "12"}, {"3", "13"}})
	check("SELECT id FROM t ORDER BY ROW_NUMBER() OVER(ORDER BY id DESC) LIMIT 2", [][]string{{"5"}, {"4"}})
	check("SELECT *,ROW_NUMBER() OVER(ORDER BY id) AS n FROM (SELECT id,v FROM t WHERE id<=2) ORDER BY id", [][]string{{"1", "10", "1"}, {"2", "10", "2"}})
	check("SELECT ROW_NUMBER() OVER() AS n", [][]string{{"1"}})
	check("SELECT DISTINCT RANK() OVER(ORDER BY v) AS r FROM t ORDER BY r", [][]string{{"1"}, {"2"}, {"3"}, {"5"}})
	check("SELECT id,COUNT(*) OVER(ORDER BY id ROWS BETWEEN 4294967296 PRECEDING AND CURRENT ROW) AS n FROM t ORDER BY id", [][]string{{"1", "1"}, {"2", "2"}, {"3", "3"}, {"4", "4"}, {"5", "5"}})
	run("CREATE VIEW win AS SELECT id,ROW_NUMBER() OVER w AS n FROM t WINDOW w AS(ORDER BY id DESC)")
	check("SELECT id,n FROM win ORDER BY n LIMIT 2", [][]string{{"5", "1"}, {"4", "2"}})
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = e.openDatabase(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	check("SELECT id,n FROM win ORDER BY n LIMIT 2", [][]string{{"5", "1"}, {"4", "2"}})
	for _, q := range []string{
		"SELECT id FROM t WHERE ROW_NUMBER() OVER()=1",
		"SELECT id FROM t GROUP BY ROW_NUMBER() OVER()",
		"SELECT COUNT(*) FROM t HAVING ROW_NUMBER() OVER()=1",
		"SELECT SUM(ROW_NUMBER() OVER()) FROM t",
		"SELECT LAG(ROW_NUMBER() OVER()) OVER() FROM t",
		"SELECT SUM(DISTINCT v) OVER() FROM t",
		"SELECT SUM(v) OVER(ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM t",
		"SELECT SUM(v) OVER(ROWS -1 PRECEDING) FROM t",
		"SELECT SUM(v) OVER(ROWS id PRECEDING) FROM t",
		"SELECT SUM(v) OVER(RANGE 1 PRECEDING) FROM t",
		"SELECT SUM(v) OVER(ORDER BY id ROWS CURRENT ROW EXCLUDE CURRENT ROW) FROM t",
		"SELECT ROW_NUMBER() OVER missing FROM t",
		"SELECT ROW_NUMBER() OVER(ORDER BY missing) FROM t",
		"SELECT NTH_VALUE(v,0) OVER() FROM t",
		"SELECT ROW_NUMBER() OVER(ORDER BY v) FROM t GROUP BY g",
	} {
		if _, err = d.Exec(q); err == nil {
			t.Errorf("expected rejection: %s", q)
		}
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWindowPreparedConcurrent(t *testing.T) {
	e := newRegressionEnv(t)
	d, err := e.openDatabase(e.path("prepared-window"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = d.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER); INSERT INTO t VALUES(1,10),(2,20),(3,30)"); err != nil {
		t.Fatal(err)
	}
	stmt, err := d.Prepare("SELECT id,SUM(v) OVER(ORDER BY id ROWS BETWEEN ? PRECEDING AND CURRENT ROW) AS s,LAG(v,?,?) OVER(ORDER BY id) AS lag FROM t ORDER BY id LIMIT ?")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for n := 0; n < 8; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			session, err := d.NewSession()
			if err != nil {
				errs <- err
				return
			}
			defer session.Close()
			for j := 0; j < 10; j++ {
				r, err := session.ExecPrepared(stmt, int64(n%2), int64(4294967296), int64(-n), int64(3))
				if err != nil {
					errs <- err
					return
				}
				want := int64(30)
				if n%2 == 1 {
					want = 50
				}
				if len(r.Rows) != 3 || r.Rows[2]["s"].Int != want || r.Rows[0]["lag"].Int != int64(-n) {
					errs <- fmt.Errorf("bound window results mismatch: %v", r.Rows)
					return
				}
			}
		}(n)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if _, err := stmt.Exec(int64(-1), int64(1), int64(0), int64(3)); err == nil {
		t.Error("negative bound accepted")
	}
	if _, err := stmt.Exec(int64(9223372036854775807), int64(1), int64(0), int64(3)); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}

// Answers are independently generated by SQLite and checked into the repo;
// ordinary tests do not require SQLite or Python at runtime.
func TestWindowSQLiteReferences(t *testing.T) {
	data, err := os.ReadFile("testdata/window_reference.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Setup string
		Cases []struct {
			Query string
			Rows  [][]any
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err = decoder.Decode(&fixture); err != nil {
		t.Fatal(err)
	}
	e := newRegressionEnv(t)
	d, err := e.openDatabase(e.path("reference"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = d.Exec(fixture.Setup); err != nil {
		t.Fatal(err)
	}
	for _, c := range fixture.Cases {
		t.Run(c.Query, func(t *testing.T) {
			r, err := d.Exec(c.Query)
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Rows) != len(c.Rows) {
				t.Fatalf("rows %d want %d", len(r.Rows), len(c.Rows))
			}
			for i, want := range c.Rows {
				if len(r.Columns) != len(want) {
					t.Fatalf("columns %d want %d", len(r.Columns), len(want))
				}
				for j, w := range want {
					v := r.Rows[i][r.Columns[j].Name]
					switch x := w.(type) {
					case nil:
						if !v.Null {
							t.Fatalf("row%d col%d wantNULL got%v", i, j, v)
						}
					case string:
						if v.Null || v.Text != x {
							t.Fatalf("row%d col%d want%s got%v", i, j, x, v)
						}
					case json.Number:
						if n, err := x.Int64(); err == nil {
							if v.Null || v.Type != sql.ColInteger || v.Int != n {
								t.Fatalf("row%d col%d want%d got%v", i, j, n, v)
							}
						} else {
							n, _ := x.Float64()
							if v.Null || v.Type != sql.ColReal || math.Abs(v.Real-n) > 1e-8 {
								t.Fatalf("row%d col%d want%v got%v", i, j, n, v)
							}
						}
					}
				}
			}
		})
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWindowBoundsBudgetAndCursor(t *testing.T) {
	e := newRegressionEnv(t)
	d, err := e.openDatabase(e.path("window-boundary"), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = d.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER); INSERT INTO t VALUES(9007199254740992,1),(9007199254740993,2),(9007199254740994,3),(9223372036854775806,4),(9223372036854775807,5)")
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := d.Exec("SELECT name,ROW_NUMBER() OVER(ORDER BY name) AS n FROM sqlite_master WHERE type='table' ORDER BY name")
	if err != nil || len(catalog.Rows) != 1 || catalog.Rows[0]["n"].Int != 1 {
		t.Fatalf("catalog window: %v %v", catalog, err)
	}
	r, err := d.Exec("SELECT id,COUNT(*) OVER(ORDER BY id RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS n FROM t ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	for i, n := range []int64{2, 2, 1, 2, 1} {
		if r.Rows[i]["n"].Int != n {
			t.Fatalf("RANGE lost int64 precision: %v", r.Rows)
		}
	}
	r, err = d.Exec("SELECT *,ROW_NUMBER() OVER(ORDER BY t.id DESC) AS rn FROM t ORDER BY rn LIMIT 2")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Columns) != 3 || len(r.Rows) != 2 || r.Rows[0]["id"].Int != 9223372036854775807 {
		t.Fatal(r.Rows)
	}
	rows, err := d.Query("SELECT id,ROW_NUMBER() OVER(ORDER BY id DESC) AS n FROM t ORDER BY n LIMIT 2")
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for rows.Next() {
		var id, n int64
		if err := rows.Scan(&id, &n); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != 9223372036854775807 {
		t.Fatal(ids)
	}
	budget := int64(1024)
	if _, err := d.ExecWithOptions("SELECT ROW_NUMBER() OVER(ORDER BY id) FROM t", db.ExecOptions{MemoryBytes: &budget}); err == nil || !strings.Contains(err.Error(), "memory budget") {
		t.Fatalf("window did not enforce budget: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.ExecContext(ctx, "SELECT SUM(v) OVER() FROM t"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	for _, q := range []string{"SELECT SUM(v) OVER(ROWS -1 PRECEDING) FROM t WHERE id=0", "SELECT ROW_NUMBER() OVER(ROWS -1 PRECEDING) FROM t", "SELECT COUNT(*) OVER (w) FROM t WINDOW w AS(ROWS CURRENT ROW)", "SELECT COUNT(*) OVER(PARTITION BY *) FROM t"} {
		if _, err := d.Exec(q); err == nil {
			t.Errorf("expected invalid frame error: %s", q)
		}
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}
