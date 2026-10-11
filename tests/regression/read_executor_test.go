// Adapted from /tmp/godb/standard_read_executor_review.go; see TESTING.md.
package regression

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"slopdb/db"
	"slopdb/storage"
)

func readExecutorMust(testState *regressionEnv, e error) {
	testState.t.Helper()
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
}
func TestReadExecutor(testT *testing.T) {
	testState := newRegressionEnv(testT)

	dir := testState.path("godb_read_executor_review")
	readExecutorMust(testState, os.RemoveAll(dir))
	o := storage.DefaultOptions()
	o.WriteBufferSize = 1024
	o.BlockSize = 128
	d, e := testState.openDatabase(dir, o)
	readExecutorMust(testState, e)
	checks := 0
	exec := func(q string) *db.ExecResult {
		rs, e := d.Exec(q)
		if e != nil {
			testState.t.Fatalf("%v", q+": "+e.Error())
		}
		return rs
	}
	exec("CREATE TABLE t(id INTEGER PRIMARY KEY,c INTEGER, txt TEXT, INDEX ix(c)); INSERT INTO t VALUES(1,2,'a'),(2,1,'b'),(3,2,'c'),(4,NULL,'d'); CREATE TABLE u(id INTEGER PRIMARY KEY,n INTEGER); INSERT INTO u VALUES(1,10),(3,30)")
	scalar := func(q, key string, want int64) {
		rs := exec(q)
		if len(rs.Rows) != 1 || rs.Rows[0][key].Int != want {
			testState.t.Fatalf("%v", fmt.Sprint(q, rs.Rows))
		}
		checks++
	}
	scalar("SELECT id FROM t WHERE id=2", "id", 2)
	scalar("SELECT t.id FROM t WHERE t.id=2", "t.id", 2)
	scalar("SELECT a.id AS n FROM t a WHERE a.id=2", "n", 2)
	scalar("SELECT id+10 AS n FROM t WHERE id=2", "n", 12)
	scalar("SELECT id AS c,c AS id FROM t WHERE id=2", "id", 1)
	scalar("SELECT COUNT(*) AS n FROM t", "n", 4)
	scalar("SELECT SUM(id) AS n FROM t", "n", 10)
	scalar("SELECT COUNT(c) AS n FROM t", "n", 3)
	scalar("SELECT (SELECT u.n FROM u WHERE u.id=t.id) AS n FROM t WHERE id=3", "n", 30)
	scalar("SELECT a.id FROM t a WHERE EXISTS(SELECT u.id FROM u WHERE u.id=a.id) AND a.id=3", "a.id", 3)
	rs := exec("SELECT t.id,u.n FROM t LEFT JOIN u ON u.id=t.id ORDER BY t.id")
	if len(rs.Rows) != 4 || rs.Rows[2]["u.n"].Int != 30 || !rs.Rows[1]["u.n"].Null {
		testState.t.Fatalf("%v", "join")
	}
	checks++
	exec("CREATE VIEW v AS SELECT id,c FROM t")
	scalar("SELECT v.id FROM v WHERE v.id=3", "v.id", 3)
	rs = exec("SELECT * FROM (SELECT id,c FROM t) a WHERE a.id=3")
	if len(rs.Rows) != 1 || rs.Rows[0]["id"].Int != 3 {
		testState.t.Fatalf("%v", "derived")
	}
	checks++
	rs = exec("SELECT c,COUNT(*) AS n FROM t GROUP BY c HAVING n>1 ORDER BY c")
	if len(rs.Rows) != 1 || rs.Rows[0]["n"].Int != 2 {
		testState.t.Fatalf("%v", "group")
	}
	checks++
	rs = exec("SELECT DISTINCT c FROM t ORDER BY c")
	if len(rs.Rows) != 3 || !rs.Rows[0]["c"].Null {
		testState.t.Fatalf("%v", "distinct")
	}
	checks++
	for _, desc := range []string{"", " DESC"} {
		for _, where := range []string{" WHERE c>=1", " WHERE c<=2", " WHERE c>=1 AND id>1", ""} {
			for _, offset := range []int{0, 1, 4} {
				q := fmt.Sprintf("SELECT * FROM t%s ORDER BY c%s LIMIT 2 OFFSET %d", where, desc, offset)
				a, b := exec(q), exec(strings.Replace(q, "ORDER BY c", "ORDER BY COALESCE(c,c)", 1))
				if len(a.Rows) != len(b.Rows) {
					testState.t.Fatalf("%v", "secondary count")
				}
				for i := range a.Rows {
					if !reflect.DeepEqual(a.Rows[i]["c"], b.Rows[i]["c"]) {
						testState.t.Fatalf("%v", q)
					}
				}
				checks++
			}
		}
	}
	for _, q := range []string{"SELECT missing FROM t WHERE id=1", "SELECT t.missing FROM t WHERE id=1", "SELECT a.id FROM t WHERE id=1", "SELECT id FROM t JOIN u ON t.id=u.id", "SELECT * FROM t WHERE id IN (missing)", "SELECT * FROM t ORDER BY missing LIMIT 1"} {
		_, e = d.Exec(q)
		if e == nil {
			testState.t.Fatalf("%v", "missing error: "+q)
		}
		checks++
	}
	readExecutorMust(testState, d.Close())
	testState.t.Logf("PASS %d projection, qualification, joins, correlated subqueries, views, groups, errors and secondary range checks\n", checks)
}
