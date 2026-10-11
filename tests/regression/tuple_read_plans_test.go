// Adapted from tuple_plan_review.go; see TESTING.md.
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

func TestTupleReadPlans(testT *testing.T) {
	testState := newRegressionEnv(testT)
	dir := testState.path("slopdb_tuple_review")
	os.RemoveAll(dir)
	o := storage.DefaultOptions()
	o.DisableWALSync = true
	o.WriteBufferSize = 8192
	o.BlockSize = 256
	d, e := testState.openDatabase(dir, o)
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
	n := 0
	exec := func(q string) *db.ExecResult {
		r, e := d.Exec(q)
		if e != nil {
			testState.t.Fatalf("%v", q+": "+e.Error())
		}
		return r
	}
	exec("CREATE TABLE t(a INTEGER,b INTEGER,c INTEGER,v TEXT,PRIMARY KEY(a,b,c),INDEX bc(b,c,a))")
	for a := -2; a <= 2; a++ {
		for b := -10; b <= 10; b++ {
			for c := 0; c < 3; c++ {
				exec(fmt.Sprintf("INSERT INTO t VALUES(%d,%d,%d,'v%d')", a, b, c, c))
			}
		}
	}
	compare := func(q string) {
		slow := strings.ReplaceAll(q, "WHERE a", "WHERE (a+0)")
		slow = strings.ReplaceAll(slow, "WHERE b", "WHERE (b+0)")
		slow = strings.ReplaceAll(slow, "AND b", "AND (b+0)")
		slow = strings.ReplaceAll(slow, "AND c", "AND (c+0)")
		slow = strings.ReplaceAll(slow, "ORDER BY b", "ORDER BY (b+0)")
		slow = strings.ReplaceAll(slow, "ORDER BY c", "ORDER BY (c+0)")
		r, s := exec(q), exec(slow)
		if len(r.Rows) != len(s.Rows) || len(r.Rows) > 0 && !reflect.DeepEqual(r.Rows, s.Rows) {
			testState.t.Fatalf("%v", fmt.Sprintf("%s vs %s\ngot %v\nwant %v", q, slow, r.Rows, s.Rows))
		}
		n++
	}
	for _, w := range []string{
		"a=1", "a=1 AND b=2", "a=1 AND b>=2", "a=1 AND b>2 AND b<9", "a=1 AND b<=-2",
		"a=1 AND b IN(-10,-1,0,2,10,NULL,2)", "a IN(-2,0,2) AND b=2", "a=1 AND b=2 AND c IN(0,2,2,NULL)",
		"a=1 AND b=2 AND c>=1", "b=1 AND c=2", "b=1 AND c>=1", "b=1 AND c IN(0,2)",
		"a=1 AND b=2 AND b=3", "a=1 AND b>3 AND b<2", "a=1 AND b IN(NULL)",
		"a=1 AND b IN(1.0,2.0)", "a=1 AND b IN('1','2')", "a=1 AND b>=2 AND v='v2'",
		"a=1 AND b>=2 OR a=-2", "a=1 AND b>=9223372036854775807", "a=1 AND b<=-9223372036854775807-1",
		"a BETWEEN -1 AND 1", "a BETWEEN 0 AND 2", "a=1 AND b BETWEEN -4 AND -2",
		"a=1 AND b BETWEEN 2 AND 4", "a IN(-2,0,2) AND b BETWEEN 2 AND 4",
		"a=1 AND b BETWEEN 4 AND 2", "a=1 AND b BETWEEN 2 AND 4 AND b>2 AND b<4",
		"a=1 AND b BETWEEN 1 AND 4 AND b BETWEEN 2 AND 5", "a=1 AND b BETWEEN 2 AND 4 AND v='v2'",
		"b BETWEEN 2 AND 4", "b=1 AND c BETWEEN 0 AND 1", "a=1 AND b NOT BETWEEN 2 AND 4",
		"a BETWEEN 0 AND 1 OR a=-2", "a=1 AND b BETWEEN NULL AND 4", "a=1 AND b BETWEEN 2 AND NULL",
		"a=1 AND b BETWEEN 2.5 AND 4", "a=1 AND b BETWEEN 2 AND 3.5", "a=1 AND b BETWEEN 2 AND c",
	} {
		for _, order := range []string{"b,c,a", "b DESC,c DESC,a DESC", "c,b,a"} {
			for _, limit := range []int{0, 1, 7, 999} {
				compare(fmt.Sprintf("SELECT * FROM t WHERE %s ORDER BY %s LIMIT %d OFFSET 2", w, order, limit))
			}
		}
		compare("SELECT SUM(c),COUNT(*) FROM t WHERE " + w)
		compare("SELECT DISTINCT b,c FROM t WHERE " + w + " ORDER BY b,c")
	}
	compare("SELECT c AS b FROM t WHERE a=1 AND b>=2 ORDER BY b,c,a LIMIT 10")
	exec("BEGIN")
	exec("INSERT INTO t VALUES(1,11,0,'new')")
	exec("DELETE FROM t WHERE a=1 AND b=10")
	exec("UPDATE t SET v='newer' WHERE a=1 AND b=9")
	compare("SELECT * FROM t WHERE a=1 AND b>=8 ORDER BY b DESC,c DESC,a DESC")
	compare("SELECT * FROM t WHERE a=1 AND b BETWEEN 8 AND 11 ORDER BY b DESC,c DESC,a DESC LIMIT 5")
	compare("SELECT * FROM t WHERE b=9 AND c>=0 ORDER BY c,a")
	exec("ROLLBACK")
	exec("CREATE TABLE u(id INTEGER PRIMARY KEY,a INTEGER,b INTEGER,v TEXT,INDEX ab(a,b))")
	exec("INSERT INTO u VALUES(1,1,NULL,'null'),(2,1,0,'zero'),(3,1,-1,'minus'),(4,1,2,'two')")
	for _, q := range []string{
		"SELECT * FROM u WHERE a=1 ORDER BY b,id", "SELECT * FROM u WHERE a=1 ORDER BY b DESC,id DESC",
		"SELECT * FROM u WHERE a=1 ORDER BY b NULLS LAST,id", "SELECT * FROM u WHERE a=1 AND b>=0 ORDER BY b DESC,id DESC LIMIT 1",
		"SELECT * FROM u WHERE a=1 AND b BETWEEN -1 AND 2 ORDER BY b,id",
		"SELECT * FROM u WHERE a=1 AND b NOT BETWEEN -1 AND 2 ORDER BY b NULLS LAST,id",
		"SELECT * FROM u WHERE a=1 AND b BETWEEN NULL AND 2 ORDER BY b,id",
	} {
		compare(q)
	}
	if e := d.Close(); e != nil {
		testState.t.Fatalf("%v", e)
	}
	d, e = testState.openDatabase(dir, o)
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
	compare("SELECT * FROM t WHERE a=1 AND b>=8 ORDER BY b DESC,c DESC,a DESC")
	compare("SELECT * FROM t WHERE a=1 AND b BETWEEN 8 AND 11 ORDER BY b DESC,c DESC,a DESC LIMIT 5")
	d.Close()
	testState.t.Logf("PASS %d tuple plan / forced scan comparisons\n", n)
}
