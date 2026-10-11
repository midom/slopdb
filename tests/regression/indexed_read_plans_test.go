// Adapted from standard_read_plan_strict_review.go; see TESTING.md.
package regression

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"slopdb/db"
	"slopdb/storage"
)

func indexedReadPlansMust(testState *regressionEnv, e error) {
	testState.t.Helper()
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
}
func TestIndexedReadPlans(testT *testing.T) {
	testState := newRegressionEnv(testT)

	dir := testState.path("godb_read_plan_strict_review")
	indexedReadPlansMust(testState, os.RemoveAll(dir))
	o := storage.DefaultOptions()
	o.WriteBufferSize = 4096
	o.BlockSize = 256
	d, e := testState.openDatabase(dir, o)
	indexedReadPlansMust(testState, e)
	exec := func(q string) *db.ExecResult {
		r, e := d.Exec(q)
		if e != nil {
			testState.t.Fatalf("%v", q+": "+e.Error())
		}
		return r
	}
	exec("CREATE TABLE t (id INTEGER PRIMARY KEY,c INTEGER, INDEX cc(c))")
	for i := 1; i <= 350; i++ {
		exec(fmt.Sprintf("INSERT INTO t VALUES (%d,%d)", i, i%13))
	}
	checks := 0
	compare := func(q string) {
		slow := strings.ReplaceAll(q, "ORDER BY id", "ORDER BY COALESCE(id,id)")
		slow = strings.ReplaceAll(slow, "ORDER BY a.id", "ORDER BY COALESCE(a.id,a.id)")
		slow = strings.ReplaceAll(slow, "id IN", "(id+0) IN")
		a, b := exec(q), exec(slow)
		if len(a.Rows) != len(b.Rows) || (len(a.Rows) > 0 && !reflect.DeepEqual(a.Rows, b.Rows)) {
			testState.t.Fatalf("%v", fmt.Sprintf("%s: got %v want %v", q, a.Rows, b.Rows))
		}
		checks++
	}
	for _, where := range []string{"", " WHERE id>=120", " WHERE id>5 AND id=7", " WHERE id<9 AND id=7", " WHERE id=7 AND id>5", " WHERE id>120", " WHERE id<=120", " WHERE id<120", " WHERE 120<=id", " WHERE 120>id", " WHERE id>=120 AND id<210", " WHERE id>=120 AND c=7", " WHERE id>=351", " WHERE id<=0", " WHERE id>=1000", " WHERE id>=120 OR id<3"} {
		for _, desc := range []string{"", " DESC"} {
			for _, limit := range []int{0, 1, 10, 100, 1000} {
				compare(fmt.Sprintf("SELECT * FROM t%s ORDER BY id%s LIMIT %d OFFSET 3", where, desc, limit))
			}
		}
	}
	for _, list := range []string{"1,2,350,999", "1,1,1,NULL,2", "NULL", "1.0,2.0", "'1','2'"} {
		compare("SELECT * FROM t WHERE id IN (" + list + ") ORDER BY id")
		compare("SELECT * FROM t WHERE id IN (" + list + ") AND c=2 ORDER BY id DESC LIMIT 10")
	}
	compare("SELECT c AS id FROM t WHERE id>=10 ORDER BY id LIMIT 20")
	compare("SELECT * FROM t a WHERE a.id>=345 ORDER BY a.id DESC LIMIT 10")
	// Mixed affinity values force an order-safe fallback, including NULL indexes.
	exec("CREATE TABLE u (id INTEGER,c INTEGER,INDEX uid(id)); INSERT INTO u VALUES (1,1),(2,4),(3,5),(NULL,6),(4,0)")
	compare("SELECT * FROM u WHERE id>=2 ORDER BY id LIMIT 100")
	compare("SELECT * FROM u WHERE id<=349 ORDER BY id DESC LIMIT 100")
	exec("BEGIN")
	exec("INSERT INTO t VALUES (351,0)")
	exec("DELETE FROM t WHERE id=349")
	compare("SELECT * FROM t WHERE id>=345 ORDER BY id LIMIT 100")
	compare("SELECT * FROM t WHERE id IN (348,349,350,351) ORDER BY id")
	exec("ROLLBACK")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, e = d.ExecContext(ctx, "SELECT * FROM t WHERE id>=1 ORDER BY id LIMIT 100")
	if e == nil {
		testState.t.Fatalf("%v", "cancellation")
	}
	indexedReadPlansMust(testState, d.Close())
	d, e = testState.openDatabase(dir, o)
	indexedReadPlansMust(testState, e)
	compare("SELECT * FROM t WHERE id>=345 ORDER BY id DESC LIMIT 100")
	indexedReadPlansMust(testState, d.Close())
	testState.t.Logf("PASS: %d indexed read/fallback/transaction/order checks (%s)\n", checks, time.Now().Format(time.RFC3339))
}
