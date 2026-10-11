// Adapted from /tmp/godb/api_cache_diagnostics_review.go; see TESTING.md.
package regression

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"slopdb/storage"
)

func databaseAPIMust(testState *regressionEnv, e error) {
	testState.t.Helper()
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
}
func TestDatabaseAPI(testT *testing.T) {
	testState := newRegressionEnv(testT)

	dir := testState.path("godb_review_api")
	os.RemoveAll(dir)
	opt := storage.DefaultOptions()
	opt.WriteBufferSize = 4096
	opt.BlockSize = 128
	opt.MaxOpenFiles = 1
	opt.CompactionL0Trigger = 3
	d, e := testState.openDatabase(dir, opt)
	databaseAPIMust(testState, e)
	_, e = d.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT)")
	databaseAPIMust(testState, e)
	var q strings.Builder
	q.WriteString("INSERT INTO t VALUES ")
	for i := 0; i < 500; i++ {
		if i > 0 {
			q.WriteByte(',')
		}
		fmt.Fprintf(&q, "(%d,'%s')", i+1, strings.Repeat("x", 200))
	}
	r, e := d.Exec(q.String())
	databaseAPIMust(testState, e)
	if r.Changes != 500 {
		testState.t.Fatalf("%v", "insert change count")
	}
	databaseAPIMust(testState, d.Close())
	d, e = testState.openDatabase(dir, opt)
	databaseAPIMust(testState, e)
	r, e = d.Exec("SELECT COUNT(*) AS n FROM t")
	databaseAPIMust(testState, e)
	if r.Rows[0]["n"].Int != 500 {
		testState.t.Fatalf("%v", "500 rows persistence")
	}
	testState.t.Log(fmt.Sprint("PASS 500-row persistence and fragmented atomic commit"))
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				_, e := d.Exec(fmt.Sprintf("UPDATE t SET v='thread%d' WHERE id=%d", g, g*30+i+1))
				databaseAPIMust(testState, e)
				d.GetTableList()
				d.GetCreateTableSQL("t")
				d.Stats()
			}
		}(g)
	}
	wg.Wait()
	testState.t.Log(fmt.Sprint("PASS concurrent database API and metadata access"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, e = d.ExecContext(ctx, "INSERT INTO t VALUES(501,'bad')")
	if !errors.Is(e, context.Canceled) {
		testState.t.Fatalf("%v", fmt.Sprintf("cancelled insert error=%v", e))
	}
	r, e = d.Exec("SELECT COUNT(*) AS n FROM t")
	databaseAPIMust(testState, e)
	if r.Rows[0]["n"].Int != 500 {
		testState.t.Fatalf("%v", "cancelled mutation committed")
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*5)
	defer cancel()
	_, e = d.ExecContext(ctx, "SELECT a.id FROM t a CROSS JOIN t b CROSS JOIN t c")
	if !errors.Is(e, context.DeadlineExceeded) {
		testState.t.Fatalf("%v", fmt.Sprintf("query cancellation error=%v", e))
	}
	_, e = d.Exec("SELECT 1")
	databaseAPIMust(testState, e)
	testState.t.Log(fmt.Sprint("PASS context cancellation and executor reuse"))
	databaseAPIMust(testState, d.Close())
	_, e = d.Exec("SELECT 1")
	if e == nil {
		testState.t.Fatalf("%v", "exec after close")
	}
	testState.t.Log(fmt.Sprint("PASS closed API rejected"))
	dir = testState.path("godb_review_scanerr")
	os.RemoveAll(dir)
	o2 := storage.DefaultOptions()
	o2.VerifySSTableChanges = true
	d, e = testState.openDatabase(dir, o2)
	databaseAPIMust(testState, e)
	_, e = d.Exec("CREATE TABLE t(a INT); INSERT INTO t VALUES(1)")
	databaseAPIMust(testState, e)
	databaseAPIMust(testState, d.Close())
	d, e = testState.openDatabase(dir, o2)
	databaseAPIMust(testState, e)
	files, e := os.ReadDir(dir)
	databaseAPIMust(testState, e)
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".sst") {
			path := dir + "/" + f.Name()
			data, e := os.ReadFile(path)
			databaseAPIMust(testState, e)
			data[0] ^= 1
			databaseAPIMust(testState, os.WriteFile(path, data, 0600))
		}
	}
	_, e = d.Exec("SELECT * FROM t")
	if e == nil {
		testState.t.Fatalf("%v", "iterator read corruption ignored")
	}
	databaseAPIMust(testState, d.Close())
	testState.t.Log(fmt.Sprint("PASS iterator corruption returned as query error"))
}
