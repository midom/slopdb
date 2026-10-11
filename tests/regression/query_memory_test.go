package regression

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"slopdb/db"
	"slopdb/storage"
)

func TestQueryMemorySpillsAndTopK(t *testing.T) {
	e := newRegressionEnv(t)
	temp := e.path("spills")
	if err := os.Mkdir(temp, 0700); err != nil {
		t.Fatal(err)
	}
	opt := storage.DefaultOptions()
	opt.DisableWALSync = true
	opt.QueryMemoryBytes = 512
	opt.QueryTempDir = temp
	d, err := e.openDatabase(e.path("data"), opt)
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
	exec("CREATE TABLE t(id INTEGER PRIMARY KEY,g INTEGER,n REAL,b BLOB)")
	insert, err := d.Prepare("INSERT INTO t VALUES(?,?,?,?)")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		var n any = float64(300-i) + .25
		if i%13 == 0 {
			n = nil
		}
		if _, err = insert.Exec(i, i%20, n, []byte{byte(i % 256)}); err != nil {
			t.Fatal(err)
		}
	}
	clean := func() {
		t.Helper()
		entries, e := os.ReadDir(temp)
		if e != nil {
			t.Fatal(e)
		}
		if len(entries) != 0 {
			t.Fatalf("spill files leaked: %v", entries)
		}
	}
	for _, budget := range []int64{512, 1 << 20} {
		r, err := d.ExecWithOptions("SELECT id,n FROM t ORDER BY n DESC NULLS LAST,id LIMIT 7 OFFSET 3", db.ExecOptions{MemoryBytes: &budget})
		if err != nil {
			t.Fatal(err)
		}
		if len(r.Rows) != 7 {
			t.Fatalf("rows=%d", len(r.Rows))
		}
		for i, row := range r.Rows {
			if row["id"].Int != int64(i+4) {
				t.Fatalf("row %d: %v", i, row)
			}
		}
		clean()
	}
	r := exec("SELECT id FROM t ORDER BY b DESC,id DESC LIMIT 3")
	for i, want := range []int64{255, 254, 253} {
		if r.Rows[i]["id"].Int != want {
			t.Fatalf("blob sort: %v", r.Rows)
		}
	}
	clean()
	r = exec("SELECT id FROM t ORDER BY n,id LIMIT 9223372036854775807 OFFSET 9223372036854775807")
	if len(r.Rows) != 0 {
		t.Fatal("overflowing LIMIT+OFFSET returned rows")
	}
	clean()
	r = exec("SELECT id FROM t ORDER BY id+0 DESC")
	if len(r.Rows) != 300 {
		t.Fatal(len(r.Rows))
	}
	for i, row := range r.Rows {
		if row["id"].Int != int64(299-i) {
			t.Fatal(row)
		}
	}
	clean()
	r = exec("SELECT g,COUNT(*) AS c,SUM(id) AS s,COUNT(DISTINCT id) AS dc FROM t GROUP BY g HAVING COUNT(*)=15 ORDER BY g")
	if len(r.Rows) != 20 {
		t.Fatalf("groups=%d", len(r.Rows))
	}
	for i, row := range r.Rows {
		if row["g"].Int != int64(i) || row["c"].Int != 15 || row["s"].Int != int64(2100+15*i) || row["dc"].Int != 15 {
			t.Fatalf("group %d: %v", i, row)
		}
	}
	clean()
	r = exec("SELECT DISTINCT g FROM t ORDER BY g DESC LIMIT 3")
	for i, want := range []int64{19, 18, 17} {
		if r.Rows[i]["g"].Int != want {
			t.Fatal(r.Rows)
		}
	}
	clean()
	r = exec("SELECT COUNT(DISTINCT g) AS c,SUM(DISTINCT g) AS s FROM t")
	if r.Rows[0]["c"].Int != 20 || r.Rows[0]["s"].Int != 190 {
		t.Fatal(r.Rows)
	}
	clean()
	exec("CREATE TABLE u(id INTEGER PRIMARY KEY,g INTEGER)")
	for i := 0; i < 30; i++ {
		exec(fmt.Sprintf("INSERT INTO u VALUES(%d,%d)", i, i%20))
	}
	r = exec("SELECT COUNT(*) AS c,SUM(t.id) AS s FROM t JOIN u ON t.g=u.g")
	// g<10 has two right matches, g>=10 one: 450 joined rows.
	if r.Rows[0]["c"].Int != 450 || r.Rows[0]["s"].Int != 66525 {
		t.Fatal(r.Rows)
	}
	clean()
	p := exec("EXPLAIN ANALYZE SELECT t.id,u.id FROM t JOIN u ON t.g=u.g ORDER BY t.id,u.id LIMIT 2")
	if p.Profile == nil || p.Profile.SpillBytes <= 0 || p.Profile.SpillRuns <= 0 || p.Profile.MemoryLimitBytes != 512 {
		t.Fatalf("spill metrics: %+v", p.Profile)
	}
	clean()
	exec("BEGIN")
	exec("INSERT INTO t VALUES(300,0,0.25,X'FF')")
	r = exec("SELECT id FROM t ORDER BY id+0 DESC LIMIT 1")
	if r.Rows[0]["id"].Int != 300 {
		t.Fatal("private row invisible")
	}
	exec("ROLLBACK")
	clean()
	// Prepared executions use new snapshots and fresh operator state.
	prepared, err := d.Prepare("SELECT id FROM t ORDER BY id+0 DESC LIMIT ? OFFSET ?")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		r, err = prepared.Exec(2, i)
		if err != nil {
			t.Fatal(err)
		}
		if r.Rows[0]["id"].Int != int64(299-i) {
			t.Fatal(r.Rows)
		}
		clean()
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = e.openDatabase(e.path("data"), opt)
	if err != nil {
		t.Fatal(err)
	}
	r, err = d.Exec("SELECT COUNT(*) AS c FROM t")
	if err != nil || r.Rows[0]["c"].Int != 300 {
		t.Fatalf("reopen: %v %v", r, err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestQueryMemoryCancellationAndFailures(t *testing.T) {
	e := newRegressionEnv(t)
	temp := e.path("spills")
	if err := os.Mkdir(temp, 0700); err != nil {
		t.Fatal(err)
	}
	opt := storage.DefaultOptions()
	opt.QueryMemoryBytes = 256
	opt.QueryTempDir = temp
	opt.DisableWALSync = true
	d, err := e.openDatabase(e.path("db"), opt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = d.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY,n INTEGER);INSERT INTO t VALUES(1,1),(2,0),(3,3)"); err != nil {
		t.Fatal(err)
	}
	if _, err = d.Exec("SELECT id FROM t ORDER BY 10/n"); err == nil {
		t.Fatal("sort error swallowed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = d.ExecContext(ctx, "SELECT id FROM t ORDER BY n"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	entries, err := os.ReadDir(temp)
	if err != nil || len(entries) != 0 {
		t.Fatalf("cleanup: %v %v", entries, err)
	}
	insideOptions := storage.DefaultOptions()
	insideOptions.QueryTempDir = e.path("inside/tmp")
	if inside, err := db.Open(e.path("inside"), insideOptions); err == nil {
		inside.Close()
		t.Fatal("database-local spill directory accepted")
	}
	invalid := int64(-1)
	if _, err = d.ExecWithOptions("SELECT 1", db.ExecOptions{MemoryBytes: &invalid}); err == nil {
		t.Fatal("negative budget accepted")
	}
	missing := filepath.Join(temp, "missing")
	opt.QueryTempDir = missing
	broken, err := e.openDatabase(e.path("broken"), opt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = broken.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY,n INTEGER);INSERT INTO t VALUES(1,1),(2,2),(3,3)"); err != nil {
		t.Fatal(err)
	}
	if _, err = broken.Exec("SELECT id FROM t ORDER BY n+0"); err == nil {
		t.Fatal("spill creation failure swallowed")
	}
	if err = broken.Close(); err != nil {
		t.Fatal(err)
	}
	// Cancellation while producing spill runs must close/remove all temporary files.
	stmt, err := d.Prepare("INSERT INTO t VALUES(?,?)")
	if err != nil {
		t.Fatal(err)
	}
	for i := 4; i < 2000; i++ {
		if _, err = stmt.Exec(i, i); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err = d.ExecContext(ctx, "SELECT id FROM t ORDER BY n+0 DESC"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("running cancellation: %v", err)
	}
	entries, err = os.ReadDir(temp)
	if err != nil || len(entries) != 0 {
		t.Fatalf("cancel leaked: %v %v", entries, err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}
