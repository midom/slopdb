package db_test

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"slopdb/db"
	"slopdb/sql"
)

func operationalDB(t *testing.T) *db.Database {
	t.Helper()
	d, e := db.Open(filepath.Join(t.TempDir(), "db"), nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}
func operationalExec(t *testing.T, d *db.Database, q string) *db.ExecResult {
	t.Helper()
	r, e := d.Exec(q)
	if e != nil {
		t.Fatal(q, e)
	}
	return r
}
func TestRowsStreamingSnapshotAndOwnership(t *testing.T) {
	d := operationalDB(t)
	operationalExec(t, d, "CREATE TABLE t(a INTEGER,b INTEGER,payload BLOB,n TEXT,PRIMARY KEY(a,b)); INSERT INTO t VALUES(1,1,X'0102',NULL),(1,2,X'0304','x'),(2,1,X'0506','y')")
	rows, e := d.Query("SELECT a,b,payload,n FROM t WHERE a BETWEEN 1 AND 2 ORDER BY a,b")
	if e != nil {
		t.Fatal(e)
	}
	defer rows.Close()
	if rows.Materialized() {
		t.Fatal("eligible query materialized")
	}
	operationalExec(t, d, "UPDATE t SET payload=X'9999' WHERE a=1 AND b=1; INSERT INTO t VALUES(3,1,X'0000','z')")
	var results []int64
	var original []byte
	for rows.Next() {
		var a, b int64
		var payload []byte
		var n stdsql.NullString
		if e = rows.Scan(&a, &b, &payload, &n); e != nil {
			t.Fatal(e)
		}
		results = append(results, a*10+b)
		if original == nil {
			original = payload
			if n.Valid {
				t.Fatal("NULL lost")
			}
		}
		payload[0] = 42
	}
	if e = rows.Err(); e != nil {
		t.Fatal(e)
	}
	if fmt.Sprint(results) != "[11 12 21]" {
		t.Fatal(results)
	}
	if original[0] != 42 {
		t.Fatal("caller buffer unexpectedly changed")
	}
	current := operationalExec(t, d, "SELECT payload FROM t WHERE a=1 AND b=2")
	if current.Rows[0]["payload"].Blob[0] != 3 {
		t.Fatal("Scan modified database")
	}
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
	if original[0] != 42 {
		t.Fatal("Close invalidated values")
	}
}
func TestRowsFallbackAndScanErrors(t *testing.T) {
	d := operationalDB(t)
	operationalExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v REAL,n TEXT);INSERT INTO t VALUES(1,2.5,NULL),(2,1.5,'x')")
	rows, e := d.Query("SELECT id,v,n FROM t ORDER BY v")
	if e != nil {
		t.Fatal(e)
	}
	defer rows.Close()
	if !rows.Materialized() {
		t.Fatal("sort should report blocking executor")
	}
	if !rows.Next() {
		t.Fatal(rows.Err())
	}
	var id int64
	var v float64
	var n sql.Value
	if e = rows.Scan(&id, &v, &n); e != nil || id != 2 || v != 1.5 || n.Text != "x" {
		t.Fatal(e, id, v, n)
	}
	if e = rows.Scan(&id); e == nil {
		t.Fatal("wrong arity accepted")
	}
	var wrong string
	if e = rows.Scan(&wrong, &v, &n); e == nil {
		t.Fatal("integer to string accepted")
	}
	if _, e = d.Query("DELETE FROM t"); e == nil {
		t.Fatal("Query allowed mutation")
	}
	if _, e = d.Query("SELECT * FROM t; SELECT * FROM t"); e == nil {
		t.Fatal("Query allowed multiple statements")
	}
}
func TestRowsCancellationAndShutdown(t *testing.T) {
	d := operationalDB(t)
	operationalExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY);INSERT INTO t VALUES(1),(2)")
	session, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer session.Close()
	ctx, cancel := context.WithCancel(context.Background())
	rows, e := session.QueryContext(ctx, "SELECT * FROM t")
	if e != nil {
		t.Fatal(e)
	}
	cancel()
	// A cancelled idle cursor must release the session gate without Next/Close.
	execCtx, execCancel := context.WithTimeout(context.Background(), time.Second)
	defer execCancel()
	if _, e = session.ExecContext(execCtx, "SELECT count(*) FROM t"); e != nil {
		t.Fatal(e)
	}
	if !errors.Is(rows.Err(), context.Canceled) {
		t.Fatal(rows.Err())
	}
	idle, e := d.Query("SELECT * FROM t")
	if e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { done <- d.Close() }()
	select {
	case e = <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second * 5):
		t.Fatal("Close hung on idle Rows")
	}
	if idle.Next() {
		t.Fatal("closed query returned row")
	}
	if !errors.Is(idle.Err(), context.Canceled) {
		t.Fatal(idle.Err())
	}
}
func TestRowsTransactionOverlay(t *testing.T) {
	d := operationalDB(t)
	operationalExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT);INSERT INTO t VALUES(1,'committed')")
	tx, e := d.Begin()
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	if _, e = tx.Exec("INSERT INTO t VALUES(2,'private')"); e != nil {
		t.Fatal(e)
	}
	rows, e := tx.Query("SELECT * FROM t ORDER BY id DESC LIMIT 1")
	if e != nil {
		t.Fatal(e)
	}
	if !rows.Next() {
		t.Fatal(rows.Err())
	}
	var id int64
	var v string
	if e = rows.Scan(&id, &v); e != nil || id != 2 || v != "private" {
		t.Fatal(e, id, v)
	}
	rows.Close()
	if e = tx.Rollback(); e != nil {
		t.Fatal(e)
	}
	if operationalExec(t, d, "SELECT count(*) AS n FROM t").Rows[0]["n"].Int != 1 {
		t.Fatal("private rows committed")
	}
}
func TestBulkInsertAtomicConstraintsAndSourceFailure(t *testing.T) {
	d := operationalDB(t)
	operationalExec(t, d, "CREATE TABLE parent(id INTEGER PRIMARY KEY);INSERT INTO parent VALUES(1);CREATE TABLE child(id INTEGER PRIMARY KEY,parent INTEGER REFERENCES parent(id),v TEXT UNIQUE CHECK(length(v)>0));CREATE INDEX cv ON child(v)")
	source := func(input [][]any) db.BulkSource {
		i := 0
		return func(context.Context) ([]any, error) {
			if i == len(input) {
				return nil, io.EOF
			}
			r := input[i]
			i++
			return r, nil
		}
	}
	n, e := d.BulkInsert(context.Background(), "child", []string{"id", "parent", "v"}, source([][]any{{int64(1), int64(1), "one"}, {int64(2), int64(1), "two"}}))
	if e != nil || n != 2 {
		t.Fatal(n, e)
	}
	for _, invalid := range [][][]any{{{int64(3), int64(1), "three"}, {int64(4), int64(1), "one"}}, {{int64(3), int64(1), "three"}, {int64(4), int64(999), "four"}}, {{int64(3), int64(1), "three"}, {nil, int64(1), "four"}}, {{int64(3), int64(1), "three"}, {int64(4), int64(1), ""}}} {
		n, e = d.BulkInsert(context.Background(), "child", []string{"id", "parent", "v"}, source(invalid))
		if e == nil || n != 0 {
			t.Fatal("invalid import succeeded", n, e)
		}
	}
	injected := errors.New("broken source")
	i := 0
	if _, e = d.BulkInsert(context.Background(), "child", []string{"id", "parent", "v"}, func(context.Context) ([]any, error) {
		i++
		if i == 1 {
			return []any{int64(3), int64(1), "three"}, nil
		}
		return nil, injected
	}); !errors.Is(e, injected) {
		t.Fatal(e)
	}
	if operationalExec(t, d, "SELECT count(*) AS n FROM child").Rows[0]["n"].Int != 2 {
		t.Fatal("failed import leaked rows")
	}
	report, e := d.CheckIntegrity()
	if e != nil || report.SQL.Rows != 3 {
		t.Fatal(report, e)
	}
}
func TestBackupConsistentWithConcurrentTransactions(t *testing.T) {
	d := operationalDB(t)
	operationalExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER UNIQUE);INSERT INTO t VALUES(1,0),(2,1);CREATE INDEX tv ON t(v);CREATE VIEW values_view AS SELECT v FROM t")
	// Enough rows to overlap the backup copy with transaction publication.
	i := int64(3)
	if _, e := d.BulkInsert(context.Background(), "t", []string{"id", "v"}, func(context.Context) ([]any, error) {
		if i > 2000 {
			return nil, io.EOF
		}
		row := []any{i, i + 100000}
		i++
		return row, nil
	}); e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	writerErrors := make(chan error, 1)
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := int64(1); ; n++ {
			select {
			case <-stop:
				return
			default:
			}
			tx, e := d.Begin()
			if e != nil {
				writerErrors <- e
				return
			}
			_, e = tx.Exec(fmt.Sprintf("UPDATE t SET v=%d WHERE id=1;UPDATE t SET v=%d WHERE id=2", (n%40000)*2, (n%40000)*2+1))
			if e == nil {
				e = tx.Commit()
			} else {
				_ = tx.Rollback()
			}
			if e != nil {
				writerErrors <- e
				return
			}
		}
	}()
	target := filepath.Join(t.TempDir(), "backup")
	e := d.Backup(target)
	close(stop)
	wg.Wait()
	if e != nil {
		t.Fatal(e)
	}
	select {
	case e = <-writerErrors:
		t.Fatal(e)
	default:
	}
	restored, e := db.Open(target, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer restored.Close()
	result := operationalExec(t, restored, "SELECT id,v FROM t WHERE id<=2 ORDER BY id")
	if len(result.Rows) != 2 || result.Rows[1]["v"].Int != result.Rows[0]["v"].Int+1 {
		t.Fatal("torn backup", result.Rows)
	}
	if operationalExec(t, restored, "SELECT count(*) AS n FROM t").Rows[0]["n"].Int != 2000 {
		t.Fatal("backup lost rows")
	}
	if _, e = restored.CheckIntegrity(); e != nil {
		t.Fatal(e)
	}
	if _, e = restored.Exec("SELECT * FROM values_view LIMIT 1"); e != nil {
		t.Fatal(e)
	}
	if e = d.Backup(target); e == nil {
		t.Fatal("overwrote existing backup")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if e = d.BackupContext(cancelled, filepath.Join(t.TempDir(), "cancelled")); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}
func TestPreparedRowsOwnBoundASTAndBulkBuffers(t *testing.T) {
	d := operationalDB(t)
	operationalExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,b BLOB)")
	index := int64(0)
	buffer := []byte{0, 1}
	if _, e := d.BulkInsert(context.Background(), "t", []string{"id", "b"}, func(context.Context) ([]any, error) {
		if index == 300 {
			return nil, io.EOF
		}
		index++
		buffer[0] = byte(index)
		return []any{index, buffer}, nil
	}); e != nil {
		t.Fatal(e)
	}
	statement, e := d.Prepare("SELECT id,b FROM t WHERE id BETWEEN ? AND ? ORDER BY id LIMIT ?")
	if e != nil {
		t.Fatal(e)
	}
	defer statement.Close()
	first, e := statement.Query(int64(1), int64(2), int64(1))
	if e != nil {
		t.Fatal(e)
	}
	defer first.Close()
	second, e := statement.Query(int64(2), int64(3), int64(1))
	if e != nil {
		t.Fatal(e)
	}
	defer second.Close()
	for _, pair := range []struct {
		rows *db.Rows
		want int64
	}{{first, 1}, {second, 2}} {
		if !pair.rows.Next() {
			t.Fatal(pair.rows.Err())
		}
		var id int64
		var b []byte
		if e = pair.rows.Scan(&id, &b); e != nil || id != pair.want || b[0] != byte(pair.want) {
			t.Fatal(e, id, b)
		}
	}
	var nilScanner *stdsql.NullInt64
	if e = first.Scan(nilScanner, new([]byte)); e == nil {
		t.Fatal("nil Scanner accepted")
	}
}
func TestBackupExcludesPrivateWrites(t *testing.T) {
	d := operationalDB(t)
	operationalExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY);INSERT INTO t VALUES(1)")
	tx, e := d.Begin()
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	if _, e = tx.Exec("INSERT INTO t VALUES(2)"); e != nil {
		t.Fatal(e)
	}
	target := filepath.Join(t.TempDir(), "backup")
	if e = d.Backup(target); e != nil {
		t.Fatal(e)
	}
	restored, e := db.Open(target, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer restored.Close()
	if operationalExec(t, restored, "SELECT count(*) AS n FROM t").Rows[0]["n"].Int != 1 {
		t.Fatal("private write included in backup")
	}
}
func TestRowsEvaluatesExpressionsOnDemand(t *testing.T) {
	d := operationalDB(t)
	operationalExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY);INSERT INTO t VALUES(1),(2)")
	rows, e := d.Query("SELECT 10/(id-2) AS ratio FROM t ORDER BY id")
	if e != nil {
		t.Fatal("stream evaluated future row before Next", e)
	}
	defer rows.Close()
	if rows.Materialized() {
		t.Fatal("expression projection did not stream")
	}
	if !rows.Next() {
		t.Fatal(rows.Err())
	}
	var ratio int64
	if e = rows.Scan(&ratio); e != nil || ratio != -10 {
		t.Fatal(e, ratio)
	}
	if rows.Next() {
		t.Fatal("division by zero returned a row")
	}
	if rows.Err() == nil {
		t.Fatal("late expression error lost")
	}
	if _, e = d.Exec("SELECT count(*) FROM t"); e != nil {
		t.Fatal("late error did not release resources", e)
	}
}
func TestRowsExplicitCloseRetainsTransactionLocks(t *testing.T) {
	d := operationalDB(t)
	operationalExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO t VALUES(1,1)")
	tx, e := d.Begin()
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	rows, e := tx.Query("SELECT * FROM t WHERE id=1 FOR UPDATE")
	if e != nil {
		t.Fatal(e)
	}
	if !rows.Next() {
		t.Fatal(rows.Err())
	}
	rows.Close()
	session, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer session.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, e = session.ExecContext(ctx, "UPDATE t SET v=2 WHERE id=1"); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal("closing Rows released transaction lock", e)
	}
	if e = tx.Rollback(); e != nil {
		t.Fatal(e)
	}
	if _, e = session.Exec("UPDATE t SET v=2 WHERE id=1"); e != nil {
		t.Fatal(e)
	}
}
func TestBulkInsertValidatesEmptyImports(t *testing.T) {
	d := operationalDB(t)
	operationalExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY)")
	empty := func(context.Context) ([]any, error) { return nil, io.EOF }
	for _, target := range []struct {
		table   string
		columns []string
	}{{"missing", []string{"id"}}, {"t", []string{"missing"}}, {"t", []string{"id", "ID"}}} {
		if _, e := d.BulkInsert(context.Background(), target.table, target.columns, empty); e == nil {
			t.Fatal("invalid empty import accepted", target)
		}
	}
	if n, e := d.BulkInsert(context.Background(), "t", []string{"id"}, empty); e != nil || n != 0 {
		t.Fatal(n, e)
	}
}
