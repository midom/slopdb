package db

import (
	"context"
	"slopdb/sql"
	"sync/atomic"
	"testing"
	"time"
)

func TestStreamingRowsCarryCatalogTypesAndCancellation(t *testing.T) {
	d, _ := bulkDatabase(t)
	if _, e := d.Exec("CREATE TABLE row_types(id INTEGER PRIMARY KEY,v TEXT,b BLOB);INSERT INTO row_types VALUES(1,'hello',X'FF'),(2,'world',X'00')"); e != nil {
		t.Fatal(e)
	}
	rows, e := d.Query("SELECT id,v,b,v FROM row_types ORDER BY id")
	if e != nil {
		t.Fatal(e)
	}
	defer rows.Close()
	if rows.Materialized() {
		t.Fatal("expected cursor")
	}
	cols := rows.Columns()
	want := []sql.ColumnType{sql.ColInteger, sql.ColText, sql.ColBlob, sql.ColText}
	for i, col := range cols {
		if col.ColType != want[i] {
			t.Fatalf("column %d type %v", i, col.ColType)
		}
	}
	var calls atomic.Int64
	stop := rows.OnCancel(func() { calls.Add(1) })
	for rows.Next() {
	}
	stop()
	if e = rows.Err(); e != nil || calls.Load() != 0 {
		t.Fatal(e, calls.Load())
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rows, e = d.QueryContext(ctx, "SELECT v FROM row_types")
	if e != nil {
		t.Fatal(e)
	}
	called := make(chan struct{})
	stop = rows.OnCancel(func() { close(called) })
	defer stop()
	cancel()
	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("stream cancellation observer not called")
	}
	rows.Close()
}
func TestRowsSQLFailureDoesNotSignalExternalCancellation(t *testing.T) {
	d, _ := bulkDatabase(t)
	if _, e := d.Exec("CREATE TABLE bad_rows(id INTEGER PRIMARY KEY);INSERT INTO bad_rows VALUES(1)"); e != nil {
		t.Fatal(e)
	}
	rows, e := d.Query("SELECT id/(id-id) FROM bad_rows")
	if e != nil {
		t.Fatal(e)
	}
	defer rows.Close()
	var calls atomic.Int64
	stop := rows.OnCancel(func() { calls.Add(1) })
	defer stop()
	if rows.Next() {
		t.Fatal("division by zero succeeded")
	}
	if rows.Err() == nil {
		t.Fatal("missing arithmetic error")
	}
	stop()
	if calls.Load() != 0 {
		t.Fatal("SQL error signalled external I/O cancellation")
	}
}

func TestRowsCancellationUnregisterIsIdempotent(t *testing.T) {
	d, _ := bulkDatabase(t)
	if _, e := d.Exec("CREATE TABLE observer(id INTEGER)"); e != nil {
		t.Fatal(e)
	}
	rows, e := d.Query("SELECT id FROM observer")
	if e != nil {
		t.Fatal(e)
	}
	defer rows.Close()
	stop := rows.OnCancel(func() { t.Error("unregistered callback ran") })
	stop()
	stop()
}
