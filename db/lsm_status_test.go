package db

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"testing"
)

func TestShowLSMStatusSQLPreparedAndFilters(t *testing.T) {
	d, e := Open(t.TempDir(), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	for _, q := range []string{"CREATE TABLE status_rows(id INTEGER PRIMARY KEY, value TEXT)", "INSERT INTO status_rows VALUES(1,'one')", "VACUUM"} {
		if _, e = d.Exec(q); e != nil {
			t.Fatal(q, e)
		}
	}
	r, e := d.Exec("SHOW LSM STATUS")
	if e != nil {
		t.Fatal(e)
	}
	values := map[string]string{}
	var names []string
	for _, row := range r.Rows {
		n := row["Variable_name"].Text
		names = append(names, n)
		values[n] = row["Value"].Text
	}
	if !sort.StringsAreSorted(names) || values["engine"] != "LSM" || values["counter_scope"] != "since_open" || values["level.6.write_compression"] != "zstd" || values["wal.enabled"] != "ON" {
		t.Fatalf("status %+v", values)
	}
	if n, e := strconv.Atoi(values["level.6.files"]); e != nil || n == 0 {
		t.Fatal("missing bottom level", values)
	}
	p, e := d.Prepare("SHOW LSM STATUS LIKE 'level.%'")
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	r, e = p.Exec()
	if e != nil || len(r.Columns) != 2 || len(r.Rows) != 35 {
		t.Fatalf("prepared status %v %v", r, e)
	}
	if _, e = d.Exec("SHOW LSM STATUS WHERE Value='LSM'"); e == nil {
		t.Fatal("unsupported filter accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e = d.LSMStatus(ctx); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if d.ReplicationStorage().ChangeLogEnabled() {
		t.Fatal("status enabled journal")
	}
}

func TestLSMStatusFileFilterPruning(t *testing.T) {
	for _, tc := range []struct {
		pattern string
		want    bool
	}{
		{"cache.%", false}, {"memtable.%", false}, {"level.%", false}, {"wal.%", false}, {"file", false}, {"FILE.%", true}, {"file.00000000000000000001.bytes", true}, {"%bytes", true}, {"_ile.%", true}, {"f%", true}, {`f\ile.%`, true}, {"", false},
	} {
		if got := lsmFileFieldsSelected(&tc.pattern); got != tc.want {
			t.Errorf("%q selected=%v", tc.pattern, got)
		}
	}
	if !lsmFileFieldsSelected(nil) {
		t.Fatal("unfiltered status omitted files")
	}
}
