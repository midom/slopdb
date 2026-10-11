package db_test

import (
	"reflect"
	"testing"
)

func TestRowsProjectedOrderingAlias(t *testing.T) {
	d := operationalDB(t)
	operationalExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY);INSERT INTO t VALUES(1),(2),(3)")
	for _, tc := range []struct {
		query        string
		want         []int64
		materialized bool
	}{
		{"SELECT -id AS id FROM t ORDER BY id", []int64{-3, -2, -1}, true},
		{"SELECT id AS x FROM t ORDER BY x DESC", []int64{3, 2, 1}, false},
	} {
		rows, err := d.Query(tc.query)
		if err != nil {
			t.Fatal(err)
		}
		if rows.Materialized() != tc.materialized {
			t.Fatalf("%s materialized=%v", tc.query, rows.Materialized())
		}
		var got []int64
		for rows.Next() {
			var value int64
			if err := rows.Scan(&value); err != nil {
				t.Fatal(err)
			}
			got = append(got, value)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("%s: got %v, expected %v", tc.query, got, tc.want)
		}
	}
}
