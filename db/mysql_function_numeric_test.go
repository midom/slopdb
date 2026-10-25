package db_test

import (
	"math"
	"testing"

	"slopdb/db"
	"slopdb/sql"
)

func TestMySQLNumericPreparedPersistenceAndMetadata(t *testing.T) {
	path := t.TempDir()
	d, err := db.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	s := mysqlTypeSession(t, d)
	mysqlTypeExec(t, s, "CREATE TABLE numeric_functions(id INTEGER PRIMARY KEY,n DECIMAL(30,5),u BIGINT UNSIGNED);INSERT INTO numeric_functions VALUES(1,-12345678901234567890.12345,18446744073709551615);CREATE VIEW numeric_view AS SELECT id,FLOOR(n) AS rounded,MOD(u,17) AS remainder FROM numeric_functions")
	prepared, err := s.Prepare("SELECT TRUNCATE(n,2) AS t,CEILING(n) AS c,MOD(u,17) AS r,FORMAT(n,2) AS f FROM numeric_functions WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	for i := 0; i < 2; i++ {
		r, err := prepared.Exec(int64(1))
		if err != nil {
			t.Fatal(err)
		}
		row := r.Rows[0]
		if row["t"].Type != sql.ColDecimal || row["t"].Text != "-12345678901234567890.12" || row["c"].Text != "-12345678901234567890" || row["r"].Uint != 0 || row["f"].Text != "-12,345,678,901,234,567,890.12" {
			t.Fatal(row)
		}
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = db.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	s = mysqlTypeSession(t, d)
	r := mysqlTypeExec(t, s, "SELECT rounded,remainder FROM numeric_view WHERE id=1")
	if r.Rows[0]["rounded"].Text != "-12345678901234567891" || r.Rows[0]["remainder"].Uint != 0 {
		t.Fatal(r.Rows)
	}
	for _, query := range []string{"SELECT SQRT() FROM numeric_functions WHERE id=0", "SELECT TRUNCATE(n) FROM numeric_functions WHERE id=0", "SELECT CONV('a',16) FROM numeric_functions WHERE id=0", "SELECT ATAN2(1) FROM numeric_functions WHERE id=0"} {
		if _, err := s.Exec(query); err == nil {
			t.Fatalf("accepted invalid arity: %s", query)
		}
	}
}

func TestMySQLRandPreparedColumnSeedsAndPredicate(t *testing.T) {
	d, err := db.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s := mysqlTypeSession(t, d)
	mysqlTypeExec(t, s, "CREATE TABLE rand_rows(id INTEGER PRIMARY KEY,seed INTEGER);INSERT INTO rand_rows VALUES(1,3),(2,3),(3,3)")
	prepared, err := s.Prepare("SELECT RAND(?) AS a,RAND(3) AS b,RAND(seed) AS c FROM rand_rows ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	want := []float64{0.9057697559760601, 0.37307905813034536, 0.14808605345719125}
	for execution := 0; execution < 2; execution++ {
		rows, err := prepared.Exec(int64(3))
		if err != nil {
			t.Fatal(err)
		}
		for i, row := range rows.Rows {
			if math.Abs(row["a"].Real-want[i]) > 1e-15 || row["a"].Real != row["b"].Real || row["c"].Real != want[0] {
				t.Fatal("RAND expression/row seed semantics", rows.Rows)
			}
		}
	}
	rows := mysqlTypeExec(t, s, "SELECT id FROM rand_rows WHERE RAND(3)<0.5 ORDER BY id")
	if len(rows.Rows) != 2 || rows.Rows[0]["id"].Int != 2 || rows.Rows[1]["id"].Int != 3 {
		t.Fatal("RAND predicate was cached", rows.Rows)
	}
}
