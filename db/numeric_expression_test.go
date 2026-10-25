package db_test

import (
	"math"
	"testing"

	"slopdb/db"
	"slopdb/sql"
)

func TestNativeDecimalLiteralExpressionPrecision(t *testing.T) {
	path := t.TempDir()
	d, err := db.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	exec := func(q string) *db.ExecResult {
		t.Helper()
		r, e := d.Exec(q)
		if e != nil {
			t.Fatalf("%s: %v", q, e)
		}
		return r
	}
	exec("CREATE TABLE n(id INTEGER PRIMARY KEY,v DECIMAL(30,6)); CREATE INDEX nv ON n(v)")
	exec(`INSERT INTO n VALUES
 (1,12345678901234567890.123456),
 (2,-12345678901234567890.123456),
 (3,12345678901234567890.123455+0.000001),
 (4,-(12345678901234567890.123455+0.000001)),
 (5,12345678901234567890.123457-0.000001),
 (6,1234567890123456789.0123456*10),
 (7,12345678901234567890.123456/10),
 (8,18446744073709551615+0.123456),
 (9,-(18446744073709551615+0.123456))`)
	exec("INSERT INTO n SELECT id+10, -(12345678901234567890.123455+0.000001) FROM n WHERE id=1")
	exec("UPDATE n SET v=-(12345678901234567890.123455+0.000001) WHERE id=5")
	want := map[int64]string{1: "12345678901234567890.123456", 2: "-12345678901234567890.123456", 3: "12345678901234567890.123456", 4: "-12345678901234567890.123456", 5: "-12345678901234567890.123456", 6: "12345678901234567890.123456", 7: "1234567890123456789.012346", 8: "18446744073709551615.123456", 9: "-18446744073709551615.123456", 11: "-12345678901234567890.123456"}
	check := func() {
		t.Helper()
		for _, row := range exec("SELECT id,v FROM n ORDER BY id").Rows {
			if row["v"].Type != sql.ColDecimal || row["v"].Text != want[row["id"].Int] {
				t.Fatalf("row %+v want %s", row, want[row["id"].Int])
			}
		}
		if _, e := d.CheckIntegrity(); e != nil {
			t.Fatal(e)
		}
	}
	check()
	if _, e := d.Exec("UPDATE n SET v=12345678901234567890.123456*100000000 WHERE id IN(1,2)"); e == nil {
		t.Fatal("overflowing decimal expression accepted")
	}
	check()
	for _, q := range []string{
		"SELECT CAST(-12345678901234567890.123456 AS DECIMAL(30,6)) AS v",
		"SELECT CAST(-(12345678901234567890.123455+0.000001) AS DECIMAL(30,6)) AS v",
	} {
		if r := exec(q); r.Rows[0]["v"].Text != "-12345678901234567890.123456" {
			t.Fatal(q, r.Rows)
		}
	}
	if r := exec("SELECT CAST(1.0/3 AS DECIMAL(30,20)) AS v"); r.Rows[0]["v"].Text != "0.33333333333333333333" {
		t.Fatal(r.Rows)
	}
	if r := exec("SELECT 1/2 AS integer_half,1.0/2 AS real_half,0.1+0.2 AS real_sum"); r.Rows[0]["integer_half"].Type != sql.ColInteger || r.Rows[0]["integer_half"].Int != 0 || r.Rows[0]["real_half"].Type != sql.ColReal || r.Rows[0]["real_sum"].Real != 0.30000000000000004 {
		t.Fatal(r.Rows)
	}
	// Explicit REAL conversions and stored REALs keep float64 behavior.
	if r := exec("SELECT CAST(CAST(12345678901234567890.123456 AS REAL) AS DECIMAL(30,6)) AS v"); r.Rows[0]["v"].Text == want[1] {
		t.Fatal("REAL cast retained decimal source precision", r.Rows)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = db.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	check()
}

func TestMySQLExactDivisionInterpretedPreparedAndTyped(t *testing.T) {
	d, e := db.Open(t.TempDir(), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	s := mysqlTypeSession(t, d)
	defer s.Close()
	cases := map[string]string{
		"1/2":                    "0.5000",
		"-1/2":                   "-0.5000",
		"18446744073709551615/2": "9223372036854775807.5000",
		"CAST(-9223372036854775808 AS SIGNED)/-1": "9223372036854775808.0000",
		"1/0.0000000001": "10000000000.0000",
		"1.00/2":         "0.500000",
	}
	for expression, want := range cases {
		r := mysqlTypeExec(t, s, "SELECT "+expression+" AS v")
		if r.Rows[0]["v"].Type != sql.ColDecimal || r.Rows[0]["v"].Text != want {
			t.Fatalf("%s: %+v want %s", expression, r.Rows, want)
		}
	}
	p, e := s.Prepare("SELECT ?/? AS v")
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	r, e := p.Exec(uint64(math.MaxUint64), int64(2))
	if e != nil || r.Rows[0]["v"].Text != "9223372036854775807.5000" {
		t.Fatal(r, e)
	}
	mysqlTypeExec(t, s, "CREATE TABLE q(a BIGINT UNSIGNED PRIMARY KEY,b BIGINT); INSERT INTO q VALUES(1,2),(3,2)")
	r = mysqlTypeExec(t, s, "SELECT SUM(a/b) AS total,AVG(a/b) AS average FROM q WHERE a/b>0.1")
	if r.Rows[0]["total"].Text != "2.0000" || r.Rows[0]["average"].Text != "1.00000000" {
		t.Fatal(r.Rows)
	}
	for _, q := range []string{"SELECT 1/0", "SELECT 18446744073709551615/0"} {
		if _, e = s.Exec(q); e == nil {
			t.Fatal("zero divisor accepted", q)
		}
	}
	r = mysqlTypeExec(t, s, "SELECT CAST(1 AS DOUBLE)/CAST(2 AS DECIMAL(10,2)) AS v")
	if r.Rows[0]["v"].Type != sql.ColReal || r.Rows[0]["v"].Real != 0.5 {
		t.Fatal(r.Rows)
	}
}
