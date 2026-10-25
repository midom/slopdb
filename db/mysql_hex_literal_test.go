package db_test

import (
	"testing"

	"slopdb/db"
)

func TestMySQLHexLiteralNumericContextsAndBoundaries(t *testing.T) {
	d, err := db.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s := mysqlTypeSession(t, d)
	for _, tc := range []struct{ expression, want string }{
		{"BIN(X'31')", "110001"}, {"CONV(X'31',2,10)", "49"}, {"ABS(X'31')", "49"},
		{"BIT_COUNT(X'FFFFFFFFFFFFFFFFFFFF')", "0"}, {"BIT_COUNT(CAST(X'FFFFFFFFFFFFFFFFFFFF' AS BINARY))", "80"},
		{"ABS(X'FFFFFFFFFFFFFFFF')", "18446744073709551615"},
		{"CEILING(X'FFFFFFFFFFFFFFFF')", "18446744073709551615"},
		{"TRUNCATE(X'FFFFFFFFFFFFFFFF',-1)", "18446744073709551610"},
		{"X'31'+1", "50"}, {"-X'31'", "-49"}, {"CAST(X'31' AS UNSIGNED)", "49"},
		{"BIN(CAST(X'31' AS BINARY))", "1"}, {"BIN(BINARY X'31')", "1"},
		{"BIN((SELECT X'31'))", "110001"}, {"BIN(COALESCE(X'31',X'32'))", "1"},
		{"BIN(IF(1,X'31',X'32'))", "1"}, {"BIN(CASE WHEN 1 THEN X'31' ELSE X'32' END)", "1"},
	} {
		r := mysqlTypeExec(t, s, "SELECT "+tc.expression+" AS v")
		if r.Rows[0]["v"].String() != tc.want {
			t.Errorf("%s: got%v want%s", tc.expression, r.Rows, tc.want)
		}
	}
	mysqlTypeExec(t, s, "CREATE TABLE hex_values(id INTEGER PRIMARY KEY,b BLOB,n BIGINT UNSIGNED);INSERT INTO hex_values VALUES(1,X'31',X'31')")
	for _, q := range []string{"SELECT BIN(b) AS v FROM hex_values", "SELECT BIN(x) AS v FROM (SELECT X'31' AS x) d", "WITH d AS(SELECT X'31' AS x) SELECT BIN(x) AS v FROM d"} {
		r := mysqlTypeExec(t, s, q)
		if r.Rows[0]["v"].String() != "1" {
			t.Fatal(q, r.Rows)
		}
	}
	r := mysqlTypeExec(t, s, "SELECT n FROM hex_values")
	if r.Rows[0]["n"].Int != 49 {
		t.Fatal(r.Rows)
	}
	prepared, err := s.Prepare("SELECT BIN(?) AS b,BIT_COUNT(?) AS c,ABS(X'FFFFFFFFFFFFFFFF') AS u")
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	r, err = prepared.Exec([]byte{'1'}, []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255})
	if err != nil || r.Rows[0]["b"].Text != "1" || r.Rows[0]["c"].Int != 80 || r.Rows[0]["u"].Uint != ^uint64(0) {
		t.Fatal(r, err)
	}
	if _, err = d.Exec("SELECT X'31'+1"); err == nil {
		t.Fatal("native BLOB arithmetic changed")
	}
}
