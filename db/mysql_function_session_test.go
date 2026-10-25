package db_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"slopdb/db"
)

func TestMySQLInformationFunctionsConnectionState(t *testing.T) {
	d, err := db.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s := mysqlTypeSession(t, d)
	expect := func(q, want string) {
		t.Helper()
		r := mysqlTypeExec(t, s, q)
		if len(r.Rows) != 1 || r.Rows[0]["v"].String() != want {
			t.Fatalf("%s: %v want%s", q, r.Rows, want)
		}
	}
	expect("SELECT LAST_INSERT_ID() AS v", "0")
	mysqlTypeExec(t, s, "CREATE TABLE ids(id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,n INTEGER)")
	mysqlTypeExec(t, s, "INSERT INTO ids(n) VALUES(10),(20)")
	expect("SELECT ROW_COUNT() AS v", "2")
	expect("SELECT ROW_COUNT() AS v", "-1")
	expect("SELECT LAST_INSERT_ID() AS v", "1")
	mysqlTypeExec(t, s, "INSERT INTO ids(id,n) VALUES(30,30)")
	expect("SELECT LAST_INSERT_ID() AS v", "1")
	mysqlTypeExec(t, s, "BEGIN;INSERT INTO ids(n) VALUES(40);ROLLBACK")
	expect("SELECT LAST_INSERT_ID() AS v", "31")
	mysqlTypeExec(t, s, "CREATE TABLE audit(id BIGINT AUTO_INCREMENT PRIMARY KEY);INSERT INTO audit VALUES(1000);CREATE TRIGGER generated_audit AFTER INSERT ON ids BEGIN INSERT INTO audit DEFAULT VALUES;SELECT LAST_INSERT_ID(999);END")
	mysqlTypeExec(t, s, "INSERT INTO ids(n) VALUES(50)")
	expect("SELECT LAST_INSERT_ID() AS v", "32")
	mysqlTypeExec(t, s, "INSERT INTO ids(n) SELECT 60 UNION ALL SELECT 70")
	expect("SELECT LAST_INSERT_ID() AS v", "33")
	mysqlTypeExec(t, s, "SELECT n FROM ids LIMIT 1")
	expect("SELECT FOUND_ROWS() AS v", "1")
	rows, err := s.Query("SELECT n FROM ids ORDER BY n LIMIT 2")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	expect("SELECT FOUND_ROWS() AS v", "2")
	expect("SELECT LAST_INSERT_ID('18446744073709551615') AS v", "18446744073709551615")
	expect("SELECT LAST_INSERT_ID(18446744073709551615) AS v", "18446744073709551615")
	other := mysqlTypeSession(t, d)
	r := mysqlTypeExec(t, other, "SELECT LAST_INSERT_ID() AS v")
	if r.Rows[0]["v"].String() != "0" {
		t.Fatal("connection state leaked", r.Rows)
	}
	expect("SELECT LAST_INSERT_ID(42) AS v", "42")
	expect("SELECT BENCHMARK(4,LAST_INSERT_ID(LAST_INSERT_ID()+1)) AS v", "0")
	expect("SELECT LAST_INSERT_ID() AS v", "46")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := s.ExecContext(ctx, "SELECT BENCHMARK(18446744073709551615,SHA2('x',256))"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("benchmark cancellation", err)
	}
	expect("SELECT LAST_INSERT_ID() AS v", "46")
}
