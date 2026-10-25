package db_test

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"

	"slopdb/db"
	"slopdb/sql"
	"slopdb/storage"
)

func TestUnsignedDecimalJSONPersistentValues(t *testing.T) {
	path := t.TempDir()
	options := storage.DefaultOptions()
	options.DisableWALSync = true
	d, err := db.Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	s := mysqlTypeSession(t, d)
	mysqlTypeExec(t, s, "CREATE TABLE wide(id BIGINT UNSIGNED PRIMARY KEY,d DECIMAL(65,30),payload JSON,KEY wide_d(d))")
	mysqlTypeExec(t, s, `INSERT INTO wide VALUES(18446744073709551615,'123456789012345678901234567890.123456789012345678901234567890','{"n":18446744073709551615,"text":"🌍"}')`)
	p, err := s.Prepare("INSERT INTO wide VALUES(?,?,?)")
	if err != nil {
		t.Fatal(err)
	}
	dv, err := sql.NewDecimalValue("-0.000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.Exec(uint64(math.MaxInt64)+1, dv, `{"ok":true}`); err != nil {
		t.Fatal(err)
	}
	_ = p.Close()
	check := func() {
		t.Helper()
		r := mysqlTypeExec(t, s, "SELECT id,d,payload FROM wide ORDER BY id DESC")
		if len(r.Rows) != 2 || r.Rows[0]["id"].Type != sql.ColUnsigned || r.Rows[0]["id"].Uint != math.MaxUint64 || r.Rows[1]["id"].Uint != uint64(math.MaxInt64)+1 {
			t.Fatalf("uint order: %+v", r.Rows)
		}
		if r.Rows[0]["d"].Text != "123456789012345678901234567890.123456789012345678901234567890" {
			t.Fatalf("decimal precision: %v", r.Rows[0]["d"])
		}
		if r.Rows[0]["payload"].Type != sql.ColJSON || !strings.Contains(r.Rows[0]["payload"].Text, "18446744073709551615") {
			t.Fatalf("JSON number: %v", r.Rows)
		}
		for _, q := range []string{"SELECT id FROM wide WHERE id=18446744073709551615", "SELECT id FROM wide WHERE id IN (18446744073709551615)", "SELECT id FROM wide WHERE d>0 ORDER BY d"} {
			if rows := mysqlTypeExec(t, s, q).Rows; len(rows) != 1 || rows[0]["id"].Uint != math.MaxUint64 {
				t.Fatalf("wide predicate %s: %+v", q, rows)
			}
		}
		if _, err = d.CheckIntegrity(); err != nil {
			t.Fatal(err)
		}
	}
	check()
	for _, query := range []string{`INSERT INTO wide VALUES(1,'1','invalid')`, `INSERT INTO wide VALUES(2,'100000000000000000000000000000000000','{}')`, `INSERT INTO wide VALUES(-1,'1','{}')`} {
		if _, err = s.Exec(query); err == nil {
			t.Fatalf("accepted invalid assignment: %s", query)
		}
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = db.Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	s = mysqlTypeSession(t, d)
	check()
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestUnsignedAutoIncrementDurabilityAndTransactions(t *testing.T) {
	path := t.TempDir()
	d, err := db.Open(path, storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	s := mysqlTypeSession(t, d)
	mysqlTypeExec(t, s, "CREATE TABLE sequence(id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,v INTEGER)")
	mysqlTypeExec(t, s, "INSERT INTO sequence VALUES(9223372036854775806,0)")
	r := mysqlTypeExec(t, s, "INSERT INTO sequence(v) VALUES(1),(2)")
	if r.LastID != math.MaxInt64 || r.LastIDUnsigned != 0 {
		t.Fatalf("first generated ID: %+v", r)
	}
	r = mysqlTypeExec(t, s, "INSERT INTO sequence(v) SELECT 3")
	if r.LastIDUnsigned != uint64(math.MaxInt64)+2 {
		t.Fatalf("unsigned generated ID: %+v", r)
	}
	mysqlTypeExec(t, s, "BEGIN;INSERT INTO sequence(v) VALUES(4);ROLLBACK")
	mysqlTypeExec(t, s, "DELETE FROM sequence WHERE id>=9223372036854775808")
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = db.Open(path, storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	s = mysqlTypeSession(t, d)
	r = mysqlTypeExec(t, s, "INSERT INTO sequence(v) VALUES(5)")
	if r.LastIDUnsigned != uint64(math.MaxInt64)+3 {
		t.Fatalf("durable unsigned floor: %+v", r)
	}
	mysqlTypeExec(t, s, "UPDATE sequence SET id=18446744073709551614 WHERE v=5")
	r = mysqlTypeExec(t, s, "INSERT INTO sequence(v) VALUES(6)")
	if r.LastIDUnsigned != math.MaxUint64 {
		t.Fatalf("last unsigned ID: %+v", r)
	}
	if _, err = s.Exec("INSERT INTO sequence(v) VALUES(7)"); err == nil {
		t.Fatal("unsigned auto-increment wrapped")
	}
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestUnsignedAutoIncrementConcurrentReservations(t *testing.T) {
	d, err := db.Open(t.TempDir(), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s := mysqlTypeSession(t, d)
	mysqlTypeExec(t, s, "CREATE TABLE sequence(id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,v INTEGER)")
	mysqlTypeExec(t, s, "INSERT INTO sequence VALUES(9223372036854775807,0)")
	const workers = 8
	sessions := make([]*db.Session, workers)
	for i := range sessions {
		sessions[i] = mysqlTypeSession(t, d)
	}
	var wg sync.WaitGroup
	failures := make(chan error, workers)
	for _, session := range sessions {
		wg.Add(1)
		go func(session *db.Session) {
			defer wg.Done()
			for i := 0; i < 8; i++ {
				r, err := session.Exec("INSERT INTO sequence(v) VALUES(1)")
				if err != nil {
					failures <- err
					return
				}
				if r.LastIDUnsigned <= math.MaxInt64 {
					failures <- fmt.Errorf("unsigned ID %d", r.LastIDUnsigned)
					return
				}
			}
		}(session)
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	r := mysqlTypeExec(t, s, "SELECT COUNT(*) AS n,COUNT(DISTINCT id) AS distinct_ids FROM sequence")
	if r.Rows[0]["n"].Int != workers*8+1 || r.Rows[0]["distinct_ids"].Int != workers*8+1 {
		t.Fatal(r.Rows)
	}
}

func TestExactNumericArithmeticAndAggregates(t *testing.T) {
	d, err := db.Open(t.TempDir(), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s := mysqlTypeSession(t, d)
	for _, test := range []struct{ query, want string }{
		{"SELECT 18446744073709551615 AS n", "18446744073709551615"},
		{"SELECT 18446744073709551615-1 AS n", "18446744073709551614"},
		{"SELECT 18446744073709551614+1 AS n", "18446744073709551615"},
		{"SELECT 0.1+0.2 AS n", "0.3"},
		{"SELECT CAST('1.245' AS DECIMAL(4,2)) AS n", "1.25"},
		{"SELECT CAST('-1.245' AS DECIMAL(4,2)) AS n", "-1.25"},
		{"SELECT CAST('18446744073709551615' AS UNSIGNED) AS n", "18446744073709551615"},
		{"SELECT CAST(18446744073709551615 AS SIGNED) AS n", "-1"},
		{"SELECT 18446744073709551615>18446744073709551614 AS n", "1"},
		{"SELECT CAST('99999999999999999999999.99' AS DECIMAL(30,2))+0.01 AS n", "100000000000000000000000.00"},
	} {
		r := mysqlTypeExec(t, s, test.query)
		if got := r.Rows[0]["n"].String(); got != test.want {
			t.Fatalf("%s: got %s want %s", test.query, got, test.want)
		}
	}
	for _, query := range []string{"SELECT 18446744073709551615+1", "SELECT -18446744073709551615", "SELECT CAST('999.995' AS DECIMAL(5,2))", "CREATE TABLE bad(v DECIMAL(66,0))"} {
		if _, err = s.Exec(query); err == nil {
			t.Fatalf("accepted numeric overflow: %s", query)
		}
	}
	mysqlTypeExec(t, s, "CREATE TABLE amounts(id INTEGER PRIMARY KEY,d DECIMAL(10,2));INSERT INTO amounts VALUES(1,1.20),(2,2.30),(3,-0.50)")
	r := mysqlTypeExec(t, s, "SELECT SUM(d) AS total,AVG(d) AS average FROM amounts")
	if r.Rows[0]["total"].String() != "3.00" || r.Rows[0]["average"].String() != "1.000000" {
		t.Fatalf("decimal aggregates: %+v", r.Rows)
	}
	mysqlTypeExec(t, s, "CREATE TABLE unsigned_sum(v BIGINT UNSIGNED);INSERT INTO unsigned_sum VALUES(18446744073709551615),(18446744073709551615)")
	r = mysqlTypeExec(t, s, "SELECT SUM(v) AS total FROM unsigned_sum")
	if r.Rows[0]["total"].String() != "36893488147419103230" {
		t.Fatalf("unsigned sum: %+v", r.Rows)
	}
}

func TestExactNumericWindowsAndArguments(t *testing.T) {
	d, err := db.Open(t.TempDir(), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s := mysqlTypeSession(t, d)
	mysqlTypeExec(t, s, "CREATE TABLE measures(id INTEGER PRIMARY KEY,d DECIMAL(12,2),u BIGINT UNSIGNED);INSERT INTO measures VALUES(1,1.20,18446744073709551615),(2,2.30,18446744073709551615),(3,-0.50,1)")
	r := mysqlTypeExec(t, s, "SELECT id,SUM(d) OVER(ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS total,AVG(d) OVER(ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS average,SUM(u) OVER(ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS unsigned_total FROM measures ORDER BY id")
	for i, want := range []string{"1.20", "3.50", "1.80"} {
		if r.Rows[i]["total"].String() != want {
			t.Fatal(r.Rows)
		}
	}
	for i, want := range []string{"1.200000", "1.750000", "0.900000"} {
		if r.Rows[i]["average"].String() != want {
			t.Fatal(r.Rows)
		}
	}
	for i, want := range []string{"18446744073709551615", "36893488147419103230", "18446744073709551616"} {
		if r.Rows[i]["unsigned_total"].String() != want {
			t.Fatal(r.Rows)
		}
	}
	for _, test := range []struct{ query, want string }{
		{"SELECT GREATEST(9.0,10.0) AS v", "10.0"},
		{"SELECT LEAST(18446744073709551615,18446744073709551614) AS v", "18446744073709551614"},
		{"SELECT SUBSTRING('abcdef',2.0,3.0) AS v", "bcd"},
		{"SELECT SUBSTRING('abcdef',18446744073709551615) AS v", ""},
		{"SELECT 18446744073709551615 & 9223372036854775808 AS v", "9223372036854775808"},
		{"SELECT 18446744073709551615 >> 63 AS v", "1"},
		{"SELECT ~18446744073709551615 AS v", "0"},
	} {
		r := mysqlTypeExec(t, s, test.query)
		if r.Rows[0]["v"].String() != test.want {
			t.Fatalf("%s: %+v", test.query, r.Rows)
		}
	}
}
