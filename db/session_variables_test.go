package db

import (
	"bytes"
	"context"
	"errors"
	"slopdb/sql"
	"testing"
	"time"
)

func TestTypedSessionVariablesExpressionsSubqueriesAndOwnership(t *testing.T) {
	d := projectionDatabase(t)
	s, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.SetDialect(sql.DialectMySQL); e != nil {
		t.Fatal(e)
	}
	exec := func(q string) *ExecResult {
		t.Helper()
		r, e := s.Exec(q)
		if e != nil {
			t.Fatalf("%s: %v", q, e)
		}
		return r
	}
	if !exec("SELECT @undefined AS v").Rows[0]["v"].Null {
		t.Fatal("undefined variable is notNULL")
	}
	exec("CREATE TABLE values_table(id INTEGER PRIMARY KEY,n INTEGER); INSERT INTO values_table VALUES(1,10),(2,20)")
	exec("SET @a=(SELECT max(n) FROM values_table), @b=@a+2, @empty=(SELECT n FROM values_table WHERE id=99)")
	r := exec("SELECT @A AS a,@b AS b,@empty AS empty")
	if r.Rows[0]["a"].Int != 20 || r.Rows[0]["b"].Int != 22 || !r.Rows[0]["empty"].Null {
		t.Fatal(r.Rows)
	}
	exec("SET @counter=0")
	r = exec("SELECT @counter:=@counter+1 AS one,@counter:=@counter+1 AS second,@counter AS final")
	if r.Rows[0]["one"].Int != 1 || r.Rows[0]["second"].Int != 2 || r.Rows[0]["final"].Int != 2 {
		t.Fatal("assignment evaluatedwrongcount", r.Rows)
	}
	exec("UPDATE values_table SET n=@b WHERE id=1")
	exec("INSERT INTO values_table VALUES(3,@a)")
	r = exec("SELECT n FROM values_table WHERE id=1")
	if r.Rows[0]["n"].Int != 22 {
		t.Fatal(r.Rows)
	}
	if _, e = s.Exec("SET @a=(SELECT n FROM values_table)"); e == nil {
		t.Fatal("multirow scalar accepted")
	}
	if exec("SELECT @a AS a").Rows[0]["a"].Int != 20 {
		t.Fatal("failed RHS overwrote variable")
	}
	if _, e = s.Exec("SET @a=(SELECT id,n FROM values_table WHERE id=1)"); e == nil {
		t.Fatal("multicolumn scalar accepted")
	}
	exec("BEGIN;SET @a=7;SAVEPOINT save;SET @a=8;ROLLBACK TO save;ROLLBACK")
	if exec("SELECT @a AS a").Rows[0]["a"].Int != 8 {
		t.Fatal("transaction rollback reset variable")
	}
	p, e := s.Prepare("SET @blob=?, @dec=CAST(? AS DECIMAL(30,5)), @json=CAST(? AS JSON)")
	if e != nil {
		t.Fatal(e)
	}
	data := []byte{0, 255, 9}
	_, e = p.Exec(data, "12345678901234567890.01234", `{"n":18446744073709551615}`)
	p.Close()
	if e != nil {
		t.Fatal(e)
	}
	data[0] = 99
	r = exec("SELECT @blob AS b,@dec AS d,@json AS j")
	if !bytes.Equal(r.Rows[0]["b"].Blob, []byte{0, 255, 9}) || r.Rows[0]["d"].Text != "12345678901234567890.01234" || r.Rows[0]["j"].Type != sql.ColJSON {
		t.Fatal(r.Rows)
	}
	r.Rows[0]["b"].Blob[0] = 88
	if exec("SELECT @blob AS b").Rows[0]["b"].Blob[0] != 0 {
		t.Fatal("result blob aliases session state")
	}
	get, e := s.Prepare("SELECT @a AS a")
	if e != nil {
		t.Fatal(e)
	}
	defer get.Close()
	before, e := s.DescribePreparedTypes(context.Background(), get, sql.DescribeOptions{})
	if e != nil || len(before.Results) != 1 {
		t.Fatal(before, e)
	}
	if exec("SELECT @a AS a").Rows[0]["a"].Int != 8 {
		t.Fatal("Describe changed variables")
	}
	other, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer other.Close()
	other.SetMySQLMode(true)
	r, e = other.Exec("SELECT @a AS a")
	if e != nil || !r.Rows[0]["a"].Null {
		t.Fatal("session variables shared", r, e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, e = s.ExecContext(ctx, "SET @a=(SELECT SLEEP(10))")
	if !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal("subquery cancellation", e)
	}
	if exec("SELECT @a AS a").Rows[0]["a"].Int != 8 {
		t.Fatal("canceled RHS overwrote variable")
	}
}
func TestSessionVariableMemoryLifecycleAndDialectBoundary(t *testing.T) {
	d := projectionDatabase(t)
	s, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	s.SetMySQLMode(true)
	if _, e = s.Exec("SET @a=1"); e != nil {
		t.Fatal(e)
	}

	if e = s.Close(); e != nil {
		t.Fatal(e)
	}

	for _, q := range []string{"CREATE TABLE bad_default(v INTEGER DEFAULT @a)", "CREATE VIEW bad_view AS SELECT @a", "CREATE TABLE okay(v INTEGER)", "ALTER TABLE okay ADD COLUMN broken INTEGER DEFAULT @a"} {
		_, err := d.Exec(q)
		if q == "CREATE TABLE okay(v INTEGER)" {
			if err != nil {
				t.Fatal(err)
			}
		} else if err == nil {
			t.Fatalf("persisted variable expression: %s", q)
		}
	}
	native, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer native.Close()
	if _, e = native.Exec("SET @a=1"); e == nil {
		t.Fatal("MySQLvariables leaked native dialect")
	}
}
