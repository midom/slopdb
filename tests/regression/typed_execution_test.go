package regression

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"testing"

	"slopdb/db"
	"slopdb/sql"
	"slopdb/storage"
)

func TestTypedAggregateExecution(t *testing.T) {
	env := newRegressionEnv(t)
	options := storage.DefaultOptions()
	options.DisableWALSync = true
	d, err := env.openDatabase(env.path("typed"), options)
	if err != nil {
		t.Fatal(err)
	}
	exec := func(query string) *db.ExecResult {
		t.Helper()
		result, err := d.Exec(query)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return result
	}
	exec("CREATE TABLE lineitem (id INTEGER PRIMARY KEY, flag TEXT, quantity INTEGER, price REAL, discount REAL, tax REAL, shipdate TEXT, payload BLOB, unused TEXT)")
	insert, err := d.Prepare("INSERT INTO lineitem VALUES (?,?,?,?,?,?,?,?,?)")
	if err != nil {
		t.Fatal(err)
	}
	// Three complete batches and a partial tail, including NULLs and both
	// rejected and accepted rows. Expected answers come from independent
	// arithmetic over generated inputs, not from another SlopDB query.
	type expectedGroup struct {
		count, sumQuantity, discountedCount int64
		sumPrice, sumDiscounted, sumCharge  float64
		discounts                           map[int]bool
	}
	groups := map[string]*expectedGroup{}
	var revenue float64
	for i := 0; i < 389; i++ {
		flag := []string{"A", "B"}[i%2]
		quantity := int64(i%30 + 1)
		price := float64(i+1) / 4
		discount := float64(i%5+4) / 100
		var boundDiscount any = discount
		if i%11 == 0 {
			boundDiscount = nil
		}
		date := "1994-06-01"
		if i%7 == 0 {
			date = "1995-01-01"
		}
		if _, err := insert.Exec(i+1, flag, quantity, price, boundDiscount, 0.02, date, []byte{byte(i % 3)}, "unused variable-width input"); err != nil {
			t.Fatal(err)
		}
		if date < "1995-01-01" {
			g := groups[flag]
			if g == nil {
				g = &expectedGroup{discounts: map[int]bool{}}
				groups[flag] = g
			}
			g.count++
			g.sumQuantity += quantity
			g.sumPrice += price
			if boundDiscount != nil {
				g.sumDiscounted += price * (1 - discount)
				g.sumCharge += price * (1 - discount) * 1.02
				g.discountedCount++
				g.discounts[i%5+4] = true
				if discount >= 0.05 && discount <= 0.07 && quantity < 24 {
					revenue += price * discount
				}
			}
		}
	}
	q1 := "SELECT x.flag, SUM(x.quantity) AS qty, SUM(x.price) AS base, SUM(x.price*(1-x.discount)) AS discounted, SUM(x.price*(1-x.discount)*(1+x.tax)) AS charge, AVG(x.quantity) AS average, COUNT(*) AS n, COUNT(x.discount) AS nonnull, COUNT(DISTINCT x.discount) AS distinct_n, MIN(x.payload) AS min_blob, MAX(x.payload) AS max_blob FROM lineitem AS x WHERE x.shipdate<'1995-01-01' GROUP BY x.flag ORDER BY x.flag"
	q6 := "SELECT SUM(price*discount) AS revenue FROM lineitem WHERE shipdate>='1994-01-01' AND shipdate<'1995-01-01' AND discount BETWEEN 0.05 AND 0.07 AND quantity<24"
	closeEnough := func(label string, got sql.Value, want float64) {
		t.Helper()
		if got.Null || got.Type != sql.ColReal || math.Abs(got.Real-want) > 1e-8 {
			t.Fatalf("%s: got %#v, want %.12f", label, got, want)
		}
	}
	verify := func(stage string) {
		t.Helper()
		result := exec(q1)
		if len(result.Rows) != 2 {
			t.Fatalf("%s: got %d groups", stage, len(result.Rows))
		}
		for i, flag := range []string{"A", "B"} {
			row, want := result.Rows[i], groups[flag]
			for name, value := range map[string]int64{"qty": want.sumQuantity, "n": want.count, "nonnull": want.discountedCount, "distinct_n": int64(len(want.discounts))} {
				if row[name].Null || row[name].Type != sql.ColInteger || row[name].Int != value {
					t.Fatalf("%s %s %s: got %#v, want %d", stage, flag, name, row[name], value)
				}
			}
			if row["x.flag"].Text != flag || !reflect.DeepEqual(row["min_blob"].Blob, []byte{0}) || !reflect.DeepEqual(row["max_blob"].Blob, []byte{2}) {
				t.Fatalf("%s: bad group/text/BLOB values %#v", stage, row)
			}
			closeEnough("base", row["base"], want.sumPrice)
			closeEnough("discounted", row["discounted"], want.sumDiscounted)
			closeEnough("charge", row["charge"], want.sumCharge)
			closeEnough("average", row["average"], float64(want.sumQuantity)/float64(want.count))
		}
		closeEnough(stage+" revenue", exec(q6).Rows[0]["revenue"], revenue)
		// A supported deterministic function forces general evaluation, with
		// equivalent NULL-preserving semantics for every input row.
		fallback := exec("SELECT SUM(price*discount) AS revenue FROM lineitem WHERE COALESCE(shipdate>='1994-01-01' AND shipdate<'1995-01-01' AND discount BETWEEN 0.05 AND 0.07 AND quantity<24,NULL)")
		closeEnough(stage+" fallback", fallback.Rows[0]["revenue"], revenue)
	}
	verify("full scan")
	exec("CREATE INDEX shipdate_idx ON lineitem(shipdate)")
	verify("indexed scan")
	// OR access branches overlap. The common physical reader must count an
	// origin row once, including covering index payloads where available.
	count := exec("SELECT COUNT(*) AS n FROM lineitem WHERE shipdate='1994-06-01' OR quantity BETWEEN 1 AND 30")
	if count.Rows[0]["n"].Int != 389 {
		t.Fatal(count.Rows)
	}
	prepared, err := d.Prepare("SELECT SUM(price*discount) AS revenue FROM lineitem WHERE shipdate>=? AND shipdate<? AND discount BETWEEN ? AND ? AND quantity<?")
	if err != nil {
		t.Fatal(err)
	}
	for _, threshold := range []int{24, 0, 24} {
		result, err := prepared.Exec("1994-01-01", "1995-01-01", 0.05, 0.07, threshold)
		if err != nil {
			t.Fatal(err)
		}
		if threshold == 0 {
			if !result.Rows[0]["revenue"].Null {
				t.Fatal("empty SUM must be NULL")
			}
		} else {
			closeEnough("prepared", result.Rows[0]["revenue"], revenue)
		}
	}
	exec("BEGIN")
	exec("INSERT INTO lineitem VALUES(390,'A',1,100.0,0.06,0.02,'1994-06-01',X'00','overlay')")
	closeEnough("own writes", exec(q6).Rows[0]["revenue"], revenue+6)
	exec("ROLLBACK")
	verify("rollback")
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.ExecContext(canceled, q1); err == nil {
		t.Fatal("canceled analytical query succeeded")
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = env.openDatabase(env.path("typed"), options)
	if err != nil {
		t.Fatal(err)
	}
	verify("reopened")
	// Empty global aggregation, integer sum overflow, and expression failures
	// retain their ordinary SQL results/errors after batching.
	exec("CREATE TABLE ints(id INTEGER PRIMARY KEY, v INTEGER)")
	exec(fmt.Sprintf("INSERT INTO ints VALUES (1,%d),(2,1)", int64(math.MaxInt64)))
	if _, err := d.Exec("SELECT SUM(v) FROM ints"); err == nil {
		t.Fatal("SUM integer overflow accepted")
	}
	if _, err := d.Exec("SELECT SUM(1/(id-1)) FROM ints"); err == nil {
		t.Fatal("aggregate argument division by zero accepted")
	}
	if result := exec("SELECT COUNT(*) AS n, SUM(v) AS total FROM ints WHERE id<0"); result.Rows[0]["n"].Int != 0 || !result.Rows[0]["total"].Null {
		t.Fatal(result.Rows)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}
