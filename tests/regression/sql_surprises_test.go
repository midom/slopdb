package regression

import (
	"testing"

	"slopdb/db"
)

// Keep these examples aligned with docs/sql-surprises.md. The examples exercise
// the surprising result and its explicit alternative, rather than a plan shape.
func TestDocumentedSQLSurprises(t *testing.T) {
	env := newRegressionEnv(t)
	database, err := env.openDatabase(env.path("sql-surprises"), nil)
	if err != nil {
		t.Fatal(err)
	}
	exec := func(query string) *db.ExecResult {
		t.Helper()
		result, err := database.Exec(query)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return result
	}
	reject := func(query string) {
		t.Helper()
		if _, err := database.Exec(query); err == nil {
			t.Fatalf("accepted %s", query)
		}
	}
	r := exec("SELECT NULL = NULL AS equality, NULL IS NULL AS missing")
	if !r.Rows[0]["equality"].Null || r.Rows[0]["missing"].Int != 1 {
		t.Fatal(r.Rows)
	}
	if r = exec("SELECT 2 NOT IN (1, NULL) AS excluded"); !r.Rows[0]["excluded"].Null {
		t.Fatal(r.Rows)
	}
	exec("CREATE TABLE customers(id INTEGER PRIMARY KEY)")
	exec("CREATE TABLE orders(id INTEGER PRIMARY KEY, customer_id INTEGER, paid INTEGER)")
	exec("INSERT INTO customers VALUES(1),(2)")
	exec("INSERT INTO orders VALUES(10,1,1)")
	r = exec("SELECT c.id AS customer_id, o.id AS order_id FROM customers c LEFT JOIN orders o ON o.customer_id=c.id AND o.paid=1 ORDER BY c.id")
	if len(r.Rows) != 2 || r.Rows[0]["order_id"].Int != 10 || !r.Rows[1]["order_id"].Null {
		t.Fatal(r.Rows)
	}
	r = exec("SELECT c.id AS customer_id, o.id AS order_id FROM customers c LEFT JOIN orders o ON o.customer_id=c.id WHERE o.paid=1 ORDER BY c.id")
	if len(r.Rows) != 1 || r.Rows[0]["customer_id"].Int != 1 {
		t.Fatal(r.Rows)
	}
	r = exec("SELECT 5/2 AS integer_ratio, CAST(5 AS REAL)/2 AS real_ratio")
	if r.Rows[0]["integer_ratio"].Int != 2 || r.Rows[0]["real_ratio"].Real != 2.5 {
		t.Fatal(r.Rows)
	}
	reject("SELECT 1/0")
	reject("SELECT 9223372036854775807+1")
	reject("SELECT CAST('oops' AS INTEGER)")
	exec("CREATE TABLE readings(id INTEGER PRIMARY KEY, value INTEGER UNIQUE CHECK(value>0))")
	exec("INSERT INTO readings VALUES(1,NULL),(2,NULL)")
	reject("INSERT INTO readings VALUES(3,0)")
	exec("CREATE TABLE required_readings(id INTEGER PRIMARY KEY, value INTEGER NOT NULL UNIQUE CHECK(value>0))")
	reject("INSERT INTO required_readings VALUES(1,NULL)")
	exec("PRAGMA collation='NFKC_CI'")
	r = exec("SELECT 'Ａ'='a' AS folded, BINARY 'Ａ'='a' AS bytewise")
	if r.Rows[0]["folded"].Int != 1 || r.Rows[0]["bytewise"].Int != 0 {
		t.Fatal(r.Rows)
	}
	exec("CREATE TABLE names(id INTEGER PRIMARY KEY, name VARCHAR(1))")
	exec("INSERT INTO names VALUES(1,'é')")
	reject("INSERT INTO names VALUES(2,'e\u0301')")

	// An independent session's commit becomes visible to the next statement.
	session, err := database.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	exec("BEGIN")
	r = exec("SELECT COUNT(*) AS n FROM customers")
	if r.Rows[0]["n"].Int != 2 {
		t.Fatal(r.Rows)
	}
	if _, err := session.Exec("INSERT INTO customers VALUES(3)"); err != nil {
		t.Fatal(err)
	}
	r = exec("SELECT COUNT(*) AS n FROM customers")
	if r.Rows[0]["n"].Int != 3 {
		t.Fatal(r.Rows)
	}
	exec("COMMIT")
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}
