// Adapted from standard_inline_indexes_review.go; see TESTING.md.
package regression

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"slopdb/db"
	"slopdb/sql"
	"slopdb/storage"
)

func inlineIndexesCheck(testState *regressionEnv, ok bool, msg string) {
	testState.t.Helper()
	testState.checks++
	if !ok {
		testState.t.Fatalf("%v", msg)
	}
}
func inlineIndexesMust(testState *regressionEnv, err error) {
	testState.t.Helper()
	if err != nil {
		testState.t.Fatalf("%v", err)
	}
}
func inlineIndexesOpen(testState *regressionEnv, dir string) *db.Database {
	testState.t.Helper()
	d, err := testState.openDatabase(dir, nil)
	inlineIndexesMust(testState, err)
	return d
}
func inlineIndexesRun(testState *regressionEnv, d *db.Database, q string) *db.ExecResult {
	testState.t.Helper()
	r, err := d.Exec(q)
	if err != nil {
		testState.t.Fatalf("%v", fmt.Sprintf("%s: %v", q, err))
	}
	return r
}
func inlineIndexesFail(testState *regressionEnv, d *db.Database, q string) {
	testState.t.Helper()
	_, err := d.Exec(q)
	inlineIndexesCheck(testState, err != nil, "expected failure: "+q)
}
func inlineIndexesDdl(testState *regressionEnv, d *db.Database, table string) string {
	testState.t.Helper()
	return inlineIndexesRun(testState, d, "SHOW CREATE TABLE "+table).Rows[0]["Create table"].Text
}
func inlineIndexesRows(testState *regressionEnv, d *db.Database, q string, n int) {
	testState.t.Helper()
	inlineIndexesCheck(testState, len(inlineIndexesRun(testState, d, q).Rows) == n, "row count: "+q)
}

func TestInlineIndexes(testT *testing.T) {
	testState := newRegressionEnv(testT)

	dir := testState.path("godb_inline_indexes")
	inlineIndexesMust(testState, os.RemoveAll(dir))
	d := inlineIndexesOpen(testState, dir)
	inlineIndexesRun(testState, d, `CREATE TABLE orders (id INTEGER PRIMARY KEY, customer INTEGER, created TEXT, note TEXT,
 KEY by_customer (customer, created), INDEX by_note (note), UNIQUE KEY uq_pair (created, note), INDEX (customer), KEY (customer), UNIQUE INDEX uq_id_note (id,note));`)
	r := inlineIndexesRun(testState, d, "SHOW INDEX FROM orders")
	inlineIndexesCheck(testState, len(r.Rows) == 10, "SHOW INDEX compound row count")
	for _, name := range []string{"by_customer", "by_note", "uq_pair", "customer", "customer_2", "uq_id_note", "orders_id_idx"} {
		found := false
		for _, v := range d.GetIndexList() {
			found = found || v == name
		}
		inlineIndexesCheck(testState, found, "index not created: "+name)
	}
	original := inlineIndexesDdl(testState, d, "orders")
	inlineIndexesCheck(testState, strings.Contains(original, `KEY "by_customer" ("customer", "created")`), "missing inline compound key")
	inlineIndexesCheck(testState, strings.Contains(original, `UNIQUE KEY "uq_pair" ("created", "note")`), "missing inline unique key")
	inlineIndexesCheck(testState, !strings.Contains(original, `KEY "orders_id_idx"`), "duplicate implicit PK index")
	_, err := sql.Parse(original)
	inlineIndexesMust(testState, err)
	inlineIndexesRun(testState, d, `INSERT INTO orders VALUES (1,10,'2026','a'),(2,10,'2026','b'),(3,20,'2027','a'),(4,10,NULL,'a'),(5,10,NULL,'a');`)
	inlineIndexesFail(testState, d, `INSERT INTO orders VALUES (6,40,'2026','a')`)
	inlineIndexesRows(testState, d, "SELECT id FROM orders WHERE customer=10", 4)
	inlineIndexesRows(testState, d, "SELECT id FROM orders WHERE created='2026'", 2)
	inlineIndexesRows(testState, d, "SELECT id FROM orders WHERE note='a'", 4)
	inlineIndexesFail(testState, d, `UPDATE orders SET created='2026',note='a' WHERE id=3`)
	inlineIndexesRun(testState, d, `UPDATE orders SET customer=30,created='2028' WHERE id=3; DELETE FROM orders WHERE id=2;`)
	inlineIndexesRows(testState, d, "SELECT id FROM orders WHERE customer=20", 0)
	inlineIndexesRows(testState, d, "SELECT id FROM orders WHERE customer=30", 1)
	inlineIndexesRows(testState, d, "SELECT id FROM orders WHERE customer=10", 3)
	inlineIndexesRun(testState, d, `REPLACE INTO orders VALUES (9,40,'2026','a');`)
	inlineIndexesRows(testState, d, "SELECT id FROM orders WHERE id=1", 0)
	inlineIndexesRows(testState, d, "SELECT id FROM orders WHERE id=9", 1)
	inlineIndexesRun(testState, d, `CREATE INDEX later ON orders(note, customer)`)
	inlineIndexesCheck(testState, strings.Contains(inlineIndexesDdl(testState, d, "orders"), `KEY "later" ("note", "customer")`), "external index absent from SHOW CREATE")
	inlineIndexesRun(testState, d, `DROP INDEX later`)
	inlineIndexesCheck(testState, !strings.Contains(inlineIndexesDdl(testState, d, "orders"), `KEY "later"`), "dropped index still in SHOW CREATE")
	inlineIndexesRun(testState, d, `CREATE TABLE named (a TEXT, b TEXT, UNIQUE unique_pair (a,b), CONSTRAINT unique_a UNIQUE (a), INDEX (b));`)
	inlineIndexesRun(testState, d, `CREATE TABLE unnamed (a TEXT, b TEXT, UNIQUE KEY (a,b), KEY (a));`)
	inlineIndexesCheck(testState, strings.Contains(inlineIndexesDdl(testState, d, "unnamed"), `UNIQUE KEY "a"`), "unnamed unique name")
	inlineIndexesCheck(testState, strings.Contains(inlineIndexesDdl(testState, d, "unnamed"), `KEY "a_2"`), "unnamed duplicate suffix")
	inlineIndexesRun(testState, d, `CREATE TABLE "odd table" ("select" TEXT, "constraint" TEXT, "unique" TEXT, "key" TEXT, "index" TEXT, INDEX "select index" ("select", "constraint"), UNIQUE KEY "odd table_real_idx" ("unique", "key"));`)
	inlineIndexesRun(testState, d, "INSERT INTO \"odd table\" VALUES ('a:b','x\x00y','u','k','i')")
	inlineIndexesRows(testState, d, `SELECT * FROM "odd table" WHERE "select"='a:b'`, 1)
	inlineIndexesCheck(testState, strings.Contains(inlineIndexesDdl(testState, d, `"odd table"`), `KEY "select index" ("select", "constraint")`), "quoted names")
	for _, q := range []string{
		`CREATE TABLE bad (a INT, KEY ix(missing))`,
		`CREATE TABLE bad (a INT, KEY dup(a), INDEX dup(a))`,
		`CREATE TABLE bad (a INT, KEY by_note(a))`,
		`CREATE TABLE bad (a INT, KEY ix(a,a))`,
		`CREATE TABLE bad (a INT, KEY ix())`,
		`CREATE TABLE bad (a INT, INDEX ix(a,))`,
		`CREATE TABLE bad (a INT, INDEX ix(a DESC))`,
		`CREATE TABLE bad (a INT, INDEX ix(a(2)))`,
		`CREATE TABLE bad (a INT, KEY "PRIMARY"(a))`,
		`CREATE TABLE bad (a INTEGER PRIMARY KEY, INDEX bad_a_idx(a))`,
	} {
		inlineIndexesFail(testState, d, q)
		inlineIndexesCheck(testState, d.GetCreateTableSQL("bad") == "", "failed CREATE leaves table")
		inlineIndexesCheck(testState, !strings.Contains(fmt.Sprint(d.GetIndexList()), "dup"), "failed CREATE leaves index")
	}
	inlineIndexesRun(testState, d, `CREATE TABLE IF NOT EXISTS orders (a INT, INDEX by_note(missing))`)
	inlineIndexesCheck(testState, len(inlineIndexesRun(testState, d, "DESCRIBE orders").Rows) == 4, "IF NOT EXISTS changed schema")
	inlineIndexesRun(testState, d, `CREATE TABLE src (a INT, b INT); INSERT INTO src VALUES(1,2),(1,3),(2,3),(NULL,2),(NULL,2); CREATE UNIQUE INDEX uq_src ON src(a,b);`)
	inlineIndexesFail(testState, d, `INSERT INTO src VALUES(1,2)`)
	inlineIndexesRun(testState, d, `INSERT INTO src VALUES(2,2),(NULL,2);`)
	inlineIndexesFail(testState, d, `CREATE UNIQUE INDEX bad_unique ON src(b)`)
	inlineIndexesCheck(testState, d.GetIndexSQL("bad_unique") == "", "failed unique creation leaves index")
	inlineIndexesRun(testState, d, `BEGIN; CREATE TABLE pending (a INT, KEY pending_idx(a)); ROLLBACK;`)
	inlineIndexesCheck(testState, d.GetCreateTableSQL("pending") == "" && d.GetIndexSQL("pending_idx") == "", "DDL rollback")
	inlineIndexesRun(testState, d, `BEGIN; SAVEPOINT x; ALTER TABLE orders RENAME COLUMN created TO changed; ROLLBACK TO x; COMMIT;`)
	inlineIndexesCheck(testState, strings.Contains(inlineIndexesDdl(testState, d, "orders"), `"customer", "created"`), "rename rollback corrupted index columns")
	inlineIndexesRun(testState, d, `ALTER TABLE orders RENAME COLUMN created TO happened; ALTER TABLE orders RENAME TO history;`)
	inlineIndexesCheck(testState, strings.Contains(inlineIndexesDdl(testState, d, "history"), `"customer", "happened"`), "renamed columns not shown")
	inlineIndexesRows(testState, d, "SELECT * FROM history WHERE customer=40", 1)
	inlineIndexesFail(testState, d, `INSERT INTO history VALUES(10,50,'2026','a')`)
	before := inlineIndexesDdl(testState, d, "history")
	dump := d.Dump()
	inlineIndexesMust(testState, d.Close())
	d = inlineIndexesOpen(testState, dir)
	inlineIndexesCheck(testState, inlineIndexesDdl(testState, d, "history") == before, "reopen index metadata mismatch")
	inlineIndexesRows(testState, d, "SELECT * FROM history WHERE customer=40", 1)
	inlineIndexesFail(testState, d, `INSERT INTO history VALUES(10,50,'2026','a')`)
	inlineIndexesCheck(testState, strings.Contains(d.GetIndexSQL("uq_src"), `("a", "b")`), "compound standalone DDL")
	restore := testState.path("godb_inline_restore")
	inlineIndexesMust(testState, os.RemoveAll(restore))
	d2 := inlineIndexesOpen(testState, restore)
	inlineIndexesRun(testState, d2, dump)
	inlineIndexesCheck(testState, inlineIndexesDdl(testState, d2, "history") == before, "dump index metadata mismatch")
	inlineIndexesCheck(testState, strings.Contains(inlineIndexesDdl(testState, d2, `"odd table"`), `"odd table_real_idx"`), "dump incorrectly skipped explicit auto-looking name")
	inlineIndexesRows(testState, d2, "SELECT * FROM history WHERE customer=40", 1)
	inlineIndexesFail(testState, d2, `INSERT INTO history VALUES(10,50,'2026','a')`)
	inlineIndexesMust(testState, d2.Close())
	roundtrip := testState.path("godb_inline_ddl_restore")
	inlineIndexesMust(testState, os.RemoveAll(roundtrip))
	d2 = inlineIndexesOpen(testState, roundtrip)
	inlineIndexesRun(testState, d2, before)
	inlineIndexesCheck(testState, inlineIndexesDdl(testState, d2, "history") == before, "SHOW CREATE roundtrip mismatch")
	inlineIndexesRun(testState, d2, `INSERT INTO history VALUES(1,10,'date','n')`)
	inlineIndexesFail(testState, d2, `INSERT INTO history VALUES(2,20,'date','n')`)
	inlineIndexesMust(testState, d2.Close())
	inlineIndexesMust(testState, d.Close())
	// Remove physical index entries and legacy metadata fields, then reopen.
	l, err := testState.openStorage(dir, nil)
	inlineIndexesMust(testState, err)
	it := l.NewIterator()
	var keys [][]byte
	for it.Valid() {
		if strings.HasPrefix(string(it.Key().UserKey), "\x02") {
			keys = append(keys, append([]byte(nil), it.Key().UserKey...))
		}
		it.Next()
	}
	it.Close()
	for _, k := range keys {
		inlineIndexesMust(testState, l.Delete(k))
	}
	raw, ok, err := l.Get([]byte("\x00__schema_indexes__"))
	inlineIndexesMust(testState, err)
	inlineIndexesCheck(testState, ok, "missing index metadata")
	var indexes map[string]map[string]interface{}
	inlineIndexesMust(testState, json.Unmarshal(raw, &indexes))
	for _, idx := range indexes {
		delete(idx, "auto")
	}
	raw, err = json.Marshal(indexes)
	inlineIndexesMust(testState, err)
	inlineIndexesMust(testState, l.Put([]byte("\x00__schema_indexes__"), raw))
	inlineIndexesMust(testState, l.Close())
	// Normal opens trust atomically maintained indexes; externally corrupted
	// entries are repaired only when diagnostic verification is requested.
	options := storage.DefaultOptions()
	options.VerifyIndexesOnOpen = true
	d, err = testState.openDatabase(dir, options)
	inlineIndexesMust(testState, err)
	inlineIndexesRows(testState, d, "SELECT * FROM history WHERE customer=40", 1)
	inlineIndexesRows(testState, d, `SELECT * FROM "odd table" WHERE "select"='a:b'`, 1)
	inlineIndexesRun(testState, d, "DROP TABLE history")
	for _, name := range []string{"by_customer", "uq_pair", "by_note", "customer", "customer_2", "uq_id_note"} {
		inlineIndexesCheck(testState, d.GetIndexSQL(name) == "", "DROP TABLE left index "+name)
	}
	inlineIndexesMust(testState, d.Close())
	testState.t.Logf("PASS %d inline/compound index checks: SQL syntax, index maintenance, uniqueness, quoting, validation, transactions, ALTER, persistence, repair, dump and SHOW CREATE roundtrip\n", testState.checks)
}
