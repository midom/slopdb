// Adapted from prepared_statement_review.go; see TESTING.md.
package regression

import (
	"context"
	"errors"
	"math"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"slopdb/db"
	"slopdb/sql"
	"slopdb/storage"
)

func preparedStatementsMust(testState *regressionEnv, e error) {
	testState.t.Helper()
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
}
func preparedStatementsCheck(testState *regressionEnv, b bool, s string) {
	testState.t.Helper()
	if !b {
		testState.t.Fatalf("%v", s)
	}
	testState.concurrentChecks.Add(1)
}
func preparedStatementsExec(testState *regressionEnv, d *db.Database, q string) *db.ExecResult {
	testState.t.Helper()
	r, e := d.Exec(q)
	preparedStatementsMust(testState, e)
	return r
}
func preparedStatementsPrep(testState *regressionEnv, d *db.Database, q string) *db.Stmt {
	testState.t.Helper()
	s, e := d.Prepare(q)
	preparedStatementsMust(testState, e)
	return s
}
func preparedStatementsRun(testState *regressionEnv, s *db.Stmt, args ...any) *db.ExecResult {
	testState.t.Helper()
	r, e := s.Exec(args...)
	preparedStatementsMust(testState, e)
	return r
}
func TestPreparedStatements(testT *testing.T) {
	testState := newRegressionEnv(testT)

	dir := testState.path("slopdb_prepared_review")
	preparedStatementsMust(testState, os.RemoveAll(dir))
	o := storage.DefaultOptions()
	o.DisableWALSync = true
	o.DeadlockDetection = true
	d, e := testState.openDatabase(dir, o)
	preparedStatementsMust(testState, e)
	defer d.Close()
	preparedStatementsExec(testState, d, "CREATE TABLE p(id INTEGER PRIMARY KEY,n INTEGER NOT NULL,t TEXT,r REAL,b BLOB)")
	ins := preparedStatementsPrep(testState, d, "INSERT INTO p VALUES(?,?,?,?,?)")
	preparedStatementsCheck(testState, ins.NumInput() == 5, "arity")
	blob := []byte{0, 1, 255}
	r := preparedStatementsRun(testState, ins, int64(1), int32(7), "x'); DROP TABLE p; --", float32(1.5), blob)
	preparedStatementsCheck(testState, r.Changes == 1, "insert")
	blob[0] = 77
	r = preparedStatementsExec(testState, d, "SELECT * FROM p WHERE id=1")
	preparedStatementsCheck(testState, r.Rows[0]["b"].Blob[0] == 0, "blob ownership")
	preparedStatementsCheck(testState, r.Rows[0]["t"].Text == "x'); DROP TABLE p; --", "safe string binding")
	for i := 2; i <= 12; i++ {
		preparedStatementsRun(testState, ins, i, i, "text", float64(i), nil)
	}
	many := preparedStatementsPrep(testState, d, "SELECT id FROM p WHERE id IN (?,?,?,?,?,?,?,?,?,?,?,?,?) ORDER BY id LIMIT ? OFFSET ?")
	args := []any{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 100, int64(4294967296), int64(0)}
	r = preparedStatementsRun(testState, many, args...)
	testState.t.Logf("large bind rows=%d values=%v\n", len(r.Rows), r.Rows)
	preparedStatementsCheck(testState, len(r.Rows) == 12, "large limit and 15 args")
	args[14] = int64(4294967296)
	preparedStatementsCheck(testState, len(preparedStatementsRun(testState, many, args...).Rows) == 0, "large offset")
	args[13], args[14] = int64(math.MaxInt64), int64(math.MaxInt64)
	preparedStatementsCheck(testState, len(preparedStatementsRun(testState, many, args...).Rows) == 0, "saturated window")
	for _, args := range [][]any{{1}, {nil, 1, "", 1., nil}, {uint64(math.MaxUint64), 1, "", 1., nil}, {13, 1, "", math.NaN(), nil}, {13, 1, struct{}{}, 1., nil}} {
		_, e := ins.Exec(args...)
		preparedStatementsCheck(testState, e != nil, "invalid argument accepted")
	}
	preparedStatementsCheck(testState, len(preparedStatementsExec(testState, d, "SELECT * FROM p").Rows) == 12, "invalid binds changed rows")
	for _, q := range []string{"SELECT ?1", "SELECT ?; SELECT ?", "SELECT * FROM ?", "CREATE TABLE bad(id INTEGER DEFAULT ?)", "CREATE VIEW bad AS SELECT ?", "", "SELECT ? AS ?"} {
		_, e := d.Prepare(q)
		preparedStatementsCheck(testState, e != nil, "invalid template accepted: "+q)
	}
	_, e = d.Exec("SELECT ?")
	preparedStatementsCheck(testState, e != nil, "unbound marker executed")
	s := preparedStatementsPrep(testState, d, "SELECT '?' AS t,? AS x,? AS y,? AS b")
	r = preparedStatementsRun(testState, s, nil, true, []byte{3})
	preparedStatementsCheck(testState, r.Rows[0]["t"].Text == "?" && r.Rows[0]["x"].Null && r.Rows[0]["y"].Int == 1 && r.Rows[0]["b"].Blob[0] == 3, "scalar typed args")
	lit := preparedStatementsPrep(testState, d, "SELECT X'010203' AS b")
	preparedStatementsRun(testState, lit).Rows[0]["b"].Blob[0] = 99
	preparedStatementsCheck(testState, preparedStatementsRun(testState, lit).Rows[0]["b"].Blob[0] == 1, "literal blob reused after result mutation")
	s = preparedStatementsPrep(testState, d, "SELECT id FROM p ORDER BY ?")
	preparedStatementsCheck(testState, len(preparedStatementsRun(testState, s, 0).Rows) == 12, "parameter treated as ORDER ordinal")
	s = preparedStatementsPrep(testState, d, "SELECT COUNT(*) AS n FROM p GROUP BY ?")
	preparedStatementsCheck(testState, preparedStatementsRun(testState, s, 0).Rows[0]["n"].Int == 12, "parameter treated as GROUP ordinal")
	s = preparedStatementsPrep(testState, d, "SELECT ? AS x UNION ALL SELECT ? LIMIT ? OFFSET ?")
	preparedStatementsCheck(testState, preparedStatementsRun(testState, s, 1, 2, 1, 1).Rows[0]["x"].Int == 2, "compound pagination parameter")
	s = preparedStatementsPrep(testState, d, "SELECT CASE WHEN ? BETWEEN ? AND ? THEN COALESCE(?,?) ELSE ? END AS x, (SELECT ?) AS y, EXISTS(SELECT id FROM p WHERE id=?) AS z")
	r = preparedStatementsRun(testState, s, 2, 1, 3, nil, "yes", "no", 42, 1)
	preparedStatementsCheck(testState, r.Rows[0]["x"].Text == "yes" && r.Rows[0]["y"].Int == 42 && r.Rows[0]["z"].Int == 1, "nested parameters")
	s = preparedStatementsPrep(testState, d, "SELECT id FROM p ORDER BY id LIMIT ?,?")
	preparedStatementsCheck(testState, preparedStatementsRun(testState, s, 2, 3).Rows[0]["id"].Int == 3 && len(preparedStatementsRun(testState, s, 2, 3).Rows) == 3, "comma offset ordering")
	for _, a := range [][]any{{nil, 1}, {1.5, 1}, {1, "2"}} {
		_, e = s.Exec(a...)
		preparedStatementsCheck(testState, e != nil, "invalid pagination argument")
	}
	selectByID := preparedStatementsPrep(testState, d, "SELECT n FROM p WHERE id=?")
	update := preparedStatementsPrep(testState, d, "UPDATE p SET n=n+? WHERE id=? RETURNING n")
	tx, e := d.Begin()
	preparedStatementsMust(testState, e)
	r, e = tx.ExecPrepared(update, 10, 1)
	preparedStatementsMust(testState, e)
	preparedStatementsCheck(testState, r.Rows[0]["n"].Int == 17, "prepared update")
	preparedStatementsCheck(testState, preparedStatementsRun(testState, selectByID, 1).Rows[0]["n"].Int == 7, "uncommitted write leaked")
	r, e = tx.ExecPrepared(selectByID, 1)
	preparedStatementsMust(testState, e)
	preparedStatementsCheck(testState, r.Rows[0]["n"].Int == 17, "read own write")
	preparedStatementsMust(testState, tx.Rollback())
	preparedStatementsCheck(testState, preparedStatementsRun(testState, selectByID, 1).Rows[0]["n"].Int == 7, "rollback")
	tx, e = d.Begin()
	preparedStatementsMust(testState, e)
	bound, e := tx.Prepare("UPDATE p SET n=n+? WHERE id=?")
	preparedStatementsMust(testState, e)
	_, e = bound.Exec(1, 1)
	preparedStatementsMust(testState, e)
	preparedStatementsMust(testState, tx.Commit())
	_, e = bound.Exec(1, 1)
	preparedStatementsCheck(testState, errors.Is(e, db.ErrTxDone), "expired prepared transaction")
	sess, e := d.NewSession()
	preparedStatementsMust(testState, e)
	bound, e = sess.Prepare("SELECT n FROM p WHERE id=?")
	preparedStatementsMust(testState, e)
	preparedStatementsCheck(testState, preparedStatementsRun(testState, bound, 1).Rows[0]["n"].Int == 8, "session statement")
	other, e := d.NewSession()
	preparedStatementsMust(testState, e)
	_, e = other.ExecPrepared(bound, 1)
	preparedStatementsCheck(testState, e != nil, "session mismatch")
	preparedStatementsMust(testState, other.Close())
	preparedStatementsMust(testState, sess.Close())
	_, e = bound.Exec(1)
	preparedStatementsCheck(testState, errors.Is(e, db.ErrSessionClosed), "closed session")
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, e = update.ExecContext(canceled, 1, 1)
	preparedStatementsCheck(testState, errors.Is(e, context.Canceled), "cancel statement")
	_, e = d.PrepareContext(canceled, "SELECT ?")
	preparedStatementsCheck(testState, errors.Is(e, context.Canceled), "cancel prepare")
	timeout := time.Duration(-1)
	_, e = update.ExecWithOptions(db.ExecOptions{Timeout: &timeout}, 1, 1)
	preparedStatementsCheck(testState, e != nil, "invalid timeout")
	d2dir := testState.path("slopdb_prepared_other")
	preparedStatementsMust(testState, os.RemoveAll(d2dir))
	d2, e := testState.openDatabase(d2dir, o)
	preparedStatementsMust(testState, e)
	tx2, e := d2.Begin()
	preparedStatementsMust(testState, e)
	_, e = tx2.ExecPrepared(selectByID, 1)
	preparedStatementsCheck(testState, e != nil, "database mismatch")
	preparedStatementsMust(testState, tx2.Rollback())
	preparedStatementsMust(testState, d2.Close())
	s = preparedStatementsPrep(testState, d, "SELECT ? AS x")
	preparedStatementsMust(testState, s.Close())
	preparedStatementsMust(testState, s.Close())
	_, e = s.Exec(1)
	preparedStatementsCheck(testState, errors.Is(e, db.ErrStmtClosed), "closed statement")
	preparedStatementsExec(testState, d, "CREATE TABLE c(id INTEGER PRIMARY KEY,n INTEGER NOT NULL)")
	preparedStatementsExec(testState, d, "INSERT INTO c VALUES(1,0)")
	increment := preparedStatementsPrep(testState, d, "UPDATE c SET n=n+? WHERE id=?")
	var wg sync.WaitGroup
	for worker := 0; worker < 10; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ss, e := d.NewSession()
			preparedStatementsMust(testState, e)
			defer ss.Close()
			for i := 0; i < 100; i++ {
				r, e := ss.ExecPrepared(increment, 1, 1)
				preparedStatementsMust(testState, e)
				preparedStatementsCheck(testState, r.Changes == 1, "concurrent update")
			}
		}()
	}
	wg.Wait()
	preparedStatementsCheck(testState, preparedStatementsExec(testState, d, "SELECT n FROM c").Rows[0]["n"].Int == 1000, "concurrent bindings lost update")
	shared := preparedStatementsPrep(testState, d, "SELECT ? AS a,? AS b")
	for worker := 0; worker < 10; worker++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ss, e := d.NewSession()
			preparedStatementsMust(testState, e)
			defer ss.Close()
			for i := 0; i < 100; i++ {
				r, e := ss.ExecPrepared(shared, w, i)
				preparedStatementsMust(testState, e)
				preparedStatementsCheck(testState, r.Rows[0]["a"].Int == int64(w) && r.Rows[0]["b"].Int == int64(i), "concurrent argument contamination")
			}
		}(worker)
	}
	wg.Wait()
	ddl := preparedStatementsPrep(testState, d, "CREATE TABLE transient(id INTEGER PRIMARY KEY,n INTEGER NOT NULL DEFAULT 3)")
	preparedStatementsRun(testState, ddl)
	preparedStatementsExec(testState, d, "DROP TABLE transient")
	preparedStatementsRun(testState, ddl)
	preparedStatementsExec(testState, d, "INSERT INTO transient(id) VALUES(1)")
	preparedStatementsCheck(testState, preparedStatementsExec(testState, d, "SELECT n FROM transient").Rows[0]["n"].Int == 3, "reused prepared DDL")
	late := preparedStatementsPrep(testState, d, "SELECT n FROM late WHERE id=?")
	_, e = late.Exec(1)
	preparedStatementsCheck(testState, e != nil, "missing table")
	preparedStatementsExec(testState, d, "CREATE TABLE late(id INTEGER PRIMARY KEY,n INTEGER)")
	preparedStatementsExec(testState, d, "INSERT INTO late VALUES(1,4)")
	preparedStatementsCheck(testState, preparedStatementsRun(testState, late, 1).Rows[0]["n"].Int == 4, "catalog late binding")
	preparedStatementsExec(testState, d, "DROP TABLE late")
	preparedStatementsExec(testState, d, "CREATE TABLE late(id INTEGER PRIMARY KEY,n TEXT)")
	preparedStatementsExec(testState, d, "INSERT INTO late VALUES(1,'new')")
	preparedStatementsCheck(testState, preparedStatementsRun(testState, late, 1).Rows[0]["n"].Text == "new", "stale prepared plan")
	for _, value := range []any{&sql.SelectStmt{}, &sql.UpdateStmt{}, &sql.DeleteStmt{}, &sql.UnionStmt{}} {
		t := reflect.TypeOf(value).Elem()
		for _, field := range []string{"Limit", "Offset"} {
			f, _ := t.FieldByName(field)
			preparedStatementsCheck(testState, f.Type.Kind() == reflect.Int64, "pagination field not int64")
		}
	}
	for _, q := range []string{"SELECT id FROM p LIMIT 4294967296", "SELECT id FROM p ORDER BY id LIMIT 9223372036854775807", "SELECT id FROM p ORDER BY id LIMIT 9223372036854775807 OFFSET 9223372036854775807", "SELECT id FROM p LIMIT -9223372036854775808", "SELECT id FROM p LIMIT 10 OFFSET 4294967296", "SELECT id FROM p LIMIT 9223372036854775807,9223372036854775807", "SELECT id FROM p UNION ALL SELECT id FROM p LIMIT 4294967296"} {
		r, e := d.Exec(q)
		preparedStatementsMust(testState, e)
		if strings.Contains(q, "OFFSET 922") || strings.Contains(q, "OFFSET 429") || strings.Contains(q, "5807,") {
			preparedStatementsCheck(testState, len(r.Rows) == 0, "huge offset wrapped")
		} else {
			preparedStatementsCheck(testState, len(r.Rows) > 0, "huge limit dropped rows")
		}
	}
	for _, q := range []string{"SELECT id FROM p LIMIT 9223372036854775808", "SELECT id FROM p LIMIT -9223372036854775809"} {
		_, e = d.Exec(q)
		preparedStatementsCheck(testState, e != nil, "pagination literal overflow")
	}
	up := preparedStatementsPrep(testState, d, "UPDATE p SET n=n+1 LIMIT ? OFFSET ?")
	preparedStatementsCheck(testState, preparedStatementsRun(testState, up, int64(math.MaxInt64), int64(math.MaxInt64)).Changes == 0, "large DML offset")
	del := preparedStatementsPrep(testState, d, "DELETE FROM p LIMIT ? OFFSET ?")
	preparedStatementsCheck(testState, preparedStatementsRun(testState, del, int64(math.MaxInt64), int64(4294967296)).Changes == 0, "large delete offset")
	preparedStatementsRun(testState, up, int64(math.MaxInt64), int64(0))
	preparedStatementsCheck(testState, len(preparedStatementsExec(testState, d, "SELECT * FROM p").Rows) == 12, "large DML limit")
	preparedStatementsMust(testState, d.Close())
	_, e = ins.Exec(50, 1, "", 1., nil)
	preparedStatementsCheck(testState, e != nil, "closed database")
	d, e = testState.openDatabase(dir, o)
	preparedStatementsMust(testState, e)
	preparedStatementsCheck(testState, preparedStatementsExec(testState, d, "SELECT n FROM c").Rows[0]["n"].Int == 1000, "persistence")
	preparedStatementsMust(testState, d.Close())
	testState.t.Logf("PASS: %d prepared API and 64-bit pagination checks\n", testState.concurrentChecks.Load())
}
