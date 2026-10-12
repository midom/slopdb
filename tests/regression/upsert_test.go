package regression

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"slopdb/db"
)

func upsertExec(t *testing.T, d interface {
	Exec(string) (*db.ExecResult, error)
}, query string) *db.ExecResult {
	t.Helper()
	r, err := d.Exec(query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return r
}
func upsertInt(t *testing.T, d interface {
	Exec(string) (*db.ExecResult, error)
}, query, column string, expected int64) {
	t.Helper()
	r := upsertExec(t, d, query)
	if len(r.Rows) != 1 || r.Rows[0][column].Null || r.Rows[0][column].Int != expected {
		t.Fatalf("%s: got %v, want %d", query, r.Rows, expected)
	}
}
func upsertError(t *testing.T, d interface {
	Exec(string) (*db.ExecResult, error)
}, query, fragment string) {
	t.Helper()
	if _, err := d.Exec(query); err == nil || !strings.Contains(err.Error(), fragment) {
		t.Fatalf("%s: want %s error, got %v", query, fragment, err)
	}
}

func TestUpsertTargetsReturningAndReopen(t *testing.T) {
	e := newRegressionEnv(t)
	dir := e.path("upsert")
	d, err := e.openDatabase(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	upsertExec(t, d, "CREATE TABLE t(a INTEGER,b INTEGER,u TEXT UNIQUE,n INTEGER NOT NULL CHECK(n>0),PRIMARY KEY(a,b))")
	upsertExec(t, d, "INSERT INTO t VALUES(1,2,'x',10),(3,4,'y',20)")
	r := upsertExec(t, d, "INSERT INTO t VALUES(1,2,'new',3),(5,6,'z',30) ON CONFLICT(b,a) DO UPDATE SET n=t.n+excluded.n,u=excluded.u RETURNING a,b,n,u")
	if r.Changes != 2 || len(r.Rows) != 2 || r.Rows[0]["n"].Int != 13 || r.Rows[0]["u"].Text != "new" {
		t.Fatalf("returning=%+v", r)
	}
	r = upsertExec(t, d, "INSERT INTO t VALUES(7,8,'new',1) ON CONFLICT(u) DO UPDATE SET a=excluded.a,b=excluded.b,n=excluded.n RETURNING a,b,n")
	if r.Changes != 1 || len(r.Rows) != 1 || r.Rows[0]["a"].Int != 7 {
		t.Fatalf("PK change=%+v", r)
	}
	upsertInt(t, d, "SELECT COUNT(*) AS n FROM t WHERE a=1", "n", 0)
	upsertInt(t, d, "SELECT n FROM t WHERE a=7 AND b=8", "n", 1)
	r = upsertExec(t, d, "INSERT INTO t VALUES(7,8,'new',9) ON CONFLICT DO NOTHING RETURNING *")
	if r.Changes != 0 || len(r.Rows) != 0 {
		t.Fatalf("nothing=%+v", r)
	}
	r = upsertExec(t, d, "INSERT INTO t VALUES(7,8,'new',9) ON CONFLICT(a,b) DO UPDATE SET n=99 WHERE excluded.n < t.n RETURNING *")
	if r.Changes != 0 || len(r.Rows) != 0 {
		t.Fatalf("false where=%+v", r)
	}
	upsertError(t, d, "INSERT INTO t VALUES(7,8,'new',9) ON CONFLICT(n) DO NOTHING", "does not match")
	upsertError(t, d, "INSERT INTO t VALUES(7,8,'new',9) ON CONFLICT(a,a) DO NOTHING", "does not match")
	upsertError(t, d, "INSERT INTO t VALUES(7,8,'new',9) ON CONFLICT(a+1) DO NOTHING", "column targets")
	upsertError(t, d, "INSERT INTO t VALUES(7,8,'new',9) ON CONFLICT(a,b) WHERE n>0 DO NOTHING", "expected keyword \"DO\"")
	upsertError(t, d, "INSERT INTO t VALUES(7,8,'new',9) ON CONFLICT(a,b) DO UPDATE SET missing=1", "no such column")
	upsertError(t, d, "INSERT INTO t VALUES(9,10,'new',9) ON CONFLICT(a,b) DO NOTHING", "UNIQUE")
	upsertExec(t, d, "CREATE TABLE src(a INTEGER,b INTEGER,u TEXT,n INTEGER)")
	upsertExec(t, d, "INSERT INTO src VALUES(7,8,'new',5),(11,12,'added',4)")
	r = upsertExec(t, d, "INSERT INTO t SELECT * FROM src ON CONFLICT(a,b) DO UPDATE SET n=n+excluded.n RETURNING n")
	if r.Changes != 2 || len(r.Rows) != 2 || r.Rows[0]["n"].Int != 6 {
		t.Fatalf("select returning=%+v", r)
	}
	upsertExec(t, d, "CREATE TABLE nullable(id INTEGER PRIMARY KEY,u TEXT UNIQUE)")
	r = upsertExec(t, d, "INSERT INTO nullable VALUES(1,NULL),(2,NULL) ON CONFLICT(u) DO NOTHING")
	if r.Changes != 2 {
		t.Fatalf("nullable Changes=%d", r.Changes)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = e.openDatabase(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	upsertInt(t, d, "SELECT n FROM t WHERE u='new'", "n", 6)
	upsertExec(t, d, "INSERT INTO t VALUES(7,8,'new',2) ON CONFLICT(a,b) DO UPDATE SET n=n+excluded.n")
	upsertInt(t, d, "SELECT n FROM t WHERE u='new'", "n", 8)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLDuplicateKeyUpdate(t *testing.T) {
	e := newRegressionEnv(t)
	d, err := e.openDatabase(e.path("mysql-upsert"), nil)
	if err != nil {
		t.Fatal(err)
	}
	upsertExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY, code TEXT UNIQUE, n INTEGER)")
	upsertExec(t, d, "INSERT INTO t VALUES(1,'a',4)")
	r := upsertExec(t, d, "INSERT INTO t VALUES(9,'a',3) ON DUPLICATE KEY UPDATE n=n+VALUES(n), id=VALUES(id) RETURNING id,n")
	if r.Changes != 1 || len(r.Rows) != 1 || r.Rows[0]["id"].Int != 9 || r.Rows[0]["n"].Int != 7 {
		t.Fatalf("upsert result=%+v", r)
	}
	upsertExec(t, d, "INSERT INTO t VALUES(2,'b',5) ON DUPLICATE KEY UPDATE n=VALUES(n)")
	upsertInt(t, d, "SELECT n FROM t WHERE code='b'", "n", 5)
}

func TestUpsertAtomicConstraintsAndTransactions(t *testing.T) {
	e := newRegressionEnv(t)
	d, err := e.openDatabase(e.path("upsert_atomic"), nil)
	if err != nil {
		t.Fatal(err)
	}
	upsertExec(t, d, "CREATE TABLE parent(id INTEGER PRIMARY KEY)")
	upsertExec(t, d, "INSERT INTO parent VALUES(1)")
	upsertExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,u TEXT UNIQUE,n INTEGER NOT NULL CHECK(n>0),p INTEGER REFERENCES parent(id))")
	upsertExec(t, d, "INSERT INTO t VALUES(1,'x',10,1),(2,'y',20,1)")
	for _, tc := range []struct{ query, fragment string }{
		{"INSERT INTO t VALUES(1,'x',NULL,1) ON CONFLICT DO NOTHING", "NOT NULL"},
		{"INSERT INTO t VALUES(1,'x',0,1) ON CONFLICT DO NOTHING", "CHECK"},
		{"INSERT INTO t VALUES(1,'x','bad',1) ON CONFLICT DO NOTHING", "datatype"},
		{"INSERT INTO t VALUES(NULL,'x',1,1) ON CONFLICT DO NOTHING", "NOT NULL"},
		{"INSERT INTO t VALUES(3,'z',1,9) ON CONFLICT DO NOTHING", "FOREIGN KEY"},
		{"INSERT INTO t VALUES(1,'x',1,1) ON CONFLICT(id) DO UPDATE SET u='y'", "UNIQUE"},
		{"INSERT INTO t VALUES(1,'x',1,1) ON CONFLICT(id) DO UPDATE SET p=9", "FOREIGN KEY"},
		{"INSERT INTO t VALUES(1,'x',1,1) ON CONFLICT(id) DO UPDATE SET n=0", "CHECK"},
		{"INSERT INTO t VALUES(1,'x',1,1) ON CONFLICT(id) DO UPDATE SET n=NULL", "NOT NULL"},
		{"INSERT INTO t VALUES(3,'z',1,1),(1,'x',1,1) ON CONFLICT(id) DO UPDATE SET u='y'", "UNIQUE"},
	} {
		upsertError(t, d, tc.query, tc.fragment)
	}
	upsertInt(t, d, "SELECT COUNT(*) AS n FROM t", "n", 2)
	upsertInt(t, d, "SELECT n FROM t WHERE id=1", "n", 10)
	tx, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	upsertExec(t, tx, "SAVEPOINT s")
	upsertExec(t, tx, "INSERT INTO t VALUES(1,'x',1,1) ON CONFLICT(id) DO UPDATE SET n=n+excluded.n")
	upsertInt(t, tx, "SELECT n FROM t WHERE id=1", "n", 11)
	upsertExec(t, tx, "ROLLBACK TO s")
	upsertInt(t, tx, "SELECT n FROM t WHERE id=1", "n", 10)
	upsertError(t, tx, "INSERT INTO t VALUES(3,'z',1,1),(1,'x',1,1) ON CONFLICT(id) DO UPDATE SET n=0", "CHECK")
	upsertInt(t, tx, "SELECT COUNT(*) AS n FROM t", "n", 2)
	upsertExec(t, tx, "INSERT INTO t VALUES(1,'x',1,1) ON CONFLICT(id) DO UPDATE SET n=n+excluded.n")
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	upsertInt(t, d, "SELECT n FROM t WHERE id=1", "n", 11)
}

func TestUpsertPreparedAndConcurrent(t *testing.T) {
	e := newRegressionEnv(t)
	d, err := e.openDatabase(e.path("upsert_prepared"), nil)
	if err != nil {
		t.Fatal(err)
	}
	upsertExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,n INTEGER)")
	q, err := d.Prepare("INSERT INTO t VALUES(?,?) ON CONFLICT(id) DO UPDATE SET n=t.n+excluded.n+? WHERE excluded.n>=? RETURNING n")
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if q.NumInput() != 4 {
		t.Fatalf("NumInput=%d", q.NumInput())
	}
	r, err := q.Exec(1, 10, 100, 0)
	if err != nil || r.Rows[0]["n"].Int != 10 {
		t.Fatalf("first %v %v", r, err)
	}
	r, err = q.Exec(1, 1, 2, 0)
	if err != nil || r.Rows[0]["n"].Int != 13 {
		t.Fatalf("second %v %v", r, err)
	}
	r, err = q.Exec(1, 1, 100, 2)
	if err != nil || r.Changes != 0 || len(r.Rows) != 0 {
		t.Fatalf("conditional %v %v", r, err)
	}
	increment, err := d.Prepare("INSERT INTO t VALUES(?,1) ON CONFLICT(id) DO UPDATE SET n=n+excluded.n")
	if err != nil {
		t.Fatal(err)
	}
	defer increment.Close()
	var wg sync.WaitGroup
	errors := make(chan error, 4)
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := d.NewSession()
			if err != nil {
				errors <- err
				return
			}
			defer s.Close()
			for i := 0; i < 25; i++ {
				if _, err := s.ExecPrepared(increment, 2); err != nil {
					errors <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	upsertInt(t, d, "SELECT n FROM t WHERE id=2", "n", 100)
	upsertInt(t, d, "SELECT n FROM t WHERE id=1", "n", 13)
	tx, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecPrepared(increment, 2); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	upsertInt(t, d, "SELECT n FROM t WHERE id=2", "n", 100)
}

func TestUpsertTriggersAndGeneratedIDs(t *testing.T) {
	e := newRegressionEnv(t)
	d, err := e.openDatabase(e.path("upsert_triggers"), nil)
	if err != nil {
		t.Fatal(err)
	}
	upsertExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT,u TEXT UNIQUE,n INTEGER DEFAULT 7)")
	upsertExec(t, d, "CREATE TABLE events(id INTEGER PRIMARY KEY AUTOINCREMENT,event TEXT)")
	for _, spec := range []struct{ timing, event, label string }{{"BEFORE", "INSERT", "bi"}, {"AFTER", "INSERT", "ai"}, {"BEFORE", "UPDATE", "bu"}, {"AFTER", "UPDATE", "au"}} {
		upsertExec(t, d, fmt.Sprintf("CREATE TRIGGER %s %s %s ON t BEGIN INSERT INTO events(event) VALUES('%s'); END", spec.label, spec.timing, spec.event, spec.label))
	}
	r := upsertExec(t, d, "INSERT INTO t(u) VALUES('x') ON CONFLICT(u) DO UPDATE SET n=excluded.n RETURNING id,n")
	if r.Changes != 1 || r.LastID <= 0 || r.Rows[0]["n"].Int != 7 {
		t.Fatalf("generated=%+v", r)
	}
	id := r.Rows[0]["id"].Int
	upsertExec(t, d, "INSERT INTO t(u,n) VALUES('x',9) ON CONFLICT(u) DO UPDATE SET n=excluded.n")
	upsertExec(t, d, "INSERT INTO t(u) VALUES('x') ON CONFLICT(u) DO NOTHING")
	upsertInt(t, d, "SELECT COUNT(*) AS n FROM events WHERE event='bi'", "n", 3)
	upsertInt(t, d, "SELECT COUNT(*) AS n FROM events WHERE event='ai'", "n", 1)
	upsertInt(t, d, "SELECT COUNT(*) AS n FROM events WHERE event='bu'", "n", 1)
	upsertInt(t, d, "SELECT COUNT(*) AS n FROM events WHERE event='au'", "n", 1)
	upsertInt(t, d, "SELECT id FROM t WHERE u='x'", "id", id)
	upsertInt(t, d, "SELECT n FROM t WHERE u='x'", "n", 9)
	r = upsertExec(t, d, "INSERT INTO t(u) VALUES('y') ON CONFLICT DO NOTHING RETURNING id")
	if r.LastID <= id || r.Rows[0]["id"].Int != r.LastID {
		t.Fatalf("next generated=%+v", r)
	}
	upsertError(t, d, "INSERT INTO t VALUES(NULL,'x',9) ON CONFLICT(u) DO NOTHING", "NOT NULL")
}

func TestUpsertUniqueIndexesAndCorrelatedExpressions(t *testing.T) {
	e := newRegressionEnv(t)
	d, err := e.openDatabase(e.path("upsert_index"), nil)
	if err != nil {
		t.Fatal(err)
	}
	upsertExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,a INTEGER,b TEXT,n INTEGER)")
	upsertExec(t, d, "CREATE UNIQUE INDEX ab ON t(a,b)")
	upsertExec(t, d, "INSERT INTO t VALUES(1,2,'x',10)")
	r := upsertExec(t, d, "INSERT INTO t VALUES(9,2,'x',3) ON CONFLICT(b,a) DO UPDATE SET n=(SELECT excluded.n)+n WHERE (SELECT excluded.n)>0 RETURNING id,n")
	if r.Changes != 1 || r.Rows[0]["id"].Int != 1 || r.Rows[0]["n"].Int != 13 {
		t.Fatalf("correlated=%+v", r)
	}
	upsertError(t, d, "INSERT INTO t VALUES(10,3,'y',1) ON CONFLICT(a,b) DO UPDATE SET n=excluded.missing", "no such column")
	q, err := d.Prepare("INSERT INTO t VALUES(?,?,?,1) ON CONFLICT(a,b) DO UPDATE SET n=n+excluded.n")
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	var wg sync.WaitGroup
	errors := make(chan error, 4)
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			s, err := d.NewSession()
			if err != nil {
				errors <- err
				return
			}
			defer s.Close()
			for i := 0; i < 25; i++ {
				if _, err := s.ExecPrepared(q, 100+worker*25+i, 2, "x"); err != nil {
					errors <- err
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	upsertInt(t, d, "SELECT n FROM t WHERE a=2 AND b='x'", "n", 113)
	upsertInt(t, d, "SELECT COUNT(*) AS n FROM t", "n", 1)
	upsertExec(t, d, "DROP INDEX ab")
	upsertError(t, d, "INSERT INTO t VALUES(11,2,'x',1) ON CONFLICT(a,b) DO NOTHING", "does not match")
	upsertExec(t, d, "CREATE UNIQUE INDEX ab ON t(a,b)")
	r = upsertExec(t, d, "INSERT INTO t VALUES(11,2,'x',1) ON CONFLICT(a,b) DO UPDATE SET a=3,b='y' RETURNING a,b")
	if r.Changes != 1 || r.Rows[0]["a"].Int != 3 {
		t.Fatalf("indexed change=%+v", r)
	}
	upsertInt(t, d, "SELECT COUNT(*) AS n FROM t WHERE a=2 AND b='x'", "n", 0)
	upsertInt(t, d, "SELECT n FROM t WHERE a=3 AND b='y'", "n", 113)
}

func TestUpsertSkippedAutoincrementSurvivesReopen(t *testing.T) {
	e := newRegressionEnv(t)
	dir := e.path("upsert_autoincrement")
	d, err := e.openDatabase(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	upsertExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT,u TEXT UNIQUE)")
	upsertInt(t, d, "INSERT INTO t(u) VALUES('x') RETURNING id", "id", 1)
	r := upsertExec(t, d, "INSERT INTO t(u) VALUES('x'),('x') ON CONFLICT DO NOTHING RETURNING id")
	if r.Changes != 0 || r.LastID != 0 || len(r.Rows) != 0 {
		t.Fatalf("skip=%+v", r)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = e.openDatabase(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	upsertInt(t, d, "INSERT INTO t(u) VALUES('y') RETURNING id", "id", 4)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestUpsertValuesObserveOriginalRowAndRollbackTriggers(t *testing.T) {
	e := newRegressionEnv(t)
	d, err := e.openDatabase(e.path("upsert_original"), nil)
	if err != nil {
		t.Fatal(err)
	}
	upsertExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,a INTEGER CHECK(a>0),b INTEGER)")
	upsertExec(t, d, "CREATE TABLE audit(id INTEGER PRIMARY KEY AUTOINCREMENT,n INTEGER)")
	upsertExec(t, d, "INSERT INTO t VALUES(1,10,20)")
	upsertExec(t, d, "CREATE TRIGGER bu BEFORE UPDATE ON t BEGIN INSERT INTO audit(n) VALUES(old.a); END")
	r := upsertExec(t, d, "INSERT INTO t VALUES(1,1,1) ON CONFLICT(id) DO UPDATE SET a=b,b=a RETURNING a,b")
	if r.Changes != 1 || r.Rows[0]["a"].Int != 20 || r.Rows[0]["b"].Int != 10 {
		t.Fatalf("simultaneous assignments=%+v", r)
	}
	upsertError(t, d, "INSERT INTO t VALUES(2,5,5),(1,1,1) ON CONFLICT DO UPDATE SET a=0", "CHECK")
	upsertInt(t, d, "SELECT COUNT(*) AS n FROM t", "n", 1)
	upsertInt(t, d, "SELECT COUNT(*) AS n FROM audit", "n", 1)
	upsertInt(t, d, "SELECT a FROM t WHERE id=1", "a", 20)
	upsertExec(t, d, "CREATE TABLE defaults(id INTEGER PRIMARY KEY AUTOINCREMENT,u TEXT DEFAULT 'x' UNIQUE,n INTEGER DEFAULT 3)")
	upsertInt(t, d, "INSERT INTO defaults DEFAULT VALUES ON CONFLICT(u) DO UPDATE SET n=n+excluded.n RETURNING n", "n", 3)
	upsertInt(t, d, "INSERT INTO defaults DEFAULT VALUES ON CONFLICT(u) DO UPDATE SET n=n+excluded.n RETURNING n", "n", 6)
	// Repeated input rows resolve conflicts against this same statement's writes.
	r = upsertExec(t, d, "INSERT INTO defaults(u,n) VALUES('x',2),('x',4) ON CONFLICT(u) DO UPDATE SET n=n+excluded.n RETURNING n")
	if r.Changes != 2 || len(r.Rows) != 2 || r.Rows[0]["n"].Int != 8 || r.Rows[1]["n"].Int != 12 {
		t.Fatalf("private overlay=%+v", r)
	}
}
