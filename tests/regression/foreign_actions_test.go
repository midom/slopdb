package regression

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"slopdb/db"
)

func foreignCount(t *testing.T, d *db.Database, q string, want int64) {
	t.Helper()
	r := alterExec(t, d, q)
	if len(r.Rows) != 1 || r.Rows[0][r.Columns[0].Name].Int != want {
		t.Fatalf("%s: %v want %d", q, r.Rows, want)
	}
}

func TestForeignActionsCompositePersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	d, e := db.Open(path, nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { d.Close() })
	alterExec(t, d, `CREATE TABLE p(a INTEGER,b TEXT, PRIMARY KEY(a,b));
 CREATE TABLE c(id INTEGER PRIMARY KEY,a INTEGER,b TEXT, CONSTRAINT cp FOREIGN KEY(a,b) REFERENCES p(a,b) ON UPDATE CASCADE ON DELETE CASCADE);
 CREATE INDEX cab ON c(a,b);
 CREATE TABLE leaf(id INTEGER PRIMARY KEY, cid INTEGER REFERENCES c(id) ON DELETE CASCADE ON UPDATE CASCADE);
 INSERT INTO p VALUES(1,'one'),(2,'two'); INSERT INTO c VALUES(10,1,'one'),(20,2,'two'); INSERT INTO leaf VALUES(100,10),(200,20);
 UPDATE p SET a=3,b='three' WHERE a=1`)
	foreignCount(t, d, "SELECT COUNT(*) FROM c WHERE a=3 AND b='three'", 1)
	alterExec(t, d, "UPDATE c SET id=30 WHERE id=10")
	foreignCount(t, d, "SELECT cid FROM leaf WHERE id=100", 30)
	alterExec(t, d, "BEGIN; DELETE FROM p WHERE a=3")
	foreignCount(t, d, "SELECT COUNT(*) FROM leaf", 1)
	alterExec(t, d, "ROLLBACK")
	foreignCount(t, d, "SELECT COUNT(*) FROM leaf", 2)
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
	d, e = db.Open(path, nil)
	if e != nil {
		t.Fatal(e)
	}
	alterExec(t, d, "DELETE FROM p WHERE a=3")
	foreignCount(t, d, "SELECT COUNT(*) FROM c WHERE a=3", 0)
	foreignCount(t, d, "SELECT COUNT(*) FROM leaf", 1)
	r := alterExec(t, d, "SELECT sql FROM sqlite_master WHERE name='c'")
	if !strings.Contains(r.Rows[0]["sql"].Text, "ON UPDATE CASCADE") {
		t.Fatal(r.Rows)
	}
	if _, e = d.CheckIntegrity(); e != nil {
		t.Fatal(e)
	}
}

func TestForeignActionsNullDefaultUniqueAndFailure(t *testing.T) {
	d, e := db.Open(filepath.Join(t.TempDir(), "db"), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	alterExec(t, d, `CREATE TABLE p(id INTEGER PRIMARY KEY,k INTEGER UNIQUE); INSERT INTO p VALUES(0,0),(1,11),(2,22);
 CREATE TABLE n(id INTEGER PRIMARY KEY,p INTEGER REFERENCES p(k) ON UPDATE CASCADE ON DELETE SET NULL);
 CREATE TABLE f(id INTEGER PRIMARY KEY,p INTEGER DEFAULT 0 REFERENCES p(id) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT);
 INSERT INTO n VALUES(1,11); INSERT INTO f VALUES(1,1); UPDATE p SET k=12 WHERE id=1`)
	foreignCount(t, d, "SELECT p FROM n", 12)
	alterExec(t, d, "DELETE FROM p WHERE id=1")
	foreignCount(t, d, "SELECT COUNT(*) FROM n WHERE p IS NULL", 1)
	foreignCount(t, d, "SELECT p FROM f", 0)
	alterBad(t, d, "DELETE FROM p WHERE id=0")
	foreignCount(t, d, "SELECT COUNT(*) FROM p WHERE id=0", 1)
	alterBad(t, d, "CREATE TABLE bad(p INTEGER NOT NULL REFERENCES p(id) ON DELETE SET NULL)")
	alterExec(t, d, "CREATE TABLE afterref(p INTEGER REFERENCES p(id) NOT NULL)")
	alterExec(t, d, "CREATE TABLE strictref(p INTEGER REFERENCES p(id) NOT DEFERRABLE INITIALLY IMMEDIATE)")
	alterExec(t, d, "CREATE TABLE restrictive(p INTEGER REFERENCES p(id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED); INSERT INTO restrictive VALUES(2); BEGIN")
	alterBad(t, d, "DELETE FROM p WHERE id=2")
	alterExec(t, d, "ROLLBACK")
	alterExec(t, d, "CREATE TABLE up(id INTEGER PRIMARY KEY,v DECIMAL(10,2), UNIQUE(v))")
	alterBad(t, d, "CREATE TABLE baddecimal(v DECIMAL(10,3) REFERENCES up(v))")
}

func TestForeignActionsSelfCycleReplaceJoined(t *testing.T) {
	d, e := db.Open(filepath.Join(t.TempDir(), "db"), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	alterExec(t, d, `CREATE TABLE tree(id INTEGER PRIMARY KEY,pid INTEGER REFERENCES tree(id) ON DELETE CASCADE ON UPDATE CASCADE);
 INSERT INTO tree VALUES(1,1),(2,1),(3,2); UPDATE tree SET id=4 WHERE id=1`)
	foreignCount(t, d, "SELECT pid FROM tree WHERE id=4", 4)
	foreignCount(t, d, "SELECT pid FROM tree WHERE id=2", 4)
	alterExec(t, d, "DELETE FROM tree WHERE id=4")
	foreignCount(t, d, "SELECT COUNT(*) FROM tree", 0)
	alterExec(t, d, `CREATE TABLE x(id INTEGER PRIMARY KEY,y INTEGER); CREATE TABLE y(id INTEGER PRIMARY KEY,x INTEGER);
 ALTER TABLE x ADD CONSTRAINT xy FOREIGN KEY(y) REFERENCES y(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;
 ALTER TABLE y ADD CONSTRAINT yx FOREIGN KEY(x) REFERENCES x(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;
 BEGIN; INSERT INTO x VALUES(1,1); INSERT INTO y VALUES(1,1); COMMIT; DELETE FROM x WHERE id=1`)
	foreignCount(t, d, "SELECT COUNT(*) FROM y", 0)
	alterExec(t, d, `CREATE TABLE p(id INTEGER PRIMARY KEY,v TEXT); CREATE TABLE c(id INTEGER PRIMARY KEY,p INTEGER REFERENCES p(id) ON DELETE CASCADE);
 INSERT INTO p VALUES(1,'a'); INSERT INTO c VALUES(10,1); REPLACE INTO p VALUES(1,'b')`)
	foreignCount(t, d, "SELECT COUNT(*) FROM c", 0)
	alterExec(t, d, "INSERT INTO c VALUES(10,1); DELETE p FROM p JOIN c ON c.p=p.id WHERE c.id=10")
	foreignCount(t, d, "SELECT COUNT(*) FROM c", 0)
}

func TestForeignDeferredCommitModesSavepoints(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	d, e := db.Open(path, nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { d.Close() })
	alterExec(t, d, `CREATE SCHEMA tenant; CREATE TABLE tenant.p(id INTEGER PRIMARY KEY);
 CREATE TABLE tenant.c(id INTEGER PRIMARY KEY,p INTEGER,CONSTRAINT cp FOREIGN KEY(p) REFERENCES tenant.p(id) DEFERRABLE INITIALLY DEFERRED);
 BEGIN; INSERT INTO tenant.c VALUES(1,100)`)
	alterBad(t, d, "COMMIT")
	alterExec(t, d, "INSERT INTO tenant.p VALUES(100); COMMIT")
	alterExec(t, d, "BEGIN; INSERT INTO tenant.c VALUES(2,200)")
	alterBad(t, d, "SET CONSTRAINTS tenant.cp IMMEDIATE")
	alterExec(t, d, "SAVEPOINT s; SET CONSTRAINTS ALL DEFERRED; INSERT INTO tenant.p VALUES(200); SET CONSTRAINTS tenant.cp IMMEDIATE")
	alterBad(t, d, "INSERT INTO tenant.c VALUES(3,300)")
	alterExec(t, d, "ROLLBACK TO s; INSERT INTO tenant.p VALUES(200); INSERT INTO tenant.c VALUES(3,300); INSERT INTO tenant.p VALUES(300); COMMIT")
	alterExec(t, d, "BEGIN; DELETE FROM tenant.p WHERE id=100; DELETE FROM tenant.c WHERE id=1; COMMIT")
	alterExec(t, d, "BEGIN")
	alterBad(t, d, "SET CONSTRAINTS missing DEFERRED")
	alterExec(t, d, "ROLLBACK")
	alterBad(t, d, "SET CONSTRAINTS ALL DEFERRED")
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
	d, e = db.Open(path, nil)
	if e != nil {
		t.Fatal(e)
	}
	alterExec(t, d, "BEGIN; INSERT INTO tenant.c VALUES(4,400)")
	alterBad(t, d, "COMMIT")
	alterExec(t, d, "ROLLBACK")
	foreignCount(t, d, "SELECT COUNT(*) FROM tenant.c", 2)
	// A dedicated Tx remains usable after deferred commit validation fails.
	tx, e := d.Begin()
	if e != nil {
		t.Fatal(e)
	}
	if _, e = tx.Exec("INSERT INTO tenant.c VALUES(5,500)"); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(); e == nil {
		t.Fatal("orphan committed")
	}
	if _, e = tx.Exec("INSERT INTO tenant.p VALUES(500)"); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
}

func TestForeignCascadeConcurrentLocking(t *testing.T) {
	d, e := db.Open(filepath.Join(t.TempDir(), "db"), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	alterExec(t, d, "CREATE TABLE p(id INTEGER PRIMARY KEY); CREATE TABLE c(id INTEGER PRIMARY KEY,p INTEGER REFERENCES p(id) ON DELETE CASCADE); INSERT INTO p VALUES(1); INSERT INTO c VALUES(1,1)")
	a, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	b, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	if _, e = a.Exec("BEGIN; DELETE FROM p WHERE id=1"); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, e = b.ExecContext(ctx, "INSERT INTO c VALUES(2,1)"); e == nil {
		t.Fatal("concurrent orphan accepted")
	}
	if _, e = a.Exec("COMMIT"); e != nil {
		t.Fatal(e)
	}
	if _, e = b.Exec("INSERT INTO c VALUES(2,1)"); e == nil {
		t.Fatal("missing parent accepted")
	}
	foreignCount(t, d, "SELECT COUNT(*) FROM c", 0)
}
