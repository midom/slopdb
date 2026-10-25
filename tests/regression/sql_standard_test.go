// Adapted from sql_standard_review.go; see TESTING.md.
package regression

import (
	"fmt"
	"os"
	"testing"

	"slopdb/db"
	"slopdb/storage"
)

func TestSQLSemantics(testT *testing.T) {
	testState := newRegressionEnv(testT)
	dir := testState.path("slopdb_standard_review")
	os.RemoveAll(dir)
	o := storage.DefaultOptions()
	o.DisableWALSync = true
	d, e := testState.openDatabase(dir, o)
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
	defer d.Close()
	n := 0
	ok := func(q string) *db.ExecResult {
		r, e := d.Exec(q)
		if e != nil {
			testState.t.Fatalf("%v", q+": "+e.Error())
		}
		n++
		return r
	}
	bad := func(q string) {
		_, e := d.Exec(q)
		if e == nil {
			testState.t.Fatalf("%v", "accepted: "+q)
		}
		n++
	}
	count := func(q string, want int64) {
		r := ok(q)
		if len(r.Rows) != 1 {
			testState.t.Fatalf("%v", q)
		}
		for _, v := range r.Rows[0] {
			if v.Int != want || v.Null {
				testState.t.Fatalf("%v", fmt.Sprintf("%s: got %v want %d", q, r.Rows, want))
			}
		}
	}
	ok("CREATE TABLE p (id INTEGER PRIMARY KEY, v TEXT)")
	for _, q := range []string{"INSERT INTO p VALUES(NULL,'x')", "INSERT INTO p(v) VALUES('x')", "INSERT INTO p VALUES(1,'good'),(NULL,'bad')", "INSERT INTO p SELECT NULL,'bad'"} {
		bad(q)
	}
	count("SELECT COUNT(*) FROM p", 0)
	ok("INSERT INTO p VALUES(1,'one'),(2,'two')")
	bad("UPDATE p SET id=NULL WHERE id=1")
	count("SELECT COUNT(*) FROM p", 2)
	ok("CREATE TABLE cp(a INTEGER,b TEXT, PRIMARY KEY(a,b))")
	bad("INSERT INTO cp VALUES(1,NULL)")
	bad("INSERT INTO cp VALUES(NULL,'a')")
	ok("INSERT INTO cp VALUES(1,'a')")
	bad("UPDATE cp SET b=NULL WHERE a=1")
	ok("CREATE TABLE auto(id INTEGER PRIMARY KEY AUTOINCREMENT,v INTEGER)")
	ok("INSERT INTO auto(v) VALUES(7)")
	bad("INSERT INTO auto VALUES(NULL,8)")
	count("SELECT id FROM auto", 1)
	for _, q := range []string{"CREATE TABLE bad1(a INTEGER PRIMARY KEY,b INTEGER PRIMARY KEY)", "CREATE TABLE bad2(a INTEGER PRIMARY KEY,PRIMARY KEY(a))", "CREATE TABLE bad3(a INTEGER,PRIMARY KEY(a),PRIMARY KEY(a))", "CREATE TABLE bad4(a INTEGER,PRIMARY KEY(a,a))", "CREATE TABLE bad5(a INTEGER AUTOINCREMENT)", "CREATE TABLE bad6(a TEXT COLLATE NOCASE)", "CREATE TABLE bad7(a INTEGER UNIQUE ON CONFLICT IGNORE)"} {
		bad(q)
	}
	ok("CREATE TABLE named(a INTEGER,CONSTRAINT pk PRIMARY KEY(a))")
	bad("INSERT INTO named VALUES(NULL)")
	ok("CREATE TABLE c(id INTEGER PRIMARY KEY,pid INTEGER REFERENCES p(id))")
	ok("INSERT INTO c VALUES(1,1),(2,NULL)")
	bad("INSERT INTO c VALUES(3,999)")
	bad("UPDATE c SET pid=999 WHERE id=1")
	bad("DELETE FROM p WHERE id=1")
	bad("UPDATE p SET id=10 WHERE id=1")
	count("SELECT COUNT(*) FROM p", 2)
	bad("DROP TABLE p")
	ok("BEGIN")
	ok("ALTER TABLE p RENAME COLUMN id TO parent_id")
	ok("ROLLBACK")
	ok("INSERT INTO c VALUES(4,2)")
	bad("INSERT INTO c VALUES(5,999)")
	ok("ALTER TABLE p RENAME TO parents")
	ok("ALTER TABLE parents RENAME COLUMN id TO parent_id")
	ok("INSERT INTO c VALUES(5,2)")
	bad("DELETE FROM parents WHERE parent_id=2")
	ok("CREATE TABLE self(id INTEGER PRIMARY KEY,parent INTEGER REFERENCES self(id))")
	ok("INSERT INTO self VALUES(1,2),(2,NULL)")
	bad("DELETE FROM self WHERE id=2")
	ok("DELETE FROM self")
	ok("CREATE TABLE uq(id INTEGER PRIMARY KEY,k INTEGER UNIQUE)")
	ok("CREATE TABLE uc(id INTEGER PRIMARY KEY,k INTEGER REFERENCES uq(k))")
	ok("INSERT INTO uq VALUES(1,7),(2,NULL),(3,NULL)")
	ok("INSERT INTO uc VALUES(1,7)")
	bad("UPDATE uq SET k=8 WHERE id=1")
	ok("CREATE TABLE fc(a INTEGER,b TEXT, FOREIGN KEY(a,b) REFERENCES cp(a,b))")
	ok("INSERT INTO fc VALUES(1,'a'),(1,NULL)")
	bad("INSERT INTO fc VALUES(1,'b')")
	bad("DELETE FROM cp WHERE a=1")
	bad("CREATE TABLE invalidfk(a INTEGER REFERENCES parents(v))")
	bad("SELECT (SELECT parent_id FROM parents)")
	count("SELECT (SELECT parent_id FROM parents WHERE parent_id=1)", 1)
	for _, q := range []string{"SELECT 1/0", "SELECT 2%0", "SELECT 9223372036854775807+1", "SELECT 9223372036854775807*2", "SELECT parent_id,COUNT(*) FROM parents", "SELECT v FROM parents GROUP BY parent_id+1", "SELECT COUNT() FROM parents", "SELECT COUNT(DISTINCT *) FROM parents", "SELECT SUM(COUNT(*)) FROM parents", "SELECT v FROM parents WHERE COUNT(*)>0", "SELECT SUM(v) FROM parents"} {
		bad(q)
	}
	count("SELECT COUNT(DISTINCT k) FROM uq", 1)
	count("SELECT SUM(DISTINCT k) FROM uq", 7)
	count("SELECT CASE WHEN COUNT(*)>1 THEN COUNT(*) ELSE 0 END FROM parents", 2)
	count("SELECT TRUE", 1)
	ok("SELECT parent_id,v FROM parents GROUP BY parent_id ORDER BY SUM(parent_id) DESC")
	ok("CREATE TABLE typed(id INTEGER PRIMARY KEY,v INTEGER)")
	bad("INSERT INTO typed VALUES(1,'bad')")
	ok("INSERT INTO typed VALUES(1,1)")
	bad("UPDATE typed SET v='bad' WHERE id=1")
	count("SELECT v FROM typed", 1)
	ok("CREATE TABLE ck(v INTEGER CHECK(v>0))")
	ok("INSERT INTO ck VALUES(NULL)")
	bad("INSERT INTO ck VALUES(0)")
	for _, q := range []string{"SELECT CAST('bad' AS INTEGER)", "SELECT CAST('NaN' AS REAL)", "SELECT CAST(1e40 AS INTEGER)", "SELECT CAST(1 AS UNKNOWN)", "SELECT 1e308*1e308", "SELECT LENGTH(*)", "SELECT mystery(v) FROM typed WHERE id=999", "SELECT COALESCE() FROM typed WHERE id=999", "SELECT SUM(v) AS total FROM typed GROUP BY total", "SELECT v,COUNT(*) FROM typed GROUP BY 0", "CREATE TABLE badcheck(v INTEGER CHECK(missing>0))", "CREATE TABLE baddefault(v INTEGER DEFAULT other)", "CREATE TABLE badsubcheck(v INTEGER CHECK(v IN(SELECT parent_id FROM parents)))"} {
		bad(q)
	}
	count("SELECT CAST('42' AS INTEGER)", 42)
	count("SELECT CAST(-1.5 AS INTEGER)", -1)
	count("SELECT COUNT(*) FROM parents GROUP BY 1+0", 2)
	ok("SELECT v,COUNT(*) FROM typed GROUP BY 1")
	ok("SELECT v+1 AS plus,COUNT(*) FROM typed GROUP BY plus")
	ok("CREATE TABLE reversefk(b TEXT,a INTEGER,FOREIGN KEY(b,a) REFERENCES cp(b,a))")
	ok("INSERT INTO reversefk VALUES('a',1)")
	bad("INSERT INTO reversefk VALUES('no',1)")
	ok("CREATE TABLE empty(id INTEGER)")
	ok("ALTER TABLE empty ADD COLUMN v INTEGER NOT NULL")
	bad("INSERT INTO empty(id) VALUES(1)")
	bad("ALTER TABLE empty ADD COLUMN wrong INTEGER CHECK(missing>0)")
	ok("CREATE TABLE actions(id INTEGER PRIMARY KEY,pid INTEGER REFERENCES parents ON DELETE RESTRICT ON UPDATE NO ACTION)")
	ok("INSERT INTO actions VALUES(1,2)")
	bad("DELETE FROM parents WHERE parent_id=2")
	ok("CREATE TABLE tableactions(pid INTEGER,FOREIGN KEY(pid) REFERENCES parents ON DELETE NO ACTION)")
	ok("INSERT INTO tableactions VALUES(2)")
	bad("INSERT INTO tableactions VALUES(999)")
	ok("CREATE TABLE cascadeok(pid INTEGER REFERENCES parents ON DELETE CASCADE)")
	for _, q := range []string{"UPDATE empty SET id=missing", "UPDATE empty SET id=mystery(id)", "DELETE FROM empty WHERE missing=1", "DELETE FROM empty WHERE COUNT(*)=1", "SELECT CAST(NULL AS UNKNOWN)", "SELECT +('bad')", "SELECT -('bad')", "SELECT ABS('bad')", "SELECT ROUND('bad')"} {
		bad(q)
	}
	ok("CREATE TABLE finite(id INTEGER PRIMARY KEY,v REAL)")
	bad("INSERT INTO finite VALUES(1,'NaN')")
	ok("INSERT INTO finite VALUES(1,1.5)")
	bad("UPDATE finite SET v='Inf' WHERE id=1")
	ok("INSERT INTO finite VALUES(2,1e308),(3,1e308)")
	bad("SELECT SUM(v) FROM finite WHERE id>1")
	bad("SELECT AVG(v) FROM finite WHERE id>1")
	if e := d.Close(); e != nil {
		testState.t.Fatalf("%v", e)
	}
	d, e = testState.openDatabase(dir, o)
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
	bad("INSERT INTO c VALUES(6,999)")
	bad("INSERT INTO parents VALUES(NULL,'bad')")
	testState.t.Logf("PASS %d standard semantics checks\n", n)
}
