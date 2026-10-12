package regression

import (
	"path/filepath"
	"testing"

	"slopdb/db"
)

func alterExec(t *testing.T, d *db.Database, q string) *db.ExecResult {
	t.Helper()
	r, e := d.Exec(q)
	if e != nil {
		t.Fatalf("%s: %v", q, e)
	}
	return r
}
func alterBad(t *testing.T, d *db.Database, q string) {
	t.Helper()
	if _, e := d.Exec(q); e == nil {
		t.Fatalf("expected failure: %s", q)
	}
}

func TestExtendedAlterColumnsConstraintsAndPersistence(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	d, e := db.Open(dir, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	alterExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT,n INTEGER); INSERT INTO t VALUES(1,'10',NULL),(2,'20',2)")
	alterBad(t, d, "ALTER TABLE t ALTER COLUMN n SET NOT NULL")
	alterBad(t, d, "ALTER TABLE t ALTER COLUMN id DROP NOT NULL")
	alterExec(t, d, "ALTER TABLE t ADD COLUMN added INTEGER NOT NULL DEFAULT 7, ADD tail TEXT AFTER v")
	r := alterExec(t, d, "SELECT * FROM t ORDER BY id")
	if len(r.Rows) != 2 || r.Rows[0]["added"].Int != 7 || r.Columns[2].Name != "tail" {
		t.Fatal(r)
	}
	alterExec(t, d, "ALTER TABLE t ALTER COLUMN v TYPE BIGINT USING CAST(v AS BIGINT)+1")
	alterBad(t, d, "ALTER TABLE t ALTER COLUMN v TYPE INTEGER USING missing")
	alterExec(t, d, "ALTER TABLE t ADD CONSTRAINT positive CHECK(v>0), ADD CONSTRAINT uv UNIQUE(v)")
	alterBad(t, d, "INSERT INTO t(id,v) VALUES(3,11)")
	alterBad(t, d, "UPDATE t SET v=-1 WHERE id=1")
	alterExec(t, d, "ALTER TABLE t RENAME CONSTRAINT uv TO amount_unique")
	alterExec(t, d, "ALTER TABLE t DROP CONSTRAINT amount_unique, DROP CONSTRAINT positive")
	alterExec(t, d, "INSERT INTO t(id,v) VALUES(3,11)")
	alterExec(t, d, "ALTER TABLE t ALTER COLUMN added SET DEFAULT 9")
	alterExec(t, d, "ALTER TABLE t MODIFY COLUMN tail TEXT FIRST")
	alterExec(t, d, "ALTER TABLE t CHANGE COLUMN v amount REAL")
	r = alterExec(t, d, "SELECT * FROM t ORDER BY id")
	if r.Columns[0].Name != "tail" || r.Rows[0]["amount"].Real != 11 {
		t.Fatal(r)
	}
	alterExec(t, d, "ALTER TABLE t DROP COLUMN tail, ALTER COLUMN added DROP DEFAULT, ALTER COLUMN added DROP NOT NULL")
	alterExec(t, d, "ALTER TABLE t ADD COLUMN IF NOT EXISTS added TEXT; ALTER TABLE t DROP COLUMN IF EXISTS absent; ALTER TABLE IF EXISTS missing ADD COLUMN x TEXT")
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
	d, e = db.Open(dir, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	alterExec(t, d, "INSERT INTO t(id,amount) VALUES(4,4.5)")
	r = alterExec(t, d, "SELECT amount,added FROM t WHERE id=4")
	if r.Rows[0]["amount"].Real != 4.5 || !r.Rows[0]["added"].Null {
		t.Fatal(r)
	}
	if _, e = d.CheckIntegrity(); e != nil {
		t.Fatal(e)
	}
}

func TestExtendedAlterFailureIsAtomic(t *testing.T) {
	d, e := db.Open(filepath.Join(t.TempDir(), "db"), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	alterExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'bad'),(2,'12')")
	alterBad(t, d, "ALTER TABLE t ADD COLUMN first INTEGER DEFAULT 5, ALTER COLUMN v TYPE INTEGER")
	r := alterExec(t, d, "PRAGMA table_info(t)")
	if len(r.Rows) != 2 {
		t.Fatal("partial multi-action DDL", r)
	}
	r = alterExec(t, d, "SELECT * FROM t ORDER BY id")
	if r.Rows[0]["v"].Text != "bad" || r.Rows[1]["v"].Text != "12" {
		t.Fatal("failed conversion changed rows", r)
	}
	alterExec(t, d, "BEGIN; SAVEPOINT s; ALTER TABLE t ADD COLUMN c INTEGER DEFAULT 1; ALTER TABLE t RENAME TO renamed; ROLLBACK TO s; COMMIT")
	r = alterExec(t, d, "SELECT * FROM t ORDER BY id")
	if len(r.Columns) != 2 {
		t.Fatal(r)
	}
	if _, e = d.CheckIntegrity(); e != nil {
		t.Fatal(e)
	}
}

func TestExtendedAlterForeignKeysAndCascade(t *testing.T) {
	d, e := db.Open(filepath.Join(t.TempDir(), "db"), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	alterExec(t, d, "CREATE TABLE p(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO p VALUES(1,'a'); CREATE TABLE c(id INTEGER PRIMARY KEY,parent INTEGER); INSERT INTO c VALUES(1,1)")
	alterExec(t, d, "ALTER TABLE c ADD CONSTRAINT parent_fk FOREIGN KEY(parent) REFERENCES p(id)")
	alterBad(t, d, "INSERT INTO c VALUES(2,99)")
	alterBad(t, d, "ALTER TABLE p DROP PRIMARY KEY")
	alterBad(t, d, "ALTER TABLE p ALTER COLUMN id TYPE INTEGER USING id+1")
	alterExec(t, d, "ALTER TABLE c DROP CONSTRAINT parent_fk")
	alterExec(t, d, "INSERT INTO c VALUES(2,99)")
	alterBad(t, d, "ALTER TABLE c ADD CONSTRAINT parent_fk FOREIGN KEY(parent) REFERENCES p(id)")
	alterExec(t, d, "DELETE FROM c WHERE id=2; ALTER TABLE c ADD CONSTRAINT parent_fk FOREIGN KEY(parent) REFERENCES p(id)")
	alterExec(t, d, "ALTER TABLE p DROP COLUMN id CASCADE")
	alterExec(t, d, "INSERT INTO c VALUES(3,99)")
	if _, e = d.CheckIntegrity(); e != nil {
		t.Fatal(e)
	}
}

func TestExtendedAlterViewsTriggersAndIndexes(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	d, e := db.Open(dir, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	alterExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'a_b'); CREATE INDEX iv ON t(v); CREATE TABLE audit(v TEXT); CREATE TRIGGER ai AFTER INSERT ON t WHEN NEW.v LIKE 'a!_%' ESCAPE '!' BEGIN INSERT INTO audit VALUES(CASE WHEN NEW.v BETWEEN 'a' AND 'z' THEN UPPER(NEW.v) ELSE 'literal v' END); END")
	alterExec(t, d, "CREATE VIEW vv AS SELECT CASE WHEN v BETWEEN 'a' AND 'z' THEN UPPER(v) ELSE 'literal v' END AS result, CAST(id AS TEXT) AS textid FROM t WHERE EXISTS(SELECT 1 FROM t AS inner_t WHERE inner_t.id=t.id)")
	alterExec(t, d, "ALTER TABLE t RENAME COLUMN v TO value; RENAME TABLE t TO renamed; ALTER INDEX iv RENAME TO value_index; ALTER INDEX value_index REBUILD")
	alterExec(t, d, "INSERT INTO renamed VALUES(2,'a_second')")
	r := alterExec(t, d, "SELECT * FROM vv ORDER BY textid")
	if len(r.Rows) != 2 || r.Rows[0]["result"].Text != "A_B" {
		t.Fatal(r)
	}
	r = alterExec(t, d, "SELECT * FROM audit")
	if len(r.Rows) != 1 || r.Rows[0]["v"].Text != "A_SECOND" {
		t.Fatal(r)
	}
	alterExec(t, d, "ALTER VIEW vv RENAME TO renamed_view; ALTER VIEW renamed_view AS SELECT id,value AS output FROM renamed")
	r = alterExec(t, d, "SELECT output FROM renamed_view ORDER BY id")
	if len(r.Rows) != 2 || r.Rows[1]["output"].Text != "a_second" {
		t.Fatal(r)
	}
	alterExec(t, d, "ALTER VIEW IF EXISTS missing AS SELECT 1; ALTER INDEX IF EXISTS missing RENAME TO unused")
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
	d, e = db.Open(dir, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	alterExec(t, d, "INSERT INTO renamed VALUES(3,'a_third')")
	r = alterExec(t, d, "SELECT COUNT(*) AS n FROM audit")
	if r.Rows[0]["n"].Int != 2 {
		t.Fatal(r)
	}
	if _, e = d.CheckIntegrity(); e != nil {
		t.Fatal(e)
	}
}

func TestExtendedAlterNamedConstraintRoundTrip(t *testing.T) {
	d, e := db.Open(filepath.Join(t.TempDir(), "db"), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	alterExec(t, d, "CREATE TABLE t(id INTEGER CONSTRAINT custom_pk PRIMARY KEY AUTOINCREMENT,v INTEGER CONSTRAINT nn NOT NULL CONSTRAINT uq UNIQUE CONSTRAINT ck CHECK(v>0)); INSERT INTO t(v) VALUES(1)")
	ddl := d.GetCreateTableSQL("t")
	if ddl == "" {
		t.Fatal("missing DDL")
	}
	restored, e := db.Open(filepath.Join(t.TempDir(), "restore"), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer restored.Close()
	alterExec(t, restored, ddl)
	alterExec(t, restored, "ALTER TABLE t DROP CONSTRAINT uq, DROP CONSTRAINT ck, DROP CONSTRAINT nn; INSERT INTO t(v) VALUES(NULL)")
	alterBad(t, restored, "ALTER TABLE t ALTER COLUMN id DROP NOT NULL")
	alterExec(t, restored, "ALTER TABLE t DROP CONSTRAINT custom_pk")
	if _, e = restored.CheckIntegrity(); e != nil {
		t.Fatal(e)
	}
}

func TestExtendedAlterSignedZeroCompositeUnique(t *testing.T) {
	d, e := db.Open(filepath.Join(t.TempDir(), "db"), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	alterExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,a REAL,b TEXT); INSERT INTO t VALUES(1,0.0,'x'); ALTER TABLE t ADD CONSTRAINT ab UNIQUE(a,b)")
	alterBad(t, d, "INSERT INTO t VALUES(2,-0.0,'x')")
	alterExec(t, d, "INSERT INTO t VALUES(3,0.0,NULL),(4,-0.0,NULL)")
	if _, e = d.CheckIntegrity(); e != nil {
		t.Fatal(e)
	}
}

func TestExtendedAlterModifyUniqueAndAlternativeForeignKey(t *testing.T) {
	d, e := db.Open(filepath.Join(t.TempDir(), "db"), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	alterExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'a'),(2,'b'); ALTER TABLE t MODIFY COLUMN v TEXT UNIQUE")
	alterBad(t, d, "INSERT INTO t VALUES(3,'a')")
	if _, e = d.CheckIntegrity(); e != nil {
		t.Fatal(e)
	}
	alterExec(t, d, "CREATE TABLE p(id INTEGER PRIMARY KEY,k INTEGER,v TEXT,CONSTRAINT uq UNIQUE(k)); CREATE UNIQUE INDEX alternative ON p(k); INSERT INTO p VALUES(1,2,'x'); CREATE TABLE c(parent INTEGER REFERENCES p(k)); INSERT INTO c VALUES(2)")
	alterExec(t, d, "ALTER TABLE p DROP CONSTRAINT uq CASCADE; ALTER TABLE p DROP COLUMN v CASCADE")
	alterBad(t, d, "INSERT INTO c VALUES(99)")
	alterExec(t, d, "CREATE VIEW named(a,b) AS SELECT id,k FROM p; ALTER VIEW named AS SELECT k AS value FROM p")
	r := alterExec(t, d, "SELECT value FROM named")
	if len(r.Rows) != 1 || r.Rows[0]["value"].Int != 2 {
		t.Fatal(r)
	}
	if _, e = d.CheckIntegrity(); e != nil {
		t.Fatal(e)
	}
}

func TestExtendedAlterNestedDependencies(t *testing.T) {
	for _, query := range []string{
		"SELECT (SELECT v FROM t WHERE id=1) AS value",
		"SELECT 0 AS value UNION ALL SELECT v FROM t",
		"SELECT id AS value FROM other WHERE EXISTS(SELECT 1 FROM t WHERE v=7)",
		"WITH source AS (SELECT v FROM t) SELECT v FROM source",
		"SELECT other.id FROM other JOIN t ON other.id=t.v",
	} {
		t.Run(query, func(t *testing.T) {
			d, err := db.Open(t.TempDir(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			alterExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER); CREATE TABLE other(id INTEGER); INSERT INTO t VALUES(1,7); INSERT INTO other VALUES(7)")
			alterExec(t, d, "CREATE VIEW dependent AS "+query)
			alterExec(t, d, "CREATE VIEW chained AS SELECT COUNT(*) AS n FROM dependent")
			alterExec(t, d, "CREATE VIEW independent AS SELECT id FROM t")
			alterBad(t, d, "ALTER TABLE t DROP COLUMN v")
			alterExec(t, d, "ALTER TABLE t DROP COLUMN v CASCADE")
			alterBad(t, d, "SELECT * FROM dependent")
			alterBad(t, d, "SELECT * FROM chained")
			alterExec(t, d, "SELECT id FROM independent")
			if _, err = d.CheckIntegrity(); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, body := range []string{
		"UPDATE t SET v=9 WHERE id=1",
		"INSERT INTO t(id,v) VALUES(2,9)",
		"INSERT INTO t VALUES(2,9)",
		"UPDATE other SET id=(SELECT v FROM t WHERE id=1)",
	} {
		t.Run(body, func(t *testing.T) {
			d, err := db.Open(t.TempDir(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			alterExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER); CREATE TABLE other(id INTEGER)")
			alterExec(t, d, "CREATE TRIGGER dependent AFTER INSERT ON other BEGIN "+body+"; END")
			alterBad(t, d, "ALTER TABLE t DROP COLUMN v")
			alterExec(t, d, "ALTER TABLE t DROP COLUMN v CASCADE")
			r := alterExec(t, d, "SELECT name FROM sqlite_master WHERE type='trigger'")
			if len(r.Rows) != 0 {
				t.Fatal("dependent trigger survived", r)
			}
			alterExec(t, d, "INSERT INTO other VALUES(1)")
		})
	}
}

func TestExtendedAlterIndexActionsAndModifyDefinition(t *testing.T) {
	d, err := db.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	alterExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER CONSTRAINT nn NOT NULL CONSTRAINT ck CHECK(v>0)); INSERT INTO t VALUES(1,1)")
	alterExec(t, d, "ALTER TABLE t ADD INDEX iv(v), ADD UNIQUE KEY uiv(v), RENAME INDEX iv TO lookup")
	alterBad(t, d, "INSERT INTO t VALUES(2,1)")
	alterExec(t, d, "ALTER TABLE t DROP KEY uiv, DROP INDEX lookup; ALTER TABLE t MODIFY v INTEGER")
	alterExec(t, d, "INSERT INTO t VALUES(2,-1),(3,NULL)")
	ddl := d.GetCreateTableSQL("t")
	restored, err := db.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	alterExec(t, restored, ddl)
	alterExec(t, restored, "INSERT INTO t VALUES(2,-1),(3,NULL)")
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
}

func TestExtendedAlterRenameCrashAtomicity(t *testing.T) {
	for _, mode := range []string{"rename-committed", "rename-uncommitted"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			runCrashChild(t, mode, dir)
			d, err := db.Open(dir, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			if mode == "rename-committed" {
				alterExec(t, d, "SELECT value FROM renamed WHERE value='one'")
				alterBad(t, d, "SELECT * FROM original")
				alterExec(t, d, "ALTER INDEX renamed_lookup REBUILD")
			} else {
				alterExec(t, d, "SELECT v FROM original WHERE v='one'")
				alterBad(t, d, "SELECT * FROM renamed")
				alterExec(t, d, "ALTER INDEX lookup REBUILD")
			}
			r := alterExec(t, d, "SELECT v FROM visible")
			if len(r.Rows) != 1 || r.Rows[0]["v"].Text != "one" {
				t.Fatal(r)
			}
			if _, err = d.CheckIntegrity(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestExtendedAlterDerivedViewOutputs(t *testing.T) {
	d, err := db.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	alterExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER); INSERT INTO t VALUES(1,7)")
	alterExec(t, d, "CREATE VIEW derived AS SELECT x.v FROM (SELECT v FROM t) AS x; CREATE VIEW star_derived AS SELECT x.v FROM (SELECT * FROM t) AS x; CREATE VIEW nested AS SELECT x.v FROM (SELECT y.v AS v FROM (SELECT * FROM t) AS y) AS x")
	alterExec(t, d, "ALTER TABLE t RENAME COLUMN v TO value; ALTER TABLE t RENAME TO renamed")
	for _, name := range []string{"derived", "star_derived", "nested"} {
		r := alterExec(t, d, "SELECT * FROM "+name)
		if len(r.Rows) != 1 || r.Rows[0]["x.v"].Int != 7 {
			t.Fatal(name, r)
		}
	}
}

func TestExtendedAlterConcurrentRenameCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	runCrashChild(t, "rename-concurrent", dir)
	d, e := db.Open(dir, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	r := alterExec(t, d, "SELECT id,v FROM original ORDER BY id")
	if len(r.Rows) != 500 {
		t.Fatalf("lost acknowledged rows: got %d, want 500", len(r.Rows))
	}
	for i, row := range r.Rows {
		if row["id"].Int != int64(i+1) || row["v"].Int != int64((i+1)*17) {
			t.Fatal("acknowledged row corrupted", row)
		}
	}
	alterBad(t, d, "SELECT * FROM uncommitted")
	r = alterExec(t, d, "SELECT id FROM original WHERE v=8500")
	if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 500 {
		t.Fatal("index/catalog did not recover with rows", r)
	}
	if _, e = d.CheckIntegrity(); e != nil {
		t.Fatal(e)
	}
}
