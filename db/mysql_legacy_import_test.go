package db

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"slopdb/storage"
)

func legacyMySQLFixture(t *testing.T, dir string) {
	t.Helper()
	path, _ := mysqlLegacyPath(dir, "archive")
	source, err := Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	objectExec(t, source, "CREATE TABLE parent(id INTEGER PRIMARY KEY AUTOINCREMENT,title TEXT,UNIQUE KEY prefix_title(title(3)),FULLTEXT KEY ft(title));CREATE TABLE child(id INTEGER PRIMARY KEY,parent_id INTEGER,CONSTRAINT owner FOREIGN KEY(parent_id) REFERENCES parent(id));CREATE TABLE log(v TEXT);INSERT INTO parent VALUES(1,'quasar nebula'),(100,'discard');DELETE FROM parent WHERE id=100;INSERT INTO child VALUES(1,1);CREATE VIEW z_base AS SELECT id,title FROM parent;CREATE VIEW a_dependent AS SELECT id,title FROM z_base;CREATE TRIGGER audit AFTER INSERT ON parent BEGIN INSERT INTO log VALUES(NEW.title);END")
	objectExec(t, source, `CREATE TABLE typed(id BIGINT UNSIGNED PRIMARY KEY,n DECIMAL(30,6),payload JSON,b VARBINARY(4));INSERT INTO typed VALUES(18446744073709551615,'12345678901234567890.123456','{"nested":[1,true]}',X'00ff')`)
	if err = source.Close(); err != nil {
		t.Fatal(err)
	}
}

func checkLegacyImport(t *testing.T, d *Database) {
	t.Helper()
	typed := objectExec(t, d, "SELECT id,n,payload,b FROM archive.typed")
	if len(typed.Rows) != 1 || typed.Rows[0]["id"].Uint != ^uint64(0) || typed.Rows[0]["n"].Text != "12345678901234567890.123456" || typed.Rows[0]["payload"].Text != `{"nested":[1,true]}` || string(typed.Rows[0]["b"].Blob) != string([]byte{0, 255}) {
		t.Fatal("typed migration changed values", typed.Rows)
	}
	r := objectExec(t, d, "SELECT id FROM archive.parent WHERE MATCH(title) AGAINST('quasar' IN BOOLEAN MODE)")
	if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 1 {
		t.Fatal(r.Rows)
	}
	r = objectExec(t, d, "SELECT title FROM archive.a_dependent")
	if len(r.Rows) != 1 || r.Rows[0]["title"].Text != "quasar nebula" {
		t.Fatal(r.Rows)
	}
	if _, err := d.Exec("INSERT INTO archive.child VALUES(2,999)"); err == nil {
		t.Fatal("foreign key lost")
	}
	if _, err := d.Exec("INSERT INTO archive.parent VALUES(2,'quartz')"); err == nil {
		t.Fatal("unique prefix lost")
	}
	r = objectExec(t, d, "INSERT INTO archive.parent(title) VALUES('galactic')")
	if r.LastID != 101 {
		t.Fatalf("sequence floor lost: %+v", r)
	}
	r = objectExec(t, d, "SELECT v FROM archive.log")
	if len(r.Rows) != 1 || r.Rows[0]["v"].Text != "galactic" {
		t.Fatal("trigger lost", r.Rows)
	}
	if _, err := d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyMySQLDatabaseLogicalImportAndReopen(t *testing.T) {
	dir := t.TempDir()
	legacyMySQLFixture(t, dir)
	d, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	objectExec(t, d, "CREATE TABLE existing(id INTEGER PRIMARY KEY);INSERT INTO existing VALUES(1)")
	oldID := d.schema.GetTableInfo("existing").ID
	if err = d.MigrateLegacyMySQLDatabases(context.Background(), []string{"main", "archive"}, "main"); err != nil {
		t.Fatal(err)
	}
	_ = d.GetCreateTableSQL("archive.parent")
	if d.schema.GetTableInfo("archive.parent").ID == oldID {
		t.Fatal("source object ID was copied")
	}
	checkLegacyImport(t, d)
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = d.MigrateLegacyMySQLDatabases(context.Background(), []string{"archive"}, "main"); err != nil {
		t.Fatal(err)
	}
	r := objectExec(t, d, "SELECT COUNT(*) AS n FROM archive.parent")
	if r.Rows[0]["n"].Int != 2 {
		t.Fatal("completed import reran", r.Rows)
	}
	sourcePath, _ := mysqlLegacyPath(dir, "archive")
	source, err := Open(sourcePath, nil)
	if err != nil {
		t.Fatal(err)
	}
	r = objectExec(t, source, "SELECT COUNT(*) AS n FROM parent")
	if r.Rows[0]["n"].Int != 1 {
		t.Fatal("source changed", r.Rows)
	}
	if err = source.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyMySQLDatabaseImportCrashRestart(t *testing.T) {
	if dir := os.Getenv("SLOPDB_MYSQL_IMPORT_CHILD"); dir != "" {
		opt := storage.DefaultOptions()
		opt.DisableWAL = true
		d, err := Open(dir, opt)
		if err != nil {
			t.Fatal(err)
		}
		d.mysqlMigrationHook = func(stage string) {
			if stage == os.Getenv("SLOPDB_MYSQL_IMPORT_STAGE") {
				os.Exit(85)
			}
		}
		if err = d.MigrateLegacyMySQLDatabases(context.Background(), []string{"archive"}, "main"); err != nil {
			t.Fatal(err)
		}
		t.Fatal("crash hook not reached")
	}
	for _, stage := range []string{"batch", "complete", "committed"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			legacyMySQLFixture(t, dir)
			cmd := exec.Command(os.Args[0], "-test.run=^TestLegacyMySQLDatabaseImportCrashRestart$", "-test.timeout=40s")
			cmd.Env = append(os.Environ(), "SLOPDB_MYSQL_IMPORT_CHILD="+dir, "SLOPDB_MYSQL_IMPORT_STAGE="+stage)
			if output, err := cmd.CombinedOutput(); err == nil {
				t.Fatal("child survived", string(output))
			} else if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 85 {
				t.Fatal(err, string(output))
			}
			d, err := Open(dir, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			_, key := mysqlLegacyPath(dir, "archive")
			data, found, e := d.lsm.Get(key)
			if e != nil || !found {
				t.Fatal(found, e)
			}
			var marker mysqlLegacyImport
			if e = json.Unmarshal(data, &marker); e != nil {
				t.Fatal(e)
			}
			if marker.Complete != (stage == "committed") {
				t.Fatal("incorrect crash completion checkpoint", marker)
			}
			if err = d.MigrateLegacyMySQLDatabases(context.Background(), []string{"archive"}, "main"); err != nil {
				t.Fatal(err)
			}
			checkLegacyImport(t, d)
			if err = d.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLegacyMySQLDatabaseImportConflictAndCancellation(t *testing.T) {
	dir := t.TempDir()
	legacyMySQLFixture(t, dir)
	d, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	objectExec(t, d, "CREATE SCHEMA archive;CREATE TABLE archive.untouched(id INTEGER)")
	if err = d.MigrateLegacyMySQLDatabases(context.Background(), []string{"archive"}, "main"); err == nil || !strings.Contains(err.Error(), "ownership") {
		t.Fatal(err)
	}
	objectExec(t, d, "SELECT * FROM archive.untouched;DROP SCHEMA archive CASCADE")
	ctx, cancel := context.WithCancel(context.Background())
	d.mysqlMigrationHook = func(stage string) {
		if stage == "batch" {
			cancel()
		}
	}
	if err = d.MigrateLegacyMySQLDatabases(ctx, []string{"archive"}, "main"); err == nil {
		t.Fatal("cancellation ignored")
	}
	d.mysqlMigrationHook = nil
	if err = d.MigrateLegacyMySQLDatabases(context.Background(), []string{"archive"}, "main"); err != nil {
		t.Fatal(err)
	}
	checkLegacyImport(t, d)
	_, key := mysqlLegacyPath(dir, "archive")
	data, ok, err := d.currentStorage.Load().Get(key)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	var marker mysqlLegacyImport
	if err = json.Unmarshal(data, &marker); err != nil || !marker.Complete {
		t.Fatal(marker, err)
	}
	path, _ := mysqlLegacyPath(dir, "archive")
	if _, err = os.Stat(filepath.Join(path, "MANIFEST")); err != nil {
		t.Fatal(err)
	}
}
