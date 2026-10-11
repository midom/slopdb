package db

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"slopdb/storage"
)

func TestIntegrityDetectsLogicalCorruption(t *testing.T) {
	cases := []struct {
		name, setup string
		mutate      func(*testing.T, *Database)
		want        string
	}{
		{"missing index", "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT);CREATE INDEX iv ON t(v);INSERT INTO t VALUES(1,'x')", func(t *testing.T, d *Database) {
			key, _ := integrityEntry(t, d, []byte("\x02iv:"))
			if e := d.lsm.Delete(key); e != nil {
				t.Fatal(e)
			}
		}, "missing entry"},
		{"stale index", "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT);CREATE INDEX iv ON t(v);INSERT INTO t VALUES(1,'x')", func(t *testing.T, d *Database) {
			key, value := integrityEntry(t, d, []byte("\x02iv:"))
			value[0] = 99
			if e := d.lsm.Put(key, value); e != nil {
				t.Fatal(e)
			}
		}, "stale payload"},
		{"orphan index", "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT);CREATE INDEX iv ON t(v);INSERT INTO t VALUES(1,'x')", func(t *testing.T, d *Database) {
			if e := d.lsm.Put([]byte("\x02iv:extra:\x01t:missing"), nil); e != nil {
				t.Fatal(e)
			}
		}, "extra or orphan"},
		{"foreign orphan", "CREATE TABLE p(id INTEGER PRIMARY KEY);CREATE TABLE c(id INTEGER PRIMARY KEY,p INTEGER REFERENCES p(id));INSERT INTO p VALUES(1);INSERT INTO c VALUES(1,1)", func(t *testing.T, d *Database) {
			key, value := integrityEntry(t, d, []byte("\x01c:"))
			binary.LittleEndian.PutUint64(value[10:18], 999)
			if e := d.lsm.Put(key, value); e != nil {
				t.Fatal(e)
			}
		}, "FOREIGN KEY"},
		{"invalid row", "CREATE TABLE t(id INTEGER PRIMARY KEY);INSERT INTO t VALUES(1)", func(t *testing.T, d *Database) {
			key, value := integrityEntry(t, d, []byte("\x01t:"))
			value[0] = 99
			if e := d.lsm.Put(key, value); e != nil {
				t.Fatal(e)
			}
		}, "unknown value tag"},
		{"catalog", "CREATE TABLE t(id INTEGER PRIMARY KEY)", func(t *testing.T, d *Database) {
			if e := d.lsm.Put([]byte("\x00__schema__"), []byte("{broken")); e != nil {
				t.Fatal(e)
			}
		}, "malformed JSON"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			d, e := Open(filepath.Join(t.TempDir(), "db"), nil)
			if e != nil {
				t.Fatal(e)
			}
			defer d.Close()
			if _, e = d.Exec(test.setup); e != nil {
				t.Fatal(e)
			}
			test.mutate(t, d)
			if _, e = d.CheckIntegrity(); e == nil || !strings.Contains(e.Error(), test.want) {
				t.Fatalf("expected %q, got %v", test.want, e)
			}
		})
	}
}
func integrityEntry(t *testing.T, d *Database, prefix []byte) ([]byte, []byte) {
	t.Helper()
	it := d.lsm.NewIteratorSeek(prefix)
	defer it.Close()
	if !it.Valid() || !bytes.HasPrefix(it.Key().UserKey, prefix) {
		t.Fatal("missing fixture entry", storage.IteratorError(it))
	}
	return append([]byte{}, it.Key().UserKey...), append([]byte{}, it.Value()...)
}
func TestIntegrityRereadsChecksumsBeyondBlockCache(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	d, e := Open(dir, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	if _, e = d.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT);INSERT INTO t VALUES(1,'payload');VACUUM;SELECT * FROM t"); e != nil {
		t.Fatal(e)
	}
	if _, e = d.CheckIntegrity(); e != nil {
		t.Fatal(e)
	}
	files, e := filepath.Glob(filepath.Join(dir, "*.sst"))
	if e != nil || len(files) == 0 {
		t.Fatal(files, e)
	}
	file, e := os.OpenFile(files[0], os.O_RDWR, 0)
	if e != nil {
		t.Fatal(e)
	}
	first := []byte{0}
	if _, e = file.ReadAt(first, 0); e != nil {
		t.Fatal(e)
	}
	first[0] ^= 0xff
	if _, e = file.WriteAt(first, 0); e != nil {
		t.Fatal(e)
	}
	if e = file.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e = d.CheckIntegrity(); e == nil || !strings.Contains(e.Error(), "CRC mismatch") {
		t.Fatalf("cached data hid corruption: %v", e)
	}
}
func TestBackupRejectsDestinationAliasInsideSource(t *testing.T) {
	d, e := Open(filepath.Join(t.TempDir(), "source"), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	alias := filepath.Join(t.TempDir(), "alias")
	if e = os.Symlink(d.dir, alias); e != nil {
		t.Skipf("symlink unavailable: %v", e)
	}
	if e = d.Backup(filepath.Join(alias, "nested-backup")); e == nil || !strings.Contains(e.Error(), "outside") {
		t.Fatal("backup accepted destination alias inside source", e)
	}
}
func TestBackupSupportsRelativeSourceDirectory(t *testing.T) {
	current, e := os.Getwd()
	if e != nil {
		t.Fatal(e)
	}
	source := filepath.Join(t.TempDir(), "source")
	relative, e := filepath.Rel(current, source)
	if e != nil {
		t.Fatal(e)
	}
	d, e := Open(relative, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	if _, e = d.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY);INSERT INTO t VALUES(1)"); e != nil {
		t.Fatal(e)
	}
	target := filepath.Join(t.TempDir(), "backup")
	if e = d.Backup(target); e != nil {
		t.Fatal(e)
	}
	restored, e := Open(target, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer restored.Close()
	result, e := restored.Exec("SELECT id FROM t")
	if e != nil || len(result.Rows) != 1 || result.Rows[0]["id"].Int != 1 {
		t.Fatal(e, result)
	}
}
func TestBackupPersistsSourceWithWALDisabled(t *testing.T) {
	options := storage.DefaultOptions()
	options.DisableWAL = true
	d, e := Open(filepath.Join(t.TempDir(), "source"), options)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	if _, e = d.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY);INSERT INTO t VALUES(1)"); e != nil {
		t.Fatal(e)
	}
	target := filepath.Join(t.TempDir(), "backup")
	if e = d.Backup(target); e != nil {
		t.Fatal(e)
	}
	restored, e := Open(target, nil)
	if e != nil {
		t.Fatal(e)
	}
	result, e := restored.Exec("SELECT id FROM t")
	if e != nil || len(result.Rows) != 1 || result.Rows[0]["id"].Int != 1 {
		t.Fatal(e, result)
	}
	if e = restored.Close(); e != nil {
		t.Fatal(e)
	}
}
