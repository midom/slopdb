package db

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type storedObjectEntry struct {
	Sequence uint64
	Value    string
}

func objectData(t *testing.T, d *Database) map[string]storedObjectEntry {
	t.Helper()
	out := make(map[string]storedObjectEntry)
	it := d.lsm.NewIterator()
	defer it.Close()
	for it.Valid() {
		key := it.Key()
		if len(key.UserKey) > 0 && (key.UserKey[0] == 1 || key.UserKey[0] == 2) {
			out[string(key.UserKey)] = storedObjectEntry{key.SeqNum, string(it.Value())}
		}
		it.Next()
	}
	return out
}
func objectExec(t *testing.T, d *Database, q string) *ExecResult {
	t.Helper()
	r, e := d.Exec(q)
	if e != nil {
		t.Fatalf("%s: %v", q, e)
	}
	return r
}
func objectIndexID(t *testing.T, d *Database, name string) uint64 {
	t.Helper()
	b, ok, e := d.lsm.Get([]byte("\x00__schema_indexes__"))
	if e != nil || !ok {
		t.Fatal(e)
	}
	var defs map[string]struct{ ID uint64 }
	if e = json.Unmarshal(b, &defs); e != nil {
		t.Fatal(e)
	}
	return defs[name].ID
}

func TestObjectIDsMakeRenameCatalogOnly(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	d, e := Open(dir, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	objectExec(t, d, "CREATE TABLE old_name(id INTEGER PRIMARY KEY AUTOINCREMENT,v TEXT UNIQUE); CREATE INDEX old_index ON old_name(v); CREATE TABLE log(v TEXT); CREATE TRIGGER ai AFTER INSERT ON old_name BEGIN INSERT INTO log VALUES(NEW.v); END; CREATE VIEW visible(id,v) AS SELECT id,v FROM old_name;")
	var values []string
	for i := 0; i < 600; i++ {
		values = append(values, fmt.Sprintf("(%d,'value-%d')", i+1, i))
	}
	objectExec(t, d, "INSERT INTO old_name VALUES"+strings.Join(values, ","))
	id := d.schema.GetTableInfo("old_name").ID
	idxID := objectIndexID(t, d, "old_index")
	if id == 0 || idxID == 0 || id == idxID {
		t.Fatal("missing/distinct IDs", id, idxID)
	}
	before := objectData(t, d)
	objectExec(t, d, "ALTER TABLE old_name RENAME TO renamed; ALTER INDEX old_index RENAME TO renamed_index;")
	if !reflect.DeepEqual(before, objectData(t, d)) {
		t.Fatal("rename rewrote rows/index entries or their sequence numbers")
	}
	if d.schema.GetTableInfo("renamed").ID != id || objectIndexID(t, d, "renamed_index") != idxID {
		t.Fatal("rename changed storage IDs")
	}
	for key := range before {
		raw := []byte(key)
		if len(raw) < 11 || raw[1] != 0xff || raw[10] != ':' || binary.BigEndian.Uint64(raw[2:10]) == 0 {
			t.Fatalf("not an ID prefix: %q", raw)
		}
	}
	objectExec(t, d, "ALTER TABLE renamed RENAME COLUMN v TO value")
	if !reflect.DeepEqual(before, objectData(t, d)) {
		t.Fatal("column rename rewrote row/index data")
	}
	objectExec(t, d, "INSERT INTO renamed(value) VALUES('next')")
	r := objectExec(t, d, "SELECT id FROM renamed WHERE value='next'")
	if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 601 {
		t.Fatal(r)
	}
	r = objectExec(t, d, "SELECT v FROM visible WHERE id=601")
	if len(r.Rows) != 1 || r.Rows[0]["v"].Text != "next" {
		t.Fatal(r)
	}
	r = objectExec(t, d, "SELECT COUNT(*) AS n FROM log WHERE v='next'")
	if r.Rows[0]["n"].Int != 1 {
		t.Fatal("trigger did not follow rename", r)
	}
	if _, e = d.CheckIntegrity(); e != nil {
		t.Fatal(e)
	}
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
	d, e = Open(dir, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	if d.schema.GetTableInfo("renamed").ID != id || objectIndexID(t, d, "renamed_index") != idxID {
		t.Fatal("IDs changed after reopen")
	}
	objectExec(t, d, "INSERT INTO renamed(value) VALUES('reopened')")
	if _, e = d.CheckIntegrity(); e != nil {
		t.Fatal(e)
	}
}

func TestObjectIDRenameRollbackAndNameReuse(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	d, e := Open(dir, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'original'); CREATE INDEX iv ON t(v)")
	id := d.schema.GetTableInfo("t").ID
	before := objectData(t, d)
	objectExec(t, d, "BEGIN; SAVEPOINT s; ALTER TABLE t RENAME TO temporary; ALTER INDEX iv RENAME TO temp_index; ROLLBACK TO s; COMMIT;")
	if d.schema.GetTableInfo("t") == nil || d.schema.GetTableInfo("t").ID != id || !reflect.DeepEqual(before, objectData(t, d)) {
		t.Fatal("rename rollback changed data/IDs")
	}
	objectExec(t, d, "CREATE TABLE occupied(x INTEGER)")
	if _, e = d.Exec("ALTER TABLE t RENAME TO occupied"); e == nil {
		t.Fatal("rename overwrote existing object")
	}
	objectExec(t, d, "ALTER TABLE t RENAME TO renamed; CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'fresh')")
	if d.schema.GetTableInfo("t").ID == id {
		t.Fatal("name reuse reused a live ID")
	}
	r := objectExec(t, d, "SELECT v FROM t WHERE id=1")
	if r.Rows[0]["v"].Text != "fresh" {
		t.Fatal(r)
	}
	r = objectExec(t, d, "SELECT v FROM renamed WHERE id=1")
	if r.Rows[0]["v"].Text != "original" {
		t.Fatal(r)
	}
	newID := d.schema.GetTableInfo("t").ID
	objectExec(t, d, "DROP TABLE t")
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
	d, e = Open(dir, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT)")
	if d.schema.GetTableInfo("t").ID <= newID {
		t.Fatal("dropped IDs were reused after restart")
	}
	if _, e = d.CheckIntegrity(); e != nil {
		t.Fatal(e)
	}
	for key := range objectData(t, d) {
		if bytes.HasPrefix([]byte(key), []byte("\x01t:")) {
			t.Fatal("name-based row keys")
		}
	}
}

func TestObjectIDAtomicMultiRename(t *testing.T) {
	d, e := Open(filepath.Join(t.TempDir(), "db"), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	objectExec(t, d, "CREATE TABLE a(id INTEGER PRIMARY KEY,v TEXT); CREATE TABLE b(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO a VALUES(1,'a'); INSERT INTO b VALUES(1,'b'); CREATE VIEW va AS SELECT v FROM a")
	a, b := d.schema.GetTableInfo("a").ID, d.schema.GetTableInfo("b").ID
	before := objectData(t, d)
	objectExec(t, d, "RENAME TABLE a TO temporary, b TO a, temporary TO b")
	if d.schema.GetTableInfo("a").ID != b || d.schema.GetTableInfo("b").ID != a || !reflect.DeepEqual(before, objectData(t, d)) {
		t.Fatal("multi rename moved data/IDs")
	}
	r := objectExec(t, d, "SELECT v FROM va")
	if r.Rows[0]["v"].Text != "a" {
		t.Fatal(r)
	}
	if _, e = d.Exec("RENAME TABLE a TO temporary, b TO temporary"); e == nil {
		t.Fatal("expected conflicting rename")
	}
	if d.schema.GetTableInfo("a").ID != b || d.schema.GetTableInfo("b").ID != a || d.schema.GetTableInfo("temporary") != nil {
		t.Fatal("partial multi rename committed")
	}
}
