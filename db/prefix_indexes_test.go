package db

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func TestPhysicalPrefixIndexEqualityRangesAndUnique(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	objectExec(t, d, "CREATE TABLE p(id INTEGER PRIMARY KEY,t TEXT,b BLOB,KEY it(t(2)),KEY ib(b(2)),UNIQUE KEY uq(t(2),b(2)))")
	objectExec(t, d, "INSERT INTO p VALUES(1,'é猫tail-a',X'616263'),(2,'é狗tail-b',X'616264'),(3,NULL,X'616263'),(4,'é猫tail-b',X'7a7a00')")
	if _, err = d.Exec("INSERT INTO p VALUES(5,'é猫other',X'61627a')"); err == nil {
		t.Fatal("unique prefix accepted collision")
	}
	it := d.lsm.NewIteratorRange([]byte{2}, []byte{3}, false)
	count := 0
	for ; it.Valid(); it.Next() {
		if bytes.Contains(it.Key().UserKey, []byte(hex.EncodeToString([]byte("tail")))) || bytes.Contains(it.Value(), []byte("tail")) {
			t.Fatal("full indexed value retained in physical prefix entry")
		}
		count++
	}
	it.Close()
	if count != 12 {
		t.Fatalf("physical prefix entries=%d", count)
	}

	check := func() {
		t.Helper()
		for _, q := range []string{"SELECT id,t FROM p WHERE t='é猫tail-a'", "SELECT id FROM p WHERE t='é猫tail-a' AND b=X'616263'", "SELECT id FROM p WHERE t>'é猫tail-a' AND t<'é猫zz' ORDER BY t"} {
			r := objectExec(t, d, q)
			want := int64(1)
			if strings.Contains(q, "t>") {
				want = 4
			}
			if len(r.Rows) != 1 || r.Rows[0]["id"].Int != want {
				t.Fatalf("%s: %+v", q, r.Rows)
			}
		}
		r := objectExec(t, d, "SELECT id,t FROM p WHERE t IN('é猫tail-a','é猫tail-b') ORDER BY t")
		if len(r.Rows) != 2 || r.Rows[0]["id"].Int != 1 || r.Rows[1]["id"].Int != 4 {
			t.Fatal(r.Rows)
		}
		r = objectExec(t, d, "SELECT id FROM p ORDER BY t LIMIT 2")
		if len(r.Rows) != 2 || r.Rows[0]["id"].Int != 3 || r.Rows[1]["id"].Int != 2 {
			t.Fatal(r.Rows)
		}
	}
	check()
	objectExec(t, d, "UPDATE p SET t='é鳥new' WHERE id=4")
	if _, err = d.Exec("UPDATE p SET t='é猫different' WHERE id=2"); err == nil {
		t.Fatal("update bypassed prefix uniqueness")
	}
	objectExec(t, d, "UPDATE p SET t='é猫tail-b' WHERE id=4")
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	check()
	if _, err = d.Exec("INSERT INTO p VALUES(5,'é猫again',X'61626c')"); err == nil {
		t.Fatal("reopened unique prefix accepted duplicate")
	}
	objectExec(t, d, "CREATE TABLE normal(id INTEGER PRIMARY KEY,v TEXT UNIQUE,KEY short(v(1)))")
	objectExec(t, d, "INSERT INTO normal VALUES(1,'a-one'),(2,'a-two')")
}

func TestUniquePrefixConflictActionsAndConcurrentWait(t *testing.T) {
	d, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	objectExec(t, d, "CREATE TABLE p(id INTEGER PRIMARY KEY,v TEXT,score INTEGER,UNIQUE KEY uq(v(2)))")
	objectExec(t, d, "INSERT INTO p VALUES(1,'alpha',10)")
	objectExec(t, d, "INSERT INTO p VALUES(2,'alpine',20) ON DUPLICATE KEY UPDATE score=20")
	r := objectExec(t, d, "SELECT id,score FROM p")
	if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 1 || r.Rows[0]["score"].Int != 20 {
		t.Fatal(r.Rows)
	}
	objectExec(t, d, "REPLACE INTO p VALUES(2,'alpine',30)")
	r = objectExec(t, d, "SELECT id,score FROM p")
	if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 2 {
		t.Fatal(r.Rows)
	}
	a, _ := d.NewSession()
	defer a.Close()
	b, _ := d.NewSession()
	defer b.Close()
	if _, err = a.Exec("BEGIN;INSERT INTO p VALUES(3,'beta',40)"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, e := b.Exec("INSERT INTO p VALUES(4,'best',50)"); done <- e }()
	waitForReportedLock(t, d)
	if _, err = a.Exec("COMMIT"); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err == nil {
		t.Fatal("waiter accepted same prefix after owner commit")
	}
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
}

func TestNamedPrefixConstraintsAlterAndLike(t *testing.T) {
	d, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	objectExec(t, d, "CREATE TABLE p(id INTEGER PRIMARY KEY,v TEXT,CONSTRAINT prefix_unique UNIQUE KEY(v(2)))")
	objectExec(t, d, "INSERT INTO p VALUES(1,'alpha')")
	if _, err = d.Exec("INSERT INTO p VALUES(2,'alpine')"); err == nil {
		t.Fatal("named prefix ignored")
	}
	objectExec(t, d, "CREATE TABLE copy LIKE p;INSERT INTO copy VALUES(1,'beta')")
	if _, err = d.Exec("INSERT INTO copy VALUES(2,'best')"); err == nil {
		t.Fatal("LIKE lost prefix uniqueness")
	}
	objectExec(t, d, "ALTER TABLE copy DROP CONSTRAINT prefix_unique;INSERT INTO copy VALUES(2,'best')")
	if _, err = d.Exec("ALTER TABLE copy ADD CONSTRAINT new_unique UNIQUE KEY(v(2))"); err == nil {
		t.Fatal("ALTER prefix ignored duplicates")
	}
	objectExec(t, d, "DELETE FROM copy WHERE id=2;ALTER TABLE copy ADD CONSTRAINT new_unique UNIQUE KEY(v(2))")
	if _, err = d.Exec("INSERT INTO copy VALUES(2,'best')"); err == nil {
		t.Fatal("ALTER prefix lost")
	}
}

func TestLegacyFullValuePrefixIndexesUpgradeOnOpen(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	objectExec(t, d, "CREATE TABLE p(id INTEGER PRIMARY KEY,v TEXT);INSERT INTO p VALUES(1,'same-tail-one'),(2,'same-tail-two');CREATE INDEX iv ON p(v)")
	data, ok, err := d.lsm.Get([]byte("\x00__schema_indexes__"))
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	var catalog map[string]map[string]any
	if err = json.Unmarshal(data, &catalog); err != nil {
		t.Fatal(err)
	}
	catalog["iv"]["prefix_lengths"] = []int{4}
	catalog["iv"]["prefix_encoding"] = 0
	data, err = json.Marshal(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if err = d.lsm.Put([]byte("\x00__schema_indexes__"), data); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	r := objectExec(t, d, "SELECT id FROM p WHERE v='same-tail-two'")
	if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 2 {
		t.Fatal(r.Rows)
	}
	it := d.lsm.NewIteratorRange([]byte{2}, []byte{3}, false)
	for ; it.Valid(); it.Next() {
		if bytes.Contains(it.Key().UserKey, []byte("tail")) || bytes.Contains(it.Key().UserKey, []byte(hex.EncodeToString([]byte("tail")))) || bytes.Contains(it.Value(), []byte("tail")) {
			it.Close()
			t.Fatal("legacy full-value index data survived rewrite")
		}
	}
	it.Close()
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
}

func TestPrefixUniqueCollationChangeIsAtomic(t *testing.T) {
	d, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	objectExec(t, d, "CREATE TABLE p(id INTEGER PRIMARY KEY,v TEXT,UNIQUE KEY uq(v(2)));INSERT INTO p VALUES(1,'CA-one'),(2,'ca-two')")
	if _, err = d.Exec("PRAGMA collation='NFC_CI'"); err == nil {
		t.Fatal("collation accepted duplicate prefixes")
	}
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
	objectExec(t, d, "INSERT INTO p VALUES(3,'Ca-three')")
}
