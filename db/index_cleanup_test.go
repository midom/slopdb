package db

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"slopdb/storage"
)

const testIndexCleanupPrefix = "\x00__index_cleanup__:"

func assertIndexCleanupState(t *testing.T, d *Database, id uint64, entries bool) {
	t.Helper()
	key := binary.BigEndian.AppendUint64([]byte(testIndexCleanupPrefix), id)
	if _, found, err := d.lsm.Get(key); err != nil || found {
		t.Fatalf("cleanup marker remains: found=%v err=%v", found, err)
	}
	prefix := binary.BigEndian.AppendUint64([]byte{2, 0xff}, id)
	prefix = append(prefix, ':')
	it := d.lsm.NewIteratorSeek(prefix)
	found := it.Valid() && bytes.HasPrefix(it.Key().UserKey, prefix)
	err := storage.IteratorError(it)
	it.Close()
	if err != nil || found != entries {
		t.Fatalf("index entries: found=%v want=%v err=%v", found, entries, err)
	}
}

func TestIndexCleanupOrdinaryCommitSkipsMetadata(t *testing.T) {
	d, _, _ := onlineTestDatabase(t)
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER); INSERT INTO t VALUES(1,0)")
	// Corrupt cleanup metadata would fail any cleanup scan. Ordinary DML must
	// never inspect it; recovery/DDL cleanup still validates it strictly.
	key := binary.BigEndian.AppendUint64([]byte(testIndexCleanupPrefix), 999)
	if err := d.lsm.Put(key, []byte("invalid JSON")); err != nil {
		t.Fatal(err)
	}
	objectExec(t, d, "BEGIN; UPDATE t SET v=v+1 WHERE id=1; COMMIT; SAVEPOINT outer_tx; UPDATE t SET v=v+1 WHERE id=1; RELEASE outer_tx")
	s, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, err = s.Exec("BEGIN; UPDATE t SET v=v+1 WHERE id=1; COMMIT"); err != nil {
		t.Fatal(err)
	}
	if got := objectExec(t, d, "SELECT v FROM t WHERE id=1").Rows[0]["v"].Int; got != 3 {
		t.Fatal("committed updates", got)
	}
	if err = d.lsm.Delete(key); err != nil {
		t.Fatal(err)
	}
}

func TestIndexCleanupTransactionBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, before, after string
		retained            bool
		session             bool
	}{
		{"commit", "BEGIN; DROP INDEX iv", "COMMIT", false, false},
		{"session_commit", "BEGIN; DROP INDEX iv", "COMMIT", false, true},
		{"outer_release", "SAVEPOINT outer_tx; ALTER TABLE t DROP INDEX iv", "RELEASE outer_tx", false, false},
		{"inner_release", "BEGIN; SAVEPOINT inner_tx; DROP INDEX iv; RELEASE inner_tx", "COMMIT", false, false},
		{"rollback", "BEGIN; DROP INDEX iv", "ROLLBACK; BEGIN; COMMIT", true, false},
		{"rollback_to", "BEGIN; SAVEPOINT inner_tx; DROP INDEX iv", "ROLLBACK TO inner_tx; COMMIT", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, dir, opt := onlineTestDatabase(t)
			objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'one'),(2,'two'); CREATE INDEX iv ON t(v)")
			id := objectIndexID(t, d, "iv")
			exec := d.Exec
			if tc.session {
				s, err := d.NewSession()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s.Close() })
				exec = s.Exec
			}
			if _, err := exec(tc.before); err != nil {
				t.Fatal(err)
			}
			assertIndexCleanupState(t, d, id, true)
			// Other sessions/online build closure may clean committed garbage
			// while this transaction's drop marker is still private.
			if err := d.schema.CleanupIndexGarbage(); err != nil {
				t.Fatal(err)
			}
			if _, err := exec(tc.after); err != nil {
				t.Fatal(err)
			}
			assertIndexCleanupState(t, d, id, tc.retained)
			if _, err := d.CheckIntegrity(); err != nil {
				t.Fatal(err)
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(dir, opt)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			assertIndexCleanupState(t, reopened, id, tc.retained)
			if _, err = reopened.CheckIntegrity(); err != nil {
				t.Fatal(err)
			}
			if err = reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestIndexCleanupCommitRetriesAfterFailure(t *testing.T) {
	d, _, _ := onlineTestDatabase(t)
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'one'); CREATE INDEX iv ON t(v)")
	id := objectIndexID(t, d, "iv")
	// Sort before the valid marker so the failed cleanup leaves it pending.
	badKey := binary.BigEndian.AppendUint64([]byte(testIndexCleanupPrefix), 0)
	if err := d.lsm.Put(badKey, []byte("invalid JSON")); err != nil {
		t.Fatal(err)
	}
	objectExec(t, d, "BEGIN; DROP INDEX iv")
	if _, err := d.Exec("COMMIT"); err == nil || !strings.Contains(err.Error(), "schema change committed; index cleanup pending") {
		t.Fatal("expected committed-with-cleanup-error", err)
	}
	if got := objectIndexID(t, d, "iv"); got != 0 {
		t.Fatal("failed cleanup undid committed drop", got)
	}
	if err := d.lsm.Delete(badKey); err != nil {
		t.Fatal(err)
	}
	objectExec(t, d, "BEGIN; UPDATE t SET v='updated' WHERE id=1; COMMIT")
	assertIndexCleanupState(t, d, id, false)
	if _, err := d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
}
