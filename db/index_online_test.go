package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"slopdb/sql"
	"slopdb/storage"
)

func onlineTestDatabase(t *testing.T) (*Database, string, *storage.Options) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "db")
	opt := storage.DefaultOptions()
	opt.DisableWALSync = true
	opt.QueryMemoryBytes = 64 << 10
	opt.QueryTempDir = t.TempDir()
	d, err := Open(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d, dir, opt
}

func onlineWait(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("online index operation did not finish")
		return nil
	}
}

func onlineBarrier(t *testing.T, d *Database, phase string) (<-chan struct{}, func()) {
	t.Helper()
	reached, released := make(chan struct{}), make(chan struct{})
	var once, reachedOnce sync.Once
	release := func() { once.Do(func() { close(released) }) }
	t.Cleanup(release)
	d.onlineIndexHook = func(current string) {
		if current == phase {
			reachedOnce.Do(func() { close(reached) })
			<-released
		}
	}
	return reached, release
}

func onlineReached(t *testing.T, reached <-chan struct{}) {
	t.Helper()
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		t.Fatal("online build did not reach synchronization barrier")
	}
}

func onlineStart(t *testing.T, d *Database, ctx context.Context, query string) <-chan error {
	t.Helper()
	s, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { defer s.Close(); _, err := s.ExecContext(ctx, query); done <- err }()
	return done
}

func TestOnlineIndexConcurrentCommittedChanges(t *testing.T) {
	for _, statement := range []string{"CREATE INDEX iv ON t(v)", "CREATE UNIQUE INDEX iv ON t(v)", "ALTER TABLE t ADD INDEX iv(v)"} {
		t.Run(statement, func(t *testing.T) {
			d, dir, opt := onlineTestDatabase(t)
			objectExec(t, d, "PRAGMA collation=NFC_CI; CREATE TABLE t(id INTEGER PRIMARY KEY,v VARCHAR(64), payload VARBINARY(16)); INSERT INTO t VALUES(1,'One',X'01'),(2,'Two',X'02'),(3,NULL,X'03'),(4,NULL,X'04')")
			prepared, err := d.Prepare("SELECT id FROM t WHERE v=? ORDER BY id")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = prepared.Close() })
			if _, err = prepared.Exec("ONE"); err != nil {
				t.Fatal(err)
			}
			reached, release := onlineBarrier(t, d, "scanned")
			done := onlineStart(t, d, context.Background(), statement)
			onlineReached(t, reached)
			if _, err = d.CheckIntegrity(); err != nil {
				t.Fatal("integrity during build", err)
			}
			tx, err := d.Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, err = tx.Exec("UPDATE t SET v='Three' WHERE id=1; DELETE FROM t WHERE id=2; INSERT INTO t VALUES(5,'Café',X'05')"); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(); err != nil {
				t.Fatal("writer could not commit while scan remained online", err)
			}
			// Autocommit point-update and repeated changes must also enter capture.
			objectExec(t, d, "UPDATE t SET v='Four' WHERE id=1; UPDATE t SET payload=X'0102' WHERE id=1; INSERT INTO t VALUES(6,'Six',X'06'); DELETE FROM t WHERE id=6")
			release()
			if err = onlineWait(t, done); err != nil {
				t.Fatal(err)
			}
			if _, err = d.CheckIntegrity(); err != nil {
				t.Fatal(err)
			}
			r, err := prepared.Exec("CAFE\u0301")
			if err != nil || len(r.Rows) != 1 || r.Rows[0]["id"].Int != 5 {
				t.Fatalf("prepared collation probe: %#v, %v", r, err)
			}
			r = objectExec(t, d, "SELECT id FROM t WHERE v='FOUR'")
			if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 1 {
				t.Fatal(r)
			}
			objectExec(t, d, "INSERT INTO t VALUES(7,'Seven',X'07'); UPDATE t SET v='Eight' WHERE id=7")
			if err = d.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(dir, opt)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			if _, err = reopened.CheckIntegrity(); err != nil {
				t.Fatal(err)
			}
			r = objectExec(t, reopened, "SELECT id FROM t WHERE v='EIGHT'")
			if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 7 {
				t.Fatal(r)
			}
			if err = reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOnlineIndexBoundedPublicationCatchUp(t *testing.T) {
	d, _, _ := onlineTestDatabase(t)
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER); INSERT INTO t VALUES(1,0)")
	reached, released := make(chan struct{}), make(chan struct{})
	var retry atomic.Int64
	var once sync.Once
	release := func() { once.Do(func() { close(released) }) }
	t.Cleanup(release)
	d.onlineIndexHook = func(phase string) {
		if phase == "caught-up" {
			close(reached)
			<-released
		}
		if phase == "catch-up-retry" {
			retry.Add(1)
		}
	}
	done := onlineStart(t, d, context.Background(), "CREATE INDEX iv ON t(v)")
	onlineReached(t, reached)
	for i := 1; i <= 300; i++ {
		objectExec(t, d, fmt.Sprintf("UPDATE t SET v=%d WHERE id=1", i))
	}
	release()
	if err := onlineWait(t, done); err != nil {
		t.Fatal(err)
	}
	if retry.Load() == 0 {
		t.Fatal("large final delta was not caught up outside the exclusive publication lock")
	}
	if _, err := d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
	r := objectExec(t, d, "SELECT id FROM t WHERE v=300")
	if len(r.Rows) != 1 {
		t.Fatal(r)
	}
}

func TestOnlineUniqueIndexConcurrentDuplicates(t *testing.T) {
	for _, repair := range []bool{false, true} {
		t.Run(fmt.Sprintf("repair=%v", repair), func(t *testing.T) {
			d, _, _ := onlineTestDatabase(t)
			objectExec(t, d, "PRAGMA collation=NFC_CI; CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'Café')")
			if repair {
				objectExec(t, d, "INSERT INTO t VALUES(2,'CAFÉ')")
			}
			reached, release := onlineBarrier(t, d, "scanned")
			done := onlineStart(t, d, context.Background(), "CREATE UNIQUE INDEX iv ON t(v)")
			onlineReached(t, reached)
			if repair {
				objectExec(t, d, "DELETE FROM t WHERE id=2")
			} else {
				objectExec(t, d, "INSERT INTO t VALUES(2,'CAFE\u0301')")
			}
			release()
			err := onlineWait(t, done)
			if repair && err != nil {
				t.Fatal(err)
			}
			if !repair && (err == nil || !strings.Contains(err.Error(), "UNIQUE constraint failed")) {
				t.Fatal("missed concurrent normalized duplicate", err)
			}
			if _, err = d.CheckIntegrity(); err != nil {
				t.Fatal(err)
			}
			if !repair {
				if id := objectIndexID(t, d, "iv"); id != 0 {
					t.Fatal("failed UNIQUE index published", id)
				}
				if _, err = d.Exec("DELETE FROM t WHERE id=2; CREATE UNIQUE INDEX iv ON t(v)"); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestOnlineIndexCancellationCleansStaging(t *testing.T) {
	d, dir, opt := onlineTestDatabase(t)
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'one'),(2,'two')")
	before := objectData(t, d)
	reached, release := onlineBarrier(t, d, "scanned")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := onlineStart(t, d, ctx, "CREATE INDEX iv ON t(v)")
	onlineReached(t, reached)
	backup := filepath.Join(t.TempDir(), "backup")
	if err := d.Backup(backup); err != nil {
		t.Fatal(err)
	}
	cancel()
	release()
	if err := onlineWait(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	after := objectData(t, d)
	if len(after) != len(before) {
		t.Fatalf("canceled index left visible physical entries: before=%d after=%d", len(before), len(after))
	}
	if _, err := d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{dir, backup} {
		reopened, err := Open(path, opt)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = reopened.CheckIntegrity(); err != nil {
			t.Fatal(err)
		}
		if id := objectIndexID(t, reopened, "iv"); id != 0 {
			t.Fatal("unpublished index became visible after reopen", id)
		}
		if len(objectData(t, reopened)) != len(before) {
			t.Fatal("staged entries were not cleaned on reopen")
		}
		if err = reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOnlineIndexDropRetainsPinnedReaders(t *testing.T) {
	d, dir, opt := onlineTestDatabase(t)
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'same'),(2,'same'),(3,'other'); CREATE INDEX iv ON t(v)")
	id := objectIndexID(t, d, "iv")
	snapshot := d.lsm.Snapshot()
	defer func() {
		if snapshot != 0 {
			d.lsm.ReleaseSnapshot(snapshot)
		}
	}()
	rows, err := d.Query("SELECT id FROM t WHERE v='same' ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	objectExec(t, d, "ALTER TABLE t DROP INDEX iv")
	var got []int64
	for rows.Next() {
		var v int64
		if err = rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatal(got)
	}
	var prefix [11]byte
	prefix[0], prefix[1], prefix[10] = 2, 0xff, ':'
	for i := 0; i < 8; i++ {
		prefix[9-i] = byte(id >> (i * 8))
	}
	current := d.lsm.NewIteratorSeek(prefix[:])
	if current.Valid() && strings.HasPrefix(string(current.Key().UserKey), string(prefix[:])) {
		current.Close()
		t.Fatal("dropped index entries remain visible")
	}
	current.Close()
	pinned := d.lsm.NewIteratorAtSeek(snapshot, prefix[:])
	count := 0
	for pinned.Valid() && strings.HasPrefix(string(pinned.Key().UserKey), string(prefix[:])) {
		count++
		pinned.Next()
	}
	pinned.Close()
	if count != 3 {
		t.Fatal("drop invalidated pinned index snapshot", count)
	}
	objectExec(t, d, "CREATE INDEX iv ON t(v)")
	if newID := objectIndexID(t, d, "iv"); newID <= id {
		t.Fatal("drop/name reuse recycled index ID", id, newID)
	}
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
	d.lsm.ReleaseSnapshot(snapshot)
	// Do not leave a snapshot registered on the engine across Close.
	snapshot = 0
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err = reopened.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
	if err = reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOnlineIndexCrashCleanup(t *testing.T) {
	if os.Getenv("SLOPDB_ONLINE_INDEX_CRASH") == "1" {
		d, err := Open(os.Getenv("SLOPDB_ONLINE_INDEX_DIR"), nil)
		if err != nil {
			t.Fatal(err)
		}
		d.onlineIndexHook = func(phase string) {
			if phase == "scanned" {
				os.Exit(83)
			}
		}
		_, err = d.Exec("CREATE INDEX iv ON t(v)")
		t.Fatalf("crash hook not reached: %v", err)
	}
	dir := filepath.Join(t.TempDir(), "db")
	d, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'one'),(2,'two')")
	before := objectData(t, d)
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	child := exec.Command(os.Args[0], "-test.run=^TestOnlineIndexCrashCleanup$")
	child.Env = append(os.Environ(), "SLOPDB_ONLINE_INDEX_CRASH=1", "SLOPDB_ONLINE_INDEX_DIR="+dir, "TMPDIR="+t.TempDir())
	output, err := child.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 83 {
		t.Fatalf("crash child: %v\n%s", err, output)
	}
	d, err = Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
	if id := objectIndexID(t, d, "iv"); id != 0 {
		t.Fatal("crashed build published", id)
	}
	if len(objectData(t, d)) != len(before) {
		t.Fatal("crashed staging entries remain")
	}
	objectExec(t, d, "CREATE INDEX iv ON t(v)")
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOnlineIndexBoundedBatchAndBinaryComposite(t *testing.T) {
	d, _, _ := onlineTestDatabase(t)
	objectExec(t, d, "PRAGMA collation=NFC_CI; CREATE TABLE t(id INTEGER PRIMARY KEY,v VARCHAR(512) COLLATE BINARY,b VARBINARY(8))")
	var values []string
	for i := 1; i <= 1600; i++ {
		values = append(values, fmt.Sprintf("(%d,'%s-%04d',X'0102')", i, strings.Repeat("X", 200), i))
	}
	objectExec(t, d, "INSERT INTO t VALUES"+strings.Join(values, ","))
	objectExec(t, d, "INSERT INTO t VALUES(1601,'é',X'01'),(1602,'é',X'01'),(1603,'É',X'01')")
	objectExec(t, d, "CREATE UNIQUE INDEX composite_idx ON t(v,b)")
	if _, err := d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"é", "é", "É"} {
		r := objectExec(t, d, "SELECT id FROM t WHERE v='"+text+"' AND b=X'01'")
		if len(r.Rows) != 1 {
			t.Fatal("binary composite index conflated distinct strings", text, r)
		}
	}
	objectExec(t, d, "DROP INDEX composite_idx; BEGIN; CREATE UNIQUE INDEX composite_idx ON t(v,b); COMMIT")
	if _, err := d.CheckIntegrity(); err != nil {
		t.Fatal("transactional UNIQUE index binary override", err)
	}
}

func TestOnlineIndexDropRollbackAndConstraints(t *testing.T) {
	d, _, _ := onlineTestDatabase(t)
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT UNIQUE); INSERT INTO t VALUES(1,'one'); CREATE UNIQUE INDEX iv ON t(v)")
	before := objectData(t, d)
	objectExec(t, d, "BEGIN; DROP INDEX iv; ROLLBACK")
	if len(objectData(t, d)) != len(before) {
		t.Fatal("rolled-back drop deleted physical entries")
	}
	if _, err := d.Exec("DROP INDEX t_v_idx"); err == nil {
		t.Fatal("drop accepted constraint index")
	}
	objectExec(t, d, "CREATE TABLE child(id INTEGER PRIMARY KEY,v TEXT REFERENCES t(v))")
	objectExec(t, d, "DROP INDEX iv") // The inline UNIQUE index still supports the FK.
	if _, err := d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
	objectExec(t, d, "CREATE TABLE parent(id INTEGER PRIMARY KEY,v TEXT); CREATE UNIQUE INDEX parent_key ON parent(v); CREATE TABLE dependent(id INTEGER PRIMARY KEY,v TEXT REFERENCES parent(v))")
	if _, err := d.Exec("DROP INDEX parent_key"); err == nil {
		t.Fatal("drop removed the only FK candidate-key index")
	}
}

func TestOnlineIndexConcurrentBuilders(t *testing.T) {
	d, _, _ := onlineTestDatabase(t)
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'one'),(2,'two')")
	reached, released := make(chan struct{}), make(chan struct{})
	var scanned atomic.Int64
	var once sync.Once
	release := func() { once.Do(func() { close(released) }) }
	t.Cleanup(release)
	d.onlineIndexHook = func(phase string) {
		if phase == "scanned" {
			if scanned.Add(1) == 2 {
				close(reached)
			}
			<-released
		}
	}
	first := onlineStart(t, d, context.Background(), "CREATE INDEX iv ON t(v)")
	second := onlineStart(t, d, context.Background(), "CREATE UNIQUE INDEX other ON t(v)")
	onlineReached(t, reached)
	objectExec(t, d, "UPDATE t SET v='changed' WHERE id=1")
	release()
	if err := onlineWait(t, first); err != nil {
		t.Fatal(err)
	}
	if err := onlineWait(t, second); err != nil {
		t.Fatal(err)
	}
	if _, err := d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
}

func TestOnlineIndexPublicationRevalidatesSchema(t *testing.T) {
	for _, change := range []string{"ALTER TABLE t RENAME TO renamed", "PRAGMA collation=NFC_CI"} {
		t.Run(change, func(t *testing.T) {
			d, _, _ := onlineTestDatabase(t)
			objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'one')")
			reached, release := onlineBarrier(t, d, "publication-gap")
			done := onlineStart(t, d, context.Background(), "CREATE INDEX iv ON t(v)")
			onlineReached(t, reached)
			objectExec(t, d, change)
			release()
			err := onlineWait(t, done)
			if !errors.Is(err, sql.ErrConflict) {
				t.Fatal("schema change was not rejected safely", err)
			}
			if _, err = d.CheckIntegrity(); err != nil {
				t.Fatal(err)
			}
			if id := objectIndexID(t, d, "iv"); id != 0 {
				t.Fatal("stale index published", id)
			}
		})
	}
}

func TestOnlineIndexWaitsOnlyAtPublicationForWriter(t *testing.T) {
	d, _, _ := onlineTestDatabase(t)
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'one')")
	tx, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec("UPDATE t SET v='two' WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	reached, release := onlineBarrier(t, d, "scanned")
	done := onlineStart(t, d, context.Background(), "CREATE INDEX iv ON t(v)")
	onlineReached(t, reached) // Scan finished while the writer transaction stayed open.
	release()
	waitCatalogRequest(t, d, true)
	if _, err = tx.Exec("UPDATE t SET v='three' WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = onlineWait(t, done); err != nil {
		t.Fatal(err)
	}
	r := objectExec(t, d, "SELECT id FROM t WHERE v='three'")
	if len(r.Rows) != 1 {
		t.Fatal(r)
	}
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
}
