package regression

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"slopdb/db"
	"slopdb/storage"
)

func requireExec(t *testing.T, d *db.Database, query string) *db.ExecResult {
	t.Helper()
	r, err := d.Exec(query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return r
}

func requireNoWAL(t *testing.T, dir string) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "WAL-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatalf("unexpected WAL files: %v", files)
	}
}

func crashCommand(t *testing.T, mode, dir string) *exec.Cmd {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestCrashHelper$")
	cmd.Env = append(os.Environ(), "SLOPDB_REGRESSION_CHILD="+mode, "SLOPDB_REGRESSION_DIR="+dir)
	return cmd
}

func runCrashChild(t *testing.T, mode, dir string) {
	t.Helper()
	if output, err := crashCommand(t, mode, dir).CombinedOutput(); err != nil {
		t.Fatalf("crash helper %s: %v\n%s", mode, err, output)
	}
}

// These helper modes deliberately skip Close, cleanup and transaction rollback.
// Running only this test in a separate process avoids depending on a CLI binary
// or treating the go test command's flags as the old standalone program's args.
func TestCrashHelper(t *testing.T) {
	mode := os.Getenv("SLOPDB_REGRESSION_CHILD")
	if mode == "" {
		return
	}
	dir := os.Getenv("SLOPDB_REGRESSION_DIR")
	if dir == "" {
		t.Fatal("crash helper database path missing")
	}
	options := storage.DefaultOptions()
	options.DisableWALSync = strings.Contains(mode, "no-fsync")
	options.DisableWAL = mode == "recover-no-wal"
	d, err := db.Open(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	switch mode {
	case "wal", "no-fsync", "close-no-fsync":
		requireExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT UNIQUE)")
		// Force fragmented WAL records and include a committed savepoint rollback.
		requireExec(t, d, "BEGIN; INSERT INTO t VALUES(1,'"+strings.Repeat("fragment", 10000)+"'),(2,'two'); SAVEPOINT s; INSERT INTO t VALUES(8,'discard'); ROLLBACK TO s; COMMIT")
		requireExec(t, d, "BEGIN; INSERT INTO t VALUES(9,'uncommitted')")
		if mode == "close-no-fsync" {
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
		}
	case "rename-concurrent":
		requireExec(t, d, "CREATE TABLE original(id INTEGER PRIMARY KEY,v INTEGER UNIQUE); CREATE VIEW live(id,v) AS SELECT id,v FROM original; CREATE TRIGGER ins INSTEAD OF INSERT ON live BEGIN INSERT INTO original VALUES(NEW.id,NEW.v); END")
		var wg sync.WaitGroup
		errs := make(chan error, 11)
		for worker := 0; worker < 10; worker++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				session, e := d.NewSession()
				if e != nil {
					errs <- e
					return
				}
				defer session.Close()
				for n := 1; n <= 50; n++ {
					id := worker*50 + n
					if _, e = session.Exec(fmt.Sprintf("INSERT INTO live VALUES(%d,%d)", id, id*17)); e != nil {
						errs <- e
						return
					}
				}
			}(worker)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			session, e := d.NewSession()
			if e != nil {
				errs <- e
				return
			}
			defer session.Close()
			for n := 0; n < 20; n++ {
				if _, e = session.Exec("ALTER TABLE original RENAME TO renamed; ALTER TABLE renamed RENAME TO original"); e != nil {
					errs <- e
					return
				}
			}
		}()
		wg.Wait()
		close(errs)
		for e := range errs {
			if e != nil {
				t.Fatal(e)
			}
		}
		// Crash with another rename still private. Every earlier acknowledged
		// insert and committed catalog update must survive together.
		requireExec(t, d, "BEGIN; ALTER TABLE original RENAME TO uncommitted")
	case "rename-committed", "rename-uncommitted":
		requireExec(t, d, "CREATE TABLE original(id INTEGER PRIMARY KEY,v TEXT UNIQUE); CREATE INDEX lookup ON original(v); INSERT INTO original VALUES(1,'one'); CREATE VIEW visible(v) AS SELECT v FROM original")
		requireExec(t, d, "BEGIN; ALTER TABLE original RENAME TO renamed; ALTER INDEX lookup RENAME TO renamed_lookup; ALTER TABLE renamed RENAME COLUMN v TO value")
		if mode == "rename-committed" {
			requireExec(t, d, "COMMIT")
		}
	case "recover-no-fsync":
		if got := requireExec(t, d, "SELECT COUNT(*) AS n FROM t").Rows[0]["n"].Int; got != 2 {
			t.Fatalf("recovered %d rows, want 2", got)
		}
		requireExec(t, d, "INSERT INTO t VALUES(3,'after recovery')")
	case "recover-no-wal":
		requireNoWAL(t, dir)
		if got := requireExec(t, d, "SELECT COUNT(*) AS n FROM t").Rows[0]["n"].Int; got != 2 {
			t.Fatalf("recovered %d rows, want 2", got)
		}
		requireExec(t, d, "INSERT INTO t VALUES(3,'unflushed')")
		requireNoWAL(t, dir)
	default:
		t.Fatalf("unknown crash helper mode %q", mode)
	}
	os.Exit(0)
}

// Adapted from recovery_review.go, no_wal_review.go and no_wal_fsync_review.go.
func TestCrashRecovery(t *testing.T) {
	for _, mode := range []string{"wal", "no-fsync", "recover-no-fsync", "recover-no-wal"} {
		t.Run(mode, func(t *testing.T) {
			env := newRegressionEnv(t)
			dir := env.path("database")
			initial := mode
			if strings.HasPrefix(mode, "recover-") {
				initial = "wal"
			}
			runCrashChild(t, initial, dir)
			want := int64(2)
			if mode != initial {
				runCrashChild(t, mode, dir)
				if mode == "recover-no-wal" {
					requireNoWAL(t, dir)
				} else {
					want = 3
				}
			}
			for reopen := 0; reopen < 2; reopen++ {
				d, err := env.openDatabase(dir, storage.DefaultOptions())
				if err != nil {
					t.Fatal(err)
				}
				rows := requireExec(t, d, "SELECT id FROM t ORDER BY id").Rows
				if int64(len(rows)) != want || rows[0]["id"].Int != 1 || rows[1]["id"].Int != 2 {
					t.Fatalf("reopen %d: recovered rows %v, want ids 1..%d", reopen, rows, want)
				}
				if _, err := db.Open(dir, nil); err == nil {
					t.Fatal("second open bypassed database lock")
				}
				if err := d.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestRecoveryRejectsCorruption(t *testing.T) {
	t.Run("manifest", func(t *testing.T) {
		env, d, dir, options := referenceDatabase(t, "CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES(1)")
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "MANIFEST")
		original, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, original[:len(original)/2], 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := env.openDatabase(dir, options); err == nil {
			t.Fatal("truncated manifest accepted")
		}
		if err := os.WriteFile(path, original, 0600); err != nil {
			t.Fatal(err)
		}
		d, err = env.openDatabase(dir, options)
		if err != nil {
			t.Fatalf("failed open leaked lock: %v", err)
		}
		if len(requireExec(t, d, "SELECT * FROM t").Rows) != 1 {
			t.Fatal("repair changed persisted rows")
		}
	})
	t.Run("WAL", func(t *testing.T) {
		env := newRegressionEnv(t)
		dir := env.path("database")
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "WAL-1.log")
		w, err := storage.NewWalWriter(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.AddRecord(storage.EncodeWALRecord(1, []byte("key"), []byte("value"), 1)); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		original, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		bad := bytes.Clone(original)
		bad[0] ^= 1
		if err := os.WriteFile(path, bad, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := env.openStorage(dir, nil); err == nil {
			t.Fatal("bad WAL checksum accepted")
		}
		retained, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(retained, bad) {
			t.Fatalf("failed recovery changed corrupt WAL: %v", err)
		}
		if err := os.WriteFile(path, original, 0600); err != nil {
			t.Fatal(err)
		}
		l, err := env.openStorage(dir, nil)
		if err != nil {
			t.Fatal(err)
		}
		value, found, err := l.Get([]byte("key"))
		if err != nil || !found || string(value) != "value" {
			t.Fatalf("repaired WAL recovery: value=%q, found=%v, err=%v", value, found, err)
		}
	})
}

func TestNoWALRecoveryFailurePreservesWAL(t *testing.T) {
	env := newRegressionEnv(t)
	dir := env.path("database")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "WAL-1.log")
	w, err := storage.NewWalWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AddRecord(storage.EncodeWALRecord(1, []byte("legacy"), []byte("value"), 1)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	blocked := filepath.Join(dir, "MANIFEST-3.tmp")
	if err := os.Mkdir(blocked, 0700); err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultOptions()
	options.DisableWAL = true
	if _, err := env.openStorage(dir, options); err == nil {
		t.Fatal("failed recovery checkpoint accepted")
	}
	retained, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(retained, original) {
		t.Fatalf("failed checkpoint changed WAL: %v", err)
	}
	if err := os.Remove(blocked); err != nil {
		t.Fatal(err)
	}
	l, err := env.openStorage(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	value, found, err := l.Get([]byte("legacy"))
	if err != nil || !found || string(value) != "value" {
		t.Fatalf("checkpoint retry: value=%q, found=%v, err=%v", value, found, err)
	}
	requireNoWAL(t, dir)
}
