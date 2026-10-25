package db

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"slopdb/storage"
)

func temporaryLSMCount(t *testing.T, d *Database) int {
	t.Helper()
	it := d.lsm.NewIteratorRange([]byte{3}, []byte{4}, false)
	defer it.Close()
	n := 0
	for ; it.Valid(); it.Next() {
		n++
	}
	if err := storage.IteratorError(it); err != nil {
		t.Fatal(err)
	}
	return n
}
func TestTemporaryTablesUseWALlessLSMRanges(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	s, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	execute := func(q string) {
		t.Helper()
		if _, e := s.Exec(q); e != nil {
			t.Fatal(q, e)
		}
	}
	execute("CREATE TABLE durable(id INTEGER PRIMARY KEY,v TEXT)")
	execute("CREATE TEMPORARY TABLE scratch(id INTEGER PRIMARY KEY,v TEXT,KEY iv(v))")
	execute("BEGIN")
	execute("INSERT INTO durable VALUES(1,'durable-marker-927')")
	execute("INSERT INTO scratch VALUES(1,'temporary-marker-618')")
	execute("COMMIT")
	if n := temporaryLSMCount(t, d); n != 2 {
		t.Fatalf("temporary physical rows/indexes=%d, want2", n)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	durable := false
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "WAL-") {
			continue
		}
		b, e := os.ReadFile(filepath.Join(dir, entry.Name()))
		if e != nil {
			t.Fatal(e)
		}
		if bytes.Contains(b, []byte("temporary-marker-618")) {
			t.Fatal("temporary data in WAL")
		}
		durable = durable || bytes.Contains(b, []byte("durable-marker-927"))
	}
	if !durable {
		t.Fatal("mixed transaction durable data missing from WAL")
	}
	// LSM maintenance really flushes private data into SSTables; it is not held
	// in a separate session map. Session close removes the whole reserved range.
	if err = d.lsm.CompactRange(); err != nil {
		t.Fatal(err)
	}
	if n := temporaryLSMCount(t, d); n != 2 {
		t.Fatal(n)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if n := temporaryLSMCount(t, d); n != 0 {
		t.Fatalf("session-close leftovers=%d", n)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n := temporaryLSMCount(t, d); n != 0 {
		t.Fatal(n)
	}
	r := objectExec(t, d, "SELECT v FROM durable WHERE id=1")
	if len(r.Rows) != 1 || r.Rows[0]["v"].Text != "durable-marker-927" {
		t.Fatal(r.Rows)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTemporaryCrashRangesCleanedOnOpen(t *testing.T) {
	if os.Getenv("SLOPDB_TEMP_LSM_CRASH") == "1" {
		d, err := Open(os.Getenv("SLOPDB_TEMP_LSM_DIR"), nil)
		if err != nil {
			t.Fatal(err)
		}
		objectExec(t, d, "CREATE TABLE durable(id INTEGER PRIMARY KEY);INSERT INTO durable VALUES(7);CREATE TEMPORARY TABLE scratch(id INTEGER PRIMARY KEY,v TEXT,KEY iv(v));INSERT INTO scratch VALUES(1,'crash-private')")
		if os.Getenv("SLOPDB_TEMP_LSM_FLUSH") == "1" {
			if err = d.lsm.CompactRange(); err != nil {
				t.Fatal(err)
			}
		}
		if temporaryLSMCount(t, d) == 0 {
			t.Fatal("no private LSM data")
		}
		os.Exit(86)
	}
	for _, flush := range []string{"0", "1"} {
		t.Run("flush="+flush, func(t *testing.T) {
			dir := t.TempDir()
			child := exec.Command(os.Args[0], "-test.run=^TestTemporaryCrashRangesCleanedOnOpen$")
			child.Env = append(os.Environ(), "SLOPDB_TEMP_LSM_CRASH=1", "SLOPDB_TEMP_LSM_DIR="+dir, "SLOPDB_TEMP_LSM_FLUSH="+flush)
			output, err := child.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 86 {
				t.Fatalf("crash child: %v\n%s", err, output)
			}
			d, err := Open(dir, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = d.Close() })
			if n := temporaryLSMCount(t, d); n != 0 {
				t.Fatalf("startup private leftovers=%d", n)
			}
			r := objectExec(t, d, "SELECT id FROM durable")
			if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 7 {
				t.Fatal(r.Rows)
			}
			if _, err = d.Exec("SELECT * FROM scratch"); err == nil {
				t.Fatal("temporary catalog persisted")
			}
			objectExec(t, d, "CREATE TEMPORARY TABLE scratch(id INTEGER PRIMARY KEY,v TEXT,KEY iv(v));INSERT INTO scratch VALUES(1,'new-session')")
			r = objectExec(t, d, "SELECT v FROM scratch")
			if len(r.Rows) != 1 || r.Rows[0]["v"].Text != "new-session" {
				t.Fatal(r.Rows)
			}
			if _, err = d.CheckIntegrity(); err != nil {
				t.Fatal(err)
			}
			if err = d.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
