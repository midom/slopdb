package regression

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"slopdb/storage"
)

// Adapted from no_wal_review.go: clean Close must persist data and catalog
// changes even when WAL recording is disabled.
func TestNoWALSQLPersistence(t *testing.T) {
	env := newRegressionEnv(t)
	dir := env.path("database")
	options := storage.DefaultOptions()
	options.DisableWAL = true
	options.WriteBufferSize = 1024
	options.BlockSize = 128
	d, err := env.openDatabase(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	requireExec(t, d, `CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT,v TEXT UNIQUE DEFAULT 'default');
		CREATE TABLE audit(v TEXT); CREATE INDEX audit_idx ON audit(v);
		CREATE VIEW tv AS SELECT * FROM t;
		CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO audit VALUES(NEW.v); END`)
	for i := 0; i < 100; i++ {
		requireExec(t, d, fmt.Sprintf("INSERT INTO t(v) VALUES('row%d')", i))
	}
	requireExec(t, d, "BEGIN; UPDATE t SET v='changed' WHERE id=1; SAVEPOINT s; INSERT INTO t(v) VALUES('discard'); ROLLBACK TO s; COMMIT")
	requireExec(t, d, "DELETE FROM t WHERE id=2; PRAGMA user_version=17; BEGIN; INSERT INTO t(v) VALUES('unfinished'); CREATE TABLE uncommitted(v TEXT)")
	requireNoWAL(t, dir)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	requireNoWAL(t, dir)
	d, err = env.openDatabase(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		query, column string
		want          int64
	}{
		{"SELECT COUNT(*) AS n FROM tv", "n", 99},
		{"SELECT COUNT(*) AS n FROM t WHERE v IN('discard','unfinished')", "n", 0},
		{"PRAGMA user_version", "user_version", 17},
	} {
		if got := requireExec(t, d, c.query).Rows[0][c.column].Int; got != c.want {
			t.Fatalf("%s: got %d, want %d", c.query, got, c.want)
		}
	}
	if requireExec(t, d, "SELECT v FROM t WHERE id=1").Rows[0]["v"].Text != "changed" {
		t.Fatal("committed update lost")
	}
	if len(requireExec(t, d, "SHOW TABLES LIKE 'uncommitted'").Rows) != 0 {
		t.Fatal("Close persisted uncommitted schema")
	}
	if len(requireExec(t, d, "SHOW TRIGGERS").Rows) != 1 {
		t.Fatal("trigger catalog lost")
	}
	if got := requireExec(t, d, "INSERT INTO t(v) VALUES('after reopen')").LastID; got != 101 {
		t.Fatalf("autoincrement after reopen: got %d, want 101", got)
	}
	if got := requireExec(t, d, "SELECT COUNT(*) AS n FROM audit WHERE v='after reopen'").Rows[0]["n"].Int; got != 1 {
		t.Fatal("persisted trigger did not fire")
	}
	if _, err := d.Exec("INSERT INTO t(v) VALUES('changed')"); err == nil {
		t.Fatal("persisted unique constraint ignored")
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	requireNoWAL(t, dir)
}

// Adapted from no_wal_fsync_review.go: disabling fsync must retain WAL records
// and write errors, and options supplied to Open must be copied.
func TestNoFsyncWALRecording(t *testing.T) {
	env := newRegressionEnv(t)
	dir := env.path("database")
	options := storage.DefaultOptions()
	options.DisableWALSync = true
	l, err := env.openStorage(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	options.DisableWALSync = false
	if err := l.WriteBatch([]storage.Mutation{{Key: []byte("visible"), Value: []byte("record")}}); err != nil {
		t.Fatal(err)
	}
	if stats := l.Stats(); !stats.WALSyncDisabled || !stats.WALActive {
		t.Fatal("effective WAL options changed after caller mutation")
	}
	files, err := filepath.Glob(filepath.Join(dir, "WAL-*"))
	if err != nil {
		t.Fatal(err)
	}
	records := 0
	for _, path := range files {
		r, err := storage.NewWalReader(path)
		if err != nil {
			t.Fatal(err)
		}
		for {
			_, err := r.ReadRecord()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			records++
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if records != 1 {
		t.Fatalf("acknowledged WAL batch: got %d readable records, want 1", records)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDurabilityWriteErrors(t *testing.T) {
	t.Run("WAL creation", func(t *testing.T) {
		env := newRegressionEnv(t)
		dir := env.path("database")
		options := storage.DefaultOptions()
		options.DisableWALSync = true
		l, err := env.openStorage(dir, options)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(filepath.Join(dir, "WAL-1.log"), 0700); err != nil {
			t.Fatal(err)
		}
		if err := l.WriteBatch([]storage.Mutation{{Key: []byte("failed"), Value: []byte("value")}}); err == nil || l.Stats().SeqNum != 0 {
			t.Fatal("failed WAL batch acknowledged or advanced sequence")
		}
		if _, found, err := l.Get([]byte("failed")); err != nil || found {
			t.Fatalf("failed WAL batch published: found=%v, err=%v", found, err)
		}
	})
	t.Run("no WAL flush", func(t *testing.T) {
		env := newRegressionEnv(t)
		dir := env.path("database")
		options := storage.DefaultOptions()
		options.DisableWAL = true
		d, err := env.openDatabase(dir, options)
		if err != nil {
			t.Fatal(err)
		}
		requireExec(t, d, "CREATE TABLE t(v TEXT); INSERT INTO t VALUES('pending')")
		if err := os.Mkdir(filepath.Join(dir, fmt.Sprintf("%d.sst", d.Stats().FileNum)), 0700); err != nil {
			t.Fatal(err)
		}
		if d.Close() == nil || d.Close() == nil {
			t.Fatal("Close discarded SSTable flush error")
		}
		requireNoWAL(t, dir)
	})
}

// This extra Linux check is optional; the ordinary recovery and WAL-record
// tests above have no strace dependency. It verifies actual fsync syscalls.
func TestWALFsyncSyscalls(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("strace fsync verification requires Linux")
	}
	strace, err := exec.LookPath("strace")
	if err != nil {
		t.Skip("strace is unavailable")
	}
	for _, mode := range []string{"wal", "no-fsync", "close-no-fsync"} {
		t.Run(mode, func(t *testing.T) {
			env := newRegressionEnv(t)
			trace := env.path("syscalls.log")
			child := crashCommand(t, mode, env.path("database"))
			args := append([]string{"-f", "-yy", "-e", "trace=fsync,fdatasync", "-o", trace}, child.Args...)
			cmd := exec.Command(strace, args...)
			cmd.Env = child.Env
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("strace: %v\n%s", err, output)
			}
			data, err := os.ReadFile(trace)
			if err != nil {
				t.Fatal(err)
			}
			walSynced := bytes.Contains(data, []byte("/WAL-"))
			if walSynced != (mode == "wal") {
				t.Fatalf("mode %s: WAL fsync=%v\n%s", mode, walSynced, data)
			}
			if mode == "close-no-fsync" && (!bytes.Contains(data, []byte(".sst>")) || !bytes.Contains(data, []byte("MANIFEST-"))) {
				t.Fatalf("SSTable or MANIFEST synchronization missing:\n%s", data)
			}
		})
	}
}

// Adapted from the CLI and combined-options checks in no_wal_fsync_review.go.
func TestNoFsyncCLI(t *testing.T) {
	for _, flag := range []string{"--no-wal-fsync", "--no-fsync"} {
		t.Run(flag, func(t *testing.T) {
			env := newRegressionEnv(t)
			first, second := env.path("first"), env.path("second")
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, env.cli(), flag, "--allow-dot-commands", first)
			cmd.Stdin = strings.NewReader("CREATE TABLE t(id INTEGER); INSERT INTO t VALUES(1);\n.manifest\n.open " + second + "\nCREATE TABLE t(id INTEGER); INSERT INTO t VALUES(2);\n.manifest\n")
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("CLI %s: %v\n%s", flag, err, output)
			}
			if strings.Count(string(output), "active (fsync disabled)") != 2 {
				t.Fatalf(".open lost WAL fsync mode:\n%s", output)
			}
			for i, path := range []string{first, second} {
				d, err := env.openDatabase(path, nil)
				if err != nil {
					t.Fatal(err)
				}
				rows := requireExec(t, d, "SELECT id FROM t").Rows
				if len(rows) != 1 || rows[0]["id"].Int != int64(i+1) {
					t.Fatalf("CLI persistence: got %v, want id %d", rows, i+1)
				}
				if err := d.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestDisableWALPrecedence(t *testing.T) {
	env := newRegressionEnv(t)
	dir := env.path("database")
	options := storage.DefaultOptions()
	options.DisableWAL = true
	options.DisableWALSync = true
	l, err := env.openStorage(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if stats := l.Stats(); stats.WALActive || stats.WALSyncDisabled {
		t.Fatal("DisableWAL did not take precedence over DisableWALSync")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	requireNoWAL(t, dir)
}
