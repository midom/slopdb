//go:build linux

package replication

import (
	"context"
	"fmt"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Inspect real syscalls around established-term writes. No timing or throughput
// threshold is involved: ordinary async commits must skip WAL fsync, while an
// explicit durable barrier must perform it.
func TestDurabilityWALSyscalls(t *testing.T) {
	if mode := os.Getenv("SLOPDB_DURABILITY_SYSCALL_CHILD"); mode != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		node, err := Open(ctx, Config{ID: "syscalls", Directory: os.Getenv("SLOPDB_DURABILITY_SYSCALL_DIRECTORY"), Listen: "tcp://127.0.0.1:0", ControlListen: "tcp://127.0.0.1:0", Token: testToken, Bootstrap: true, Durability: Durability(mode), DurabilityInterval: time.Hour, MaxDurabilityLag: 2 * time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = node.Close() })
		if err = node.WaitReady(ctx); err != nil {
			t.Fatal(err)
		}
		exec(t, node, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); BEGIN; INSERT INTO items VALUES(1,'stable'); COMMIT DURABLE")
		fmt.Println("SLOPDB_COMMIT_BEGIN")
		exec(t, node, "BEGIN; INSERT INTO items VALUES(2,'next'); COMMIT ASYNC")
		fmt.Println("SLOPDB_COMMIT_END")
		fmt.Println("SLOPDB_FLUSH_BEGIN")
		if err = node.FlushDurable(ctx); err != nil {
			t.Fatal(err)
		}
		fmt.Println("SLOPDB_FLUSH_END")
		if err = node.Close(); err != nil {
			t.Fatal(err)
		}
		return
	}
	strace, err := osexec.LookPath("strace")
	if err != nil {
		t.Skip("strace is unavailable")
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []Durability{DurabilitySync, DurabilityAsync} {
		t.Run(string(mode), func(t *testing.T) {
			root := t.TempDir()
			trace := filepath.Join(root, "syscalls.log")
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			child := osexec.CommandContext(ctx, strace, "-f", "-yy", "-e", "trace=write,fsync,fdatasync", "-o", trace, executable, "-test.run=^TestDurabilityWALSyscalls$", "-test.count=1")
			child.Env = append(os.Environ(), "SLOPDB_DURABILITY_SYSCALL_CHILD="+string(mode), "SLOPDB_DURABILITY_SYSCALL_DIRECTORY="+filepath.Join(root, "node"))
			if output, err := child.CombinedOutput(); err != nil {
				t.Fatalf("syscall child: %v\n%s", err, output)
			}
			value, err := os.ReadFile(trace)
			if err != nil {
				t.Fatal(err)
			}
			walSyncs := func(begin, end string) int {
				t.Helper()
				trace := string(value)
				start, finish := strings.Index(trace, begin), strings.Index(trace, end)
				if start < 0 || finish <= start {
					t.Fatal("missing syscall phase markers")
				}
				count := 0
				for _, line := range strings.Split(trace[start:finish], "\n") {
					if strings.Contains(line, "/WAL-") && (strings.Contains(line, "fsync(") || strings.Contains(line, "fdatasync(")) {
						count++
					}
				}
				return count
			}
			if count := walSyncs("SLOPDB_COMMIT_BEGIN", "SLOPDB_COMMIT_END"); (count > 0) != (mode == DurabilitySync) {
				t.Fatalf("%s commit WAL fsync count=%d", mode, count)
			}
			if count := walSyncs("SLOPDB_FLUSH_BEGIN", "SLOPDB_FLUSH_END"); mode == DurabilityAsync && count == 0 {
				t.Fatal("durable barrier skipped WAL fsync")
			}
		})
	}
}
