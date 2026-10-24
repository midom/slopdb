//go:build linux

package storage

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWALSyncDirectorySyscalls(t *testing.T) {
	if dir := os.Getenv("SLOPDB_WAL_DIRECTORY_SYNC_CHILD"); dir != "" {
		wal, err := NewWalWriter(filepath.Join(dir, "generation.log"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		for i := 1; i <= 2; i++ {
			if err := wal.AddRecord([]byte(fmt.Sprintf("record-%d", i))); err != nil {
				t.Fatal(err)
			}
			fmt.Printf("WAL_SYNC_%d_BEGIN\n", i)
			if err := wal.Sync(); err != nil {
				t.Fatal(err)
			}
			fmt.Printf("WAL_SYNC_%d_END\n", i)
		}
		if err := wal.Close(); err != nil {
			t.Fatal(err)
		}
		return
	}
	strace, err := exec.LookPath("strace")
	if err != nil {
		t.Skip("strace is unavailable")
	}
	root := t.TempDir()
	dir := filepath.Join(root, "wal")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	trace := filepath.Join(root, "trace.log")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, strace, "-f", "-yy", "-e", "trace=write,fsync,fdatasync", "-o", trace, os.Args[0], "-test.run=^TestWALSyncDirectorySyscalls$", "-test.count=1")
	child.Env = append(os.Environ(), "SLOPDB_WAL_DIRECTORY_SYNC_CHILD="+dir)
	if output, err := child.CombinedOutput(); err != nil {
		t.Fatalf("syscall child: %v\n%s", err, output)
	}
	data, err := os.ReadFile(trace)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 2; i++ {
		value := string(data)
		begin, end := strings.Index(value, fmt.Sprintf("WAL_SYNC_%d_BEGIN", i)), strings.Index(value, fmt.Sprintf("WAL_SYNC_%d_END", i))
		if begin < 0 || end <= begin {
			t.Fatal("missing sync phase markers")
		}
		files, directories := 0, 0
		for _, line := range strings.Split(value[begin:end], "\n") {
			if !strings.Contains(line, "fsync(") && !strings.Contains(line, "fdatasync(") {
				continue
			}
			if strings.Contains(line, "<"+filepath.Join(dir, "generation.log")+">") {
				files++
			}
			if strings.Contains(line, "<"+dir+">") {
				directories++
			}
		}
		wantDirectory := 0
		if i == 1 {
			wantDirectory = 1
		}
		if files != 1 || directories != wantDirectory {
			t.Fatalf("sync %d: file fsyncs=%d directory fsyncs=%d want=1/%d\n%s", i, files, directories, wantDirectory, value[begin:end])
		}
	}
}
