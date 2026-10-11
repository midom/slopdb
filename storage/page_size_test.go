package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func pageFixtureEntries(start, count int) []Mutation {
	entries := make([]Mutation, count)
	payload := bytes.Repeat([]byte("v"), 1024)
	for i := range entries {
		entries[i] = Mutation{Key: []byte(fmt.Sprintf("key%08d", start+i)), Value: payload}
	}
	return entries
}
func pageTestOptions() *Options {
	options := DefaultOptions()
	options.DisableWALSync = true
	options.CompressionType = NoCompression
	options.CompactionL0Trigger = 1000
	options.WriteBufferSize = 8 << 20
	return options
}
func physicalPageBlocks(t *testing.T, l *LSMTree) []int {
	t.Helper()
	var sizes []int
	for _, level := range l.Stats().Levels {
		for _, file := range level.Files {
			reader, err := OpenSSTable(filepath.Join(l.dir, fmt.Sprintf("%d.sst", file.Number)))
			if err != nil {
				t.Fatal(err)
			}
			index := reader.indexBlock.NewIterator()
			for index.Valid() {
				handle := index.Value()
				offset, n := binary.Uvarint(handle)
				if n <= 0 {
					t.Fatal("bad block handle")
				}
				size, n := binary.Uvarint(handle[n:])
				if n <= 0 {
					t.Fatal("bad block length")
				}
				raw, err := reader.loadBlock(offset, size)
				if err != nil {
					t.Fatal(err)
				}
				sizes = append(sizes, len(raw))
				index.Next()
			}
			if err := index.Err(); err != nil {
				t.Fatal(err)
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	return sizes
}
func requirePhysicalTarget(t *testing.T, l *LSMTree, target int64) {
	t.Helper()
	sizes := physicalPageBlocks(t, l)
	full := 0
	for _, size := range sizes {
		if int64(size) >= target {
			full++
		}
		if int64(size) > target+target/100+2048 {
			t.Fatalf("block %d too large for target %d (sizes %v)", size, target, sizes)
		}
	}
	if full == 0 {
		t.Fatalf("no full %d-byte blocks among %v", target, sizes)
	}
}
func TestPageSizeWritesRealBlocksAndPersists(t *testing.T) {
	for _, size := range []int64{16 << 10, 64 << 10, 1 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "db")
			options := pageTestOptions()
			options.PageSize = size
			l, err := NewLSMTree(directory, options)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = l.Close() })
			if l.PageSize() != size || l.Stats().PageSize != size {
				t.Fatal(l.PageSize(), l.Stats().PageSize)
			}
			if err = l.WriteBatch(pageFixtureEntries(0, 3000)); err != nil {
				t.Fatal(err)
			}
			if err = l.Flush(); err != nil {
				t.Fatal(err)
			}
			requirePhysicalTarget(t, l, size)
			for _, key := range []string{"key00000000", "key00001500", "key00002999"} {
				value, found, e := l.Get([]byte(key))
				if e != nil || !found || len(value) != 1024 {
					t.Fatal(key, e, found, len(value))
				}
			}
			if err = l.Close(); err != nil {
				t.Fatal(err)
			}
			options = pageTestOptions()
			l, err = NewLSMTree(directory, options)
			if err != nil {
				t.Fatal(err)
			}
			if l.PageSize() != size {
				t.Fatal("reopen lost page size", l.PageSize())
			}
			requirePhysicalTarget(t, l, size)
			if _, err = l.CheckIntegrity(nil); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestPageSizeMixedFilesSnapshotsAndVacuumRewrite(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "db")
	options := pageTestOptions()
	options.PageSize = 16 << 10
	l, err := NewLSMTree(directory, options)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err = l.WriteBatch(pageFixtureEntries(0, 3000)); err != nil {
		t.Fatal(err)
	}
	if err = l.Flush(); err != nil {
		t.Fatal(err)
	}
	snapshot := l.Snapshot()
	defer l.ReleaseSnapshot(snapshot)
	if err = l.SetPageSize(64 << 10); err != nil {
		t.Fatal(err)
	}
	if err = l.WriteBatch(pageFixtureEntries(3000, 3000)); err != nil {
		t.Fatal(err)
	}
	if err = l.Put([]byte("key00000000"), []byte("updated")); err != nil {
		t.Fatal(err)
	}
	if err = l.Flush(); err != nil {
		t.Fatal(err)
	}
	sizes := physicalPageBlocks(t, l)
	small, large := false, false
	for _, size := range sizes {
		small = small || (size >= 16<<10 && size < 20<<10)
		large = large || size >= 64<<10
	}
	if !small || !large {
		t.Fatal("changing target rewrote existing files or did not affect new writers", sizes)
	}
	if err = l.SetPageSize(1 << 20); err != nil {
		t.Fatal(err)
	}
	if err = l.CompactRange(); err != nil {
		t.Fatal(err)
	}
	requirePhysicalTarget(t, l, 1<<20)
	oldCount := len(physicalPageBlocks(t, l))
	value, found, err := l.GetAt([]byte("key00000000"), snapshot)
	if err != nil || !found || len(value) != 1024 {
		t.Fatal("compaction broke pinned snapshot", err, found, len(value))
	}
	if err = l.SetPageSize(16 << 10); err != nil {
		t.Fatal(err)
	}
	// Everything is already at L6. This second manual compaction must rewrite it.
	if err = l.CompactRange(); err != nil {
		t.Fatal(err)
	}
	requirePhysicalTarget(t, l, 16<<10)
	if count := len(physicalPageBlocks(t, l)); count <= oldCount*20 {
		t.Fatal("bottom-level files were not rewritten", oldCount, count)
	}
	value, found, err = l.GetAt([]byte("key00000000"), snapshot)
	if err != nil || !found || len(value) != 1024 {
		t.Fatal("rewrite broke pinned snapshot", err, found, len(value))
	}
	value, found, err = l.Get([]byte("key00000000"))
	if err != nil || !found || string(value) != "updated" {
		t.Fatal(err, found, string(value))
	}
	if _, err = l.CheckIntegrity(nil); err != nil {
		t.Fatal(err)
	}
}
func TestPageSizeLegacyTargetsAndValidation(t *testing.T) {
	for _, size := range []int{64, 127, 4096} {
		directory := filepath.Join(t.TempDir(), "db")
		options := pageTestOptions()
		options.BlockSize = size
		l, err := NewLSMTree(directory, options)
		if err != nil {
			t.Fatal(err)
		}
		if l.PageSize() != int64(size) {
			t.Fatal(l.PageSize(), size)
		}
		if err = l.Close(); err != nil {
			t.Fatal(err)
		}
		l, err = NewLSMTree(directory, pageTestOptions())
		if err != nil {
			t.Fatal(err)
		}
		if l.PageSize() != int64(size) {
			t.Fatal("legacy target lost", l.PageSize(), size)
		}
		if err = l.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for _, size := range []int64{-1, 1, 511, 513, 3 << 10, 1 << 32, 1 << 62} {
		options := pageTestOptions()
		options.PageSize = size
		if l, err := NewLSMTree(filepath.Join(t.TempDir(), "db"), options); err == nil {
			l.Close()
			t.Fatal("invalid page size accepted", size)
		}
	}
}
func TestPageSizeOptionsCorruptionRetainsWAL(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "db")
	l, err := NewLSMTree(directory, pageTestOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	optionsFile := filepath.Join(directory, pageOptionsName)
	data, err := os.ReadFile(optionsFile)
	if err != nil {
		t.Fatal(err)
	}
	data[12] ^= 1
	if err = os.WriteFile(optionsFile, data, 0600); err != nil {
		t.Fatal(err)
	}
	evidence := filepath.Join(directory, "WAL-999.log")
	if err = os.WriteFile(evidence, []byte("retain evidence"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = NewLSMTree(directory, pageTestOptions()); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatal(err)
	}
	if data, err = os.ReadFile(evidence); err != nil || string(data) != "retain evidence" {
		t.Fatal("invalid options destroyed WAL evidence", err)
	}
}
func TestPageSizeConcurrentChangesAndFlushes(t *testing.T) {
	l, err := NewLSMTree(filepath.Join(t.TempDir(), "db"), pageTestOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	var workers sync.WaitGroup
	failures := make(chan error, 2)
	workers.Add(2)
	go func() {
		defer workers.Done()
		for i := 0; i < 20; i++ {
			if err := l.SetPageSize([]int64{16 << 10, 64 << 10, 1 << 20}[i%3]); err != nil {
				failures <- err
				return
			}
		}
	}()
	go func() {
		defer workers.Done()
		for i := 0; i < 20; i++ {
			if err := l.WriteBatch(pageFixtureEntries(i*10, 10)); err != nil {
				failures <- err
				return
			}
			if err := l.Flush(); err != nil {
				failures <- err
				return
			}
		}
	}()
	workers.Wait()
	close(failures)
	for failure := range failures {
		t.Fatal(failure)
	}
	if _, err = l.CheckIntegrity(nil); err != nil {
		t.Fatal(err)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	if err = l.SetPageSize(16 << 10); err == nil {
		t.Fatal("closed storage accepted size change")
	}
}
func TestPageSizeCrashHelper(t *testing.T) {
	directory := os.Getenv("SLOPDB_PAGE_SIZE_CRASH_DIR")
	if directory == "" {
		return
	}
	options := pageTestOptions()
	options.PageSize = 1 << 20
	options.WriteBufferSize = 64 << 20
	l, err := NewLSMTree(directory, options)
	if err != nil {
		t.Fatal(err)
	}
	if err = l.WriteBatch(pageFixtureEntries(0, 3000)); err != nil {
		t.Fatal(err)
	}
	os.Exit(0) // deliberately omit Close: all data must come from WAL recovery
}
func TestPageSizeRecoveryUsesPersistedTargetBeforeCheckpoint(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "db")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(executable, "-test.run=^TestPageSizeCrashHelper$")
	child.Env = append(os.Environ(), "SLOPDB_PAGE_SIZE_CRASH_DIR="+directory)
	if output, err := child.CombinedOutput(); err != nil {
		t.Fatal(err, string(output))
	}
	l, err := NewLSMTree(directory, pageTestOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if l.PageSize() != 1<<20 {
		t.Fatal("recovery lost persisted setting", l.PageSize())
	}
	requirePhysicalTarget(t, l, 1<<20)
	value, found, err := l.Get([]byte("key00002999"))
	if err != nil || !found || len(value) != 1024 {
		t.Fatal("WAL recovery lost data", err, found, len(value))
	}
}
