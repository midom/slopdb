package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func requireOnlyBottomLevel(t *testing.T, l *LSMTree) {
	t.Helper()
	stats := l.Stats()
	for level, files := range stats.Levels {
		if level != maxLevels-1 && files.FileCount != 0 {
			t.Fatalf("full compaction left %d files at L%d", files.FileCount, level)
		}
	}
	if stats.Levels[maxLevels-1].FileCount == 0 {
		t.Fatal("full compaction produced no bottom-level files")
	}
	paths, err := filepath.Glob(filepath.Join(l.dir, "*.sst"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != stats.Levels[maxLevels-1].FileCount {
		t.Fatalf("obsolete SSTables remain: paths=%v levels=%+v", paths, stats.Levels)
	}
	if stats.MemSize != 0 || stats.ImmSize != 0 {
		t.Fatalf("full compaction did not flush writes: %+v", stats)
	}
}

func TestFullCompactionMergesEveryLevelAndRewritesAllPages(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "db")
	options := pageTestOptions()
	options.PageSize = 16 << 10
	l, err := NewLSMTree(directory, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	if err = l.WriteBatch(pageFixtureEntries(0, 1000)); err != nil {
		t.Fatal(err)
	}
	// CompactRange itself must flush a nonempty memtable, even with no SSTables.
	if err = l.CompactRange(); err != nil {
		t.Fatal(err)
	}
	requireOnlyBottomLevel(t, l)
	snapshot := l.Snapshot()
	defer l.ReleaseSnapshot(snapshot)
	oldIterator := l.NewIteratorAt(snapshot)
	defer oldIterator.Close()
	if err = l.WriteBatch(pageFixtureEntries(1000, 1000)); err != nil {
		t.Fatal(err)
	}
	if err = l.Flush(); err != nil {
		t.Fatal(err)
	}
	// Create a real intermediate level with the normal incremental compaction path.
	l.mu.Lock()
	err = l.compactLevelLocked(0)
	if err == nil {
		err = l.compactLevelLocked(1)
	}
	l.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err = l.DeleteRange([]byte("key00000100"), []byte("key00000200")); err != nil {
		t.Fatal(err)
	}
	if err = l.Put([]byte("key00000500"), []byte("updated")); err != nil {
		t.Fatal(err)
	}
	if err = l.Flush(); err != nil {
		t.Fatal(err)
	}
	before := l.Stats()
	if before.Levels[0].FileCount == 0 || before.Levels[2].FileCount == 0 || before.Levels[maxLevels-1].FileCount == 0 {
		t.Fatalf("fixture needs L0, L2 and L6: %+v", before.Levels)
	}
	// This last pending write must also be part of the full output.
	if err = l.Put([]byte("key00002000"), []byte("pending")); err != nil {
		t.Fatal(err)
	}
	if err = l.SetPageSize(1 << 20); err != nil {
		t.Fatal(err)
	}
	if err = l.CompactRange(); err != nil {
		t.Fatal(err)
	}
	requireOnlyBottomLevel(t, l)
	requirePhysicalTarget(t, l, 1<<20)
	after := l.Stats()
	if after.CompletedCompactions != before.CompletedCompactions+1 {
		t.Fatalf("full compaction should perform one merge: before=%d after=%d", before.CompletedCompactions, after.CompletedCompactions)
	}
	originalFiles := make(map[uint64]bool)
	for _, level := range before.Levels {
		for _, file := range level.Files {
			originalFiles[file.Number] = true
		}
	}
	for _, file := range after.Levels[maxLevels-1].Files {
		if originalFiles[file.Number] {
			t.Fatalf("file %d was not rewritten", file.Number)
		}
	}
	count := 0
	for oldIterator.Valid() {
		if !bytes.Equal(oldIterator.Value(), bytes.Repeat([]byte("v"), 1024)) {
			t.Fatal("pinned reader changed during rewrite")
		}
		count++
		oldIterator.Next()
	}
	if err = IteratorError(oldIterator); err != nil {
		t.Fatal(err)
	}
	if count != 1000 {
		t.Fatalf("pinned reader lost rows: %d", count)
	}
	for _, key := range []string{"key00000100", "key00000199", "key00000500"} {
		value, found, e := l.GetAt([]byte(key), snapshot)
		if e != nil || !found || len(value) != 1024 {
			t.Fatalf("old snapshot %s: found=%v len=%d err=%v", key, found, len(value), e)
		}
	}
	checkCurrent := func() {
		for _, key := range []string{"key00000100", "key00000199"} {
			_, found, e := l.Get([]byte(key))
			if e != nil || found {
				t.Fatalf("range tombstone lost for %s: found=%v err=%v", key, found, e)
			}
		}
		for key, expected := range map[string]string{"key00000500": "updated", "key00002000": "pending"} {
			value, found, e := l.Get([]byte(key))
			if e != nil || !found || string(value) != expected {
				t.Fatalf("current %s: found=%v value=%q err=%v", key, found, value, e)
			}
		}
		if _, e := l.CheckIntegrity(nil); e != nil {
			t.Fatal(e)
		}
	}
	checkCurrent()
	// Release readers before reopening, while testing their complete old contents above.
	oldIterator.Close()
	l.ReleaseSnapshot(snapshot)
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err = NewLSMTree(directory, pageTestOptions())
	if err != nil {
		t.Fatal(err)
	}
	requireOnlyBottomLevel(t, l)
	requirePhysicalTarget(t, l, 1<<20)
	checkCurrent()
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFullCompactionManifestFailurePreservesEveryInput(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "db")
	l, err := NewLSMTree(directory, pageTestOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	if err = l.WriteBatch(pageFixtureEntries(0, 100)); err != nil {
		t.Fatal(err)
	}
	if err = l.Flush(); err != nil {
		t.Fatal(err)
	}
	before := l.Stats()
	manifest := filepath.Join(directory, "MANIFEST")
	backup := filepath.Join(directory, "manifest-backup")
	if err = os.Rename(manifest, backup); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(manifest, 0700); err != nil {
		t.Fatal(err)
	}
	restored := false
	restore := func() {
		if !restored {
			_ = os.Remove(manifest)
			_ = os.Rename(backup, manifest)
			restored = true
		}
	}
	defer restore()
	if err = l.CompactRange(); err == nil {
		t.Fatal("compaction ignored failed manifest installation")
	}
	if after := l.Stats(); !reflect.DeepEqual(before.Levels, after.Levels) || before.CompletedCompactions != after.CompletedCompactions {
		t.Fatalf("failed compaction changed inputs/counters: before=%+v after=%+v", before, after)
	}
	paths, err := filepath.Glob(filepath.Join(directory, "*.sst"))
	if err != nil || len(paths) != before.Levels[0].FileCount {
		t.Fatalf("failed compaction leaked outputs: paths=%v err=%v", paths, err)
	}
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%08d", i))
		value, found, e := l.Get(key)
		if e != nil || !found || len(value) != 1024 {
			t.Fatalf("input row lost after failed compaction: key=%q found=%v err=%v", key, found, e)
		}
	}
	restore()
	if err = l.CompactRange(); err != nil {
		t.Fatal(err)
	}
	requireOnlyBottomLevel(t, l)
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
}
