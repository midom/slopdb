package storage

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestInstalledStorageWorkCounters(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "database")
	options := DefaultOptions()
	options.DisableWALSync = true
	options.CompactionL0Trigger = 100
	l, err := NewLSMTree(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	for i := 0; i < 100; i++ {
		if err := l.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	before := l.Stats()
	if before.CompletedFlushes != 1 || before.FlushOutputBytes != before.Levels[0].TotalSize {
		t.Fatalf("flush counters do not describe the installed table: %+v", before)
	}
	if err := l.CompactRange(); err != nil {
		t.Fatal(err)
	}
	after := l.Stats()
	if after.CompletedCompactions != maxLevels-1 || after.CompactionInputBytes <= 0 || after.CompactionOutputBytes <= 0 {
		t.Fatalf("missing installed compaction counters: %+v", after)
	}
	if after.CompletedFlushes != before.CompletedFlushes || after.FlushOutputBytes != before.FlushOutputBytes {
		t.Fatal("compaction counted as a memtable flush")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err = NewLSMTree(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	if s := l.Stats(); s.CompletedCompactions != 0 || s.CompletedFlushes != 0 {
		t.Fatal("per-open runtime counters persisted as data")
	}
}
