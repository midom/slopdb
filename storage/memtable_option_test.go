package storage

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestMemTableDefaultScanDoesNotBuildSortedCopy(t *testing.T) {
	m := NewMemTable()
	for n := 10000; n > 0; n-- {
		m.Put(uint64(10001-n), kTypeValue, []byte(fmt.Sprintf("%05d", n)), []byte("value"))
	}
	// Open a narrow scan after every write. A default scan must not copy and
	// sort the entire mutable table, even when its contents change between scans.
	for n := 0; n < 10; n++ {
		m.Put(uint64(10001+n), kTypeValue, []byte("05000"), []byte("updated"))
		it := m.NewIterator()
		it.Seek([]byte("05000"))
		if !it.Valid() || string(it.Value()) != "updated" {
			t.Fatal("scan did not see committed update")
		}
		it.SkipUserKey([]byte("05000"))
		if !it.Valid() || string(it.Key().UserKey) != "05001" {
			t.Fatal("scan skipped wrong key")
		}
		it.Close()
	}
	if len(m.nodes) != 0 || m.sorted.Load() != nil {
		t.Fatal("default scan built a hash append-log sort")
	}
}

func TestMemTableModeFlushAndReopen(t *testing.T) {
	for _, hash := range []bool{false, true} {
		for _, noWAL := range []bool{false, true} {
			t.Run(fmt.Sprintf("hash=%v/noWAL=%v", hash, noWAL), func(t *testing.T) {
				opts := DefaultOptions()
				opts.HashMemTable = hash
				opts.DisableWAL = noWAL
				opts.DisableWALSync = true
				opts.WriteBufferSize = 8 << 10
				opts.CompactionL0Trigger = 100
				dir := t.TempDir()
				l, err := NewLSMTree(dir, opts)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = l.Close() }()
				for round := 0; round < 3; round++ {
					batch := make([]Mutation, 100)
					for n := range batch {
						batch[n] = Mutation{Key: []byte(fmt.Sprintf("%03d", n)), Value: bytes.Repeat([]byte{byte(round)}, 64)}
					}
					if err = l.WriteBatch(batch); err != nil {
						t.Fatal(err)
					}
					if err = l.Flush(); err != nil {
						t.Fatal(err)
					}
					if (l.mem.head == nil) != hash {
						t.Fatal("flush changed memtable mode")
					}
				}
				if err = l.Put([]byte("099"), []byte("last")); err != nil {
					t.Fatal(err)
				}
				if err = l.Close(); err != nil {
					t.Fatal(err)
				}
				if noWAL {
					logs, e := filepath.Glob(filepath.Join(dir, "WAL-*.log"))
					if e != nil || len(logs) != 0 {
						t.Fatal("WAL-disabled mode wrote WAL", logs, e)
					}
				}
				// The mode is an open-time option; existing SSTables can use either mode.
				opts.HashMemTable = !hash
				l, err = NewLSMTree(dir, opts)
				if err != nil {
					t.Fatal(err)
				}
				if (l.mem.head == nil) != !hash {
					t.Fatal("reopen ignored selected mode")
				}
				for n := 0; n < 100; n++ {
					v, ok, e := l.Get([]byte(fmt.Sprintf("%03d", n)))
					want := bytes.Repeat([]byte{2}, 64)
					if n == 99 {
						want = []byte("last")
					}
					if e != nil || !ok || !bytes.Equal(v, want) {
						t.Fatalf("row %d: %q %v %v", n, v, ok, e)
					}
				}
			})
		}
	}
}

func TestMemTableModeRecoveryHelper(t *testing.T) {
	dir := os.Getenv("SLOPDB_MEMTABLE_RECOVERY_DIR")
	if dir == "" {
		return
	}
	opts := DefaultOptions()
	opts.HashMemTable = os.Getenv("SLOPDB_MEMTABLE_RECOVERY_HASH") == "true"
	opts.WriteBufferSize = 8 << 20
	l, err := NewLSMTree(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	batch := make([]Mutation, 100)
	for n := range batch {
		batch[n] = Mutation{Key: []byte(fmt.Sprintf("%03d", n)), Value: []byte("from WAL")}
	}
	if err = l.WriteBatch(batch); err != nil {
		t.Fatal(err)
	}
	os.Exit(0) // Omit Close so recovery must read the WAL.
}

func TestMemTableModeWALRecovery(t *testing.T) {
	for _, hash := range []bool{false, true} {
		t.Run(fmt.Sprintf("hash=%v", hash), func(t *testing.T) {
			dir := t.TempDir()
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			child := exec.Command(exe, "-test.run=^TestMemTableModeRecoveryHelper$")
			child.Env = append(os.Environ(), "SLOPDB_MEMTABLE_RECOVERY_DIR="+dir, fmt.Sprintf("SLOPDB_MEMTABLE_RECOVERY_HASH=%v", !hash))
			if output, e := child.CombinedOutput(); e != nil {
				t.Fatal(e, string(output))
			}
			opts := DefaultOptions()
			opts.HashMemTable = hash
			l, err := NewLSMTree(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			if (l.mem.head == nil) != hash {
				t.Fatal("WAL recovery ignored selected mode")
			}
			it := l.NewIterator()
			defer it.Close()
			n := 0
			for ; it.Valid(); it.Next() {
				if string(it.Key().UserKey) != fmt.Sprintf("%03d", n) || string(it.Value()) != "from WAL" {
					t.Fatal("WAL recovery scan mismatch", n, it.Key(), string(it.Value()))
				}
				n++
			}
			if n != 100 {
				t.Fatal("WAL recovery lost rows", n)
			}
		})
	}
}
