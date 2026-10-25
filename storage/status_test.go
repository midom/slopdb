package storage

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestLSMStatusMemtablesFilesAndPassiveCounters(t *testing.T) {
	for _, hash := range []bool{false, true} {
		t.Run(fmt.Sprintf("hash=%v", hash), func(t *testing.T) {
			opt := DefaultOptions()
			opt.HashMemTable = hash
			l, err := NewLSMTree(t.TempDir(), opt)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = l.Close() })
			if err = l.WriteBatch([]Mutation{{Key: []byte("a"), Value: bytes.Repeat([]byte("value"), 100)}}); err != nil {
				t.Fatal(err)
			}
			snapshot := l.Snapshot()
			reader := l.SnapshotForReader(91)
			s := l.Status()
			if s.Memtable.Entries != 1 || s.Memtable.SizeBytes <= 0 || s.SnapshotPins != 1 || s.ReaderSnapshotPins != 1 || s.OldestSnapshot != snapshot || s.WALRecords != 1 || s.WALSyncs != 1 {
				t.Fatalf("memtable status: %+v", s)
			}
			if hash && (s.Memtable.Kind != "hash" || s.Memtable.HashBuckets == 0) || !hash && (s.Memtable.Kind != "skiplist" || s.Memtable.SkipListHeight == 0) {
				t.Fatalf("mode %+v", s.Memtable)
			}
			l.ReleaseSnapshot(snapshot)
			l.ReleaseSnapshotForReader(91, reader)
			if err = l.CompactRange(); err != nil {
				t.Fatal(err)
			}
			before := l.Status()
			bottom := before.Levels[maxLevels-1]
			if before.Memtable.Entries != 0 || bottom.FileCount == 0 || bottom.TotalSize == 0 || !bottom.Ordered || bottom.WriteCompression != ZstdCompression || before.CompletedFlushes == 0 {
				t.Fatalf("compacted status: %+v", before)
			}
			for _, f := range bottom.Files {
				if f.MaxSequence == 0 || f.IndexBytes == 0 || f.Size == 0 {
					t.Fatalf("file metadata: %+v", f)
				}
			}
			for i := 0; i < 3; i++ {
				_ = l.Status()
			}
			after := l.Status()
			if before.WALRecords != after.WALRecords || before.DataBlockReadAttempts != after.DataBlockReadAttempts || before.BlockCacheMisses != after.BlockCacheMisses || before.CacheMisses != after.CacheMisses || before.SeqNum != after.SeqNum {
				t.Fatal("status performed reads or writes")
			}
			if _, found, err := l.Get([]byte("a")); err != nil || !found {
				t.Fatalf("read %v %v", found, err)
			}
			if after = l.Status(); after.DataBlockReadAttempts <= before.DataBlockReadAttempts || after.DataBlockReadBytes == 0 || after.DecodedBlockBytes == 0 {
				t.Fatalf("data counters: %+v", after)
			}
			if err = l.Close(); err != nil {
				t.Fatal(err)
			}
			l, err = NewLSMTree(l.dir, opt)
			if err != nil {
				t.Fatal(err)
			}
			s = l.Status()
			if s.Levels[maxLevels-1].TotalSize != bottom.TotalSize || s.WALRecords != 0 || s.WALWriteErrors != 0 || s.DataBlockReadAttempts != 0 {
				t.Fatalf("reopened gauges/counters: %+v", s)
			}
		})
	}
}

func TestLSMStatusConcurrentPublication(t *testing.T) {
	l, err := NewLSMTree(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				if e := l.Put([]byte(fmt.Sprintf("%d-%d", worker, i)), []byte("v")); e != nil {
					t.Error(e)
					return
				}
				pin := l.SnapshotForReader(uint64(worker))
				_ = l.Status()
				l.ReleaseSnapshotForReader(uint64(worker), pin)
			}
		}(worker)
	}
	wg.Wait()
	if s := l.Status(); s.Memtable.Entries != 160 || s.ReaderSnapshotPins != 0 || s.WALWriteErrors != 0 {
		t.Fatalf("concurrent status %+v", s)
	}
}
