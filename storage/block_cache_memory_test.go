package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestBlockCacheMemoryValidationBeforeCreate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing")
	o := DefaultOptions()
	o.BlockCacheMemory = "unknown"
	if l, err := NewLSMTree(path, o); err == nil {
		l.Close()
		t.Fatal("invalid cache memory mode accepted")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("invalid cache memory mode created a database", err)
	}
}

func TestBlockCacheMemoryDefaultAndReopen(t *testing.T) {
	if DefaultOptions().BlockCacheMemory != BlockCacheMmap {
		t.Fatal("default cache memory is not mmap")
	}
	path := filepath.Join(t.TempDir(), "database")
	o := pageTestOptions()
	o.BlockCacheSize = 4 << 20
	o.BlockCacheMemory = ""
	o.CompressionType = ZstdCompression
	l, err := NewLSMTree(path, o)
	if err != nil {
		t.Fatal(err)
	}
	if l.Stats().BlockCacheMemory != BlockCacheMmap || o.BlockCacheMemory != "" {
		l.Close()
		t.Fatal("empty memory setting did not select mmap without changing caller options")
	}
	if err = l.WriteBatch(pageFixtureEntries(0, 80)); err != nil {
		l.Close()
		t.Fatal(err)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	// The same SSTables can be reopened with either allocator and policy.
	for _, memory := range []BlockCacheMemory{BlockCacheHeap, BlockCacheMmap, BlockCacheHeap} {
		for _, policy := range []BlockCachePolicy{BlockCacheLRU, BlockCacheCLOCK} {
			t.Run(string(memory)+"/"+string(policy), func(t *testing.T) {
				o.BlockCacheMemory, o.BlockCachePolicy = memory, policy
				l, err := NewLSMTree(path, o)
				if err != nil {
					t.Fatal(err)
				}
				defer l.Close()
				seq := l.SnapshotForReader(21)
				var dst []byte
				for pass := 0; pass < 2; pass++ {
					for _, entry := range pageFixtureEntries(0, 80) {
						value, found, err := l.GetAtForReaderInto(entry.Key, seq, 21, dst)
						if err != nil || !found || !bytes.Equal(value, entry.Value) {
							t.Fatal("cache mode changed a snapshot row", err)
						}
						dst = value
					}
				}
				l.ReleaseSnapshotForReader(21, seq)
				stats := l.Stats()
				if stats.BlockCacheMemory != memory || stats.BlockCachePolicy != policy || stats.BlockCacheHits == 0 || stats.BlockCacheBytes <= 0 || stats.BlockCacheBytes > int64(o.BlockCacheSize) {
					t.Fatal("wrong effective mode or cache counters", stats)
				}
				if memory == BlockCacheHeap {
					if stats.BlockCacheArenaBytes != 0 || stats.BlockCacheArenaInUseBytes != 0 || stats.BlockCacheArenaFailures != 0 {
						t.Fatal("heap cache reported arena use", stats)
					}
				} else if stats.BlockCacheArenaBytes == 0 || stats.BlockCacheArenaInUseBytes == 0 || stats.BlockCacheArenaFailures != 0 {
					t.Fatal("mmap cache did not allocate arena pages", stats)
				}
				it := l.NewIterator()
				if !it.Valid() {
					t.Fatal("empty iterator")
				}
				held := it.Value()
				want := append([]byte(nil), held...)
				it.Close()
				if err := l.Close(); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(held, want) || l.Stats().BlockCacheBytes != 0 {
					t.Fatal("close invalidated a returned value or retained cache membership")
				}
			})
		}
	}
}

func TestHeapBlockCacheOwnsDecodeScratch(t *testing.T) {
	c := newBlockCacheWithMemory(blockCacheShards*128, BlockCacheLRU, BlockCacheHeap)
	defer c.close()
	key := blockCacheKey{table: 1}
	buffer := blockDecodeBuffers.Get().(*blockDecodeBuffer)
	clear(buffer[:])
	buffer[0] = 42
	r := borrowedBlockReader(buffer[:64])
	r.decodeBuffer = buffer
	c.addReader(key, r)
	// The private input still belongs to the caller until release. A heap
	// cache entry must not retain this slice or the pooled reader object.
	buffer[0] = 43
	r.release()
	held, found := c.lookupReader(key, false)
	if !found || held.data[0] != 42 || held.arena != nil || held.decodeBuffer != nil || held.pooled {
		t.Fatal("heap cache retained private decode scratch")
	}
	held.release()
	c.close()
	runtime.GC()
	if held.data[0] != 42 {
		t.Fatal("heap borrower changed after release, close or GC")
	}
}

func TestHeapBlockCacheUsesGoMemory(t *testing.T) {
	c := newBlockCacheWithMemory(blockCacheShards*(8<<20), BlockCacheLRU, BlockCacheHeap)
	defer c.close()
	data := make([]byte, 4<<20)
	data[0] = 42
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	key := blockCacheKey{table: 1}
	c.add(key, data)
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(data)
	if after.HeapAlloc < before.HeapAlloc+(3<<20) {
		t.Fatal("heap cache data was not charged to Go memory")
	}
	mapped, inUse, failures := c.arenaStatistics()
	if mapped != 0 || inUse != 0 || failures != 0 || c.shard(key).arena != nil {
		t.Fatal("heap cache created an arena")
	}
	if c.used.Load() != int64(len(data)) {
		t.Fatal("heap cache did not charge the decoded length")
	}
}
