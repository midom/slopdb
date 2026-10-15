//go:build unix

package storage

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestBlockArenaReferencesWithoutAllocatorLock(t *testing.T) {
	var stats blockArenaStats
	a := &blockArena{capacity: 1 << 20, stats: &stats}
	data, off, ok := a.allocate(128)
	if !ok {
		t.Fatal("allocation failed")
	}
	copy(data, "pinned data")
	a.mu.Lock()
	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 1000; j++ {
					a.pin(off, true)
					a.release(off)
				}
			}()
		}
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		a.mu.Unlock()
	case <-time.After(5 * time.Second):
		a.mu.Unlock()
		<-done
		t.Fatal("ordinary references waited for the allocator mutex")
	}
	if string(data[:11]) != "pinned data" {
		t.Fatal("concurrent references changed page data")
	}
	a.close()
	if stats.mapped.Load() == 0 {
		t.Fatal("close unmapped a pinned page")
	}
	a.release(off)
	if stats.mapped.Load() != 0 || stats.inUse.Load() != 0 {
		t.Fatal("references leaked or final release did not unmap")
	}
}

func TestBlockArenaOutsideGoHeapAndClose(t *testing.T) {
	var stats blockArenaStats
	a := &blockArena{capacity: 32 << 20, stats: &stats}
	defer a.close()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	data, off, ok := a.allocate(16 << 20)
	if !ok {
		t.Fatal("arena allocation failed")
	}
	for i := range data {
		data[i] = byte(i)
	}
	runtime.ReadMemStats(&after)
	if after.HeapAlloc > before.HeapAlloc+4<<20 {
		t.Fatal("arena payload was charged to the Go heap")
	}
	runtime.GC()
	if data[12345] != byte(12345%256) {
		t.Fatal("GC changed arena data")
	}
	a.close()
	if stats.mapped.Load() == 0 {
		t.Fatal("close unmapped a borrowed allocation")
	}
	a.release(off)
	if stats.mapped.Load() != 0 || stats.inUse.Load() != 0 {
		t.Fatal("last release did not unmap the closed arena")
	}
	if _, _, ok := a.allocate(64); ok {
		t.Fatal("closed arena accepted allocation")
	}
}

func TestArenaCompactionSnapshotAndLevelIteratorRelease(t *testing.T) {
	o := pageTestOptions()
	o.BlockCacheMemory = BlockCacheMmap
	o.BlockCacheSize = 4 << 20
	o.BaseLevelSize = 1
	o.CompressionType = ZstdCompression
	l, err := NewLSMTree(filepath.Join(t.TempDir(), "database"), o)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err = l.WriteBatch(pageFixtureEntries(0, 1200)); err != nil {
		t.Fatal(err)
	}
	if err = l.Flush(); err != nil {
		t.Fatal(err)
	}
	seq := l.SnapshotForReader(21)
	defer l.ReleaseSnapshotForReader(21, seq)
	var held []byte
	for i := 0; i < 1200; i++ {
		v, ok, e := l.GetAtForReader([]byte(fmt.Sprintf("key%08d", i)), seq, 21)
		if e != nil || !ok || len(v) != 1024 || v[0] != 'v' {
			t.Fatal("bad initial point read", e)
		}
		if i == 0 {
			held = v
		}
	}
	entries := pageFixtureEntries(0, 1200)
	for i := range entries {
		entries[i].Value = bytes.Repeat([]byte{'w'}, 1024)
	}
	if err = l.WriteBatch(entries); err != nil {
		t.Fatal(err)
	}
	if err = l.Flush(); err != nil {
		t.Fatal(err)
	}
	if err = l.CompactRange(); err != nil {
		t.Fatal(err)
	}
	if len(l.view.files) < 2 {
		t.Fatal("fixture did not create multiple bottom-level files")
	}
	for _, old := range []bool{true, false} {
		var it Iterator
		marker := byte('w')
		if old {
			it = l.NewIteratorAt(seq)
			marker = 'v'
		} else {
			it = l.NewIterator()
		}
		count := 0
		for it.Valid() {
			v := it.Value()
			if len(v) != 1024 || v[0] != marker {
				t.Fatal("compaction changed a snapshot value")
			}
			count++
			it.Next()
		}
		if count != 1200 || IteratorError(it) != nil {
			t.Fatal("bad compacted scan", count, IteratorError(it))
		}
		it.Close()
	}
	// Early close and repeated seeks exercise wrapper ownership, not just EOF.
	it := l.NewIteratorAt(seq)
	it.Seek([]byte("key00000600"))
	seekIteratorLast(it)
	it.Prev()
	it.Seek([]byte("key00000000"))
	retained := it.Value()
	it.Close()
	fi := l.openFileIteratorAt(l.view.files[0].meta, seq)
	fi.Seek(nil)
	fi.Close()
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	mapped, inUse, _ := l.tableCache.blocks.arenaStatistics()
	if mapped != 0 || inUse != 0 {
		t.Fatal("compaction or iterator wrapper leaked arena pins", mapped, inUse)
	}
	if len(held) != 1024 || held[0] != 'v' || len(retained) != 1024 || retained[0] != 'v' {
		t.Fatal("public values changed after close")
	}
}

func TestArenaIteratorRetainedValuesAndShutdown(t *testing.T) {
	o := pageTestOptions()
	o.BlockCacheMemory = BlockCacheMmap
	o.BlockCacheSize = 2 << 20
	o.CompressionType = ZstdCompression
	l, err := NewLSMTree(filepath.Join(t.TempDir(), "database"), o)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err = l.WriteBatch(pageFixtureEntries(0, 80)); err != nil {
		t.Fatal(err)
	}
	if err = l.Flush(); err != nil {
		t.Fatal(err)
	}
	// The first scan admits pages. The second scan must borrow arena pages.
	for pass := 0; pass < 2; pass++ {
		it := l.NewIterator()
		for it.Valid() {
			it.Next()
		}
		if err = IteratorError(it); err != nil {
			t.Fatal(err)
		}
		it.Close()
	}
	if l.Stats().BlockCacheHits == 0 || l.Stats().BlockCacheArenaInUseBytes == 0 {
		t.Fatal("fixture did not use cached arena pages")
	}
	it := l.NewIterator()
	if !it.Valid() {
		t.Fatal("empty iterator")
	}
	held := it.Value()
	expect := append([]byte(nil), held...)
	it.Next()
	seekIteratorLast(it)
	it.Prev()
	it.Seek([]byte("key00000000"))
	// Close removes cache ownership; the live iterator keeps its page valid.
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(it.Value(), expect) {
		t.Fatal("cache close changed pinned iterator page")
	}
	it.Close()
	if !bytes.Equal(held, expect) {
		t.Fatal("retained public value changed after unmapping")
	}
	mapped, inUse, _ := l.tableCache.blocks.arenaStatistics()
	if mapped != 0 || inUse != 0 {
		t.Fatal("iterator close leaked arena references")
	}
}

func TestBlockArenaSplitCoalesceAndBound(t *testing.T) {
	var stats blockArenaStats
	a := &blockArena{capacity: 64 << 10, stats: &stats}
	defer a.close()
	type allocation struct {
		data   []byte
		off    int
		marker byte
	}
	var live []allocation
	rng := rand.New(rand.NewSource(617))
	for n := 0; n < 5000; n++ {
		if len(live) > 0 && rng.Intn(3) == 0 {
			i := rng.Intn(len(live))
			x := live[i]
			if !bytes.Equal(x.data, bytes.Repeat([]byte{x.marker}, len(x.data))) {
				t.Fatal("another allocation overwrote live data")
			}
			a.release(x.off)
			live[i] = live[len(live)-1]
			live = live[:len(live)-1]
		} else if data, off, ok := a.allocate(rng.Intn(7000) + 1); ok {
			marker := byte(n)
			for i := range data {
				data[i] = marker
			}
			live = append(live, allocation{data, off, marker})
		}
		if stats.inUse.Load() > stats.mapped.Load() || stats.mapped.Load() > 128<<10 {
			t.Fatal("arena exceeded its mapping reserve")
		}
	}
	for _, x := range live {
		if !bytes.Equal(x.data, bytes.Repeat([]byte{x.marker}, len(x.data))) {
			t.Fatal("live data changed")
		}
		a.release(x.off)
	}
	if stats.inUse.Load() != 0 {
		t.Fatal("arena leaked allocation spans")
	}
	data, off, ok := a.allocate(128<<10 - arenaHeader - arenaFooter)
	if !ok || len(data) != 128<<10-arenaHeader-arenaFooter {
		t.Fatal("free spans did not coalesce")
	}
	a.release(off)
}

func TestBlockArenaMappingFailureBypassesCache(t *testing.T) {
	c := newBlockCacheWithMemory(1<<20, BlockCacheLRU, BlockCacheMmap)
	defer c.close()
	key := blockCacheKey{table: 1}
	c.shard(key).arena.mapMemory = func(int) ([]byte, error) { return nil, errors.New("test mapping failure") }
	c.add(key, make([]byte, 64))
	_, _, failures := c.arenaStatistics()
	if c.used.Load() != 0 || c.skippedAdmissions() != 1 || failures != 1 {
		t.Fatal("mapping failure did not bypass admission")
	}
	if _, ok := c.get(key); ok {
		t.Fatal("cache fell back to Go-heap payloads")
	}
}

func TestBlockArenaPinnedEvictionAndConcurrentClose(t *testing.T) {
	for _, policy := range []BlockCachePolicy{BlockCacheLRU, BlockCacheCLOCK} {
		t.Run(string(policy), func(t *testing.T) {
			c := newBlockCacheWithMemory(blockCacheShards*128, policy, BlockCacheMmap)
			keys := sameShardKeys(c, 3)
			c.add(keys[0], bytes.Repeat([]byte{42}, 64))
			reader, ok := c.lookupReader(keys[0], true)
			if !ok || reader.arena == nil {
				t.Fatal("cached page is not in arena")
			}
			c.add(keys[1], bytes.Repeat([]byte{43}, 128))
			c.add(keys[2], bytes.Repeat([]byte{44}, 128))
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); c.close() }()
			go func() {
				defer wg.Done()
				for n := 0; n < 1000; n++ {
					if !bytes.Equal(reader.data, bytes.Repeat([]byte{42}, 64)) {
						t.Error("pinned eviction changed during close")
						return
					}
				}
			}()
			wg.Wait()
			mapped, inUse, _ := c.arenaStatistics()
			if c.used.Load() != 0 || inUse == 0 || mapped == 0 {
				t.Fatal("close did not preserve just the borrowed allocation")
			}
			reader.release()
			mapped, inUse, _ = c.arenaStatistics()
			if inUse != 0 || mapped != 0 {
				t.Fatal("borrower release leaked closed mapping")
			}
			c.close()
		})
	}
}
