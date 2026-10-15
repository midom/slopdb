package storage

import (
	"bytes"
	"encoding/binary"
	"sync"
	"testing"
	"time"
)

func testBlockCacheEvictionRetainsBorrowedPage(t *testing.T, policy BlockCachePolicy, memory BlockCacheMemory) {
	c := newBlockCacheWithMemory(blockCacheShards*128, policy, memory)
	defer c.close()
	keys := sameShardKeys(c, 3)
	c.add(keys[0], make([]byte, 64))
	c.add(keys[1], make([]byte, 64))
	s := c.shard(keys[0])
	retained, ok := c.lookupReader(keys[0], false)
	if !ok {
		t.Fatal("missing victim")
	}
	defer retained.release()
	data := make([]byte, 96)
	data[0] = 7
	c.add(keys[2], data)
	if memory == BlockCacheMmap {
		if s.blocks[keys[2]] == 0 || len(s.blocks) != 1 || s.lru.Len() != 1 || s.used != 96 {
			t.Fatal("multiple-victim replacement did not retain exactly one arena record")
		}
	} else if s.heapBlocks[keys[2]] == nil || len(s.heapBlocks) != 1 || s.heapLRU.Len() != 1 || s.used != 96 {
		t.Fatal("multiple-victim replacement did not retain exactly one heap record")
	}
	if !bytes.Equal(retained.data, make([]byte, 64)) {
		t.Fatal("eviction modified an already borrowed immutable reader")
	}
}

func TestBlockCacheAdmissionDoesNotQueueBehindReaders(t *testing.T) {
	for _, memory := range []BlockCacheMemory{BlockCacheHeap, BlockCacheMmap} {
		t.Run(string(memory), func(t *testing.T) { testBlockCacheAdmissionDoesNotQueueBehindReaders(t, memory) })
	}
}

func testBlockCacheAdmissionDoesNotQueueBehindReaders(t *testing.T, memory BlockCacheMemory) {
	for _, policy := range []BlockCachePolicy{BlockCacheLRU, BlockCacheCLOCK} {
		t.Run(string(policy), func(t *testing.T) {
			c := newBlockCacheWithMemory(blockCacheShards*512, policy, memory)
			defer c.close()
			keys := sameShardKeys(c, 2)
			c.add(keys[0], make([]byte, 64))
			s := c.shard(keys[0])
			s.mu.RLock()
			done := make(chan struct{})
			go func() { c.add(keys[1], make([]byte, 64)); close(done) }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				s.mu.RUnlock()
				t.Fatal("cache admission queued behind a reader")
			}
			s.mu.RUnlock()
			if c.skippedAdmissions() != 1 {
				t.Fatal("skipped admission was not recorded")
			}
			if _, ok := c.get(keys[0]); !ok {
				t.Fatal("bypass evicted an existing page")
			}
			c.add(keys[1], make([]byte, 64))
			if _, ok := c.get(keys[1]); !ok {
				t.Fatal("later uncontended admission failed")
			}
		})
	}
}

func sameShardKeys(c *blockCache, n int) []blockCacheKey {
	keys := []blockCacheKey{{table: 1}}
	for offset := uint64(1); len(keys) < n; offset++ {
		key := blockCacheKey{table: 1, offset: offset}
		if c.shard(key) == c.shard(keys[0]) {
			keys = append(keys, key)
		}
	}
	return keys
}

func TestBlockCacheSampledTouchesAndScanLookups(t *testing.T) {
	for _, memory := range []BlockCacheMemory{BlockCacheHeap, BlockCacheMmap} {
		t.Run(string(memory), func(t *testing.T) { testBlockCacheSampledTouchesAndScanLookups(t, memory) })
	}
}

func testBlockCacheSampledTouchesAndScanLookups(t *testing.T, memory BlockCacheMemory) {
	for _, touch := range []bool{false, true} {
		c := newBlockCacheWithMemory(blockCacheShards*128, BlockCacheLRU, memory)
		keys := sameShardKeys(c, 3)
		// Begin admission pressure, then restore the two pages under test.
		c.add(keys[2], make([]byte, 128))
		c.add(keys[0], make([]byte, 64))
		c.add(keys[1], make([]byte, 64))
		for n := 0; n < 16; n++ {
			if _, ok := c.lookup(keys[0], touch); !ok {
				t.Fatal("missing cached block")
			}
		}
		c.add(keys[2], make([]byte, 64))
		_, first := c.lookup(keys[0], false)
		_, second := c.lookup(keys[1], false)
		if first != touch || second == touch {
			t.Fatalf("touch=%v: first=%v second=%v", touch, first, second)
		}
		c.close()
		if c.used.Load() != 0 {
			t.Fatal("closed cache retained byte budget")
		}
	}
}

func TestBlockCacheFittingWorkingSetNeedsNoLRUWriter(t *testing.T) {
	for _, memory := range []BlockCacheMemory{BlockCacheHeap, BlockCacheMmap} {
		t.Run(string(memory), func(t *testing.T) { testBlockCacheFittingWorkingSetNeedsNoLRUWriter(t, memory) })
	}
}

func testBlockCacheFittingWorkingSetNeedsNoLRUWriter(t *testing.T, memory BlockCacheMemory) {
	c := newBlockCacheWithMemory(blockCacheShards*256, BlockCacheLRU, memory)
	defer c.close()
	keys := sameShardKeys(c, 2)
	c.add(keys[0], make([]byte, 64))
	c.add(keys[1], make([]byte, 64))
	s := c.shard(keys[0])
	first := s.blocks[keys[0]]
	heapFirst := s.heapBlocks[keys[0]]
	for n := 0; n < 32; n++ {
		if _, ok := c.get(keys[0]); !ok {
			t.Fatal("missing fitting page")
		}
	}
	if s.evicted || (memory == BlockCacheMmap && s.lru.Back() != first) || (memory == BlockCacheHeap && s.heapLRU.Back() != heapFirst) {
		t.Fatal("fitting working set performed LRU writes")
	}
	// Eviction begins maintaining approximate recency for future hits.
	pressure := keys[0]
	pressure.size = 99
	c.add(pressure, make([]byte, 192))
	if !s.evicted {
		t.Fatal("eviction did not enable recency maintenance")
	}
	for n := 0; n < 16; n++ {
		c.get(keys[1])
	}
	if (memory == BlockCacheMmap && s.lru.Front() != s.blocks[keys[1]]) || (memory == BlockCacheHeap && s.heapLRU.Front() != s.heapBlocks[keys[1]]) {
		t.Fatal("recency was not maintained under admission pressure")
	}
}

func testBlockCacheConcurrentReadersEvictionAndBudget(t *testing.T, policy BlockCachePolicy, memory BlockCacheMemory) {
	const capacity = blockCacheShards * 512
	c := newBlockCacheWithMemory(capacity, policy, memory)
	defer c.close()
	var wg sync.WaitGroup
	for worker := 0; worker < 100; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for n := 0; n < 200; n++ {
				key := blockCacheKey{table: uint64(worker%8 + 1), offset: uint64(n % 32)}
				data := make([]byte, 64)
				binary.LittleEndian.PutUint64(data, key.offset)
				c.add(key, data)
				if got, ok := c.lookup(key, n%3 != 0); ok && (len(got) != 64 || binary.LittleEndian.Uint64(got) != key.offset) {
					t.Error("wrong cached data")
					return
				}
				if used := c.used.Load(); used < 0 || used > capacity {
					t.Errorf("cache exceeds budget: %d", used)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	hits, misses := c.statistics()
	if hits+misses != 100*200 {
		t.Fatalf("lost lookup counters: hits=%d misses=%d", hits, misses)
	}
}

func testBlockCacheConcurrentClose(t *testing.T, policy BlockCachePolicy, memory BlockCacheMemory) {
	c := newBlockCacheWithMemory(blockCacheShards*512, policy, memory)
	var ready, wg sync.WaitGroup
	start := make(chan struct{})
	for worker := 0; worker < 100; worker++ {
		ready.Add(1)
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			ready.Done()
			<-start
			for n := 0; n < 100; n++ {
				key := blockCacheKey{table: uint64(worker), offset: uint64(n % 4)}
				c.add(key, make([]byte, 64))
				c.get(key)
			}
		}(worker)
	}
	ready.Wait()
	close(start)
	c.close()
	wg.Wait()
	if c.used.Load() != 0 {
		t.Fatal("closed cache was repopulated")
	}
	if _, ok := c.get(blockCacheKey{}); ok {
		t.Fatal("closed cache returned a block")
	}
}

func TestBlockCachePolicies(t *testing.T) {
	for _, memory := range []BlockCacheMemory{BlockCacheHeap, BlockCacheMmap} {
		for _, policy := range []BlockCachePolicy{BlockCacheLRU, BlockCacheCLOCK} {
			t.Run(string(memory)+"/"+string(policy), func(t *testing.T) {
				t.Run("RetainedVictim", func(t *testing.T) { testBlockCacheEvictionRetainsBorrowedPage(t, policy, memory) })
				t.Run("ConcurrentBudget", func(t *testing.T) { testBlockCacheConcurrentReadersEvictionAndBudget(t, policy, memory) })
				t.Run("ConcurrentClose", func(t *testing.T) { testBlockCacheConcurrentClose(t, policy, memory) })
			})
		}
	}
}

func TestBlockCacheClockSecondChanceAndScanReads(t *testing.T) {
	for _, memory := range []BlockCacheMemory{BlockCacheHeap, BlockCacheMmap} {
		t.Run(string(memory), func(t *testing.T) { testBlockCacheClockSecondChanceAndScanReads(t, memory) })
	}
}

func testBlockCacheClockSecondChanceAndScanReads(t *testing.T, memory BlockCacheMemory) {
	for _, touch := range []bool{false, true} {
		c := newBlockCacheWithMemory(blockCacheShards*192, BlockCacheCLOCK, memory)
		keys := sameShardKeys(c, 5)
		for i := 0; i < 4; i++ {
			c.add(keys[i], make([]byte, 64))
		}
		// The first sweep clears A/B/C, evicts A, and leaves the hand at B.
		if _, ok := c.lookup(keys[0], false); ok {
			t.Fatal("first CLOCK victim was not A")
		}
		if _, ok := c.lookup(keys[1], touch); !ok {
			t.Fatal("missing B")
		}
		c.add(keys[4], make([]byte, 64))
		_, b := c.lookup(keys[1], false)
		_, third := c.lookup(keys[2], false)
		if b != touch || third == touch {
			t.Fatalf("touch=%v: B=%v C=%v", touch, b, third)
		}
		if _, ok := c.lookup(keys[3], false); !ok {
			t.Fatal("newly admitted page lost its second chance")
		}
		c.close()
		if c.used.Load() != 0 || c.shard(keys[0]).hand != 0 || c.shard(keys[0]).heapHand != nil {
			t.Fatal("closed CLOCK cache retained state")
		}
	}
}
