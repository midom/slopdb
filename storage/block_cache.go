package storage

import (
	"sync"
	"sync/atomic"
)

const defaultBlockCacheBytes = 128 << 20
const blockCacheShards = 128

type blockCacheKey struct {
	table, offset, size uint64
	modified            int64
}

type blockCacheShard struct {
	mu                                                sync.RWMutex
	capacity, used                                    int
	lru                                               blockCacheList
	hand                                              uint64                   // arena offset+1; zero is an empty CLOCK ring
	blocks                                            map[blockCacheKey]uint64 // pointer-free arena offsets
	arena                                             *blockArena
	arenaStats                                        blockArenaStats
	heapBlocks                                        map[blockCacheKey]*cachedHeapBlock
	heapLRU                                           heapBlockCacheList
	heapHand                                          *cachedHeapBlock
	closed, evicted                                   bool
	hits, misses                                      atomic.Int64
	readAttempts, readBytes, readErrors, decodedBytes atomic.Int64
	admissionSkips                                    atomic.Int64
	padding                                           [16]byte
}

// Both memory backends share shard locks, budgets, policies and counters.
// mmap stores resident data and records outside the Go heap; heap keeps owned
// Go slices and immutable readers with typed intrusive cache records.
type blockCache struct {
	policy        BlockCachePolicy
	memory        BlockCacheMemory
	policyPadding [32]byte
	shards        [blockCacheShards]blockCacheShard
	used          atomic.Int64
}

func newBlockCacheWithMemory(capacity int, policy BlockCachePolicy, memory BlockCacheMemory) *blockCache {
	if memory == "" {
		memory = BlockCacheMmap
	}
	c := &blockCache{policy: policy, memory: memory}
	for i := range c.shards {
		s := &c.shards[i]
		s.capacity = capacity / blockCacheShards
		if i < capacity%blockCacheShards {
			s.capacity++
		}
		if memory == BlockCacheMmap {
			s.blocks = make(map[blockCacheKey]uint64)
			s.arena = &blockArena{capacity: s.capacity, stats: &s.arenaStats}
			s.lru.arena = s.arena
		} else {
			s.heapBlocks = make(map[blockCacheKey]*cachedHeapBlock)
		}
	}
	return c
}
func (c *blockCache) shard(key blockCacheKey) *blockCacheShard {
	x := key.offset ^ (key.table * 0x9e3779b97f4a7c15)
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	return &c.shards[x&(blockCacheShards-1)]
}
func (c *blockCache) close() {
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		s.closed = true
		if c.memory == BlockCacheMmap {
			for _, link := range s.blocks {
				s.arena.release(int(link - 1))
			}
			s.blocks = nil
			s.lru.Init()
			s.hand = 0
			s.arena.close()
		} else {
			s.heapBlocks = nil
			s.heapLRU.Init()
			s.heapHand = nil
		}
		c.used.Add(-int64(s.used))
		s.used = 0
		s.mu.Unlock()
	}
}
func (c *blockCache) get(key blockCacheKey) ([]byte, bool) { return c.lookup(key, true) }
func (c *blockCache) lookup(key blockCacheKey, touch bool) ([]byte, bool) {
	r, ok := c.lookupReader(key, touch)
	if !ok {
		return nil, false
	}
	defer r.release()
	return append([]byte(nil), r.data...), true
}
func (c *blockCache) lookupReader(key blockCacheKey, touch bool) (*BlockReader, bool) {
	if c.memory != BlockCacheMmap {
		return c.lookupHeapReader(key, touch)
	}
	s := c.shard(key)
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, false
	}
	link := s.blocks[key]
	if link == 0 {
		s.misses.Add(1)
		s.mu.RUnlock()
		return nil, false
	}
	hits := s.hits.Add(1)
	reader := s.arena.reader(int(link-1), touch && s.evicted && c.policy == BlockCacheCLOCK)
	refresh := touch && s.evicted && c.policy != BlockCacheCLOCK && hits%16 == 0
	s.mu.RUnlock()
	if refresh && s.mu.TryLock() {
		// The outstanding pin prevents this offset from being reused before
		// membership is rechecked after the lock upgrade.
		if !s.closed && s.blocks[key] == link {
			s.lru.MoveToFront(link)
		}
		s.mu.Unlock()
	}
	return reader, true
}
func (c *blockCache) statistics() (hits, misses int64) {
	for i := range c.shards {
		hits += c.shards[i].hits.Load()
		misses += c.shards[i].misses.Load()
	}
	return
}
func (c *blockCache) skippedAdmissions() int64 {
	var n int64
	for i := range c.shards {
		n += c.shards[i].admissionSkips.Load()
	}
	return n
}
func (c *blockCache) arenaStatistics() (mapped, inUse, failures int64) {
	for i := range c.shards {
		s := &c.shards[i].arenaStats
		mapped += s.mapped.Load()
		inUse += s.inUse.Load()
		failures += s.failures.Load()
	}
	return
}
func (c *blockCache) add(key blockCacheKey, data []byte) {
	reader := NewBlockReader(data)
	reader.data = data
	c.addReader(key, reader)
	reader.release()
}

// The input reader is private. Successful admission transfers its data to the
// arena in place: allocation ownership stays with the caller, and cache
// membership acquires another reference. No resident Go reader is created.
func (c *blockCache) addReader(key blockCacheKey, reader *BlockReader) {
	if c.memory != BlockCacheMmap {
		c.addHeapReader(key, reader)
		return
	}
	data := reader.data
	s := c.shard(key)
	if len(data) > s.capacity {
		return
	}
	if !s.mu.TryLock() {
		s.admissionSkips.Add(1)
		return
	}
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if link := s.blocks[key]; link != 0 {
		if c.policy == BlockCacheCLOCK {
			atomic.StoreUint64(s.arena.atomicWord(int(link-1)+72), 1)
		} else {
			s.lru.MoveToFront(link)
		}
		return
	}
	stored, off, ok := s.arena.allocate(len(data))
	if !ok {
		s.admissionSkips.Add(1)
		return
	}
	copy(stored, data)
	s.arena.setCacheKey(off, key)
	s.arena.set(off+80, uint64(reader.numRestarts))
	s.arena.pin(off, true) // cache ownership, alongside the caller's allocation
	if len(reader.restarts) > 0 {
		reader.restarts = stored[len(stored)-4-reader.numRestarts*4 : len(stored)-4]
	}
	reader.data = stored
	if reader.decodeBuffer != nil {
		blockDecodeBuffers.Put(reader.decodeBuffer)
		reader.decodeBuffer = nil
	}
	reader.arena, reader.arenaOffset = s.arena, off
	before := s.used
	for s.used+len(stored) > s.capacity {
		victim := s.lru.Back()
		if c.policy == BlockCacheCLOCK {
			victim = s.clockVictim()
		}
		if victim == 0 {
			panic("block cache budget has no eviction victim")
		}
		v := int(victim - 1)
		s.used -= int(s.arena.word(v + 16))
		delete(s.blocks, s.arena.cacheKey(v))
		s.lru.Remove(victim)
		if s.lru.Len() == 0 {
			s.hand = 0
		} else if s.hand == victim {
			s.hand = s.lru.Front()
		}
		s.arena.release(v)
		s.evicted = true
	}
	link := uint64(off + 1)
	if c.policy == BlockCacheCLOCK {
		s.lru.PushBack(link)
		if s.hand == 0 {
			s.hand = link
		}
	} else {
		s.lru.PushFront(link)
	}
	s.blocks[key] = link
	s.used += len(stored)
	if delta := s.used - before; delta != 0 {
		c.used.Add(int64(delta))
	}
}

// Writer ownership excludes hits while reference bits are swept, so a victim
// is found within two revolutions. Release of borrowed views only changes the
// allocation reference count and cannot alter an attached CLOCK record.
func (s *blockCacheShard) clockVictim() uint64 {
	if s.hand == 0 {
		s.hand = s.lru.Front()
	}
	if s.hand == 0 {
		return 0
	}
	for {
		link := s.hand
		s.hand = s.lru.Next(link)
		if s.hand == 0 {
			s.hand = s.lru.Front()
		}
		off := int(link - 1)
		if atomic.LoadUint64(s.arena.atomicWord(off+72)) == 0 {
			return link
		}
		atomic.StoreUint64(s.arena.atomicWord(off+72), 0)
	}
}
