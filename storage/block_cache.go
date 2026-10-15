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
type cachedBlock struct {
	key        blockCacheKey
	reader     *BlockReader
	prev, next *cachedBlock
	referenced atomic.Uint32
}
type blockCacheShard struct {
	mu             sync.RWMutex
	capacity, used int
	lru            blockCacheList
	hand           *cachedBlock // next CLOCK candidate; protected by mu
	blocks         map[blockCacheKey]*cachedBlock
	closed         bool
	evicted        bool // LRU refreshes are unnecessary until there is admission pressure
	hits, misses   atomic.Int64
	admissionSkips atomic.Int64
	padding        [16]byte // separate neighboring shard reader/writer counters
}

// Shard both metadata and the byte budget so independent point reads do not
// serialize on one LRU mutex. Retained decoded bytes never exceed capacity.
type blockCache struct {
	policy        BlockCachePolicy
	policyPadding [48]byte // keep immutable policy off mutable shard cache lines
	shards        [blockCacheShards]blockCacheShard
	used          atomic.Int64
}

func newBlockCache(capacity int) *blockCache {
	return newBlockCacheWithPolicy(capacity, BlockCacheLRU)
}
func newBlockCacheWithPolicy(capacity int, policy BlockCachePolicy) *blockCache {
	c := &blockCache{policy: policy}
	for i := range c.shards {
		s := &c.shards[i]
		s.capacity = capacity / blockCacheShards
		if i < capacity%blockCacheShards {
			s.capacity++
		}
		s.blocks = make(map[blockCacheKey]*cachedBlock)
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
		s.blocks = nil
		s.lru.Init()
		s.hand = nil
		c.used.Add(-int64(s.used))
		s.used = 0
		s.mu.Unlock()
	}
}
func (c *blockCache) get(key blockCacheKey) ([]byte, bool) { return c.lookup(key, true) }
func (c *blockCache) lookup(key blockCacheKey, touch bool) ([]byte, bool) {
	reader, ok := c.lookupReader(key, touch)
	if !ok {
		return nil, false
	}
	return reader.data, true
}
func (c *blockCache) lookupReader(key blockCacheKey, touch bool) (*BlockReader, bool) {
	s := c.shard(key)
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, false
	}
	elem, ok := s.blocks[key]
	if !ok {
		s.misses.Add(1)
		s.mu.RUnlock()
		return nil, false
	}
	hits := s.hits.Add(1)
	entry := elem
	reader := entry.reader
	refresh := false
	if touch && s.evicted {
		if c.policy == BlockCacheCLOCK {
			// A second chance needs no list mutation or writer lock. Avoid
			// repeated stores when another hit already set the reference bit.
			if entry.referenced.Load() == 0 {
				entry.referenced.Store(1)
			}
		} else {
			refresh = hits%16 == 0
		}
	}
	s.mu.RUnlock()
	// Immutable blocks can be shared under a reader lock. Approximate LRU
	// refreshes avoid serializing all hits on list writes. Recheck membership
	// after upgrading: eviction/close may have removed the old list element.
	if refresh {
		if !s.mu.TryLock() {
			return reader, true
		}
		if !s.closed && s.blocks[key] == elem {
			s.lru.MoveToFront(elem)
		}
		s.mu.Unlock()
	}
	return reader, true
}

func (c *blockCache) statistics() (hits, misses int64) {
	for i := range c.shards {
		shard := &c.shards[i]
		hits += shard.hits.Load()
		misses += shard.misses.Load()
	}
	return
}

func (c *blockCache) skippedAdmissions() int64 {
	var skipped int64
	for i := range c.shards {
		skipped += c.shards[i].admissionSkips.Load()
	}
	return skipped
}
func (c *blockCache) add(key blockCacheKey, data []byte) {
	reader := NewBlockReader(data)
	reader.data = data
	c.addReader(key, reader)
}
func (c *blockCache) addReader(key blockCacheKey, reader *BlockReader) {
	data := reader.data
	s := c.shard(key)
	if cap(data) > s.capacity {
		return
	}
	// The decoded page already satisfies the read. Admission is best effort:
	// avoid queued writer preference stalling other readers of this shard.
	if !s.mu.TryLock() {
		s.admissionSkips.Add(1)
		return
	}
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if elem := s.blocks[key]; elem != nil {
		if c.policy == BlockCacheCLOCK {
			elem.referenced.Store(1)
		} else {
			s.lru.MoveToFront(elem)
		}
		return
	}
	before := s.used
	var reusable *cachedBlock
	for s.used+cap(data) > s.capacity {
		elem := s.lru.Back()
		if c.policy == BlockCacheCLOCK {
			elem = s.clockVictim(reusable)
		}
		if elem == nil {
			break
		}
		entry := elem
		s.evicted = true
		s.used -= cap(entry.reader.data)
		delete(s.blocks, entry.key)
		if reusable == nil {
			// Keep one node attached for the incoming page. Reusing its record
			// avoids allocation/GC assistance while holding the writer lock on
			// steady-state misses. Other victims (larger pages) are removed.
			reusable = elem
			if c.policy != BlockCacheCLOCK {
				s.lru.MoveToFront(elem)
			}
		} else {
			s.lru.Remove(elem)
		}
	}
	if reusable != nil {
		entry := reusable
		entry.key, entry.reader = key, reader
		entry.referenced.Store(1)
		s.blocks[key] = reusable
	} else {
		entry := &cachedBlock{key: key, reader: reader}
		entry.referenced.Store(1)
		if c.policy == BlockCacheCLOCK {
			elem := s.lru.PushBack(entry)
			s.blocks[key] = elem
			if s.hand == nil {
				s.hand = elem
			}
		} else {
			s.blocks[key] = s.lru.PushFront(entry)
		}
	}
	s.used += cap(data)
	if delta := s.used - before; delta != 0 {
		c.used.Add(int64(delta))
	}
}

// Called with the shard writer lock. Reader hits cannot reset reference bits
// during a sweep, so a victim is found within two revolutions. A reusable node
// is already uncharged/deleted but remains in the ring until replacement ends.
func (s *blockCacheShard) clockVictim(reusable *cachedBlock) *cachedBlock {
	if s.hand == nil {
		s.hand = s.lru.Front()
	}
	if s.hand == nil {
		return nil
	}
	for {
		elem := s.hand
		s.hand = elem.Next()
		if s.hand == nil {
			s.hand = s.lru.Front()
		}
		if elem == reusable {
			continue
		}
		if elem.referenced.Swap(0) == 0 {
			return elem
		}
	}
}
