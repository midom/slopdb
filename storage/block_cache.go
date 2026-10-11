package storage

import (
	"container/list"
	"sync"
	"sync/atomic"
)

const defaultBlockCacheBytes = 128 << 20
const blockCacheShards = 16

type blockCacheKey struct {
	table, offset, size uint64
	modified            int64
}
type cachedBlock struct {
	key    blockCacheKey
	data   []byte
	reader *BlockReader
}
type blockCacheShard struct {
	mu             sync.Mutex
	capacity, used int
	lru            *list.List
	blocks         map[blockCacheKey]*list.Element
	closed         bool
	hits, misses   int64 // protected by this shard's mutex
}

// Shard both metadata and the byte budget so independent point reads do not
// serialize on one LRU mutex. Retained decoded bytes never exceed capacity.
type blockCache struct {
	shards [blockCacheShards]blockCacheShard
	used   atomic.Int64
}

func newBlockCache(capacity int) *blockCache {
	c := &blockCache{}
	for i := range c.shards {
		s := &c.shards[i]
		s.capacity = capacity / blockCacheShards
		if i < capacity%blockCacheShards {
			s.capacity++
		}
		s.lru = list.New()
		s.blocks = make(map[blockCacheKey]*list.Element)
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, false
	}
	elem, ok := s.blocks[key]
	if !ok {
		s.misses++
		return nil, false
	}
	s.hits++
	if touch {
		s.lru.MoveToFront(elem)
	}
	return elem.Value.(cachedBlock).reader, true
}

func (c *blockCache) statistics() (hits, misses int64) {
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.Lock()
		hits += shard.hits
		misses += shard.misses
		shard.mu.Unlock()
	}
	return
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if elem := s.blocks[key]; elem != nil {
		s.lru.MoveToFront(elem)
		return
	}
	before := s.used
	for s.used+cap(data) > s.capacity {
		elem := s.lru.Back()
		if elem == nil {
			break
		}
		entry := elem.Value.(cachedBlock)
		s.used -= cap(entry.data)
		delete(s.blocks, entry.key)
		s.lru.Remove(elem)
	}
	s.blocks[key] = s.lru.PushFront(cachedBlock{key: key, data: data, reader: reader})
	s.used += cap(data)
	c.used.Add(int64(s.used - before))
}
