package storage

import (
	"container/list"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
)

type tableCache struct {
	shards        []tableCacheShard
	dir           string
	blocks        *blockCache
	verifyChanges bool
	hits, misses  atomic.Int64
}
type tableCacheShard struct {
	mu       sync.Mutex
	capacity int
	lru      *list.List
	entries  map[uint64]*list.Element
	loading  map[uint64]*tableLoad
	closed   bool
}
type tableLoad struct {
	done chan struct{}
	err  error
}
type cacheEntry struct {
	number  uint64
	reader  *SSTableReader
	refs    atomic.Int64
	touches atomic.Uint32
}

func (tc *tableCache) Hits() int64   { return tc.hits.Load() }
func (tc *tableCache) Misses() int64 { return tc.misses.Load() }
func newTableCache(dir string, capacity, blockBytes int, policy BlockCachePolicy, memory BlockCacheMemory) *tableCache {
	n := 1
	for n < 16 && n*2 <= capacity {
		n *= 2
	}
	tc := &tableCache{shards: make([]tableCacheShard, n), dir: dir, blocks: newBlockCacheWithMemory(blockBytes, policy, memory)}
	for i := range tc.shards {
		s := &tc.shards[i]
		s.capacity = capacity / n
		if i < capacity%n {
			s.capacity++
		}
		s.lru = list.New()
		s.entries = make(map[uint64]*list.Element)
		s.loading = make(map[uint64]*tableLoad)
	}
	return tc
}
func (tc *tableCache) shard(number uint64) *tableCacheShard {
	return &tc.shards[number%uint64(len(tc.shards))]
}
func (tc *tableCache) get(number uint64) (*SSTableReader, error) {
	e, err := tc.acquire(number)
	if err != nil {
		return nil, err
	}
	r := e.reader
	tc.release(e)
	return r, nil
}

// File opens/validation happen outside metadata locks. A per-file promise
// coalesces misses; references protect readers independently of LRU membership.
func (tc *tableCache) acquire(number uint64) (*cacheEntry, error) {
	s := tc.shard(number)
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return nil, fmt.Errorf("table cache closed")
		}
		if elem := s.entries[number]; elem != nil {
			e := elem.Value.(*cacheEntry)
			e.refs.Add(1)
			if e.touches.Add(1)%16 == 0 {
				s.lru.MoveToFront(elem)
			}
			tc.hits.Add(1)
			s.mu.Unlock()
			return e, nil
		}
		if pending := s.loading[number]; pending != nil {
			s.mu.Unlock()
			<-pending.done
			if pending.err != nil {
				return nil, pending.err
			}
			continue
		}
		pending := &tableLoad{done: make(chan struct{})}
		s.loading[number] = pending
		tc.misses.Add(1)
		s.mu.Unlock()
		reader, err := openSSTable(filepath.Join(tc.dir, fmt.Sprintf("%d.sst", number)), tc.blocks, number)
		if err == nil {
			reader.verifyChanges = tc.verifyChanges
		}
		s.mu.Lock()
		delete(s.loading, number)
		if err == nil && s.closed {
			reader.Close()
			err = fmt.Errorf("table cache closed")
		}
		var entry *cacheEntry
		if err == nil {
			if elem := s.entries[number]; elem != nil {
				reader.Close()
				entry = elem.Value.(*cacheEntry)
			} else {
				entry = tc.installLocked(s, number, reader)
			}
			entry.refs.Add(1)
		}
		pending.err = err
		close(pending.done)
		s.mu.Unlock()
		return entry, err
	}
}
func (tc *tableCache) release(e *cacheEntry) {
	if e.refs.Add(-1) == 0 {
		e.reader.Close()
	}
}
func (tc *tableCache) install(number uint64, reader *SSTableReader) {
	reader.verifyChanges = tc.verifyChanges
	s := tc.shard(number)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.entries[number] != nil {
		reader.Close()
		return
	}
	tc.installLocked(s, number, reader)
}
func (tc *tableCache) installLocked(s *tableCacheShard, number uint64, reader *SSTableReader) *cacheEntry {
	if s.lru.Len() >= s.capacity {
		elem := s.lru.Back()
		if elem != nil {
			e := elem.Value.(*cacheEntry)
			delete(s.entries, e.number)
			s.lru.Remove(elem)
			tc.release(e)
		}
	}
	e := &cacheEntry{number: number, reader: reader}
	e.refs.Store(1)
	s.entries[number] = s.lru.PushFront(e)
	return e
}
func (tc *tableCache) evict(number uint64) {
	s := tc.shard(number)
	s.mu.Lock()
	defer s.mu.Unlock()
	if elem := s.entries[number]; elem != nil {
		delete(s.entries, number)
		s.lru.Remove(elem)
		tc.release(elem.Value.(*cacheEntry))
	}
}
func (tc *tableCache) close() {
	for i := range tc.shards {
		s := &tc.shards[i]
		s.mu.Lock()
		s.closed = true
		for _, elem := range s.entries {
			tc.release(elem.Value.(*cacheEntry))
		}
		s.entries = make(map[uint64]*list.Element)
		s.lru.Init()
		s.mu.Unlock()
	}
	tc.blocks.close()
}
