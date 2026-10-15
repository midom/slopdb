package storage

import "sync/atomic"

// Cache records can be reused after eviction, but their readers remain
// immutable. A borrower keeps its reader and Go-owned bytes alive after close.
type cachedHeapBlock struct {
	key        blockCacheKey
	reader     *BlockReader
	prev, next *cachedHeapBlock
	referenced atomic.Uint32
}

func (c *blockCache) lookupHeapReader(key blockCacheKey, touch bool) (*BlockReader, bool) {
	s := c.shard(key)
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, false
	}
	entry := s.heapBlocks[key]
	if entry == nil {
		s.misses.Add(1)
		s.mu.RUnlock()
		return nil, false
	}
	hits := s.hits.Add(1)
	reader := entry.reader
	refresh := false
	if touch && s.evicted {
		if c.policy == BlockCacheCLOCK {
			if entry.referenced.Load() == 0 {
				entry.referenced.Store(1)
			}
		} else {
			refresh = hits%16 == 0
		}
	}
	s.mu.RUnlock()
	if refresh && s.mu.TryLock() {
		if !s.closed && s.heapBlocks[key] == entry {
			s.heapLRU.MoveToFront(entry)
		}
		s.mu.Unlock()
	}
	return reader, true
}

func (c *blockCache) addHeapReader(key blockCacheKey, reader *BlockReader) {
	s := c.shard(key)
	if len(reader.data) > s.capacity {
		return
	}
	// Admission is best effort for both backends. Never queue a writer behind
	// active readers just to retain a page that has already satisfied the read.
	if !s.mu.TryLock() {
		s.admissionSkips.Add(1)
		return
	}
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if entry := s.heapBlocks[key]; entry != nil {
		if c.policy == BlockCacheCLOCK {
			entry.referenced.Store(1)
		} else {
			s.heapLRU.MoveToFront(entry)
		}
		return
	}
	// The incoming reader can hold pooled decoding scratch. The cache must own
	// an exact-length Go slice and a nonpooled reader before scratch is returned.
	data := make([]byte, len(reader.data))
	copy(data, reader.data)
	owned := NewBlockReader(data)
	owned.data = data // preserve the full payload even for a corrupt block
	before := s.used
	var reusable *cachedHeapBlock
	for s.used+len(data) > s.capacity {
		entry := s.heapLRU.Back()
		if c.policy == BlockCacheCLOCK {
			entry = s.heapClockVictim(reusable)
		}
		if entry == nil {
			panic("heap block cache budget has no eviction victim")
		}
		s.evicted = true
		s.used -= len(entry.reader.data)
		delete(s.heapBlocks, entry.key)
		if reusable == nil {
			reusable = entry
			if c.policy != BlockCacheCLOCK {
				s.heapLRU.MoveToFront(entry)
			}
		} else {
			s.heapLRU.Remove(entry)
		}
	}
	if reusable != nil {
		reusable.key, reusable.reader = key, owned
		reusable.referenced.Store(1)
		s.heapBlocks[key] = reusable
	} else {
		entry := &cachedHeapBlock{key: key, reader: owned}
		entry.referenced.Store(1)
		if c.policy == BlockCacheCLOCK {
			s.heapLRU.PushBack(entry)
			if s.heapHand == nil {
				s.heapHand = entry
			}
		} else {
			s.heapLRU.PushFront(entry)
		}
		s.heapBlocks[key] = entry
	}
	s.used += len(data)
	if delta := s.used - before; delta != 0 {
		c.used.Add(int64(delta))
	}
}

// Called with the shard writer lock. One reused node remains attached to the
// ring, but is already uncharged and absent from the lookup map during eviction.
func (s *blockCacheShard) heapClockVictim(reusable *cachedHeapBlock) *cachedHeapBlock {
	if s.heapHand == nil {
		s.heapHand = s.heapLRU.Front()
	}
	if s.heapHand == nil {
		return nil
	}
	for {
		entry := s.heapHand
		s.heapHand = entry.Next()
		if s.heapHand == nil {
			s.heapHand = s.heapLRU.Front()
		}
		if entry != reusable && entry.referenced.Swap(0) == 0 {
			return entry
		}
	}
}
