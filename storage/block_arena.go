package storage

import (
	"encoding/binary"
	"math/bits"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	arenaAlignment = 64
	arenaHeader    = 128
	arenaFooter    = 8
	arenaBins      = 64 * 16
)

type blockArenaStats struct {
	mapped   atomic.Int64
	inUse    atomic.Int64
	failures atomic.Int64
}

// blockArena owns an anonymous mapping, independent of the Go heap. Headers,
// footers and free-list links are integers stored inside the mapping; no Go
// pointers are written there. The shard owns admission; this separate mutex
// protects allocation and the last-reference free. Ordinary pins and releases
// use an aligned atomic reference count in each page, without this mutex.
type blockArena struct {
	mu        sync.Mutex
	capacity  int
	data      []byte
	heads     [arenaBins]uint64 // offset+1; zero is an empty list
	nonempty  [arenaBins / 64]uint64
	live      int
	closed    bool
	unmapped  bool // data's slice header stays immutable after initial mapping
	stats     *blockArenaStats
	mapMemory func(int) ([]byte, error) // nil selects the platform allocator
}

func arenaBin(size int) int {
	log := bits.Len(uint(size)) - 1
	base := uint(1) << log
	return log*16 + int((uint(size)-base)>>(log-4))
}

func (a *blockArena) word(off int) uint64       { return binary.LittleEndian.Uint64(a.data[off:]) }
func (a *blockArena) set(off int, value uint64) { binary.LittleEndian.PutUint64(a.data[off:], value) }
func (a *blockArena) span(off int) int          { return int(a.word(off) &^ 1) }

// Mappings and allocation headers are at least 64-byte aligned. These pointers
// address integer words only; no Go pointers are stored in the mapping. An
// existing reference (or cache shard lock) keeps the mapping alive during use.
func (a *blockArena) atomicWord(off int) *uint64 {
	return (*uint64)(unsafe.Pointer(&a.data[off]))
}
func (a *blockArena) mark(off, size int, free bool) {
	v := uint64(size)
	if free {
		v |= 1
	}
	a.set(off, v)
	a.set(off+size-arenaFooter, v)
}
func (a *blockArena) insert(off, size int) {
	a.mark(off, size, true)
	bin := arenaBin(size)
	head := a.heads[bin]
	a.set(off+8, 0)
	a.set(off+16, head)
	if head != 0 {
		a.set(int(head-1)+8, uint64(off+1))
	}
	a.heads[bin] = uint64(off + 1)
	a.nonempty[bin/64] |= uint64(1) << (bin % 64)
}
func (a *blockArena) remove(off int) {
	bin := arenaBin(a.span(off))
	prev, next := a.word(off+8), a.word(off+16)
	if prev != 0 {
		a.set(int(prev-1)+16, next)
	} else {
		a.heads[bin] = next
	}
	if next != 0 {
		a.set(int(next-1)+8, prev)
	}
	if a.heads[bin] == 0 {
		a.nonempty[bin/64] &^= uint64(1) << (bin % 64)
	}
}

// Virtual address space includes bounded room for headers, alignment and
// fragmentation. Physical pages are populated on demand. Cache membership is
// still limited to the configured decoded-byte budget. Extra pinned pages can
// occupy this reserve; when it is exhausted, admission is skipped.
func (a *blockArena) allocate(size int) ([]byte, int, bool) {
	if size < 0 || size > int(^uint(0)>>1)-arenaHeader-arenaFooter-arenaAlignment {
		return nil, 0, false
	}
	need := (size + arenaHeader + arenaFooter + arenaAlignment - 1) &^ (arenaAlignment - 1)
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.capacity <= 0 {
		return nil, 0, false
	}
	if a.data == nil {
		page := os.Getpagesize()
		if a.capacity > (int(^uint(0)>>1)-(page-1))/2 {
			a.stats.failures.Add(1)
			return nil, 0, false
		}
		mapped := (2*a.capacity + page - 1) &^ (page - 1)
		mapper := a.mapMemory
		if mapper == nil {
			mapper = mapBlockArena
		}
		data, err := mapper(mapped)
		if err != nil {
			a.stats.failures.Add(1)
			return nil, 0, false
		}
		a.data = data
		a.stats.mapped.Add(int64(len(data)))
		a.insert(0, len(data)&^(arenaAlignment-1))
	}
	for bin := arenaBin(need); bin < len(a.heads); bin++ {
		mask := a.nonempty[bin/64] & (^uint64(0) << (bin % 64))
		if mask == 0 {
			bin = (bin/64+1)*64 - 1
			continue
		}
		bin = bin/64*64 + bits.TrailingZeros64(mask)
		for link := a.heads[bin]; link != 0; link = a.word(int(link-1) + 16) {
			off := int(link - 1)
			span := a.span(off)
			if span < need {
				continue
			}
			a.remove(off)
			if span > need {
				a.insert(off+need, span-need)
			}
			a.mark(off, need, false)
			atomic.StoreUint64(a.atomicWord(off+8), 1)
			a.set(off+16, uint64(size))
			a.live++
			a.stats.inUse.Add(int64(need))
			return a.data[off+arenaHeader : off+arenaHeader+size : off+arenaHeader+size], off, true
		}
	}
	a.stats.failures.Add(1)
	return nil, 0, false
}

func (a *blockArena) release(off int) {
	refs := atomic.AddUint64(a.atomicWord(off+8), ^uint64(0))
	if refs == ^uint64(0) {
		panic("block arena reference underflow")
	}
	if refs != 0 {
		return
	}
	// No new pin can appear after zero: cache membership or an existing pin
	// is required to acquire a reference. live still includes this allocation,
	// so close cannot unmap it while the final release waits for the mutex.
	a.mu.Lock()
	defer a.mu.Unlock()
	span := a.span(off)
	if a.word(off)&1 != 0 {
		panic("double free in block arena")
	}
	a.stats.inUse.Add(-int64(span))
	a.live--
	if off > 0 && a.word(off-arenaFooter)&1 != 0 {
		prev := int(a.word(off-arenaFooter) &^ 1)
		a.remove(off - prev)
		off -= prev
		span += prev
	}
	if next := off + span; next < len(a.data) && a.word(next)&1 != 0 {
		n := a.span(next)
		a.remove(next)
		span += n
	}
	a.insert(off, span)
	if a.closed && a.live == 0 {
		a.unmap()
	}
}

func (a *blockArena) close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closed = true
	if a.live == 0 {
		a.unmap()
	}
}

func (a *blockArena) pin(off int, referenced bool) {
	if atomic.AddUint64(a.atomicWord(off+8), 1) <= 1 {
		panic("pinning an unowned arena allocation")
	}
	if referenced {
		atomic.StoreUint64(a.atomicWord(off+72), 1)
	}
}

// The cache shard lock protects immutable header fields until pin acquires
// ownership. Only a short-lived reader view is created on the Go heap.
func (a *blockArena) reader(off int, referenced bool) *BlockReader {
	a.pin(off, referenced)
	r := blockReaderViews.Get().(*BlockReader)
	*r = BlockReader{arena: a, arenaOffset: off, pooled: true}
	size := int(a.word(off + 16))
	r.data = a.data[off+arenaHeader : off+arenaHeader+size : off+arenaHeader+size]
	r.numRestarts = int(a.word(off + 80))
	if r.numRestarts > 0 {
		r.restarts = r.data[size-4-r.numRestarts*4 : size-4]
	}
	return r
}

func (a *blockArena) cacheKey(off int) blockCacheKey {
	return blockCacheKey{table: a.word(off + 24), offset: a.word(off + 32), size: a.word(off + 40), modified: int64(a.word(off + 48))}
}

func (a *blockArena) setCacheKey(off int, key blockCacheKey) {
	a.set(off+24, key.table)
	a.set(off+32, key.offset)
	a.set(off+40, key.size)
	a.set(off+48, uint64(key.modified))
}
func (a *blockArena) unmap() {
	if a.data == nil || a.unmapped {
		return
	}
	if err := unmapBlockArena(a.data); err != nil {
		a.stats.failures.Add(1)
		return
	}
	a.stats.mapped.Add(-int64(len(a.data)))
	// Ordinary reference operations read the slice header without the allocator
	// mutex. Leave it immutable; closed prevents future allocations, and pins
	// prevent access to the mapping after the final live allocation is freed.
	a.unmapped = true
	a.heads = [arenaBins]uint64{}
	a.nonempty = [arenaBins / 64]uint64{}
}
