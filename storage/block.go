package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"
)

const blockRestartInterval = 16

type BlockBuilder struct {
	buf      []byte
	restarts []uint32
	counter  int
	lastKey  []byte
	finished bool
}

func NewBlockBuilder(blockSize int) *BlockBuilder {
	return &BlockBuilder{
		buf:      make([]byte, 0, blockSize),
		restarts: make([]uint32, 0, 64),
	}
}

// reset reuses private writer buffers after their bytes have been consumed.
func (b *BlockBuilder) reset() {
	b.buf = b.buf[:0]
	b.restarts = b.restarts[:0]
	b.counter = 0
	b.lastKey = b.lastKey[:0]
	b.finished = false
}

func (b *BlockBuilder) Add(key, value []byte) {
	if b.counter == 0 {
		b.restarts = append(b.restarts, uint32(len(b.buf)))
		b.counter = 1
		putVarint(&b.buf, 0)
		putVarint(&b.buf, uint64(len(key)))
		putVarint(&b.buf, uint64(len(value)))
		b.buf = append(b.buf, key...)
		b.buf = append(b.buf, value...)
		b.lastKey = append(b.lastKey[:0], key...)
		return
	}

	shared := 0
	for shared < len(b.lastKey) && shared < len(key) && b.lastKey[shared] == key[shared] {
		shared++
	}

	nonShared := len(key) - shared
	putVarint(&b.buf, uint64(shared))
	putVarint(&b.buf, uint64(nonShared))
	putVarint(&b.buf, uint64(len(value)))
	b.buf = append(b.buf, key[shared:]...)
	b.buf = append(b.buf, value...)
	b.lastKey = append(b.lastKey[:0], key...)
	b.counter++

	if b.counter >= blockRestartInterval {
		b.counter = 0
	}
}

func (b *BlockBuilder) Finish() []byte {
	if b.finished {
		return b.buf
	}
	b.finished = true
	for _, r := range b.restarts {
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, r)
		b.buf = append(b.buf, tmp...)
	}
	tmp := make([]byte, 4)
	binary.LittleEndian.PutUint32(tmp, uint32(len(b.restarts)))
	b.buf = append(b.buf, tmp...)
	crc := crc32.ChecksumIEEE(b.buf)
	binary.LittleEndian.PutUint32(tmp, crc)
	b.buf = append(b.buf, tmp...)
	return b.buf
}

func (b *BlockBuilder) Empty() bool { return len(b.buf) == 0 && len(b.restarts) == 0 }

type BlockReader struct {
	data         []byte
	restarts     []byte
	numRestarts  int
	err          error
	arena        *blockArena
	arenaOffset  int
	decodeBuffer *blockDecodeBuffer // private transient decode; never shared in cache
	pooled       bool
}

var blockReaderViews = sync.Pool{New: func() any { return new(BlockReader) }}

// Each internal view owns one pin. Releasing it also returns private Go
// decode buffers and the cleared view to their bounded pools.
func (r *BlockReader) release() {
	if r != nil && r.arena != nil {
		r.arena.release(r.arenaOffset)
	} else if r != nil && r.decodeBuffer != nil {
		blockDecodeBuffers.Put(r.decodeBuffer)
		r.decodeBuffer = nil
	}
	if r != nil && r.pooled {
		*r = BlockReader{}
		blockReaderViews.Put(r)
	}
}

func NewBlockReader(data []byte) *BlockReader {
	r := new(BlockReader)
	r.reset(data)
	return r
}

func borrowedBlockReader(data []byte) *BlockReader {
	r := blockReaderViews.Get().(*BlockReader)
	r.reset(data)
	r.pooled = true
	return r
}

func (r *BlockReader) reset(data []byte) {
	*r = BlockReader{}
	if len(data) < 4 {
		r.err = fmt.Errorf("block too short")
		return
	}
	numRestarts := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
	if numRestarts > (len(data)-4)/4 {
		r.err = fmt.Errorf("invalid restart array")
		return
	}
	restartOffset := len(data) - 4 - numRestarts*4
	restarts := data[restartOffset : len(data)-4]
	var previous uint32
	for i := 0; i < numRestarts; i++ {
		offset := binary.LittleEndian.Uint32(restarts[i*4:])
		if int(offset) >= restartOffset || (i > 0 && offset <= previous) {
			r.err = fmt.Errorf("invalid restart array")
			return
		}
		previous = offset
	}
	r.data, r.restarts, r.numRestarts = data, restarts, numRestarts
}

func (r *BlockReader) restart(i int) int { return int(binary.LittleEndian.Uint32(r.restarts[i*4:])) }

// Restart keys have no shared prefix and can be compared without decoding or
// allocating a temporary iterator/key for every binary-search probe.
func (r *BlockReader) restartKey(i int) ([]byte, error) {
	data := r.data[r.restart(i) : len(r.data)-4-r.numRestarts*4]
	shared, size, valueSize, n := blockEntryHeader(data)
	if n <= 0 || shared != 0 || size > uint64(len(data)-n) || valueSize > uint64(len(data)-n)-size {
		return nil, fmt.Errorf("invalid restart entry")
	}
	return data[n : n+int(size)], nil
}

type BlockIterator struct {
	block                    *BlockReader
	offset                   int
	prevOffset               int
	prevOffset2              int
	key                      []byte
	prevKey                  []byte
	value                    []byte
	prevValue                []byte
	valid                    bool
	err                      error
	reuseKey                 bool // private lookups and explicitly borrowed streaming iterators
	reverseKeyScratch        []byte
	reverseEntries           []blockPosition
	reverseStart, reverseEnd int
}

type blockPosition struct {
	offset, next int
	key, value   []byte
}

func (r *BlockReader) NewIterator() *BlockIterator {
	it := &BlockIterator{block: r, offset: 0, err: r.err}
	if len(r.restarts) > 0 {
		it.offset = r.restart(0)
	}
	it.Next()
	return it
}

func (it *BlockIterator) Valid() bool { return it.valid }
func (it *BlockIterator) Err() error  { return it.err }

func (it *BlockIterator) Key() []byte   { return it.key }
func (it *BlockIterator) Value() []byte { return it.value }

func (it *BlockIterator) Next() {
	if it.err != nil {
		it.valid = false
		return
	}
	end := len(it.block.data) - it.block.numRestarts*4 - 4
	if it.offset < 0 || it.offset >= end {
		it.valid = false
		return
	}
	start := it.offset
	shared, unshared, size, n := blockEntryHeader(it.block.data[it.offset:end])
	if n <= 0 {
		it.err = fmt.Errorf("invalid block varint")
		it.valid = false
		return
	}
	it.offset += n
	if shared > uint64(len(it.key)) || unshared > uint64(end-it.offset) || size > uint64(end-it.offset)-unshared {
		it.err = fmt.Errorf("invalid block entry length")
		it.valid = false
		return
	}
	var key []byte
	if it.reuseKey {
		key = it.key[:int(shared)]
	} else {
		key = append([]byte(nil), it.key[:int(shared)]...)
	}
	key = append(key, it.block.data[it.offset:it.offset+int(unshared)]...)
	it.offset += int(unshared)
	it.value = it.block.data[it.offset : it.offset+int(size)]
	it.offset += int(size)
	it.key = key
	it.trimReusableScratch()
	it.prevOffset = start
	it.valid = true
}

func (it *BlockIterator) Prev() {
	target := it.prevOffset
	if !it.valid {
		target = len(it.block.data) - it.block.numRestarts*4 - 4
	}
	if it.err != nil {
		it.valid = false
		return
	}
	if len(it.reverseEntries) > 0 && target > it.reverseStart && target <= it.reverseEnd {
		it.restorePrevious(target)
		return
	}
	lo, hi := 0, it.block.numRestarts
	for lo < hi {
		mid := (lo + hi) / 2
		if it.block.restart(mid) < target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == 0 {
		it.valid = false
		it.offset = 0
		return
	}
	start := it.block.restart(lo - 1)
	scan := BlockIterator{block: it.block, offset: start, reuseKey: it.reuseKey}
	if it.reuseKey {
		scan.key = it.reverseKeyScratch[:0]
	}
	old := it.reverseEntries
	it.reverseEntries = it.reverseEntries[:0]
	for scan.offset < target {
		offset := scan.offset
		scan.Next()
		if !scan.Valid() {
			break
		}
		key := scan.key
		if it.reuseKey {
			var dst []byte
			if len(it.reverseEntries) < cap(old) {
				dst = old[:cap(old)][len(it.reverseEntries)].key
			}
			key = append(dst[:0], key...)
		}
		it.reverseEntries = append(it.reverseEntries, blockPosition{offset: offset, next: scan.offset, key: key, value: scan.value})
	}
	if it.reuseKey {
		it.reverseKeyScratch = scan.key
	}
	if scan.err != nil {
		it.err = scan.err
		it.valid = false
		return
	}
	if len(it.reverseEntries) == 0 {
		it.valid = false
		it.offset = 0
		return
	}
	it.reverseStart = start
	it.reverseEnd = target
	it.restorePrevious(target)
}

// Reconstruct each restart interval once while walking backward, then reuse
// its decoded positions. Keys remain immutable for callers retaining them.
func (it *BlockIterator) restorePrevious(target int) {
	lo, hi := 0, len(it.reverseEntries)
	for lo < hi {
		mid := (lo + hi) / 2
		if it.reverseEntries[mid].offset < target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	position := it.reverseEntries[lo-1]
	it.prevOffset = position.offset
	it.offset = position.next
	if it.reuseKey {
		it.key = append(it.key[:0], position.key...)
	} else {
		it.key = position.key
	}
	it.value = position.value
	it.valid = true
	it.trimReusableScratch()
}

func (it *BlockIterator) SeekToLast() {
	if it.err != nil || it.block.numRestarts == 0 {
		it.valid = false
		return
	}
	it.valid = false
	it.Prev()
}

func (it *BlockIterator) Seek(key []byte) {
	it.valid = false
	reuse := it.reuseKey
	if reuse {
		it.key = it.key[:0]
	} else {
		it.key = nil
	}
	// Intermediate seek keys are private. Reconstruct them in one buffer;
	// restore immutable-key iteration before exposing the final position.
	it.reuseKey = true
	defer func() { it.reuseKey = reuse }()
	it.value = nil
	it.offset = 0
	if len(it.block.restarts) == 0 {
		return
	}
	// Choose the restart strictly before the target, preserving older versions
	// when one user key spans restart points.
	if it.block.numRestarts == 1 {
		// Either outcome of the binary search selects restart zero.
		it.offset = it.block.restart(0)
	} else {
		lo, hi := 0, it.block.numRestarts
		for lo < hi {
			mid := (lo + hi) / 2
			probe, err := it.block.restartKey(mid)
			if err != nil {
				it.err = err
				return
			}
			if bytes.Compare(probe, key) < 0 {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		if lo > 0 {
			it.offset = it.block.restart(lo - 1)
		}
	}
	for it.Next(); it.Valid(); it.Next() {
		if bytes.Compare(it.key, key) >= 0 {
			return
		}
	}
}

func bytesLE(a, b []byte) bool {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return true
		}
		if a[i] > b[i] {
			return false
		}
	}
	return len(a) <= len(b)
}

func putVarint(buf *[]byte, v uint64) {
	tmp := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(tmp, v)
	*buf = append(*buf, tmp[:n]...)
}

func getVarint(data []byte) (uint64, int) {
	return binary.Uvarint(data)
}

// Most pages use one-byte prefix lengths and one- or two-byte value lengths.
// Decode that common header together, retaining the full varint decoder and
// overflow/truncation checks for long keys and values.
func blockEntryHeader(data []byte) (shared, unshared, size uint64, n int) {
	if len(data) >= 3 && data[0] < 128 && data[1] < 128 {
		if data[2] < 128 {
			return uint64(data[0]), uint64(data[1]), uint64(data[2]), 3
		}
		if len(data) >= 4 && data[3] < 128 {
			return uint64(data[0]), uint64(data[1]), uint64(data[2]&127) | uint64(data[3])<<7, 4
		}
	}
	shared, n = binary.Uvarint(data)
	if n <= 0 {
		return 0, 0, 0, 0
	}
	offset := n
	unshared, n = binary.Uvarint(data[offset:])
	if n <= 0 {
		return 0, 0, 0, 0
	}
	offset += n
	size, n = binary.Uvarint(data[offset:])
	if n <= 0 {
		return 0, 0, 0, 0
	}
	return shared, unshared, size, offset + n
}
