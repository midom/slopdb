package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unsafe"
)

// SSTable indexes are immutable and already resident for the reader lifetime.
// Decode their keys/handles once, into two pointer-free arrays, rather than
// reconstruct prefix-compressed index keys on every point read. Iterators keep
// the original block representation; no persistent format changes are needed.
type pointIndexEntry struct {
	keyEnd  int
	handle  BlockHandle
	leading uint64
}

func (r *SSTableReader) pointIndexBytes() int64 {
	return int64(cap(r.pointIndex))*int64(unsafe.Sizeof(pointIndexEntry{})) + int64(cap(r.pointIndexKeys))
}

func (r *SSTableReader) buildPointIndex() error {
	estimate := r.indexBlock.numRestarts * blockRestartInterval
	if estimate > len(r.indexData)/3 {
		estimate = len(r.indexData) / 3
	}
	r.pointIndex = make([]pointIndexEntry, 0, estimate)
	it := BlockIterator{block: r.indexBlock, reuseKey: true, err: r.indexBlock.err}
	for it.Next(); it.Valid(); it.Next() {
		value := it.Value()
		offset, n := binary.Uvarint(value)
		if n <= 0 {
			return fmt.Errorf("invalid block handle")
		}
		size, n := binary.Uvarint(value[n:])
		if n <= 0 || offset > r.fileSize || size > r.fileSize-offset {
			return fmt.Errorf("invalid block handle")
		}
		if len(r.pointIndex) > 0 && bytes.Compare(r.pointIndexKey(len(r.pointIndex)-1), it.Key()) > 0 {
			return fmt.Errorf("unordered SSTable index")
		}
		r.pointIndexKeys = append(r.pointIndexKeys, it.Key()...)
		r.pointIndex = append(r.pointIndex, pointIndexEntry{keyEnd: len(r.pointIndexKeys), handle: BlockHandle{offset, size}})
	}
	if err := it.Err(); err != nil {
		return err
	}
	if len(r.pointIndex) > 0 {
		first, last := r.pointIndexKey(0), r.pointIndexKey(len(r.pointIndex)-1)
		common := 0
		for common < len(first) && common < len(last) && first[common] == last[common] {
			common++
		}
		r.pointIndexPrefix = first[:common]
		for i := range r.pointIndex {
			r.pointIndex[i].leading = pointLeading(r.pointIndexKey(i)[common:])
		}
	}
	return nil
}

func (r *SSTableReader) pointIndexKey(i int) []byte {
	start := 0
	if i > 0 {
		start = r.pointIndex[i-1].keyEnd
	}
	return r.pointIndexKeys[start:r.pointIndex[i].keyEnd]
}

func (r *SSTableReader) pointIndexSeek(key []byte) int {
	prefix := r.pointIndexPrefix
	n := len(prefix)
	if len(key) < n {
		n = len(key)
	}
	cmp := bytes.Compare(key[:n], prefix[:n])
	if cmp < 0 || cmp == 0 && len(key) < len(prefix) {
		return 0
	}
	if cmp > 0 {
		return len(r.pointIndex)
	}
	leading := pointLeading(key[len(prefix):])
	lo, hi := 0, len(r.pointIndex)
	for lo < hi {
		mid := lo + (hi-lo)/2
		entry := &r.pointIndex[mid]
		// Zero-padded leading bytes preserve order when unequal. Equal
		// prefixes (including embedded NULs and duplicate separators) require
		// the full key comparison; fingerprints never establish equality.
		if entry.leading < leading || entry.leading == leading && bytes.Compare(r.pointIndexKey(mid), key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func pointLeading(key []byte) uint64 {
	if len(key) >= 8 {
		return binary.BigEndian.Uint64(key)
	}
	var leading uint64
	for i, b := range key {
		leading |= uint64(b) << uint(56-8*i)
	}
	return leading
}
