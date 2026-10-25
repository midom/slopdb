package storage

import (
	"bytes"
	"container/heap"
)

// iteratorScratchLimit bounds idle reusable key/value buffers. A current row
// may exceed the limit; its buffers are discarded on the next movement.
const iteratorScratchLimit = 64 << 10

// Clear page-backed references when releasing a block, while retaining only
// bounded, owned decoding scratch for the next page. Ordinary block iterators
// never use this reset because their exposed keys must remain immutable.
func (it *BlockIterator) resetReusableBlock() {
	key, scan, positions := it.key, it.reverseKeyScratch, it.reverseEntries
	size := cap(key) + cap(scan) + cap(positions)*64
	for i := range positions[:cap(positions)] {
		p := &positions[:cap(positions)][i]
		size += cap(p.key)
		p.value = nil
	}
	if size > iteratorScratchLimit {
		key, scan, positions = nil, nil, nil
	}
	*it = BlockIterator{reuseKey: true, key: key[:0], reverseKeyScratch: scan[:0], reverseEntries: positions[:0]}
}

// Reverse positions own their keys. Keep the restart cache only while its
// aggregate scratch fits the streaming budget; unusually large keys can be
// reconstructed again rather than retained for subsequent rows.
func (it *BlockIterator) trimReusableScratch() {
	if !it.reuseKey {
		return
	}
	if cap(it.key) > iteratorScratchLimit && len(it.key) <= iteratorScratchLimit {
		it.key = append([]byte(nil), it.key...)
	}
	size := cap(it.key) + cap(it.reverseKeyScratch) + cap(it.reverseEntries)*64
	for _, p := range it.reverseEntries[:cap(it.reverseEntries)] {
		size += cap(p.key)
	}
	if size > iteratorScratchLimit {
		it.reverseEntries = nil
		it.reverseKeyScratch = nil
	}
}

// IteratorValueInto copies the current value into dst, reusing its capacity.
// The returned bytes belong to the caller and survive movement and Close.
// dst must be exclusively caller-owned; its previous contents are overwritten.
// Implementations can copy directly from a borrowed value, avoiding an
// intermediate owned Value allocation. Invalid iterators return dst[:0].
func IteratorValueInto(it Iterator, dst []byte) []byte {
	if !it.Valid() {
		return dst[:0]
	}
	if into, ok := it.(interface{ ValueInto([]byte) []byte }); ok {
		return into.ValueInto(dst)
	}
	return append(dst[:0], it.Value()...)
}

// ValueInto copies the current encoded value directly into exclusively
// caller-owned dst, overwriting its previous contents and reusing its capacity.
func (it *SSTableIterator) ValueInto(dst []byte) []byte {
	if !it.Valid() {
		return dst[:0]
	}
	return append(dst[:0], it.value...)
}
func (it *levelIterator) ValueInto(dst []byte) []byte {
	if !it.Valid() {
		return dst[:0]
	}
	return it.iter.ValueInto(dst)
}
func (it *maxSeqIter) ValueInto(dst []byte) []byte        { return IteratorValueInto(it.inner, dst) }
func (it *fileIterator) ValueInto(dst []byte) []byte      { return IteratorValueInto(it.iter, dst) }
func (it *viewTableIterator) ValueInto(dst []byte) []byte { return IteratorValueInto(it.Iterator, dst) }
func (it *boundedIterator) ValueInto(dst []byte) []byte {
	if !it.Valid() {
		return dst[:0]
	}
	return IteratorValueInto(it.Iterator, dst)
}

func (m *mergeIterator) nextReusable() {
	if m.reverse {
		if !m.valid {
			return
		}
		m.Seek(m.lastKey.UserKey)
		if m.err != nil {
			return
		}
	}
	m.visibleReusable(false)
}

// A child can overwrite its key and release its page on movement. Copy the
// grouping key and winning value before advancing it, including skipped MVCC
// versions. Every winner has the same user key, so the stable grouping copy
// also owns the winning InternalKey's user key.
func (m *mergeIterator) visibleReusable(reverse bool) {
	m.lastKey = InternalKey{}
	m.lastVal = nil
	if cap(m.groupScratch)+cap(m.valueScratch) > iteratorScratchLimit {
		m.groupScratch, m.valueScratch = nil, nil
	}
	if m.err != nil {
		m.valid = false
		return
	}
	for m.pq.Len() > 0 {
		m.groupScratch = append(m.groupScratch[:0], m.pq[0].iter.Key().UserKey...)
		key := m.groupScratch
		var best InternalKey
		found := false
		for m.pq.Len() > 0 && bytes.Equal(m.pq[0].iter.Key().UserKey, key) {
			top := m.pq[0]
			for top.iter.Valid() && bytes.Equal(top.iter.Key().UserKey, key) {
				k := top.iter.Key()
				if k.OpType != kTypeRangeDelete && (!found || k.SeqNum > best.SeqNum) {
					best = k
					best.UserKey = key
					if !m.keysOnly {
						m.valueScratch = IteratorValueInto(top.iter, m.valueScratch)
					}
					found = true
				}
				if reverse {
					top.iter.Prev()
				} else {
					// Forward order puts the newest visible version first.
					if k.OpType != kTypeRangeDelete {
						skipIteratorKey(top.iter, key)
						break
					}
					top.iter.Next()
				}
			}
			if err := IteratorError(top.iter); err != nil {
				m.err = err
				m.valid = false
				return
			}
			if top.iter.Valid() {
				heap.Fix(&m.pq, 0)
			} else {
				heap.Pop(&m.pq)
			}
		}
		if !found || best.OpType == kTypeDelete {
			continue
		}
		deleted := false
		for _, rd := range m.ranges {
			if rd.Key.SeqNum > best.SeqNum && bytes.Compare(key, rd.Key.UserKey) >= 0 && bytes.Compare(key, rd.Value) < 0 {
				deleted = true
				break
			}
		}
		if deleted {
			continue
		}
		// A skipped large version may have expanded scratch during this same
		// movement. Do not retain it behind a subsequently selected small row.
		if cap(m.groupScratch)+cap(m.valueScratch) > iteratorScratchLimit && len(m.groupScratch)+len(m.valueScratch) <= iteratorScratchLimit {
			keyCopy := make([]byte, len(m.groupScratch))
			valueCopy := make([]byte, len(m.valueScratch))
			copy(keyCopy, m.groupScratch)
			copy(valueCopy, m.valueScratch)
			m.groupScratch, m.valueScratch = keyCopy, valueCopy
			best.UserKey = keyCopy
		}
		m.lastKey, m.lastVal = best, m.valueScratch
		m.valid, m.afterEnd = true, false
		return
	}
	m.valid, m.afterEnd = false, !reverse
	if cap(m.groupScratch)+cap(m.valueScratch) > iteratorScratchLimit {
		m.groupScratch, m.valueScratch = nil, nil
	}
}
