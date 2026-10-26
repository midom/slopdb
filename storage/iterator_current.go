package storage

import "bytes"

// currentKey fuses Valid/Key through native wrappers. Keys have precisely the
// underlying iterator's lifetime; reusable merge heap entries refresh their
// borrowed cache before any comparison after moving that child.
type currentKeyIterator interface{ currentKey() (InternalKey, bool) }

func iteratorCurrentKey(it Iterator) (InternalKey, bool) {
	if current, ok := it.(currentKeyIterator); ok {
		return current.currentKey()
	}
	if !it.Valid() {
		return InternalKey{}, false
	}
	return it.Key(), true
}

func (it *SSTableIterator) currentKey() (InternalKey, bool) { return it.key, it.valid }
func (it *levelIterator) currentKey() (InternalKey, bool) {
	if it.err != nil || it.iter == nil {
		return InternalKey{}, false
	}
	return it.iter.currentKey()
}
func (it *viewTableIterator) currentKey() (InternalKey, bool) { return iteratorCurrentKey(it.Iterator) }
func (it *maxSeqIter) currentKey() (InternalKey, bool)        { return iteratorCurrentKey(it.inner) }
func (it *fileIterator) currentKey() (InternalKey, bool)      { return it.iter.currentKey() }
func (it *MemTableIterator) currentKey() (InternalKey, bool) {
	if it.table.head != nil {
		if it.skip.current == nil {
			return InternalKey{}, false
		}
		return it.skip.current.key, true
	}
	if it.pos < 0 || it.pos >= len(it.nodes) {
		return InternalKey{}, false
	}
	return it.nodes[it.pos].key, true
}

func (m *mergeIterator) newHeapItem(it Iterator, reverse bool) *mergeHeapItem {
	item := &mergeHeapItem{iter: it, reverse: reverse, cached: m.reusable}
	if item.cached {
		item.key, _ = iteratorCurrentKey(it)
	}
	return item
}

// Skip duplicate versions using native cursor fields, without repeatedly
// copying InternalKey through Valid/Key wrappers for the common single version.
func (it *SSTableIterator) SkipUserKey(key []byte) {
	for it.valid && bytes.Equal(it.key.UserKey, key) {
		it.Next()
	}
}
func (it *levelIterator) SkipUserKey(key []byte) {
	for {
		current, valid := it.currentKey()
		if !valid || !bytes.Equal(current.UserKey, key) {
			return
		}
		it.iter.SkipUserKey(key)
		for it.iter != nil && !it.iter.valid && it.iter.err == nil {
			it.open(it.index + 1)
			if it.iter != nil {
				it.iter.Seek(nil)
			}
		}
	}
}

// Values are copied only after currentKey has established source validity.
// This avoids repeating Valid through every wrapper before an owned copy.
type currentValueIterator interface{ currentValueInto([]byte) []byte }

func iteratorCurrentValueInto(it Iterator, dst []byte) []byte {
	if current, ok := it.(currentValueIterator); ok {
		return current.currentValueInto(dst)
	}
	return IteratorValueInto(it, dst)
}
func (it *SSTableIterator) currentValueInto(dst []byte) []byte { return append(dst[:0], it.value...) }
func (it *levelIterator) currentValueInto(dst []byte) []byte   { return it.iter.currentValueInto(dst) }
func (it *maxSeqIter) currentValueInto(dst []byte) []byte {
	return iteratorCurrentValueInto(it.inner, dst)
}
func (it *viewTableIterator) currentValueInto(dst []byte) []byte {
	return iteratorCurrentValueInto(it.Iterator, dst)
}
func (it *fileIterator) currentValueInto(dst []byte) []byte { return it.iter.currentValueInto(dst) }
func (it *MemTableIterator) currentValueInto(dst []byte) []byte {
	if it.table.head != nil {
		return append(dst[:0], it.skip.current.value...)
	}
	return append(dst[:0], it.nodes[it.pos].value...)
}

// Context checks cover skipped/deleted groups as well as emitted rows. The
// ordinary Iterator API pays no cancellation bookkeeping when no context exists.
func (m *mergeIterator) pollScanContext() bool {
	if m.scanContext == nil {
		return false
	}
	m.scanSteps++
	if m.scanSteps&127 != 0 {
		return false
	}
	if err := m.scanContext.Err(); err != nil {
		m.err = err
		return true
	}
	return false
}

func (m *mergeIterator) skipReusableKey(it Iterator, key []byte, current InternalKey) (InternalKey, bool) {
	if m.scanContext == nil {
		skipIteratorKey(it, key)
		return iteratorCurrentKey(it)
	}
	valid := true
	for valid && bytes.Equal(current.UserKey, key) {
		if m.pollScanContext() {
			return InternalKey{}, false
		}
		it.Next()
		current, valid = iteratorCurrentKey(it)
	}
	return current, valid
}
