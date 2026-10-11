package storage

import (
	"bytes"
	"sort"
)

// A non-overlapping level needs one active SSTable iterator. Opening later
// files is deferred until traversal reaches them; small LIMITs read no blocks
// from files beyond their result range. The read view owns every file handle.
type levelIterator struct {
	files []viewFile
	index int
	iter  *SSTableIterator
	err   error
}

func (it *levelIterator) open(index int) {
	it.index = index
	it.iter = nil
	if index >= 0 && index < len(it.files) {
		it.iter = it.files[index].entry.reader.newIterator(false, true, false)
	}
}
func (it *levelIterator) Valid() bool { return it.err == nil && it.iter != nil && it.iter.Valid() }
func (it *levelIterator) Err() error {
	if it.err != nil {
		return it.err
	}
	if it.iter != nil {
		return it.iter.Err()
	}
	return nil
}
func (it *levelIterator) Key() InternalKey {
	if it.Valid() {
		return it.iter.Key()
	}
	return InternalKey{}
}
func (it *levelIterator) Value() []byte {
	if it.Valid() {
		return it.iter.Value()
	}
	return nil
}
func (it *levelIterator) Seek(key []byte) {
	it.err = nil
	index := sort.Search(len(it.files), func(i int) bool { return bytes.Compare(it.files[i].meta.Largest.UserKey, key) >= 0 })
	it.open(index)
	for it.iter != nil {
		it.iter.Seek(key)
		if it.iter.Valid() || it.iter.Err() != nil {
			return
		}
		it.open(it.index + 1)
	}
}
func (it *levelIterator) Next() {
	if !it.Valid() {
		return
	}
	it.iter.Next()
	for it.iter != nil && !it.iter.Valid() && it.iter.Err() == nil {
		it.open(it.index + 1)
		if it.iter != nil {
			it.iter.Seek(nil)
		}
	}
}
func (it *levelIterator) Prev() {
	if it.Err() != nil {
		return
	}
	if it.iter != nil {
		it.iter.Prev()
	}
	for it.iter == nil || (!it.iter.Valid() && it.iter.Err() == nil) {
		it.open(it.index - 1)
		if it.iter == nil {
			return
		}
		it.iter.SeekToLast()
	}
}
func (it *levelIterator) SeekToLast() {
	it.err = nil
	it.open(len(it.files))
	it.Prev()
}
func (it *levelIterator) RangeTombstones() []KeyValue {
	var ranges []KeyValue
	for _, file := range it.files {
		ranges = append(ranges, file.entry.reader.rangeDeletes...)
	}
	return ranges
}
func (it *levelIterator) Close() { it.iter = nil }

type boundedIterator struct {
	Iterator
	lower, upper            []byte
	beforeLower, afterUpper bool
}

// BoundIterator limits visibility to [lower, upper), retaining child positions
// beyond a boundary so changing direction restores the last in-range entry.
func BoundIterator(it Iterator, lower, upper []byte) Iterator {
	bounded := &boundedIterator{Iterator: it, lower: lower, upper: upper}
	bounded.afterUpper = !bounded.Valid()
	return bounded
}
func (it *boundedIterator) Valid() bool {
	return it.Iterator.Valid() &&
		(it.lower == nil || bytes.Compare(it.Iterator.Key().UserKey, it.lower) >= 0) &&
		(it.upper == nil || bytes.Compare(it.Iterator.Key().UserKey, it.upper) < 0)
}
func (it *boundedIterator) Seek(key []byte) {
	if it.lower != nil && bytes.Compare(key, it.lower) < 0 {
		key = it.lower
	}
	it.Iterator.Seek(key)
	it.beforeLower = false
	it.afterUpper = !it.Valid()
}
func (it *boundedIterator) Next() {
	if it.beforeLower {
		it.Seek(it.lower)
		return
	}
	if it.afterUpper {
		return
	}
	if it.Iterator.Valid() && bytes.Compare(it.Iterator.Key().UserKey, it.lower) < 0 {
		it.Iterator.Seek(it.lower)
		return
	}
	it.Iterator.Next()
	it.afterUpper = !it.Valid()
}
func (it *boundedIterator) Prev() {
	if it.beforeLower {
		return
	}
	if it.upper != nil && (it.afterUpper || !it.Iterator.Valid() || bytes.Compare(it.Iterator.Key().UserKey, it.upper) >= 0) {
		if reverse, ok := it.Iterator.(interface{ SeekBefore([]byte) }); ok {
			reverse.SeekBefore(it.upper)
		} else {
			it.Iterator.Seek(it.upper)
			it.Iterator.Prev()
		}
		it.afterUpper = false
		it.beforeLower = !it.Valid()
		return
	}
	it.Iterator.Prev()
	it.afterUpper = false
	it.beforeLower = !it.Valid()
}
func (it *boundedIterator) SeekToLast() {
	if it.upper != nil {
		it.Iterator.Seek(it.upper)
		it.beforeLower = false
		it.afterUpper = true
		it.Prev()
	} else {
		seekIteratorLast(it.Iterator)
		it.beforeLower = !it.Valid()
		it.afterUpper = false
	}
}
