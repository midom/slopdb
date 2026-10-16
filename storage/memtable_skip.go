package storage

import (
	"bytes"
	"math/rand"
	"sort"
	"sync/atomic"
)

const (
	maxHeight = 12
	branching = 4
)

type skipNode struct {
	key   InternalKey
	value []byte
	next  []atomic.Pointer[skipNode]
}

func (m *MemTable) randomHeight() int {
	h := 1
	for h < maxHeight && rand.Intn(branching) == 0 {
		h++
	}
	return h
}

func (m *MemTable) insertSorted(ik InternalKey, value []byte) {
	var update [maxHeight]*skipNode
	m.insertWithFinger(ik, value, &update, false)
}

// A sorted atomic batch can reuse predecessors from its previous key rather
// than restart every skip-list search at the head. Sequence numbers retain
// their original WAL order; only installation order changes.
func (m *MemTable) insertSortedBatch(entries []KeyValue) {
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key.Compare(entries[j].Key) < 0 })
	var update [maxHeight]*skipNode
	for _, entry := range entries {
		m.insertWithFinger(entry.Key, entry.Value, &update, true)
	}
}

// Caller serializes writers with mu; readers follow immutable atomic links.
func (m *MemTable) insertWithFinger(ik InternalKey, value []byte, update *[maxHeight]*skipNode, reuse bool) {
	node := m.head

	for i := int(m.height.Load()) - 1; i >= 0; i-- {
		if reuse && update[i] != nil && (node == m.head || update[i].key.Compare(node.key) > 0) {
			node = update[i]
		}
		for next := node.next[i].Load(); next != nil && next.key.Compare(ik) < 0; next = node.next[i].Load() {
			node = next
		}
		update[i] = node
	}

	h := m.randomHeight()
	height := int(m.height.Load())
	if h > height {
		for i := height; i < h; i++ {
			update[i] = m.head
		}
	}

	newNode := &skipNode{
		key:   ik,
		value: append([]byte{}, value...),
		next:  make([]atomic.Pointer[skipNode], h),
	}
	for i := 0; i < h; i++ {
		newNode.next[i].Store(update[i].next[i].Load())
	}
	// Initialize every link before publishing the immutable node. Writers are
	// serialized; readers follow atomic links and filter by their sequence.
	for i := 0; i < h; i++ {
		update[i].next[i].Store(newNode)
		if reuse {
			update[i] = newNode
		}
	}
	if h > height {
		m.height.Store(int32(h))
	}
	m.approxSz.Add(int64(len(ik.UserKey) + len(value) + 32))
	m.entries++
}

// Keep the successor observed at the bottom search step. Reloading the
// predecessor's link after that step could pick up a newly inserted smaller
// key and incorrectly report the requested key missing.
func (m *MemTable) findGreaterOrEqual(target InternalKey) *skipNode {
	node := m.head
	var next *skipNode
	for level := int(m.height.Load()) - 1; level >= 0; level-- {
		for {
			next = node.next[level].Load()
			if next == nil || next.key.Compare(target) >= 0 {
				break
			}
			node = next
		}
	}
	return next
}

type sortedMemTableIterator struct {
	table    *MemTable
	current  *skipNode
	maxSeq   uint64
	afterEnd bool
}

// Concurrent insertion only changes atomic links, never the current node.
func (it *sortedMemTableIterator) visibleForward(node *skipNode) *skipNode {
	for node != nil && node.key.SeqNum > it.maxSeq {
		node = node.next[0].Load()
	}
	return node
}

func (it *sortedMemTableIterator) Valid() bool { return it.current != nil }
func (it *sortedMemTableIterator) Next() {
	if it.current != nil {
		it.current = it.visibleForward(it.current.next[0].Load())
		it.afterEnd = it.current == nil
	}
}
func (it *sortedMemTableIterator) Prev() {
	if it.current == nil && !it.afterEnd {
		return
	}
	target := it.current
	for {
		node := it.table.head
		for level := int(it.table.height.Load()) - 1; level >= 0; level-- {
			for next := node.next[level].Load(); next != nil && (target == nil || next.key.Compare(target.key) < 0); next = node.next[level].Load() {
				node = next
			}
		}
		if node == it.table.head {
			it.current = nil
			it.afterEnd = false
			return
		}
		if node.key.SeqNum <= it.maxSeq {
			it.current = node
			it.afterEnd = false
			return
		}
		target = node
	}
}
func (it *sortedMemTableIterator) Key() InternalKey {
	if it.Valid() {
		return it.current.key
	}
	return InternalKey{}
}
func (it *sortedMemTableIterator) Value() []byte {
	if it.Valid() {
		return it.current.value
	}
	return nil
}
func (it *sortedMemTableIterator) Seek(key []byte) {
	target := InternalKey{UserKey: key, SeqNum: it.maxSeq}
	it.current = it.visibleForward(it.table.findGreaterOrEqual(target))
	it.afterEnd = it.current == nil
}
func (it *sortedMemTableIterator) SkipUserKey(key []byte) {
	node := it.table.head
	var next *skipNode
	for level := int(it.table.height.Load()) - 1; level >= 0; level-- {
		for {
			next = node.next[level].Load()
			if next == nil || bytes.Compare(next.key.UserKey, key) > 0 {
				break
			}
			node = next
		}
	}
	it.current = it.visibleForward(next)
	it.afterEnd = it.current == nil
}

func (it *sortedMemTableIterator) Close() { it.current = nil; it.afterEnd = false }

func (it *sortedMemTableIterator) SeekToLast() { it.current = nil; it.afterEnd = true; it.Prev() }
