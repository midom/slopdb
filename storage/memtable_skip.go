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

// insertSorted owns its input bytes and accounts for one installed node.
func (m *MemTable) insertSorted(ik InternalKey, value []byte) {
	var update [maxHeight]*skipNode
	m.insertWithFinger(ik, value, &update, false)
	m.accountSorted(int64(len(ik.UserKey)+len(value)+32), 1)
}

func (m *MemTable) accountSorted(size, count int64) {
	m.approxSz.Add(size)
	m.entries.Add(count)
}

// A private sorted batch can reuse predecessors. Competing insertions only
// advance links; a saved predecessor remains a safe starting point.
func (m *MemTable) insertSortedBatch(entries []KeyValue) {
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key.Compare(entries[j].Key) < 0 })
	var update [maxHeight]*skipNode
	var size int64
	for _, entry := range entries {
		m.insertWithFinger(entry.Key, entry.Value, &update, true)
		size += int64(len(entry.Key.UserKey) + len(entry.Value) + 32)
	}
	m.accountSorted(size, int64(len(entries)))
}

// No writer mutex is needed. Publish the bottom link first, then add upper
// links with CAS. Keys and values are immutable and owned by this memtable.
// Other inserters can change a predecessor or successor while we search;
// failed CAS retries advance from a predecessor that is still before the key.
func (m *MemTable) insertWithFinger(ik InternalKey, value []byte, update *[maxHeight]*skipNode, reuse bool) {
	h := m.randomHeight()
	for height := m.height.Load(); int32(h) > height; height = m.height.Load() {
		if m.height.CompareAndSwap(height, int32(h)) {
			break
		}
	}
	node := m.head
	for i := int(m.height.Load()) - 1; i >= 0; i-- {
		if reuse && update[i] != nil && update[i] != m.head && update[i].key.Compare(ik) < 0 && (node == m.head || update[i].key.Compare(node.key) > 0) {
			node = update[i]
		}
		for next := node.next[i].Load(); next != nil && next.key.Compare(ik) < 0; next = node.next[i].Load() {
			node = next
		}
		update[i] = node
	}
	// One owned allocation for key and value rather than two row copies.
	data := make([]byte, len(ik.UserKey)+len(value))
	keyEnd := len(ik.UserKey)
	copy(data, ik.UserKey)
	copy(data[keyEnd:], value)
	ik.UserKey = data[:keyEnd:keyEnd]
	inserted := &skipNode{key: ik, value: data[keyEnd:len(data):len(data)], next: make([]atomic.Pointer[skipNode], h)}
	for i := 0; i < h; i++ {
		for {
			previous := update[i]
			next := previous.next[i].Load()
			for next != nil && next.key.Compare(ik) < 0 {
				previous = next
				next = previous.next[i].Load()
			}
			update[i] = previous
			inserted.next[i].Store(next)
			if previous.next[i].CompareAndSwap(next, inserted) {
				break
			}
		}
		if reuse {
			update[i] = inserted
		}
	}
}

// Group participants install their own mutations. The leader accounts and
// publishes the whole group only after every participant has completed.
func (m *MemTable) installSortedMutations(batch []Mutation, base uint64) {
	var update [maxHeight]*skipNode
	for i, w := range batch {
		typ := uint8(kTypeValue)
		if w.Delete {
			typ = kTypeDelete
		}
		m.insertWithFinger(InternalKey{UserKey: w.Key, SeqNum: base + uint64(i) + 1, OpType: typ}, w.Value, &update, false)
	}
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
	if it.current != nil && bytes.Equal(it.current.key.UserKey, key) {
		node := it.current.next[0].Load()
		for node != nil && bytes.Equal(node.key.UserKey, key) {
			node = node.next[0].Load()
		}
		it.current = it.visibleForward(node)
		it.afterEnd = it.current == nil
		return
	}
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
