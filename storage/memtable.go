package storage

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
)

const (
	maxHeight        = 12
	branching        = 4
	kTypeValue       = 1
	kTypeDelete      = 0
	kTypeRangeDelete = 2
)

type InternalKey struct {
	UserKey []byte
	SeqNum  uint64
	OpType  uint8
}

func (k InternalKey) Compare(other InternalKey) int {
	c := bytes.Compare(k.UserKey, other.UserKey)
	if c != 0 {
		return c
	}
	if k.SeqNum > other.SeqNum {
		return -1
	} else if k.SeqNum < other.SeqNum {
		return 1
	}
	return 0
}

func (k InternalKey) String() string {
	return string(k.UserKey)
}

type KeyValue struct {
	Key   InternalKey
	Value []byte
}

type skipNode struct {
	key   InternalKey
	value []byte
	next  []atomic.Pointer[skipNode]
}

type MemTable struct {
	mu        sync.RWMutex
	head      *skipNode
	height    atomic.Int32
	approxSz  atomic.Int64
	arena     []byte
	arenaOff  int
	refs      atomic.Int32
	ranges    []KeyValue
	maxSeq    atomic.Uint64
	hasRanges atomic.Bool
}

func NewMemTable() *MemTable {
	head := &skipNode{
		next: make([]atomic.Pointer[skipNode], maxHeight),
	}
	m := &MemTable{head: head, arena: make([]byte, 1024*1024)}
	m.height.Store(1)
	return m
}

func (m *MemTable) Ref()   { m.refs.Add(1) }
func (m *MemTable) Unref() { m.refs.Add(-1) }

func (m *MemTable) randomHeight() int {
	h := 1
	for h < maxHeight && rand.Intn(branching) == 0 {
		h++
	}
	return h
}

func (m *MemTable) Put(seq uint64, opType uint8, key, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ik := InternalKey{UserKey: append([]byte(nil), key...), SeqNum: seq, OpType: opType}
	m.insert(ik, value)
	if opType == kTypeRangeDelete {
		m.ranges = append(m.ranges, KeyValue{Key: ik, Value: append([]byte(nil), value...)})
		m.hasRanges.Store(true)
	}
	if seq > m.maxSeq.Load() {
		m.maxSeq.Store(seq)
	}
}

func (m *MemTable) insert(ik InternalKey, value []byte) {
	var update [maxHeight]*skipNode
	m.insertWithFinger(ik, value, &update, false)
}

// A sorted atomic batch can reuse predecessors from its previous key rather
// than restart every skip-list search at the head. Sequence numbers retain
// their original WAL order; only installation order changes.
func (m *MemTable) insertBatch(entries []KeyValue) {
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

func (m *MemTable) Get(key []byte) ([]byte, bool) {
	v, _, ok := m.GetWithSeq(key)
	return v, ok
}

func (m *MemTable) GetWithSeq(key []byte) ([]byte, uint64, bool) {
	// Capture publication before traversing: installed but unpublished nodes
	// must not replace the previous committed value.
	kv, found, _ := m.lookupAt(key, m.maxSeq.Load())
	if !found {
		return nil, 0, false
	}
	if kv.Key.OpType == kTypeDelete {
		return nil, kv.Key.SeqNum, false
	}
	return kv.Value, kv.Key.SeqNum, true
}

func (m *MemTable) IsDeletedByRange(key []byte) bool {
	_, covered := m.IsDeletedByRangeSeq(key)
	return covered
}

func (m *MemTable) IsDeletedByRangeSeq(key []byte) (uint64, bool) {
	snapshot := m.maxSeq.Load()
	var maxRangeSeq uint64
	found := false
	node := m.head.next[0].Load()
	for node != nil {
		if node.key.OpType == kTypeRangeDelete && node.key.SeqNum <= snapshot {
			start := node.key.UserKey
			end := node.value
			if bytes.Compare(key, start) >= 0 && bytes.Compare(key, end) < 0 {
				if node.key.SeqNum > maxRangeSeq {
					maxRangeSeq = node.key.SeqNum
					found = true
				}
			}
		}
		node = node.next[0].Load()
	}
	return maxRangeSeq, found
}

func (m *MemTable) ApproximateSize() int { return int(m.approxSz.Load()) }

// Nodes and values are immutable; links are atomic. Cap the sequence so
// concurrent inserts never enter an iterator's view.
func (m *MemTable) NewIterator() *MemTableIterator {
	return m.NewIteratorAt(^uint64(0))
}

func (m *MemTable) NewIteratorAt(snapshot uint64) *MemTableIterator {
	if maxSeq := m.maxSeq.Load(); snapshot > maxSeq {
		snapshot = maxSeq
	}
	it := &MemTableIterator{table: m, maxSeq: snapshot}
	it.current = it.visibleForward(m.head.next[0].Load())
	it.afterEnd = it.current == nil
	return it
}

type MemTableIterator struct {
	table    *MemTable
	current  *skipNode
	maxSeq   uint64
	afterEnd bool
}

// Concurrent insertion only changes atomic links, never the current node.
func (it *MemTableIterator) visibleForward(node *skipNode) *skipNode {
	for node != nil && node.key.SeqNum > it.maxSeq {
		node = node.next[0].Load()
	}
	return node
}

func (it *MemTableIterator) Valid() bool { return it.current != nil }
func (it *MemTableIterator) Next() {
	if it.current != nil {
		it.current = it.visibleForward(it.current.next[0].Load())
		it.afterEnd = it.current == nil
	}
}
func (it *MemTableIterator) Prev() {
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
func (it *MemTableIterator) Key() InternalKey {
	if it.Valid() {
		return it.current.key
	}
	return InternalKey{}
}
func (it *MemTableIterator) Value() []byte {
	if it.Valid() {
		return it.current.value
	}
	return nil
}
func (it *MemTableIterator) Seek(key []byte) {
	target := InternalKey{UserKey: key, SeqNum: it.maxSeq}
	it.current = it.visibleForward(it.table.findGreaterOrEqual(target))
	it.afterEnd = it.current == nil
}
func (it *MemTableIterator) SkipUserKey(key []byte) {
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
func (it *MemTableIterator) RangeTombstones() []KeyValue {
	if !it.table.hasRanges.Load() {
		return nil
	}
	it.table.mu.RLock()
	defer it.table.mu.RUnlock()
	var ranges []KeyValue
	for _, rd := range it.table.ranges {
		if rd.Key.SeqNum <= it.maxSeq {
			ranges = append(ranges, rd)
		}
	}
	return ranges
}
func (it *MemTableIterator) Close() { it.current = nil; it.afterEnd = false }

func (m *MemTable) lookupAt(key []byte, snapshot uint64) (KeyValue, bool, uint64) {
	var rangeSeq uint64
	if m.hasRanges.Load() {
		m.mu.RLock()
		for _, rd := range m.ranges {
			if rd.Key.SeqNum <= snapshot && rd.Key.SeqNum > rangeSeq && bytes.Compare(key, rd.Key.UserKey) >= 0 && bytes.Compare(key, rd.Value) < 0 {
				rangeSeq = rd.Key.SeqNum
			}
		}
		m.mu.RUnlock()
	}
	for node := m.findGreaterOrEqual(InternalKey{UserKey: key, SeqNum: snapshot}); node != nil && bytes.Equal(node.key.UserKey, key); node = node.next[0].Load() {
		if node.key.SeqNum <= snapshot && node.key.OpType != kTypeRangeDelete {
			return KeyValue{Key: node.key, Value: node.value}, true, rangeSeq
		}
	}
	return KeyValue{}, false, rangeSeq
}

func ParseInternalKey(data []byte) InternalKey {
	if len(data) < 8 {
		return InternalKey{UserKey: data}
	}
	userKey := data[:len(data)-8]
	tag := binary.LittleEndian.Uint64(data[len(data)-8:])
	return InternalKey{
		UserKey: userKey,
		SeqNum:  tag >> 8,
		OpType:  uint8(tag & 0xFF),
	}
}

func EncodeInternalKey(ik InternalKey) []byte {
	buf := make([]byte, len(ik.UserKey)+8)
	copy(buf, ik.UserKey)
	tag := (ik.SeqNum << 8) | uint64(ik.OpType)
	binary.LittleEndian.PutUint64(buf[len(ik.UserKey):], tag)
	return buf
}

func (it *MemTableIterator) SeekToLast() { it.current = nil; it.afterEnd = true; it.Prev() }
