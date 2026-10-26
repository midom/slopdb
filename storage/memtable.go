package storage

import (
	"bytes"
	"encoding/binary"
	"slices"
	"sync"
	"sync/atomic"
)

const (
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

// memNode is immutable after publication. Hash chains can contain several
// versions of a key; point reads select the greatest visible sequence.
type memNode struct {
	key   InternalKey
	value []byte
	next  *memNode
	order uint64
}

const memHashBuckets = 1 << 16

type memSorted struct{ nodes []*memNode }

type MemTable struct {
	mu         sync.RWMutex
	head       *skipNode
	height     atomic.Int32
	buckets    []atomic.Pointer[memNode]
	nodes      []*memNode // mu protects the append log and sorted-cache publication
	sorted     atomic.Pointer[memSorted]
	approxSz   atomic.Int64
	entries    atomic.Int64
	nodeChunk  []memNode
	nodeOffset int
	byteChunk  []byte
	byteOffset int
	refs       atomic.Int32
	ranges     []KeyValue
	maxSeq     atomic.Uint64
	ordered    atomic.Bool
	lastSeq    uint64 // mu serializes insertion order
	hasRanges  atomic.Bool
}

func NewMemTable() *MemTable {
	return newMemTableSized(0, false)
}

func newMemTableSized(writeBufferSize int, hash bool) *MemTable {
	if !hash {
		m := &MemTable{head: &skipNode{next: make([]atomic.Pointer[skipNode], maxHeight)}}
		m.height.Store(1)
		return m
	}
	buckets := memHashBuckets
	for buckets < writeBufferSize/128 && buckets < 1<<20 {
		buckets *= 2
	}
	m := &MemTable{buckets: make([]atomic.Pointer[memNode], buckets)}
	m.ordered.Store(true)
	return m
}
func (m *MemTable) Ref()   { m.refs.Add(1) }
func (m *MemTable) Unref() { m.refs.Add(-1) }

func memKeyHash(key []byte) uint64 {
	h := uint64(14695981039346656037)
	for _, b := range key {
		h = (h ^ uint64(b)) * 1099511628211
	}
	// Spread short integer keys across all bucket bits.
	h ^= h >> 32
	return h
}

func (m *MemTable) Put(seq uint64, opType uint8, key, value []byte) {
	ik := InternalKey{UserKey: key, SeqNum: seq, OpType: opType}
	if m.head != nil {
		m.insertSorted(ik, value)
		if opType == kTypeRangeDelete {
			m.mu.Lock()
			m.ranges = append(m.ranges, KeyValue{Key: InternalKey{UserKey: bytes.Clone(key), SeqNum: seq, OpType: opType}, Value: bytes.Clone(value)})
			m.hasRanges.Store(true)
			m.mu.Unlock()
		}
		for previous := m.maxSeq.Load(); seq > previous; previous = m.maxSeq.Load() {
			if m.maxSeq.CompareAndSwap(previous, seq) {
				break
			}
		}
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.insert(ik, value)
	if opType == kTypeRangeDelete {
		m.ranges = append(m.ranges, KeyValue{Key: InternalKey{UserKey: bytes.Clone(key), SeqNum: seq, OpType: opType}, Value: bytes.Clone(value)})
		m.hasRanges.Store(true)
	}
	if seq > m.maxSeq.Load() {
		m.maxSeq.Store(seq)
	}
}

// Hash writers hold mu and copy inputs into immutable chunks. Skip-list
// insertion owns its bytes and uses atomic links without a writer mutex.
// Readers filter nodes using the committed sequence watermark.
func (m *MemTable) insert(ik InternalKey, value []byte) {
	if m.head != nil {
		m.insertSorted(ik, value)
		return
	}
	size := len(ik.UserKey) + len(value)
	if len(m.byteChunk)-m.byteOffset < size {
		m.byteChunk = make([]byte, max(64<<10, size))
		m.byteOffset = 0
	}
	start := m.byteOffset
	keyEnd := start + len(ik.UserKey)
	end := start + size
	copy(m.byteChunk[start:keyEnd], ik.UserKey)
	copy(m.byteChunk[keyEnd:end], value)
	ik.UserKey = m.byteChunk[start:keyEnd:keyEnd]
	ownedValue := m.byteChunk[keyEnd:end:end]
	m.byteOffset = end
	if m.nodeOffset == len(m.nodeChunk) {
		m.nodeChunk = make([]memNode, 512)
		m.nodeOffset = 0
	}
	node := &m.nodeChunk[m.nodeOffset]
	m.nodeOffset++
	bucket := &m.buckets[memKeyHash(ik.UserKey)&uint64(len(m.buckets)-1)]
	if ik.SeqNum < m.lastSeq {
		m.ordered.Store(false)
	}
	m.lastSeq = ik.SeqNum
	*node = memNode{key: ik, value: ownedValue, next: bucket.Load(), order: uint64(m.entries.Load())}
	m.nodes = append(m.nodes, node)
	m.sorted.Store(nil)
	bucket.Store(node)
	m.approxSz.Add(int64(len(ik.UserKey) + len(value) + 32))
	m.entries.Add(1)
}

func (m *MemTable) Get(key []byte) ([]byte, bool) { v, _, ok := m.GetWithSeq(key); return v, ok }
func (m *MemTable) GetWithSeq(key []byte) ([]byte, uint64, bool) {
	kv, found, _ := m.lookupAt(key, m.maxSeq.Load())
	if !found {
		return nil, 0, false
	}
	if kv.Key.OpType == kTypeDelete {
		return nil, kv.Key.SeqNum, false
	}
	return kv.Value, kv.Key.SeqNum, true
}
func (m *MemTable) IsDeletedByRange(key []byte) bool { _, ok := m.IsDeletedByRangeSeq(key); return ok }
func (m *MemTable) IsDeletedByRangeSeq(key []byte) (uint64, bool) {
	if !m.hasRanges.Load() {
		return 0, false
	}
	snapshot := m.maxSeq.Load()
	m.mu.RLock()
	defer m.mu.RUnlock()
	var seq uint64
	found := false
	for _, rd := range m.ranges {
		if rd.Key.SeqNum <= snapshot && bytes.Compare(key, rd.Key.UserKey) >= 0 && bytes.Compare(key, rd.Value) < 0 {
			if !found || rd.Key.SeqNum > seq {
				seq = rd.Key.SeqNum
			}
			found = true
		}
	}
	return seq, found
}
func (m *MemTable) rangeSequence(key []byte, snapshot uint64) uint64 {
	if !m.hasRanges.Load() {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var seq uint64
	for _, rd := range m.ranges {
		if rd.Key.SeqNum <= snapshot && rd.Key.SeqNum > seq && bytes.Compare(key, rd.Key.UserKey) >= 0 && bytes.Compare(key, rd.Value) < 0 {
			seq = rd.Key.SeqNum
		}
	}
	return seq
}
func (m *MemTable) ApproximateSize() int { return int(m.approxSz.Load()) }

// Scans sort a private copy of the append log. Frozen tables reuse that sorted
// copy. Sorting holds no writer lock, and iterators never observe later nodes.
func (m *MemTable) sortedNodes() []*memNode {
	if cached := m.sorted.Load(); cached != nil {
		return cached.nodes
	}
	m.mu.RLock()
	nodes := append([]*memNode(nil), m.nodes...)
	m.mu.RUnlock()
	slices.SortFunc(nodes, func(a, b *memNode) int {
		if c := a.key.Compare(b.key); c != 0 {
			return c
		}
		if a.order > b.order {
			return -1
		}
		if a.order < b.order {
			return 1
		}
		return 0
	})
	m.mu.Lock()
	if len(m.nodes) == len(nodes) {
		m.sorted.Store(&memSorted{nodes: nodes})
	}
	m.mu.Unlock()
	return nodes
}
func (m *MemTable) NewIterator() *MemTableIterator { return m.NewIteratorAt(^uint64(0)) }
func (m *MemTable) NewIteratorAt(snapshot uint64) *MemTableIterator {
	if maxSeq := m.maxSeq.Load(); snapshot > maxSeq {
		snapshot = maxSeq
	}
	if m.head != nil {
		it := &sortedMemTableIterator{table: m, maxSeq: snapshot}
		it.current = it.visibleForward(m.head.next[0].Load())
		it.afterEnd = it.current == nil
		return &MemTableIterator{table: m, maxSeq: snapshot, skip: *it}
	}
	it := &MemTableIterator{table: m, nodes: m.sortedNodes(), maxSeq: snapshot}
	it.skipForward()
	return it
}

type MemTableIterator struct {
	table  *MemTable
	skip   sortedMemTableIterator
	nodes  []*memNode
	pos    int
	maxSeq uint64
}

func (it *MemTableIterator) skipForward() {
	for it.pos < len(it.nodes) && it.nodes[it.pos].key.SeqNum > it.maxSeq {
		it.pos++
	}
}
func (it *MemTableIterator) Valid() bool {
	if it.table.head != nil {
		return it.skip.Valid()
	}
	return it.pos >= 0 && it.pos < len(it.nodes)
}
func (it *MemTableIterator) Next() {
	if it.table.head != nil {
		it.skip.Next()
		return
	}
	if it.Valid() {
		it.pos++
		it.skipForward()
	}
}
func (it *MemTableIterator) Prev() {
	if it.table.head != nil {
		it.skip.Prev()
		return
	}
	if it.pos < 0 {
		return
	}
	it.pos--
	for it.pos >= 0 && it.nodes[it.pos].key.SeqNum > it.maxSeq {
		it.pos--
	}
}
func (it *MemTableIterator) Key() InternalKey {
	if it.table.head != nil {
		return it.skip.Key()
	}
	if it.Valid() {
		return it.nodes[it.pos].key
	}
	return InternalKey{}
}
func (it *MemTableIterator) Value() []byte {
	if it.table.head != nil {
		return it.skip.Value()
	}
	if it.Valid() {
		return it.nodes[it.pos].value
	}
	return nil
}
func (it *MemTableIterator) Seek(key []byte) {
	if it.table.head != nil {
		it.skip.Seek(key)
		return
	}
	target := InternalKey{UserKey: key, SeqNum: it.maxSeq}
	it.pos, _ = slices.BinarySearchFunc(it.nodes, target, func(n *memNode, k InternalKey) int { return n.key.Compare(k) })
	it.skipForward()
}
func (it *MemTableIterator) SkipUserKey(key []byte) {
	if it.table.head != nil {
		it.skip.SkipUserKey(key)
		return
	}
	if it.pos >= 0 && it.pos < len(it.nodes) && bytes.Equal(it.nodes[it.pos].key.UserKey, key) {
		for it.pos < len(it.nodes) && bytes.Equal(it.nodes[it.pos].key.UserKey, key) {
			it.pos++
		}
		it.skipForward()
		return
	}
	lo, hi := 0, len(it.nodes)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if bytes.Compare(it.nodes[mid].key.UserKey, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	it.pos = lo
	it.skipForward()
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
func (it *MemTableIterator) Close() {
	if it.table.head != nil {
		it.skip.Close()
	}
	it.nodes = nil
	it.pos = -1
}
func (it *MemTableIterator) SeekToLast() {
	if it.table.head != nil {
		it.skip.SeekToLast()
		return
	}
	it.pos = len(it.nodes)
	it.Prev()
}

func (m *MemTable) lookupAt(key []byte, snapshot uint64) (KeyValue, bool, uint64) {
	rangeSeq := m.rangeSequence(key, snapshot)
	if m.head != nil {
		for node := m.findGreaterOrEqual(InternalKey{UserKey: key, SeqNum: snapshot}); node != nil && bytes.Equal(node.key.UserKey, key); node = node.next[0].Load() {
			if node.key.SeqNum <= snapshot && node.key.OpType != kTypeRangeDelete {
				return KeyValue{Key: node.key, Value: node.value}, true, rangeSeq
			}
		}
		return KeyValue{}, false, rangeSeq
	}
	var best *memNode
	node := m.buckets[memKeyHash(key)&uint64(len(m.buckets)-1)].Load()
	ordered := m.ordered.Load()
	for ; node != nil; node = node.next {
		if node.key.SeqNum <= snapshot && node.key.OpType != kTypeRangeDelete && bytes.Equal(node.key.UserKey, key) && (best == nil || node.key.SeqNum > best.key.SeqNum) {
			best = node
			// WAL installation is in increasing sequence order. The first visible
			// matching node is therefore newest; direct out-of-order Put falls back.
			if ordered {
				break
			}
		}
	}
	if best != nil {
		return KeyValue{Key: best.key, Value: best.value}, true, rangeSeq
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
