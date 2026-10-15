package storage

// Intrusive typed links avoid a separate container/list node and interface for
// every resident page. Only shard-locked code reads or changes these links.
// Borrowed BlockReaders remain immutable when their cache records are reused.
type heapBlockCacheList struct {
	front, back *cachedHeapBlock
	length      int
}

func (l *heapBlockCacheList) Init()                   { *l = heapBlockCacheList{} }
func (l *heapBlockCacheList) Front() *cachedHeapBlock { return l.front }
func (l *heapBlockCacheList) Back() *cachedHeapBlock  { return l.back }
func (l *heapBlockCacheList) Len() int                { return l.length }
func (b *cachedHeapBlock) Next() *cachedHeapBlock     { return b.next }

func (l *heapBlockCacheList) PushFront(b *cachedHeapBlock) *cachedHeapBlock {
	b.prev, b.next = nil, l.front
	if l.front != nil {
		l.front.prev = b
	} else {
		l.back = b
	}
	l.front = b
	l.length++
	return b
}

func (l *heapBlockCacheList) PushBack(b *cachedHeapBlock) *cachedHeapBlock {
	b.prev, b.next = l.back, nil
	if l.back != nil {
		l.back.next = b
	} else {
		l.front = b
	}
	l.back = b
	l.length++
	return b
}

func (l *heapBlockCacheList) Remove(b *cachedHeapBlock) {
	if b.prev != nil {
		b.prev.next = b.next
	} else {
		l.front = b.next
	}
	if b.next != nil {
		b.next.prev = b.prev
	} else {
		l.back = b.prev
	}
	b.prev, b.next = nil, nil
	l.length--
}

func (l *heapBlockCacheList) MoveToFront(b *cachedHeapBlock) {
	if l.front != b {
		l.Remove(b)
		l.PushFront(b)
	}
}
