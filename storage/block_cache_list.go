package storage

// Intrusive typed links avoid a separate container/list node and interface for
// every resident page. Only shard-locked code reads or changes these links.
// Borrowed BlockReaders remain immutable when their cache records are reused.
type blockCacheList struct {
	front, back *cachedBlock
	length      int
}

func (l *blockCacheList) Init()               { *l = blockCacheList{} }
func (l *blockCacheList) Front() *cachedBlock { return l.front }
func (l *blockCacheList) Back() *cachedBlock  { return l.back }
func (l *blockCacheList) Len() int            { return l.length }
func (b *cachedBlock) Next() *cachedBlock     { return b.next }

func (l *blockCacheList) PushFront(b *cachedBlock) *cachedBlock {
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

func (l *blockCacheList) PushBack(b *cachedBlock) *cachedBlock {
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

func (l *blockCacheList) Remove(b *cachedBlock) {
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

func (l *blockCacheList) MoveToFront(b *cachedBlock) {
	if l.front != b {
		l.Remove(b)
		l.PushFront(b)
	}
}
