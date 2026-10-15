package storage

// List links are arena offsets, stored in each page's header. No persistent
// per-page list nodes or reader records live on the Go heap. Shard writer
// ownership protects links; zero is the sentinel and offsets are encoded +1.
type blockCacheList struct {
	arena       *blockArena
	front, back uint64
	length      int
}

func (l *blockCacheList) Init()                   { l.front, l.back, l.length = 0, 0, 0 }
func (l *blockCacheList) Front() uint64           { return l.front }
func (l *blockCacheList) Back() uint64            { return l.back }
func (l *blockCacheList) Len() int                { return l.length }
func (l *blockCacheList) Next(link uint64) uint64 { return l.arena.word(int(link-1) + 64) }
func (l *blockCacheList) PushFront(link uint64) {
	off := int(link - 1)
	l.arena.set(off+56, 0)
	l.arena.set(off+64, l.front)
	if l.front != 0 {
		l.arena.set(int(l.front-1)+56, link)
	} else {
		l.back = link
	}
	l.front = link
	l.length++
}
func (l *blockCacheList) PushBack(link uint64) {
	off := int(link - 1)
	l.arena.set(off+56, l.back)
	l.arena.set(off+64, 0)
	if l.back != 0 {
		l.arena.set(int(l.back-1)+64, link)
	} else {
		l.front = link
	}
	l.back = link
	l.length++
}
func (l *blockCacheList) Remove(link uint64) {
	off := int(link - 1)
	prev, next := l.arena.word(off+56), l.arena.word(off+64)
	if prev != 0 {
		l.arena.set(int(prev-1)+64, next)
	} else {
		l.front = next
	}
	if next != 0 {
		l.arena.set(int(next-1)+56, prev)
	} else {
		l.back = prev
	}
	l.arena.set(off+56, 0)
	l.arena.set(off+64, 0)
	l.length--
}
func (l *blockCacheList) MoveToFront(link uint64) {
	if l.front != link {
		l.Remove(link)
		l.PushFront(link)
	}
}
