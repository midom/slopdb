package storage

import (
	"bytes"
	"fmt"
	"sort"
	"sync/atomic"
)

type viewFile struct {
	meta  *FileMetaData
	entry *cacheEntry
	level int
}
type readView struct {
	refs       atomic.Int64
	retired    atomic.Bool
	closed     atomic.Bool
	padding    [54]byte // isolate retirement flags from reader reference writes
	readerRefs [snapshotReaderShards]readerViewRefs
	mem, imm   *MemTable
	tableSeq   uint64
	files      []viewFile
	levels     [][]viewFile
	ordered    []bool
	hasRanges  []bool
	cache      *tableCache
}

type readerViewRefs struct {
	count   atomic.Int64
	padding [56]byte
}

func (v *readView) release() {
	if v.refs.Add(-1) == 0 {
		v.closeIfUnused()
	}
}

func (v *readView) releaseReader(shard uint64) {
	v.readerRefs[shard].count.Add(-1)
	if v.retired.Load() {
		v.closeIfUnused()
	}
}

func (v *readView) closeIfUnused() {
	if v.refs.Load() != 0 {
		return
	}
	for i := range v.readerRefs {
		if v.readerRefs[i].count.Load() != 0 {
			return
		}
	}
	if v.closed.CompareAndSwap(false, true) {
		for _, slot := range v.files {
			v.cache.release(slot.entry)
		}
	}
}

// Reader IDs select an independent registration lock and reference counter.
// Publication excludes all registrations, so retirement cannot free a view
// between loading its pointer and pinning it. Data reads hold no gate lock.
func (l *LSMTree) acquireViewForReader(reader uint64) (*readView, uint64, uint64) {
	index := reader & (snapshotReaderShards - 1)
	shard := &l.readerSnapshots[index]
	shard.mu.Lock()
	v, seq := l.view, l.published.Load()
	if v != nil {
		v.readerRefs[index].count.Add(1)
	}
	shard.mu.Unlock()
	return v, seq, index
}

// Callers serialize publishers with l.mu. Lock order agrees with the legacy
// view gate and snapshot registration; no reader takes viewMu under a shard.
func (l *LSMTree) replaceView(next *readView) *readView {
	l.viewMu.Lock()
	for i := range l.readerSnapshots {
		l.readerSnapshots[i].mu.Lock()
	}
	old := l.view
	l.view = next
	if old != nil {
		old.retired.Store(true)
	}
	for i := len(l.readerSnapshots) - 1; i >= 0; i-- {
		l.readerSnapshots[i].mu.Unlock()
	}
	l.viewMu.Unlock()
	return old
}

// The small view gate makes incrementing refs atomic with retirement. It never
// covers data reads, file opens, WAL work, or memtable installation.
func (l *LSMTree) acquireView() (*readView, uint64) {
	l.viewMu.RLock()
	v := l.view
	seq := l.published.Load()
	if v != nil {
		v.refs.Add(1)
	}
	l.viewMu.RUnlock()
	return v, seq
}
func (l *LSMTree) publishViewLocked() error {
	if l.closed {
		return nil
	}
	v := &readView{mem: l.mem, imm: l.imm, tableSeq: l.tableSeq, cache: l.tableCache, levels: make([][]viewFile, len(l.levels)), ordered: make([]bool, len(l.levels)), hasRanges: make([]bool, len(l.levels))}
	v.refs.Store(1)
	for level, files := range l.levels {
		for _, f := range files {
			entry, err := l.tableCache.acquire(f.Number)
			if err != nil {
				v.release()
				return err
			}
			slot := viewFile{meta: f, entry: entry, level: level}
			v.files = append(v.files, slot)
			v.levels[level] = append(v.levels[level], slot)
			v.hasRanges[level] = v.hasRanges[level] || len(entry.reader.rangeDeletes) > 0
		}
		ordered := v.levels[level]
		sort.Slice(ordered, func(i, j int) bool {
			return bytes.Compare(ordered[i].meta.Smallest.UserKey, ordered[j].meta.Smallest.UserKey) < 0
		})
		v.ordered[level] = level > 0
		for i := 1; i < len(ordered); i++ {
			if bytes.Compare(ordered[i-1].meta.Largest.UserKey, ordered[i].meta.Smallest.UserKey) >= 0 {
				v.ordered[level] = false
			}
		}
	}
	// Newer tables are more likely to contain the winning point version.
	// Once it is found, tables whose maximum sequence is older can be skipped.
	sort.SliceStable(v.levels[0], func(i, j int) bool { return v.levels[0][i].entry.reader.maxSeq > v.levels[0][j].entry.reader.maxSeq })
	old := l.replaceView(v)
	if old != nil {
		old.release()
	}
	return nil
}
func (l *LSMTree) iteratorView(snapshot uint64, latest bool) Iterator {
	return l.iteratorViewAt(snapshot, latest, nil)
}

// NewIteratorAtSeek starts at a target without first opening each table's
// first data block. The snapshot must remain pinned for the iterator lifetime.
func (l *LSMTree) NewIteratorAtSeek(snapshot uint64, key []byte) Iterator {
	return l.iteratorViewAt(snapshot, false, key)
}
func (l *LSMTree) NewIteratorSeek(key []byte) Iterator { return l.iteratorViewAt(0, true, key) }
func (l *LSMTree) iteratorViewAt(snapshot uint64, latest bool, key []byte) Iterator {
	return l.iteratorViewRange(snapshot, latest, key, nil, nil, false)
}

func (l *LSMTree) NewIteratorAtRange(snapshot uint64, lower, upper []byte, reverse bool) Iterator {
	return l.iteratorViewRange(snapshot, false, lower, lower, upper, reverse)
}
func (l *LSMTree) NewIteratorRange(lower, upper []byte, reverse bool) Iterator {
	return l.iteratorViewRange(0, true, lower, lower, upper, reverse)
}
func (l *LSMTree) iteratorViewRange(snapshot uint64, latest bool, key, lower, upper []byte, reverse bool) Iterator {
	v, seq := l.acquireView()
	if v == nil {
		return &emptyIterator{err: fmt.Errorf("database closed")}
	}
	if latest || snapshot > seq {
		snapshot = seq
	}
	iters := make([]Iterator, 0, len(v.levels)+2)
	iters = append(iters, v.mem.NewIteratorAt(snapshot))
	if v.imm != nil {
		iters = append(iters, v.imm.NewIteratorAt(snapshot))
	}
	for level, files := range v.levels {
		selected := files
		if lower != nil || upper != nil {
			if v.ordered[level] && !v.hasRanges[level] {
				lo, hi := 0, len(files)
				if lower != nil {
					lo = sort.Search(len(files), func(i int) bool { return bytes.Compare(files[i].meta.Largest.UserKey, lower) >= 0 })
				}
				if upper != nil {
					hi = sort.Search(len(files), func(i int) bool { return bytes.Compare(files[i].meta.Smallest.UserKey, upper) >= 0 })
				}
				if hi < lo {
					hi = lo
				}
				selected = files[lo:hi]
			} else {
				selected = make([]viewFile, 0, len(files))
				for _, slot := range files {
					if len(slot.entry.reader.rangeDeletes) == 0 &&
						((lower != nil && bytes.Compare(slot.meta.Largest.UserKey, lower) < 0) ||
							(upper != nil && bytes.Compare(slot.meta.Smallest.UserKey, upper) >= 0)) {
						continue
					}
					selected = append(selected, slot)
				}
			}
		}
		if len(selected) == 0 {
			continue
		}
		if v.ordered[level] {
			iters = append(iters, newMaxSeqIter(&levelIterator{files: selected, index: -1}, snapshot))
		} else {
			for _, slot := range selected {
				iters = append(iters, newMaxSeqIter(&viewTableIterator{Iterator: slot.entry.reader.newIterator(false, true, false), reader: slot.entry.reader}, snapshot))
			}
		}
	}
	if reverse {
		key = upper
	}
	merged := newMergeIteratorDirectional(iters, key, reverse, &viewCloser{view: v})
	if lower != nil || upper != nil {
		return BoundIterator(merged, lower, upper)
	}
	return merged
}

type viewCloser struct{ view *readView }

func (c *viewCloser) Close() {
	if c.view != nil {
		c.view.release()
		c.view = nil
	}
}

type viewTableIterator struct {
	Iterator
	reader *SSTableReader
}

func (it *viewTableIterator) RangeTombstones() []KeyValue { return it.reader.rangeDeletes }

func (it *viewTableIterator) Err() error { return IteratorError(it.Iterator) }

func (it *viewTableIterator) SeekToLast() { seekIteratorLast(it.Iterator) }
