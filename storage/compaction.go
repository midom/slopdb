package storage

import (
	"bytes"
	"container/heap"
	"fmt"
	"os"
	"path/filepath"
)

// Compact one higher-level file and its overlapping destination files instead
// of rewriting the entire destination level. Range tombstones can extend past
// a file's largest key, so preserve the full-level policy when they are present.
func (l *LSMTree) compactionInputsLocked(level int) []*FileMetaData {
	return l.compactionInputsForLocked(level, 0)
}
func (l *LSMTree) compactionInputsForLocked(level, start int) []*FileMetaData {
	source, destination := l.levels[level], l.levels[level+1]
	for _, files := range [][]*FileMetaData{source, destination} {
		for _, f := range files {
			if f.hasRangeDeletes {
				return append(append([]*FileMetaData(nil), source...), destination...)
			}
		}
	}
	if level > 0 {
		source = source[start : start+1]
	}
	inputs := append([]*FileMetaData(nil), source...)
	smallest, largest := source[0].Smallest.UserKey, source[0].Largest.UserKey
	for _, f := range source[1:] {
		if bytes.Compare(f.Smallest.UserKey, smallest) < 0 {
			smallest = f.Smallest.UserKey
		}
		if bytes.Compare(f.Largest.UserKey, largest) > 0 {
			largest = f.Largest.UserKey
		}
	}
	for _, f := range destination {
		if bytes.Compare(f.Largest.UserKey, smallest) >= 0 && bytes.Compare(f.Smallest.UserKey, largest) <= 0 {
			inputs = append(inputs, f)
		}
	}
	return inputs
}

type rawTableHeap []*SSTableIterator

func (h rawTableHeap) Len() int           { return len(h) }
func (h rawTableHeap) Less(i, j int) bool { return h[i].Key().Compare(h[j].Key()) < 0 }
func (h rawTableHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *rawTableHeap) Push(v any)        { *h = append(*h, v.(*SSTableIterator)) }
func (h *rawTableHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return last
}

// Merge sorted raw entries, including tombstones, with bounded output buffers.
// The normal user iterator hides old versions and tombstones and cannot be used
// here. The caller holds no storage mutex during reads, compression or file sync.
func (l *LSMTree) buildCompaction(inputs []*FileMetaData, oldest uint64, level int) (output []*FileMetaData, err error) {
	var entries []*cacheEntry
	defer func() {
		for _, entry := range entries {
			l.tableCache.release(entry)
		}
		if err != nil {
			for _, f := range output {
				l.tableCache.evict(f.Number)
				os.Remove(filepath.Join(l.dir, fmt.Sprintf("%d.sst", f.Number)))
			}
		}
	}()
	var h rawTableHeap
	for _, f := range inputs {
		entry, e := l.tableCache.acquire(f.Number)
		if e != nil {
			return output, e
		}
		entries = append(entries, entry)
		it := entry.reader.newIterator(true, false, false)
		if e := it.Err(); e != nil {
			return output, e
		}
		if it.Valid() {
			h = append(h, it)
		}
	}
	heap.Init(&h)
	// File size stays bounded even in large levels. Never split a user key's
	// versions across files, so point and snapshot reads retain their ordering.
	target := l.opt.BaseLevelSize * 1024 * 1024
	var chunk []KeyValue
	var size int64
	flush := func() error {
		l.mu.Lock()
		meta, e := l.writeTableLocked(chunk, level)
		l.mu.Unlock()
		if e != nil {
			return e
		}
		output = append(output, meta)
		chunk = chunk[:0]
		size = 0
		return nil
	}
	var key []byte
	below := false
	for h.Len() > 0 {
		it := h[0]
		kv := KeyValue{Key: it.Key(), Value: it.Value()}
		if !bytes.Equal(key, kv.Key.UserKey) {
			if size >= target && len(chunk) > 0 {
				if e := flush(); e != nil {
					return output, e
				}
			}
			key = kv.Key.UserKey
			below = false
		}
		if kv.Key.OpType == kTypeRangeDelete || kv.Key.SeqNum > oldest || !below {
			chunk = append(chunk, kv)
			size += int64(len(kv.Key.UserKey) + len(kv.Value) + 48)
			if kv.Key.OpType != kTypeRangeDelete && kv.Key.SeqNum <= oldest {
				below = true
			}
		}
		it.Next()
		if e := it.Err(); e != nil {
			return output, e
		}
		if it.Valid() {
			heap.Fix(&h, 0)
		} else {
			heap.Pop(&h)
		}
	}
	if len(chunk) > 0 {
		if e := flush(); e != nil {
			return output, e
		}
	}
	return output, nil
}
