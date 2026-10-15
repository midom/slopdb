package storage

import (
	"bytes"
	"fmt"
	"sync"
)

// Point lookup needs no public iterator state. Reusable private buffers decode
// restart keys without allocating iterator objects or returning borrowed keys.
// Data blocks still use the shared cache and normal corruption checks.
type pointWorkspace struct{ dataKey []byte }

var pointWorkspaces = sync.Pool{New: func() any { return new(pointWorkspace) }}

func releasePointWorkspace(workspace *pointWorkspace) {
	// Large user keys must not turn transient scratch into unbounded retention.
	if cap(workspace.dataKey) > 64<<10 {
		*workspace = pointWorkspace{}
	}
	pointWorkspaces.Put(workspace)
}

func (r *SSTableReader) pointLookup(key []byte, snapshot uint64, workspace *pointWorkspace) (KeyValue, bool, uint64, error) {
	var rd uint64
	for _, kv := range r.rangeDeletes {
		if kv.Key.SeqNum <= snapshot && kv.Key.SeqNum > rd && bytes.Compare(key, kv.Key.UserKey) >= 0 && bytes.Compare(key, kv.Value) < 0 {
			rd = kv.Key.SeqNum
		}
	}
	bf := NewBloomFilter(10)
	if !bf.MayContain(r.filterData, key) {
		return KeyValue{}, false, rd, nil
	}
	if workspace.dataKey == nil {
		workspace.dataKey = make([]byte, 0, 64)
	}
	var data BlockIterator
	defer func() {
		if data.key != nil {
			workspace.dataKey = data.key
		}
	}()
	for index := r.pointIndexSeek(key); index < len(r.pointIndex); index++ {
		handle := r.pointIndex[index].handle
		reader, err := r.loadBlockReaderMode(handle.Offset, handle.Size, true)
		if err != nil {
			return KeyValue{}, false, rd, err
		}
		if reader.numRestarts == 0 {
			if reader.err != nil {
				return KeyValue{}, false, rd, reader.err
			}
			return KeyValue{}, false, rd, fmt.Errorf("invalid data block")
		}
		data = BlockIterator{block: reader, key: workspace.dataKey[:0], reuseKey: true, err: reader.err}
		data.Seek(key)
		for data.Valid() {
			k := ParseInternalKey(data.Key())
			cmp := bytes.Compare(k.UserKey, key)
			if cmp > 0 {
				return KeyValue{}, false, rd, nil
			}
			if cmp == 0 && k.SeqNum <= snapshot && k.OpType != kTypeRangeDelete {
				k.UserKey = key
				return KeyValue{Key: k, Value: data.Value()}, true, rd, nil
			}
			data.Next()
		}
		if data.Err() != nil {
			return KeyValue{}, false, rd, data.Err()
		}
		workspace.dataKey = data.key
		// Versions of the same key may span multiple blocks.
	}
	return KeyValue{}, false, rd, nil
}
