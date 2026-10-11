package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Point lookup needs no public iterator state. Reusable private buffers decode
// restart keys without allocating iterator objects or returning borrowed keys.
// Data blocks still use the shared cache and normal corruption checks.
type pointWorkspace struct{ indexKey, dataKey []byte }

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
	if workspace.indexKey == nil {
		workspace.indexKey = make([]byte, 0, 64)
		workspace.dataKey = make([]byte, 0, 64)
	}
	index := BlockIterator{block: r.indexBlock, key: workspace.indexKey[:0], reuseKey: true, err: r.indexBlock.err}
	var data BlockIterator
	defer func() {
		workspace.indexKey = index.key
		if data.key != nil {
			workspace.dataKey = data.key
		}
	}()
	index.Seek(key)
	for index.Valid() {
		handle := index.Value()
		off, n := binary.Uvarint(handle)
		if n <= 0 {
			return KeyValue{}, false, rd, fmt.Errorf("invalid block handle")
		}
		size, n := binary.Uvarint(handle[n:])
		if n <= 0 || off > r.fileSize || size > r.fileSize-off {
			return KeyValue{}, false, rd, fmt.Errorf("invalid block handle")
		}
		reader, err := r.loadBlockReaderMode(off, size, true)
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
		index.Next()
	}
	return KeyValue{}, false, rd, index.Err()
}
