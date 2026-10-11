package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

var tableMetadataMagic = []byte("GDBMETA1")

// Metadata follows the index block and precedes the 48-byte footer.
func encodeTableMetadata(maxSeq uint64, filterCRC uint32, first, last InternalKey, ranges []KeyValue) []byte {
	data := append([]byte(nil), tableMetadataMagic...)
	data = binary.LittleEndian.AppendUint64(data, maxSeq)
	data = binary.LittleEndian.AppendUint32(data, filterCRC)
	data = append(data, encodeBytesBuf(EncodeInternalKey(first))...)
	data = append(data, encodeBytesBuf(EncodeInternalKey(last))...)
	data = binary.LittleEndian.AppendUint32(data, uint32(len(ranges)))
	for _, kv := range ranges {
		data = append(data, encodeBytesBuf(EncodeInternalKey(kv.Key))...)
		data = append(data, encodeBytesBuf(kv.Value)...)
	}
	return binary.LittleEndian.AppendUint32(data, crc32.ChecksumIEEE(data))
}

func (r *SSTableReader) decodeTableMetadata(data []byte) error {
	if len(data) < 28 || !bytes.Equal(data[:8], tableMetadataMagic) {
		return fmt.Errorf("invalid table metadata header")
	}
	if crc32.ChecksumIEEE(data[:len(data)-4]) != binary.LittleEndian.Uint32(data[len(data)-4:]) {
		return fmt.Errorf("table metadata CRC mismatch")
	}
	maxSeq := binary.LittleEndian.Uint64(data[8:16])
	if maxSeq > (uint64(1)<<56)-1 {
		return fmt.Errorf("invalid table metadata sequence")
	}
	if crc32.ChecksumIEEE(r.filterData) != binary.LittleEndian.Uint32(data[16:20]) {
		return fmt.Errorf("filter CRC mismatch")
	}
	remaining := data[20 : len(data)-4]
	field := func() ([]byte, error) {
		v, n := decodeBytes(remaining)
		if n <= 0 || n > len(remaining) {
			return nil, fmt.Errorf("invalid table metadata field")
		}
		remaining = remaining[n:]
		return v, nil
	}
	first, err := field()
	if err != nil || len(first) < 8 {
		return fmt.Errorf("invalid table metadata smallest key")
	}
	last, err := field()
	if err != nil || len(last) < 8 {
		return fmt.Errorf("invalid table metadata largest key")
	}
	smallest, largest := ParseInternalKey(first), ParseInternalKey(last)
	if bytes.Compare(smallest.UserKey, largest.UserKey) > 0 || smallest.SeqNum > maxSeq || largest.SeqNum > maxSeq || len(remaining) < 4 {
		return fmt.Errorf("invalid table metadata bounds")
	}
	count := binary.LittleEndian.Uint32(remaining)
	remaining = remaining[4:]
	// Each tombstone needs at least two length fields and an internal key.
	if uint64(count) > uint64(len(remaining))/10 {
		return fmt.Errorf("invalid table metadata tombstone count")
	}
	ranges := make([]KeyValue, 0, int(count))
	for i := uint32(0); i < count; i++ {
		key, err := field()
		if err != nil || len(key) < 8 {
			return fmt.Errorf("invalid table metadata tombstone key")
		}
		end, err := field()
		if err != nil {
			return err
		}
		k := ParseInternalKey(key)
		if k.OpType != kTypeRangeDelete || k.SeqNum > maxSeq {
			return fmt.Errorf("invalid table metadata tombstone")
		}
		ranges = append(ranges, KeyValue{Key: k, Value: end})
	}
	if len(remaining) != 0 {
		return fmt.Errorf("trailing table metadata")
	}
	r.maxSeq, r.rangeDeletes = maxSeq, ranges
	r.meta = FileMetaData{FileSize: r.fileSize, Smallest: smallest, Largest: largest, hasRangeDeletes: len(ranges) != 0}
	return nil
}
