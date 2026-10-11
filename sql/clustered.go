package sql

import (
	"encoding/binary"
	"math"
)

// The clustered layout stores the row itself under its sortable primary key.
// Variable-size components are escaped and terminated so composite tuples
// preserve order without collisions from embedded separators, NULs or type bytes.
func (info *TableInfo) encodePK(table string, row map[string]Value) []byte {
	key := []byte(tableDataPrefix(table))
	for _, name := range info.PKColumns {
		key = appendClusteredValue(key, row[name])
	}
	return key
}

func appendClusteredValue(key []byte, value Value) []byte {
	if value.Null {
		return append(key, 0)
	}
	switch value.Type {
	case ColInteger:
		key = append(key, 1)
		return binary.BigEndian.AppendUint64(key, uint64(value.Int)^(1<<63))
	case ColReal:
		key = append(key, 2)
		bits := math.Float64bits(value.Real)
		if value.Real == 0 {
			bits = 0 // SQL equality treats positive and negative zero alike.
		}
		if bits&(1<<63) == 0 {
			bits ^= 1 << 63
		} else {
			bits = ^bits
		}
		return binary.BigEndian.AppendUint64(key, bits)
	case ColText, ColBlob:
		var data []byte
		if value.Type == ColText {
			key = append(key, 3)
			data = []byte(value.Text)
		} else {
			key = append(key, 4)
			data = value.Blob
		}
		for _, b := range data {
			key = append(key, b)
			if b == 0 {
				key = append(key, 255)
			}
		}
		return append(key, 0, 0)
	}
	return key
}

func (s *Schema) isClusteredIndex(idx *IndexInfo) bool {
	info := s.tables[idx.Table]
	return info != nil && len(info.PKColumns) == 1 &&
		len(idx.columnNames()) == 1 && idx.Column == info.PKColumns[0] && s.isAutoIndex(idx)
}
