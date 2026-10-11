package db

import (
	"encoding/binary"
	"math"

	"slopdb/sql"
)

// SQL-equal numeric values share a key, including integer/real equivalents
// and signed zero. NaN equality is non-transitive in ValueCompare, so buckets
// containing it retain comparison-based lookup.
func lockValueKey(values []sql.Value) (string, bool) {
	var local [64]byte
	b := local[:0]
	for _, v := range values {
		if v.Null {
			b = append(b, 0)
			continue
		}
		switch v.Type {
		case sql.ColInteger:
			b = append(b, 1)
			b = binary.LittleEndian.AppendUint64(b, uint64(v.Int))
		case sql.ColReal:
			if math.IsNaN(v.Real) {
				return "", false
			}
			if v.Real >= -9223372036854775808.0 && v.Real < 9223372036854775808.0 && math.Trunc(v.Real) == v.Real {
				b = append(b, 1)
				b = binary.LittleEndian.AppendUint64(b, uint64(int64(v.Real)))
			} else {
				b = append(b, 2)
				b = binary.LittleEndian.AppendUint64(b, math.Float64bits(v.Real))
			}
		case sql.ColBlob:
			b = append(b, 4)
			b = binary.AppendUvarint(b, uint64(len(v.Blob)))
			b = append(b, v.Blob...)
		default:
			b = append(b, 3)
			b = binary.AppendUvarint(b, uint64(len(v.Text)))
			b = append(b, v.Text...)
		}
	}
	return string(b), true
}

type lockBucket struct {
	entries []*lockEntry
	keyed   map[string]*lockEntry
	unkeyed int
	empty   *lockEntry
}

func (b *lockBucket) find(values []sql.Value) *lockEntry {
	if len(values) == 0 {
		return b.empty
	}
	key, ok := lockValueKey(values)
	if ok && b.unkeyed == 0 {
		return b.keyed[key]
	}
	for _, entry := range b.entries {
		if sameValues(entry.request.Values, values) {
			return entry
		}
	}
	return nil
}
func (b *lockBucket) add(entry *lockEntry) {
	entry.position = len(b.entries)
	b.entries = append(b.entries, entry)
	if len(entry.request.Values) == 0 {
		b.empty = entry
		return
	}
	entry.valueKey, entry.keyed = lockValueKey(entry.request.Values)
	if entry.keyed {
		if b.keyed == nil {
			b.keyed = make(map[string]*lockEntry)
		}
		b.keyed[entry.valueKey] = entry
	} else {
		b.unkeyed++
	}
}
func (b *lockBucket) remove(entry *lockEntry) {
	last := len(b.entries) - 1
	moved := b.entries[last]
	b.entries[entry.position] = moved
	moved.position = entry.position
	b.entries[last] = nil
	b.entries = b.entries[:last]
	if len(entry.request.Values) == 0 {
		b.empty = nil
	} else if entry.keyed {
		delete(b.keyed, entry.valueKey)
	} else {
		b.unkeyed--
	}
}
