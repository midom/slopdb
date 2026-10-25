package sql

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"

	"slopdb/storage"
)

const unsignedAutoincPrefix = "\x00__unsigned_autoincrement__"

func (s *Schema) LastInsertIDUnsigned() uint64 { return s.lastInsertIDUnsigned }

func (s *Schema) loadUnsignedAutoinc() error {
	data, found, err := s.schemaGet([]byte(unsignedAutoincPrefix))
	if err != nil || !found {
		return err
	}
	if err = json.Unmarshal(data, &s.autoincUnsigned); err != nil {
		return fmt.Errorf("invalid unsigned auto-increment metadata: %w", err)
	}
	return nil
}

func mergeUnsignedCounter(base, local, latest map[string]uint64) map[string]uint64 {
	merged := mergeMap(base, local, latest)
	for key, value := range latest {
		if current, exists := merged[key]; exists && value > current {
			merged[key] = value
		}
	}
	return merged
}

func (s *Schema) lastUnsignedRowID(table string) (uint64, error) {
	prefix := []byte(s.tableDataPrefix(table))
	it := s.newRangeIterator(prefix, prefixSuccessor(prefix), true)
	defer it.Close()
	var maximum uint64
	if it.Valid() {
		key := it.Key().UserKey[len(prefix):]
		if len(key) != 9 {
			return 0, fmt.Errorf("invalid unsigned clustered key")
		}
		switch key[0] {
		case 1:
			n := int64(binary.BigEndian.Uint64(key[1:]) ^ (uint64(1) << 63))
			if n < 0 {
				return 0, fmt.Errorf("negative unsigned clustered key")
			}
			maximum = uint64(n)
		case 5:
			maximum = binary.BigEndian.Uint64(key[1:])
		default:
			return 0, fmt.Errorf("invalid unsigned clustered key type")
		}
	}
	return maximum, storage.IteratorError(it)
}

// Reservations are process-wide and monotonic, while committed durable floors
// publish through the normal catalog-state merge under the commit mutex. No
// logical table/counter lock is held for the lifetime of an SQL transaction.
func (s *Schema) prepareUnsignedAuto(table string, column *ColumnDef, row map[string]Value, supplied bool) error {
	value := row[column.Name]
	generate := s.autoValueRequested(column, value, supplied)
	var id uint64
	if !generate {
		if value.Type == ColUnsigned {
			id = value.Uint
		} else if value.Type == ColInteger && value.Int >= 0 {
			id = uint64(value.Int)
		} else {
			return fmt.Errorf("invalid unsigned auto-increment value")
		}
	}
	root := s
	if s.parent != nil {
		root = s.parent
	}
	root.rowIDMu.Lock()
	if root.unsignedRowReservations == nil {
		root.unsignedRowReservations = map[string]uint64{}
	}
	key := s.rowCounterKey(table)
	maximum, initialized := root.unsignedRowReservations[key]
	if !initialized {
		var err error
		maximum, err = s.lastUnsignedRowID(table)
		if err != nil {
			root.rowIDMu.Unlock()
			return err
		}
	}
	maximum = max(maximum, s.autoincUnsigned[key])
	if generate {
		maximumAllowed := uint64(math.MaxUint64)
		if column.IntegerBits > 0 && column.IntegerBits < 64 {
			maximumAllowed = (uint64(1) << column.IntegerBits) - 1
		}
		if maximum >= maximumAllowed {
			root.rowIDMu.Unlock()
			return fmt.Errorf("unsigned integer primary key exhausted")
		}
		id = maximum + 1
	}
	root.unsignedRowReservations[key] = max(maximum, id)
	root.rowIDMu.Unlock()
	if generate {
		if id <= math.MaxInt64 {
			row[column.Name] = NewIntValue(int64(id))
		} else {
			row[column.Name] = NewUnsignedValue(id)
		}
	}
	if !s.disableAutoIncrementPersistence && !s.isTemporaryTable(table) {
		if s.autoincUnsigned == nil {
			s.autoincUnsigned = map[string]uint64{}
		}
		s.autoincUnsigned[key] = max(s.autoincUnsigned[key], id)
	}
	return nil
}
