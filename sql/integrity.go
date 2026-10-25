package sql

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"

	"slopdb/storage"
)

// IntegrityReport counts checked visible rows, secondary entries and catalog
// objects. CheckIntegrity is diagnostic and never repairs data or indexes.
type IntegrityReport struct{ Rows, IndexEntries, CatalogObjects int64 }

// CheckIntegrity checks the caller's statement snapshot for malformed rows,
// constraint violations, wrong clustered keys, missing/stale/extra index entries
// and foreign-key orphans. It requires a private read session without a lock
// handler; integrity inspection must not acquire transactional row locks.
func (s *Schema) CheckIntegrity() (report IntegrityReport, err error) {
	expectedCounts := make(map[string]int64)
	actualCounts := make(map[string]int64)
	unique := make(map[string]map[string]string)
	for table, info := range s.tables {
		if err = s.foreignDefinitions(info); err != nil {
			return report, fmt.Errorf("catalog table %s: %w", table, err)
		}
	}
	iterator := s.newIterator()
	defer iterator.Close()
	for iterator.Valid() {
		if err = s.checkInterrupt(); err != nil {
			return report, err
		}
		key := iterator.Key().UserKey
		data := iterator.Value()
		if len(key) == 0 {
			return report, fmt.Errorf("empty storage key")
		}
		if handled, validationErr := storage.ValidateChangeEntry(key, data); handled {
			if validationErr != nil {
				return report, fmt.Errorf("change log %q: %w", key, validationErr)
			}
			iterator.Next()
			continue
		}
		switch key[0] {
		case 0:
			if string(key) != versionPrefix && string(key) != objectIDPrefix && !json.Valid(data) {
				return report, fmt.Errorf("catalog %q: malformed JSON", key)
			}
			if string(key) == versionPrefix && len(data) != 8 {
				return report, fmt.Errorf("catalog user_version: expected 8 bytes")
			}
			if string(key) == objectIDPrefix && (len(data) != 8 || binary.BigEndian.Uint64(data) != s.nextObjectID) {
				return report, fmt.Errorf("catalog object IDs: invalid counter")
			}
			var expected any
			switch string(key) {
			case schemaPrefix:
				expected = s.tables
			case schemaIdxPrefix:
				expected = s.indexes
			case schemaViewsPrefix:
				expected = s.views
			case schemaTrigPrefix:
				expected = s.triggers
			}
			if expected != nil {
				canonical, marshalErr := json.Marshal(expected)
				if marshalErr != nil {
					return report, marshalErr
				}
				var actual, desired any
				decoder := json.NewDecoder(bytes.NewReader(data))
				decoder.UseNumber()
				if err = decoder.Decode(&actual); err != nil {
					return report, err
				}
				decoder = json.NewDecoder(bytes.NewReader(canonical))
				decoder.UseNumber()
				if err = decoder.Decode(&desired); err != nil {
					return report, err
				}
				if !reflect.DeepEqual(actual, desired) {
					return report, fmt.Errorf("catalog %q: persisted objects differ from committed catalog", key)
				}
			}
			report.CatalogObjects++
		case 1:
			table, info := s.rowTable(key)
			if info == nil {
				return report, fmt.Errorf("row %q: unknown table", key)
			}
			row, decodeErr := decodeIntegrityRow(data, info)
			if decodeErr != nil {
				return report, fmt.Errorf("row %q: %w", key, decodeErr)
			}
			if len(info.PKColumns) > 0 && !bytes.Equal(key, info.encodePK(table, row)) {
				return report, fmt.Errorf("row %q: clustered primary key mismatch", key)
			}
			if err = s.checkConstraintsOriginalPreserving(table, info, row, key, row, row, nil); err != nil {
				return report, fmt.Errorf("row %q: %w", key, err)
			}
			if err = s.checkForeignRow(table, row); err != nil {
				return report, fmt.Errorf("row %q: %w", key, err)
			}
			for _, idx := range s.tableIndexes(table) {
				if idx.IsUnique {
					values, hasNull := indexValues(idx.columnNames(), row)
					if !hasNull {
						encoded := []byte{}
						for i, value := range values {
							encoded = appendValueIdentity(encoded, idx.keyValue(idx.columnNames()[i], value))
						}
						seen := unique[idx.Name]
						if seen == nil {
							seen = make(map[string]string)
							unique[idx.Name] = seen
						}
						if previous, exists := seen[string(encoded)]; exists && previous != string(key) {
							return report, fmt.Errorf("unique index %s: duplicate values in rows %q and %q", idx.Name, previous, key)
						}
						seen[string(encoded)] = string(key)
					}
				}
				if s.isClusteredIndex(idx) {
					continue
				}
				expectedKey := encodeRowIndexKey(idx, row, key)
				value, found, getErr := s.schemaGet(expectedKey)
				if getErr != nil {
					return report, getErr
				}
				if !found {
					return report, fmt.Errorf("index %s: missing entry for row %q", idx.Name, key)
				}
				if !bytes.Equal(value, s.encodeIndexValue(idx, row)) {
					return report, fmt.Errorf("index %s: stale payload for row %q", idx.Name, key)
				}
				expectedCounts[idx.Name]++
			}
			report.Rows++
		case 3:
			// Session-owned temporary ranges have no committed shared catalog.
			// Their physical blocks remain covered by storage integrity checks.
		case 2:
			var idx *IndexInfo
			if len(key) >= 11 && key[1] == 0xff && key[10] == ':' {
				idx = s.lookups().indexesByID[binary.BigEndian.Uint64(key[2:10])]
			} else {
				suffix := key[1:]
				colon := bytes.IndexByte(suffix, ':')
				if colon >= 0 {
					idx = s.indexes[string(suffix[:colon])]
				}
			}
			if idx == nil && len(key) >= 11 && key[1] == 0xff && key[10] == ':' {
				id := binary.BigEndian.Uint64(key[2:10])
				_, pending, cleanupErr := s.committedGet(indexCleanupKey(id))
				if cleanupErr != nil {
					return report, cleanupErr
				}
				if pending {
					iterator.Next()
					continue
				}
			}
			if idx == nil || s.isClusteredIndex(idx) {
				return report, fmt.Errorf("index key %q: unknown secondary index", key)
			}
			name := idx.Name
			actualCounts[name]++
			report.IndexEntries++
		default:
			return report, fmt.Errorf("unknown key namespace %q", key)
		}
		iterator.Next()
	}
	if err = storage.IteratorError(iterator); err != nil {
		return report, err
	}
	for name, count := range actualCounts {
		if count != expectedCounts[name] {
			return report, fmt.Errorf("index %s: %d entries, expected %d (extra or orphan entries)", name, count, expectedCounts[name])
		}
	}
	return report, nil
}

// Unlike the compatibility decoder, diagnostic decoding rejects truncated or
// unknown values. Older rows may omit columns added by ALTER TABLE; those map
// to NULL, as they do in ordinary SELECT, and constraints are still checked.
func decodeIntegrityRow(data []byte, info *TableInfo) (map[string]Value, error) {
	row := make(map[string]Value, len(info.Columns))
	offset := 0
	for _, column := range info.Columns {
		if offset == len(data) {
			row[column.Name] = NewNullValue()
			continue
		}
		remaining := data[offset:]
		size := 1
		switch remaining[0] {
		case binTypeNull:
		case binTypeInt64, binTypeFloat64, binTypeUint64:
			size = 9
		case binTypeText, binTypeBlob, binTypeDecimal, binTypeJSON:
			length, n := binary.Uvarint(remaining[1:])
			if n <= 0 || length > uint64(len(remaining)-1-n) {
				return nil, fmt.Errorf("column %s: invalid value length", column.Name)
			}
			size = 1 + n + int(length)
		default:
			return nil, fmt.Errorf("column %s: unknown value tag %d", column.Name, remaining[0])
		}
		if size > len(remaining) {
			return nil, fmt.Errorf("column %s: truncated value", column.Name)
		}
		value, n := decodeValue(remaining[:size])
		if n != size {
			return nil, fmt.Errorf("column %s: malformed value", column.Name)
		}
		row[column.Name] = decorateColumnValue(value, column)
		offset += size
	}
	if offset != len(data) {
		return nil, fmt.Errorf("%d trailing row bytes", len(data)-offset)
	}
	return row, nil
}
