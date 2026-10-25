package sql

import (
	"bytes"
	"fmt"
	"slopdb/storage"
	"strings"
)

// The caller holds the prefix-value lock. Read current committed data plus its
// transaction overlay, because a lock wait may outlive the statement snapshot.
func (s *Schema) prefixUniqueConflict(idx *IndexInfo, row map[string]Value, exclude []byte) ([]byte, error) {
	values, null := indexValues(idx.columnNames(), row)
	if null {
		return nil, nil
	}
	active := s.statementActive
	if s.lockHandler != nil {
		s.statementActive = false
	}
	defer func() { s.statementActive = active }()
	prefix := []byte(indexTuplePrefix(idx, values))
	it := s.newRangeIterator(prefix, prefixSuccessor(prefix), false)
	defer it.Close()
	for ; it.Valid(); it.Next() {
		key := it.Key().UserKey
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		rowkey := key[len(prefix):]
		if bytes.Equal(rowkey, exclude) {
			continue
		}
		return copyBytes(rowkey), nil
	}
	return nil, storage.IteratorError(it)
}

func (s *Schema) prefixedConflictIndex(table string, row, old map[string]Value) bool {
	for _, idx := range s.tableIndexes(table) {
		if !idx.IsUnique || !idx.hasPrefixes() {
			continue
		}
		match := true
		for _, name := range idx.columnNames() {
			a, b := row[name], old[name]
			if a.Null || b.Null || s.valueCompare(idx.keyValue(name, a), idx.keyValue(name, b)) != 0 {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func (s *Schema) checkPrefixUnique(idx *IndexInfo, row map[string]Value, exclude []byte) error {
	key, err := s.prefixUniqueConflict(idx, row, exclude)
	if err != nil {
		return err
	}
	if key != nil {
		return fmt.Errorf("UNIQUE constraint failed: %s.%s", idx.Table, strings.Join(idx.columnNames(), ","))
	}
	return nil
}

type uniqueSpec struct {
	columns []string
	index   *IndexInfo
}

func (s *Schema) uniqueSpecs(table string, info *TableInfo) []uniqueSpec {
	var result []uniqueSpec
	for _, group := range s.uniqueGroups(table, info) {
		result = append(result, uniqueSpec{columns: group})
	}
	for _, idx := range s.tableIndexes(table) {
		if idx.IsUnique && idx.hasPrefixes() {
			result = append(result, uniqueSpec{columns: idx.columnNames(), index: idx})
		}
	}
	return result
}

// Old releases stored complete values despite prefix metadata. Rewrite each
// affected index atomically before admitting clients; failed rewrites roll back
// both physical entries and the version marker and can be retried on open.
func (s *Schema) upgradePrefixIndexes() error {
	var names []string
	for name, idx := range s.indexes {
		if idx.hasPrefixes() && idx.PrefixEncoding == 0 {
			names = append(names, name)
		}
	}
	for _, name := range names {
		if err := s.ExecuteStatement(func() error {
			s.ensureCatalogWritable()
			idx := s.indexes[name]
			info := s.tables[idx.Table]
			if info == nil {
				return fmt.Errorf("prefix index %s has no table", name)
			}
			prefix := []byte(info.storagePrefix(info.Name))
			it := s.newRangeIterator(prefix, prefixSuccessor(prefix), false)
			var entries []storage.Mutation
			for ; it.Valid(); it.Next() {
				if err := s.checkInterrupt(); err != nil {
					it.Close()
					return err
				}
				row := decodeRow(it.Value(), info)
				entries = append(entries, storage.Mutation{Key: encodeRowIndexKey(idx, row, it.Key().UserKey), Value: s.encodeIndexValue(idx, row)})
			}
			err := storage.IteratorError(it)
			it.Close()
			if err != nil {
				return err
			}
			old := []byte(idx.storagePrefix())
			if err = s.removeRange(old, prefixSuccessor(old)); err != nil {
				return err
			}
			for _, entry := range entries {
				if err = s.write(entry.Key, entry.Value); err != nil {
					return err
				}
			}
			idx.PrefixEncoding = 1
			return s.saveIndexes()
		}); err != nil {
			return err
		}
	}
	return nil
}
