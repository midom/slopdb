package sql

import (
	"encoding/json"
	"fmt"
	"strings"

	"slopdb/storage"
)

func (s *Schema) loadCollation() error {
	s.collation = BinaryCollation
	data, ok, err := s.schemaGet([]byte(collationPrefix))
	if err != nil || !ok {
		return err
	}
	var name string
	if err := json.Unmarshal(data, &name); err != nil {
		return fmt.Errorf("invalid persisted collation: %w", err)
	}
	s.collation, err = ParseCollation(name)
	if err != nil {
		return fmt.Errorf("invalid persisted collation: %w", err)
	}
	return nil
}

func (s *Schema) bindCatalogCollation() {
	for _, info := range s.tables {
		info.collation = s.collation
	}
	for _, idx := range s.indexes {
		s.bindIndexCollation(idx)
	}
}

func (s *Schema) bindIndexCollation(idx *IndexInfo) {
	idx.collation = s.collation
	idx.binaryColumns = nil
	if info := s.tables[idx.Table]; info != nil {
		for _, column := range info.Columns {
			if column.Binary {
				if idx.binaryColumns == nil {
					idx.binaryColumns = make(map[string]bool)
				}
				idx.binaryColumns[column.Name] = true
			}
		}
	}
}

func (s *Schema) pragmaCollation(stmt *PragmaStmt) (*ResultSet, error) {
	expression := stmt.Value
	if len(stmt.Args) != 0 {
		if len(stmt.Args) != 1 || expression != nil {
			return nil, fmt.Errorf("PRAGMA collation accepts one mode")
		}
		expression = stmt.Args[0]
	}
	if expression == nil {
		return s.singleValueResult("collation", NewTextValue(string(s.collation)))
	}
	var name string
	switch value := expression.(type) {
	case *LiteralExpr:
		if value.Value.Null || value.Value.Type != ColText {
			return nil, fmt.Errorf("PRAGMA collation requires a mode name")
		}
		name = value.Value.Text
	case *ColumnRefExpr:
		name = value.Name
	default:
		return nil, fmt.Errorf("PRAGMA collation requires a mode name")
	}
	mode, err := ParseCollation(name)
	if err != nil {
		return nil, err
	}
	if mode == s.collation {
		return &ResultSet{}, nil
	}
	if err := s.rebuildCollation(mode); err != nil {
		return nil, err
	}
	return &ResultSet{}, nil
}

type collationRow struct {
	table  string
	oldKey []byte
	data   []byte
	row    map[string]Value
}

// Rebuild all collated physical keys in the statement transaction. We first
// inspect the complete old snapshot and validate collisions, so a rejected
// mode change cannot expose a partly rebuilt catalog or index.
func (s *Schema) rebuildCollation(mode Collation) error {
	var rows []collationRow
	var oldIndexes [][]byte
	iter := s.newIterator()
	for iter.Valid() {
		if err := s.checkInterrupt(); err != nil {
			iter.Close()
			return err
		}
		key := iter.Key().UserKey
		if len(key) > 0 && key[0] == indexPrefix[0] {
			oldIndexes = append(oldIndexes, copyBytes(key))
		} else if table, info := s.rowTable(key); info != nil {
			data := copyBytes(iter.Value())
			rows = append(rows, collationRow{table, copyBytes(key), data, decodeRow(data, info)})
		}
		iter.Next()
	}
	err := storage.IteratorError(iter)
	iter.Close()
	if err != nil {
		return err
	}

	s.ensureCatalogWritable()
	s.collation = mode
	s.bindCatalogCollation()
	s.catalogLookup = nil

	rowKeys := make(map[string]bool, len(rows))
	uniqueKeys := make(map[string]bool)
	newRows := make([]storage.Mutation, 0, len(rows))
	newIndexes := make([]storage.Mutation, 0, len(oldIndexes))
	for _, entry := range rows {
		if err := s.checkInterrupt(); err != nil {
			return err
		}
		info := s.tables[entry.table]
		key := entry.oldKey
		if len(info.PKColumns) != 0 {
			key = info.encodePK(entry.table, entry.row)
		}
		if rowKeys[string(key)] {
			return fmt.Errorf("collation %s creates duplicate PRIMARY KEY in %s", mode, entry.table)
		}
		rowKeys[string(key)] = true
		for _, group := range s.uniqueGroups(entry.table, info) {
			values, hasNull := indexValues(group, entry.row)
			if hasNull {
				continue
			}
			var signature []byte
			for i, value := range values {
				if !columnBinary(info, group[i]) {
					value = mode.keyValue(value)
				}
				signature = appendValueIdentity(signature, value)
			}
			groupKey := entry.table + "\x00" + strings.Join(group, "\x00") + "\x00" + string(signature)
			if uniqueKeys[groupKey] {
				return fmt.Errorf("collation %s violates UNIQUE constraint on %s(%s)", mode, entry.table, strings.Join(group, ", "))
			}
			uniqueKeys[groupKey] = true
		}
		for _, idx := range s.tableIndexes(entry.table) {
			if !idx.IsUnique || !idx.hasPrefixes() {
				continue
			}
			values, null := indexValues(idx.columnNames(), entry.row)
			if null {
				continue
			}
			identity := "prefix:" + idx.Name + "\x00" + indexTuplePrefix(idx, values)
			if uniqueKeys[identity] {
				return fmt.Errorf("collation %s violates UNIQUE prefix index %s", mode, idx.Name)
			}
			uniqueKeys[identity] = true
		}
		newRows = append(newRows, storage.Mutation{Key: key, Value: entry.data})
		for _, idx := range s.tableIndexes(entry.table) {
			if s.isClusteredIndex(idx) {
				continue
			}
			newIndexes = append(newIndexes, storage.Mutation{Key: encodeRowIndexKey(idx, entry.row, key), Value: s.encodeIndexValue(idx, entry.row)})
		}
	}
	for _, entry := range rows {
		if err := s.remove(entry.oldKey); err != nil {
			return err
		}
	}
	for _, key := range oldIndexes {
		if err := s.remove(key); err != nil {
			return err
		}
	}
	for _, mutation := range append(newRows, newIndexes...) {
		if err := s.write(mutation.Key, mutation.Value); err != nil {
			return err
		}
	}
	return nil
}

func columnBinary(info *TableInfo, name string) bool {
	for _, column := range info.Columns {
		if column.Name == name {
			return column.Binary
		}
	}
	return false
}
