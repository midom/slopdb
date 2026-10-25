package sql

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"
)

func (idx *IndexInfo) columnNames() []string {
	if len(idx.Columns) != 0 {
		return idx.Columns
	}
	return []string{idx.Column}
}

func normalizedIndexColumns(columns []string, column string) []string {
	if len(columns) == 0 && column != "" {
		columns = []string{column}
	}
	result := make([]string, len(columns))
	for i, name := range columns {
		result[i] = strings.ToLower(name)
	}
	return result
}

func validateIndexColumns(columns []ColumnDef, names []string) error {
	if len(names) == 0 {
		return fmt.Errorf("index must have columns")
	}
	available := make(map[string]bool)
	for _, col := range columns {
		available[col.Name] = true
	}
	seen := make(map[string]bool)
	for _, name := range names {
		if !available[name] {
			return fmt.Errorf("no such index column: %s", name)
		}
		if seen[name] {
			return fmt.Errorf("duplicate index column: %s", name)
		}
		seen[name] = true
	}
	return nil
}

// Index ordering assumes every non-NULL component has its declared type.
// Validate after normal column affinity has been applied; NULL remains subject
// to the column's NOT NULL/PRIMARY KEY constraints.
func checkIndexedColumnType(table string, column ColumnDef, value Value) error {
	if !value.Null && value.Type != column.ColType {
		return fmt.Errorf("datatype mismatch for indexed column %s.%s: expected %s, got %s", table, column.Name, column.ColType, value.Type)
	}
	return nil
}

func (s *Schema) validateIndexedRow(table string, row map[string]Value) error {
	for _, column := range s.lookups().indexedColumns[table] {
		if err := checkIndexedColumnType(table, column, row[column.Name]); err != nil {
			return err
		}
	}
	return nil
}

func validateIndexRow(info *TableInfo, columns []string, row map[string]Value) error {
	for _, name := range columns {
		for _, column := range info.Columns {
			if column.Name == name {
				if err := checkIndexedColumnType(info.Name, column, row[name]); err != nil {
					return err
				}
				break
			}
		}
	}
	return nil
}

func newIndexInfo(name, table string, columns []string, unique bool) *IndexInfo {
	idx := &IndexInfo{Name: name, Table: table, Column: columns[0], IsUnique: unique}
	if len(columns) > 1 {
		idx.Columns = append([]string(nil), columns...)
	}
	return idx
}

func (s *Schema) prepareInlineIndexes(table string, stmt *CreateTableStmt) ([]*IndexInfo, error) {
	reserved := make(map[string]bool)
	for name := range s.indexes {
		reserved[name] = true
	}
	for _, col := range stmt.Columns {
		if (col.PrimaryKey || col.Unique) && len(stmt.PKColumns) <= 1 {
			reserved[table+"_"+col.Name+"_idx"] = true
		}
	}
	var indexes []*IndexInfo
	for _, def := range stmt.Indexes {
		columns := normalizedIndexColumns(def.Columns, "")
		if err := validateIndexColumns(stmt.Columns, columns); err != nil {
			return nil, err
		}
		name := strings.ToLower(def.Name)
		if name == "" {
			name = columns[0]
			for n := 2; reserved[name] || name == "primary"; n++ {
				name = fmt.Sprintf("%s_%d", columns[0], n)
			}
		} else if reserved[name] {
			return nil, fmt.Errorf("index %q already exists", name)
		} else if name == "primary" {
			return nil, fmt.Errorf("index name PRIMARY is reserved")
		}
		reserved[name] = true
		idx := newIndexInfo(name, table, columns, def.Unique)
		idx.Fulltext = def.Fulltext
		if err := validateFulltextIndex(&TableInfo{Columns: stmt.Columns}, idx); err != nil {
			return nil, err
		}
		idx.PrefixLengths = append([]int64(nil), def.PrefixLengths...)
		if err := validateIndexPrefixes(stmt.Columns, idx); err != nil {
			return nil, err
		}
		indexes = append(indexes, idx)
	}
	return indexes, nil
}

func (s *Schema) isAutoIndex(idx *IndexInfo) bool { return idx.Auto }

func (s *Schema) IsAutoIndex(name string) bool {
	idx := s.indexes[strings.ToLower(name)]
	return idx != nil && s.isAutoIndex(idx)
}

func indexValues(columns []string, row map[string]Value) ([]Value, bool) {
	values := make([]Value, len(columns))
	hasNull := false
	for i, col := range columns {
		value, ok := row[col]
		if !ok {
			value = NewNullValue()
		}
		values[i] = value
		hasNull = hasNull || value.Null
	}
	return values, hasNull
}

func (c Collation) keyValue(value Value) Value {
	if !value.Null && value.Type == ColText {
		value.Text = c.Key(value.Text)
	}
	return value
}

func (idx *IndexInfo) keyValue(column string, value Value) Value {
	if idx.binaryColumns[column] {
		return value
	}
	return idx.collation.keyValue(value)
}

func equalIndexValues(a, b []Value) bool {
	for i := range a {
		if ValueCompare(a[i], b[i]) != 0 {
			return false
		}
	}
	return true
}

// Keep the existing single-column key format. Composite components are hex
// encoded so embedded separators and binary values cannot change tuple boundaries.
func encodeRowIndexKey(idx *IndexInfo, row map[string]Value, rowKey []byte) []byte {
	columns := idx.columnNames()
	if len(columns) == 1 {
		value, ok := row[columns[0]]
		if !ok {
			value = NewNullValue()
		}
		return []byte(idx.storagePrefix() + valueToSortableKey(idx.keyValue(columns[0], value)) + ":" + string(rowKey))
	}
	values, _ := indexValues(columns, row)
	var b strings.Builder
	b.WriteString(idx.storagePrefix())
	for i, value := range values {
		b.WriteString(hex.EncodeToString([]byte(valueToSortableKey(idx.keyValue(columns[i], value)))))
		b.WriteByte(':')
	}
	b.Write(rowKey)
	return []byte(b.String())
}

func indexValuePrefix(idx *IndexInfo, value Value) string {
	encoded := valueToSortableKey(idx.keyValue(idx.columnNames()[0], value))
	if len(idx.columnNames()) > 1 {
		encoded = hex.EncodeToString([]byte(encoded))
	}
	return idx.storagePrefix() + encoded + ":"
}

func indexTuplePrefix(idx *IndexInfo, values []Value) string {
	if len(values) == 1 {
		return indexValuePrefix(idx, values[0])
	}
	var b strings.Builder
	b.WriteString(idx.storagePrefix())
	for i, v := range values {
		b.WriteString(hex.EncodeToString([]byte(valueToSortableKey(idx.keyValue(idx.columnNames()[i], v)))))
		b.WriteByte(':')
	}
	return b.String()
}

// The equality scan has consumed the first component. Skip the remaining
// composite components before reading the complete row key (which may contain ':').
func indexedRowKey(idx *IndexInfo, key []byte, prefixLength int) []byte {
	remaining := key[prefixLength:]
	for i := 1; i < len(idx.columnNames()); i++ {
		n := bytes.IndexByte(remaining, ':')
		if n < 0 {
			return nil
		}
		remaining = remaining[n+1:]
	}
	return remaining
}
