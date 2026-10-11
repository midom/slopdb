package sql

import (
	"strings"
	"sync"
)

// Derived lookups travel with immutable catalog snapshots. Row statements only
// visit the indexes and unique predicates belonging to their own table.
type catalogLookups struct {
	uniqueMu       sync.RWMutex
	indexes        map[string][]*IndexInfo
	indexedColumns map[string][]ColumnDef
	covering       map[*IndexInfo]*TableInfo
	unique         map[string][][]string
	maxTableName   int
}

func (s *Schema) lookups() *catalogLookups {
	if s.catalogLookup == nil {
		l := &catalogLookups{indexes: make(map[string][]*IndexInfo), indexedColumns: make(map[string][]ColumnDef), covering: make(map[*IndexInfo]*TableInfo), unique: make(map[string][][]string)}
		for _, idx := range s.indexes {
			l.indexes[idx.Table] = append(l.indexes[idx.Table], idx)
			if !s.isAutoIndex(idx) {
				l.covering[idx] = coveringTableInfo(idx, s.tables[idx.Table])
			}
		}
		for name, info := range s.tables {
			indexed := make(map[string]bool)
			for _, idx := range l.indexes[name] {
				for _, column := range idx.columnNames() {
					indexed[column] = true
				}
			}
			for _, column := range info.PKColumns {
				indexed[column] = true
			}
			for _, columns := range info.TableUnique {
				for _, column := range columns {
					indexed[column] = true
				}
			}
			for _, column := range info.Columns {
				if indexed[column.Name] || column.PrimaryKey || column.Unique {
					l.indexedColumns[name] = append(l.indexedColumns[name], column)
				}
			}
			if len(name) > l.maxTableName {
				l.maxTableName = len(name)
			}
		}
		s.catalogLookup = l
	}
	return s.catalogLookup
}

func (s *Schema) tableIndexes(table string) []*IndexInfo { return s.lookups().indexes[table] }

// Names may contain colons. Resolve the longest registered name prefix without
// scanning the catalog or interpreting colons inside a primary key as names.
func (s *Schema) rowTable(key []byte) (string, *TableInfo) {
	if len(key) == 0 || key[0] != dataPrefix[0] {
		return "", nil
	}
	end := s.lookups().maxTableName + 1
	if end >= len(key) {
		end = len(key) - 1
	}
	var name string
	var info *TableInfo
	for i := 1; i <= end; i++ {
		if key[i] == ':' {
			if candidate := s.tables[string(key[1:i])]; candidate != nil {
				name = string(key[1:i])
				info = candidate
			}
		}
	}
	return name, info
}

func (s *Schema) uniqueGroups(table string, info *TableInfo) [][]string {
	lookup := s.lookups()
	lookup.uniqueMu.RLock()
	groups, ok := lookup.unique[table]
	lookup.uniqueMu.RUnlock()
	if ok {
		return groups
	}
	lookup.uniqueMu.Lock()
	defer lookup.uniqueMu.Unlock()
	if groups, ok := lookup.unique[table]; ok {
		return groups
	}
	groups = nil
	seen := make(map[string]bool)
	add := func(columns []string) {
		var name strings.Builder
		for _, column := range columns {
			name.WriteString(frameLockName(column))
		}
		identity := name.String()
		if !seen[identity] {
			seen[identity] = true
			groups = append(groups, columns)
		}
	}
	for _, columns := range info.TableUnique {
		add(columns)
	}
	if len(info.PKColumns) > 0 {
		add(info.PKColumns)
	}
	for _, col := range info.Columns {
		if col.Unique {
			add([]string{col.Name})
		}
	}
	for _, idx := range s.tableIndexes(table) {
		if idx.IsUnique {
			add(idx.columnNames())
		}
	}
	lookup.unique[table] = groups
	return groups
}
