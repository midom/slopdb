package sql

import "strings"

// Planned joins know their column types before loading either side. Disable
// typed probes and hash keys when MySQL compares one side as a number instead.
func (p *joinPlan) mysqlJoinHashCompatible(right int, pairs []joinKeyPair) bool {
	columnType := func(info *TableInfo, name string) (ColumnType, bool) {
		for _, col := range info.Columns {
			if col.Name == name {
				return col.ColType, true
			}
		}
		return 0, false
	}
	for _, pair := range pairs {
		left, ok := p.owners[pair.left]
		if !ok || left < 0 || left >= len(p.relations) {
			return false
		}
		name := pair.left
		if dot := strings.LastIndexByte(name, '.'); dot >= 0 {
			name = name[dot+1:]
		}
		a, aOK := columnType(p.relations[left].info, name)
		b, bOK := columnType(p.relations[right].info, pair.right)
		if !aOK || !bOK || mysqlMixedNumericTypes(a, b) {
			return false
		}
	}
	return true
}

// Join columns have validated homogeneous types. Read the first non-NULL value
// on each side to choose a hash only when both use the same equality domain.
func mysqlJoinHashCompatible(left, right []map[string]Value, pairs []joinKeyPair) bool {
	class := func(rows []map[string]Value, name string) int {
		for _, row := range rows {
			value, ok := row[name]
			if !ok || value.Null {
				continue
			}
			if isNumeric(value) {
				return 1
			}
			return 2
		}
		return 0
	}
	for _, pair := range pairs {
		a, b := class(left, pair.left), class(right, pair.right)
		if a != 0 && b != 0 && a != b {
			return false
		}
	}
	return true
}
