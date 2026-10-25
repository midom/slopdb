package sql

import "strings"

// Bytewise plan shortcuts cannot reproduce comparisons that choose a column's
// explicit collation against a differently collated operand. Keep those inputs
// on the expression evaluator until a plan can prove one shared key policy.
func columnsHaveExplicitCollation(columns []ColumnDef) bool {
	for _, column := range columns {
		if column.ColType == ColText && column.Collation != "" {
			return true
		}
	}
	return false
}
func tableHasExplicitCollation(info *TableInfo) bool {
	return info != nil && columnsHaveExplicitCollation(info.Columns)
}
func (s *Schema) nonbinaryDefaultCollation() bool {
	return s.collation != "" && s.collation != BinaryCollation
}
func joinKeyValuesHaveCollation(left, right []map[string]Value, pairs []joinKeyPair) bool {
	for _, pair := range pairs {
		for _, row := range left {
			if v := row[pair.left]; v.Type == ColText && v.Collation != "" {
				return true
			}
		}
		for _, row := range right {
			if v := row[pair.right]; v.Type == ColText && v.Collation != "" {
				return true
			}
		}
	}
	return false
}

func columnHasExplicitCollation(info *TableInfo, name string) bool {
	if info == nil {
		return false
	}
	for _, column := range info.Columns {
		if column.Name == name {
			return column.ColType == ColText && column.Collation != ""
		}
	}
	return false
}
func (p *joinPlan) pairsHaveExplicitCollation(right int, pairs []joinKeyPair) bool {
	for _, pair := range pairs {
		if columnHasExplicitCollation(p.relations[right].info, pair.right) {
			return true
		}
		left, ok := p.owners[pair.left]
		if !ok || left < 0 || left >= len(p.relations) {
			return true
		}
		column := pair.left
		if dot := strings.LastIndexByte(column, '.'); dot >= 0 {
			column = column[dot+1:]
		}
		if columnHasExplicitCollation(p.relations[left].info, column) {
			return true
		}
	}
	return false
}
func relationJoinKeysHaveExplicitCollation(info *TableInfo, pairs []joinKeyPair) bool {
	for _, pair := range pairs {
		if columnHasExplicitCollation(info, pair.right) {
			return true
		}
	}
	return false
}
