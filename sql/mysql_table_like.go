package sql

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// SQLName separates a table-local MySQL index name from its physical catalog
// key. Existing native indexes retain their original global names.
func (idx *IndexInfo) SQLName() string {
	if idx.DisplayName != "" {
		return idx.DisplayName
	}
	_, name := SplitSchemaName(idx.Name)
	return name
}

func (s *Schema) findTableIndex(table, name string) *IndexInfo {
	table, name = strings.ToLower(table), strings.ToLower(name)
	for _, idx := range s.tableIndexes(table) {
		if idx.SQLName() == name {
			return idx
		}
	}
	return nil
}

func cloneLikeExpression(expr Expression) (Expression, error) {
	if expr == nil {
		return nil, nil
	}
	return parseExpr(formatExpr(expr))
}

func (s *Schema) expandCreateTableLike(input *CreateTableStmt) (*CreateTableStmt, error) {
	if _, exists := s.tables[strings.ToLower(input.Table)]; exists && input.IfNotExists && (!input.Temporary || s.isTemporaryTable(input.Table)) {
		copy := *input
		copy.Like = ""
		return &copy, nil
	}
	source := s.tables[strings.ToLower(input.Like)]
	if source == nil {
		return nil, fmt.Errorf("source table %q does not exist", input.Like)
	}
	stmt := *input
	stmt.Like = ""
	stmt.Charset, stmt.Engine = source.Charset, source.Engine
	stmt.DefaultCollation = source.DefaultCollation
	stmt.Columns = slices.Clone(source.Columns)
	for i := range stmt.Columns {
		col := &stmt.Columns[i]
		col.EnumValues = slices.Clone(col.EnumValues)
		col.ConstraintNames = maps.Clone(col.ConstraintNames)
		// MySQL LIKE copies definitions, but does not copy foreign keys.
		col.RefTable, col.RefColumns = "", nil
		delete(col.ConstraintNames, "FOREIGN")
		var err error
		if col.Default, err = cloneLikeExpression(col.Default); err != nil {
			return nil, err
		}
		if col.Check, err = cloneLikeExpression(col.Check); err != nil {
			return nil, err
		}
	}
	stmt.PKColumns = slices.Clone(source.PKColumns)
	stmt.TableUnique = make([][]string, len(source.TableUnique))
	for i, columns := range source.TableUnique {
		stmt.TableUnique[i] = slices.Clone(columns)
	}
	stmt.TableCheckSQL = slices.Clone(source.TableCheckSQL)
	stmt.TableCheck = make([]Expression, len(source.TableCheck))
	for i, expr := range source.TableCheck {
		var err error
		if stmt.TableCheck[i], err = cloneLikeExpression(expr); err != nil {
			return nil, err
		}
	}
	stmt.Constraints = nil
	for _, constraint := range source.Constraints {
		if constraint.Kind == "FOREIGN" {
			continue
		}
		constraint.IndexID = 0
		constraint.Columns = slices.Clone(constraint.Columns)
		constraint.PrefixLengths = slices.Clone(constraint.PrefixLengths)
		var err error
		if constraint.Check, err = cloneLikeExpression(constraint.Check); err != nil {
			return nil, err
		}
		stmt.Constraints = append(stmt.Constraints, constraint)
	}
	stmt.ForeignKeys = nil
	stmt.Indexes = nil
	reserved := make(map[string]bool)
	for name := range s.indexes {
		reserved[name] = true
	}
	for _, idx := range s.tableIndexes(source.Name) {
		if idx.Auto {
			continue
		}
		base := strings.ToLower(stmt.Table) + "_" + idx.SQLName()
		name := base
		for n := 2; reserved[name]; n++ {
			name = fmt.Sprintf("%s_%d", base, n)
		}
		reserved[name] = true
		stmt.Indexes = append(stmt.Indexes, IndexDef{Name: name, DisplayName: idx.SQLName(), Columns: slices.Clone(idx.columnNames()), PrefixLengths: slices.Clone(idx.PrefixLengths), Unique: idx.IsUnique, Fulltext: idx.Fulltext})
	}
	return &stmt, nil
}

// indexIdentity separates table-scoped SQL names from the global internal map.
// Length-prefixing preserves boundaries even for quoted names containing punctuation.
func (s *Schema) indexIdentity(table, name, display string) (string, string) {
	if !s.MySQLMode() {
		return name, display
	}
	if display == "" {
		_, display = SplitSchemaName(name)
	}
	return tableLocalIndexIdentity(table, display), display
}

func displayOrName(display, name string) string {
	if display != "" {
		return display
	}
	return name
}

func tableLocalIndexIdentity(table, name string) string {
	return fmt.Sprintf("\x00mysql-index:%d:%s%s", len(table), table, name)
}
