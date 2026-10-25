package sql

import (
	"fmt"
	"strconv"
	"strings"
)

// ResultName is the SQL output label. Name remains the unique key used to find
// a value in result row maps, including when several output labels are equal.
func (c ColumnDef) ResultName() string {
	if c.DisplayName != "" {
		return c.DisplayName
	}
	if c.OriginalName != "" && c.Name == strings.ToLower(c.OriginalName) {
		return c.OriginalName
	}
	return c.Name
}

// Give each output slot its own row-map key before evaluating expressions.
// Private keys cannot accidentally overwrite an ordinary source column needed
// by WHERE, ORDER BY, grouping or a later projection expression.
func uniqueResultColumns(columns []ColumnDef, expressions []selectExprCol) bool {
	if len(columns) < 2 {
		return false
	}
	reserved := make(map[string]bool, len(columns))
	for _, c := range columns {
		reserved[c.Name] = true
	}
	seen := make(map[string]bool, len(columns))
	changed := false
	for i := range columns {
		c := &columns[i]
		old := c.Name
		if !seen[old] {
			seen[old] = true
			continue
		}
		changed = true
		if c.DisplayName == "" {
			c.DisplayName = c.ResultName()
		}
		key := "\x00result:" + strconv.Itoa(i)
		for reserved[key] {
			key += "_"
		}
		reserved[key] = true
		c.Name = key
		if i < len(expressions) {
			expressions[i].name = key
		}
	}
	return changed
}

func (s *Schema) selectProjection(stmt *SelectStmt, info *TableInfo) ([]ColumnDef, []selectExprCol, bool) {
	var columns []ColumnDef
	var expressions []selectExprCol
	appendStar := func() {
		start := len(columns)
		if info != nil {
			columns = append(columns, info.Columns...)
		}
		base := append([]ColumnDef(nil), columns...)
		rs := ResultSet{Columns: columns}
		for _, j := range stmt.Joins {
			s.addJoinColumns(&rs, j, base)
		}
		columns = rs.Columns
		if !stmt.SelectAll {
			for _, col := range columns[start:] {
				expressions = append(expressions, selectExprCol{name: col.Name, expr: &ColumnRefExpr{Name: col.Name}})
			}
		}
	}
	if stmt.SelectAll {
		appendStar()
	} else {
		index := 0
		for _, sc := range stmt.Columns {
			if sc.Star {
				appendStar()
				continue
			}
			name := sc.Alias
			if name == "" {
				switch e := sc.Expr.(type) {
				case *ColumnRefExpr:
					name = e.Name
				case *FuncCallExpr:
					name = e.ResultName()
				default:
					name = fmt.Sprintf("expr%d", index)
				}
			}
			index++
			columns = append(columns, ColumnDef{Name: name, DisplayName: sc.starLabel})
			expressions = append(expressions, selectExprCol{name: name, expr: sc.Expr})
		}
	}
	s.projectionMetadata(stmt, info, columns)
	return columns, expressions, uniqueResultColumns(columns, expressions)
}

// DeclaredName preserves identifier spelling without changing lookup keys.
func (c ColumnDef) DeclaredName() string {
	if c.OriginalName != "" {
		return c.OriginalName
	}
	return c.Name
}
