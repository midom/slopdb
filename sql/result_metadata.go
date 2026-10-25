package sql

import "strings"

// applyResultMetadata changes only descriptor types. Row map keys, duplicate
// labels and native cell values remain owned by the executing result.
func applyResultMetadata(columns, types []ColumnDef) {
	if len(columns) != len(types) {
		return
	}
	for i, t := range types {
		name, label := columns[i].Name, columns[i].DisplayName
		if label == "" {
			label = t.DisplayName
		}
		if label == "" && name != t.Name && strings.Contains(name, ".") {
			label = t.ResultName()
		}
		logical := t.LogicalType()
		t.SemanticType = logical.Kind
		t.Default, t.Check = nil, nil
		t.ParameterIndex = 0
		t.Name, t.DisplayName = name, label
		columns[i] = t
	}
}
func (s *Schema) projectionMetadata(stmt *SelectStmt, info *TableInfo, columns []ColumnDef) {
	if s.directProjectionMetadata(stmt, info, columns) {
		return
	}
	// Runtime view scaffolding historically carried names only. Obtain its real
	// declaration from this same catalog view before trusting those placeholders.
	if info != nil && s.views[info.Name] != nil {
		if cols, err := s.describeTable(info.Name, nil, 0); err == nil {
			copy := *info
			copy.Columns = cols
			info = &copy
		}
	}

	if stmt.SelectAll && len(stmt.Joins) == 0 {

		if info != nil {
			applyResultMetadata(columns, info.Columns)
		}
		for i := range columns {
			columns[i].SemanticType = columns[i].LogicalType().Kind
			columns[i].Default, columns[i].Check = nil, nil
		}
		return
	}
	env := map[string]ColumnDef{}
	var all []ColumnDef
	if info != nil {
		name := stmt.From
		if name == "" {
			name = info.Name
		}
		describeEnv(info.Columns, name, stmt.FromAlias, env)
		all = append(all, info.Columns...)
	}
	for _, j := range stmt.Joins {
		var cols []ColumnDef
		var err error
		if j.Subquery != nil {
			cols, err = s.describeStatement(j.Subquery, nil, 0)
		} else {
			cols, err = s.describeTable(j.Table, nil, 0)
		}
		if err != nil {
			return
		}
		if j.Type == "LEFT" || j.Type == "RIGHT" {
			for i := range cols {
				cols[i].NotNull = false
			}
		}
		describeEnv(cols, j.Table, j.Alias, env)
		all = append(all, cols...)
	}
	if stmt.SelectAll {
		applyResultMetadata(columns, all)
		return
	}
	types, err := s.describeProjection(stmt.Columns, all, env, nil, 0, false)
	if err == nil {
		applyResultMetadata(columns, types)
	}
}

// A small physical-column projection needs neither a qualified descriptor map
// nor an intermediate descriptor slice. Keep this bounded: larger projections
// and expressions use the general metadata resolver below. Execution still
// performs all ordinary column, scope, and grouping validation independently.
func (s *Schema) directProjectionMetadata(stmt *SelectStmt, info *TableInfo, columns []ColumnDef) bool {
	var indexes [8]int
	if info == nil || stmt.SelectAll || len(stmt.Columns) == 0 || len(stmt.Columns) > len(indexes) || len(stmt.Columns) != len(columns) || stmt.From == "" || s.tables[strings.ToLower(stmt.From)] != info || s.views[info.Name] != nil || s.cteScope != nil || stmt.With != nil || stmt.FromExpr != nil || stmt.Compound != nil || stmt.Virtual != nil || stmt.materialized != nil || len(stmt.Joins) != 0 || len(stmt.Windows) != 0 || len(stmt.GroupBy) != 0 || stmt.Having != nil || stmt.trackRows || stmt.mutationRows {
		return false
	}
	// Resolve every slot before changing any output. A later unknown or
	// ambiguous reference must leave the full resolver's original inputs intact.
	for i, selected := range stmt.Columns {
		ref, ok := selected.Expr.(*ColumnRefExpr)
		if !ok || selected.Star || selected.StarQualifier != "" || strings.Contains(ref.Name, ".") {
			return false
		}
		index := -1
		for j, source := range info.Columns {
			if source.Name == ref.Name {
				if index >= 0 {
					return false
				}
				index = j
			}
		}
		if index < 0 {
			return false
		}
		indexes[i] = index
	}
	for i, selected := range stmt.Columns {
		typed := info.Columns[indexes[i]]
		if selected.Alias != "" {
			typed.Name = selected.Alias
			typed.OriginalName, typed.DisplayName = "", ""
		} else {
			name := selected.Expr.(*ColumnRefExpr).Name
			label := typed.ResultName()
			if label == "" {
				label = name
			}
			typed.DisplayName = ""
			if label != name {
				typed.DisplayName = label
			}
			typed.Name = name
		}
		if selected.starLabel != "" {
			typed.DisplayName = selected.starLabel
		}
		// Reuse the same semantic-type promotion, expression cleanup and output
		// label handling as the full describeProjection path.
		metadata := [1]ColumnDef{typed}
		applyResultMetadata(columns[i:i+1], metadata[:])
	}
	return true
}
func (s *Schema) returningMetadata(cols []SelectColumn, info *TableInfo, result *ResultSet) {
	env := map[string]ColumnDef{}
	describeEnv(info.Columns, info.Name, "", env)
	types, err := s.describeProjection(cols, info.Columns, env, nil, 0, true)
	if err == nil {
		applyResultMetadata(result.Columns, types)
	}
}
