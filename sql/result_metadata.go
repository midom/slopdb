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
func (s *Schema) returningMetadata(cols []SelectColumn, info *TableInfo, result *ResultSet) {
	env := map[string]ColumnDef{}
	describeEnv(info.Columns, info.Name, "", env)
	types, err := s.describeProjection(cols, info.Columns, env, nil, 0, true)
	if err == nil {
		applyResultMetadata(result.Columns, types)
	}
}
