package sql

import (
	"fmt"
	"strings"
)

// MySQL virtual catalogs are built from the execution's catalog snapshot. They
// use the ordinary SELECT pipeline for predicates, grouping, ordering and limits.
func (s *Schema) resolveMySQLTables(stmt *SelectStmt) (*SelectStmt, error) {
	options, enabled := MySQLSession(s.ctx)
	if !enabled {
		return stmt, nil
	}
	copyStmt := *stmt
	copyStmt.Joins = append([]JoinClause(nil), stmt.Joins...)
	resolve := func(name string) (string, *SelectStmt, error) {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "information_schema.") || lower == "mysql.user" {
			data, err := s.mysqlCatalogTable(lower, options)
			if err != nil {
				return "", nil, err
			}
			return "", &SelectStmt{materialized: data}, nil
		}
		if dot := strings.IndexByte(name, '.'); dot >= 0 {
			if !strings.EqualFold(name[:dot], options.Database) {
				return "", nil, fmt.Errorf("unknown database %q", name[:dot])
			}
			return name[dot+1:], nil, nil
		}
		return name, nil, nil
	}
	var sub *SelectStmt
	var err error
	if copyStmt.From != "" {
		copyStmt.From, sub, err = resolve(copyStmt.From)
		if err != nil {
			return nil, err
		}
		if sub != nil {
			copyStmt.FromExpr = &SubqueryExpr{Stmt: sub}
		}
	}
	for i := range copyStmt.Joins {
		j := &copyStmt.Joins[i]
		if j.Table == "" {
			continue
		}
		j.Table, sub, err = resolve(j.Table)
		if err != nil {
			return nil, err
		}
		if sub != nil {
			j.Subquery = sub
		}
	}
	return &copyStmt, nil
}

func mysqlCatalog(columns ...string) *ResultSet {
	r := &ResultSet{}
	for _, name := range columns {
		r.Columns = append(r.Columns, ColumnDef{Name: name, ColType: ColText})
	}
	return r
}

func (s *Schema) mysqlCatalogTable(name string, options MySQLSessionOptions) (*ResultSet, error) {
	if data := options.SystemTables[name]; data != nil {
		return copyCTEResult(data), nil
	}
	if options.SystemTable != nil {
		data, err := options.SystemTable(name)
		if err != nil {
			return nil, err
		}
		if data != nil {
			return copyCTEResult(data), nil
		}
	}
	switch name {
	case "information_schema.schemata":
		r := mysqlCatalog("catalog_name", "schema_name", "default_character_set_name", "default_collation_name", "sql_path")
		schemas := options.Schemas
		if options.SchemaNames != nil {
			schemas = options.SchemaNames()
		}
		if len(schemas) == 0 && options.SchemaNames == nil {
			schemas = []string{options.Database}
		}
		for _, schema := range schemas {
			r.Rows = append(r.Rows, map[string]Value{"catalog_name": NewTextValue("def"), "schema_name": NewTextValue(schema), "default_character_set_name": NewTextValue("utf8mb4"), "default_collation_name": NewTextValue("utf8mb4_bin"), "sql_path": NewNullValue()})
		}
		return r, nil
	case "information_schema.tables":
		r := mysqlCatalog("table_catalog", "table_schema", "table_name", "table_type", "engine", "version", "row_format", "table_rows", "avg_row_length", "data_length", "max_data_length", "index_length", "data_free", "auto_increment", "create_time", "update_time", "check_time", "table_collation", "checksum", "create_options", "table_comment")
		for _, table := range s.GetTableList() {
			row := map[string]Value{}
			for _, c := range r.Columns {
				row[c.Name] = NewNullValue()
			}
			row["table_catalog"], row["table_schema"], row["table_name"] = NewTextValue("def"), NewTextValue(options.Database), NewTextValue(table)
			row["table_type"], row["engine"], row["row_format"] = NewTextValue("BASE TABLE"), NewTextValue("LSM"), NewTextValue("Dynamic")
			row["table_collation"] = NewTextValue("binary")
			row["table_comment"], row["create_options"] = NewTextValue("SlopDB transactional LSM storage"), NewTextValue("")
			for _, n := range []string{"version", "table_rows", "avg_row_length", "data_length", "max_data_length", "index_length", "data_free"} {
				row[n] = NewIntValue(0)
			}
			r.Rows = append(r.Rows, row)
		}
		return r, nil
	case "information_schema.columns":
		r := mysqlCatalog("table_catalog", "table_schema", "table_name", "column_name", "ordinal_position", "column_default", "is_nullable", "data_type", "column_type", "column_key", "extra", "character_set_name", "collation_name")
		for _, table := range s.GetTableList() {
			info := s.GetTableInfo(table)
			for i, c := range info.Columns {
				row := map[string]Value{"table_catalog": NewTextValue("def"), "table_schema": NewTextValue(options.Database), "table_name": NewTextValue(table), "column_name": NewTextValue(c.Name), "ordinal_position": NewIntValue(int64(i + 1)), "column_default": NewNullValue(), "is_nullable": NewTextValue("YES"), "data_type": NewTextValue(strings.ToLower(c.ColType.String())), "column_type": NewTextValue(strings.ToLower(c.DeclaredType())), "column_key": NewTextValue(""), "extra": NewTextValue(""), "character_set_name": NewNullValue(), "collation_name": NewNullValue()}
				if c.NotNull {
					row["is_nullable"] = NewTextValue("NO")
				}
				if c.PrimaryKey {
					row["column_key"] = NewTextValue("PRI")
				} else if c.Unique {
					row["column_key"] = NewTextValue("UNI")
				}
				if c.AutoIncrement {
					row["extra"] = NewTextValue("auto_increment")
				}
				if c.ColType == ColText {
					row["character_set_name"] = NewTextValue("utf8mb4")
					row["collation_name"] = NewTextValue("utf8mb4_bin")
				}
				if c.Default != nil {
					v, e := s.evalExpr(c.Default, nil)
					if e != nil {
						return nil, e
					}
					row["column_default"] = v
				}
				r.Rows = append(r.Rows, row)
			}
		}
		return r, nil
	default:
		return nil, fmt.Errorf("unknown information_schema table %q", name)
	}
}
