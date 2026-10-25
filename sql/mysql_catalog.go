package sql

import (
	"fmt"
	"strings"
)

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
	schemaVisible := func() bool {
		if options.SchemaNames == nil {
			return true
		}
		for _, schema := range options.SchemaNames() {
			if strings.EqualFold(schema, options.Database) {
				return true
			}
		}
		return false
	}
	view := s.catalogFacts()
	switch name {
	case "information_schema.schemata":
		r := mysqlCatalogFor(name)
		schemas := view.SchemaNames()
		for i := range schemas {
			if schemas[i] == "main" {
				root := options.RootDatabase
				if root == "" {
					root = options.Database
				}
				schemas[i] = root
			}
		}

		if len(schemas) == 0 && options.SchemaNames == nil {
			schemas = []string{options.Database}
		}
		for _, schema := range schemas {
			r.Rows = append(r.Rows, map[string]Value{"catalog_name": NewTextValue("def"), "schema_name": NewTextValue(schema), "default_character_set_name": NewTextValue("utf8mb4"), "default_collation_name": NewTextValue("utf8mb4_bin"), "sql_path": NewNullValue()})
		}
		return r, nil
	case "information_schema.tables":
		r := mysqlCatalogFor(name)
		if !schemaVisible() {
			return r, nil
		}
		for _, table := range append(view.TableNames(), view.ViewNames()...) {
			row := map[string]Value{}
			for _, c := range r.Columns {
				row[c.Name] = NewNullValue()
			}
			schema, bare := SplitSchemaName(table)
			if schema == "main" {
				schema = options.RootDatabase
				if schema == "" {
					schema = options.Database
				}
			}
			row["table_catalog"], row["table_schema"], row["table_name"] = NewTextValue("def"), NewTextValue(schema), NewTextValue(bare)
			row["table_type"], row["engine"], row["row_format"] = NewTextValue("BASE TABLE"), NewTextValue("LSM"), NewTextValue("LSM")
			row["table_collation"] = NewTextValue("binary")
			row["table_comment"], row["create_options"] = NewTextValue("SlopDB transactional LSM storage"), NewTextValue("")
			// Size/cardinality estimates are available from SHOW TABLE STATUS.
			// Unknown sizes remain NULL rather than claiming every table is empty.
			if _, view := view.views[table]; view {
				row["table_type"] = NewTextValue("VIEW")
				row["engine"] = NewNullValue()
				row["row_format"] = NewNullValue()
			}
			r.Rows = append(r.Rows, row)
		}
		return r, nil
	case "information_schema.columns":
		r := mysqlCatalogFor(name)
		if !schemaVisible() {
			return r, nil
		}
		for _, table := range view.TableNames() {
			info := s.GetTableInfo(table)
			for i, c := range info.Columns {
				schema, bare := SplitSchemaName(table)
				if schema == "main" {
					schema = options.RootDatabase
					if schema == "" {
						schema = options.Database
					}
				}
				row := map[string]Value{"table_catalog": NewTextValue("def"), "table_schema": NewTextValue(schema), "table_name": NewTextValue(bare), "column_name": NewTextValue(c.Name), "ordinal_position": NewIntValue(int64(i + 1)), "column_default": NewNullValue(), "is_nullable": NewTextValue("YES"), "data_type": NewTextValue(strings.ToLower(c.ColType.String())), "column_type": NewTextValue(strings.ToLower(c.DeclaredType())), "column_key": NewTextValue(""), "extra": NewTextValue(""), "character_set_name": NewNullValue(), "collation_name": NewNullValue()}
				dataType := strings.ToLower(c.DeclaredType())
				if index := strings.IndexAny(dataType, "( "); index >= 0 {
					dataType = dataType[:index]
				}
				row["data_type"] = NewTextValue(dataType)
				if c.NotNull {
					row["is_nullable"] = NewTextValue("NO")
				}
				if c.PrimaryKey {
					row["column_key"] = NewTextValue("PRI")
				} else if c.Unique {
					row["column_key"] = NewTextValue("UNI")
				}
				if c.AutoIncrement || c.MySQLAutoIncrement {
					row["extra"] = NewTextValue("auto_increment")
				}
				if c.ColType == ColText && c.Declared != "DATE" && c.Declared != "DATETIME" && c.Declared != "TIMESTAMP" {
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

var mysqlCatalogSchemas = map[string][]string{
	"information_schema.schemata": {"catalog_name", "schema_name", "default_character_set_name", "default_collation_name", "sql_path"},
	"information_schema.tables":   {"table_catalog", "table_schema", "table_name", "table_type", "engine", "version", "row_format", "table_rows", "avg_row_length", "data_length", "max_data_length", "index_length", "data_free", "auto_increment", "create_time", "update_time", "check_time", "table_collation", "checksum", "create_options", "table_comment"},
	"information_schema.columns":  {"table_catalog", "table_schema", "table_name", "column_name", "ordinal_position", "column_default", "is_nullable", "data_type", "column_type", "column_key", "extra", "character_set_name", "collation_name"},
}

func mysqlCatalogFor(name string) *ResultSet { return mysqlCatalog(mysqlCatalogSchemas[name]...) }
func mysqlCatalogColumns(name string) ([]ColumnDef, bool) {
	if _, ok := mysqlCatalogSchemas[name]; !ok {
		return nil, false
	}
	return mysqlCatalogFor(name).Columns, true
}
