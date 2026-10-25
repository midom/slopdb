package sql

import ()

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
	return s.informationSchemaTable(name, catalogFilter{})
}

func mysqlCatalogFor(name string) *ResultSet {
	columns, _ := informationSchemaColumns(name)
	return &ResultSet{Columns: columns}
}
func mysqlCatalogColumns(name string) ([]ColumnDef, bool) { return informationSchemaColumns(name) }
