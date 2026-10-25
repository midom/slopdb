package sql

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

func mysqlCatalogColumns(name string) ([]ColumnDef, bool) { return informationSchemaColumns(name) }
