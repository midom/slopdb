package db

import "slopdb/internal/mysqlschema"

// CDCMySQLCreate is a compatibility renderer. New adapters should consume the
// neutral ChangeTable description and render schema SQL at their boundary.
func CDCMySQLCreate(table CDCTable, root string) string {
	return mysqlschema.Create(table.changeTable(), root)
}
func CDCMySQLAlter(before, after CDCTable, root string) ([]string, error) {
	return mysqlschema.Alter(before.changeTable(), after.changeTable(), root)
}
