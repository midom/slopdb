package mysqlserver

import (
	"slopdb/db"
	"slopdb/internal/mysqlschema"
)

// Canonical main is deliberately independent of a configurable login alias:
// changing that alias after restart must not shift durable binary-log offsets.
func binlogCreateTable(table db.ChangeTable) (string, error) {
	return mysqlschema.Create(table, "main"), nil
}
func binlogAlterTable(before, after db.ChangeTable) ([]string, error) {
	return mysqlschema.Alter(before, after, "main")
}
