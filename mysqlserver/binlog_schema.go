package mysqlserver

import "slopdb/db"

// Canonical main is deliberately independent of a configurable login alias:
// changing that alias after restart must not shift durable binary-log offsets.
func binlogCreateTable(table db.CDCTable) (string, error) {
	return db.CDCMySQLCreate(table, "main"), nil
}
func binlogAlterTable(before, after db.CDCTable) ([]string, error) {
	return db.CDCMySQLAlter(before, after, "main")
}
