package main

import (
	"fmt"
	"os"

	"slopdb/db"
	sqldb "slopdb/sql"
)

func planSchemaChange(database *db.Database, query string) (*db.ExecResult, error) {
	statements, err := sqldb.Parse(query)
	if err != nil {
		return nil, err
	}
	if len(statements) != 1 {
		return nil, fmt.Errorf(".plan requires one ALTER or CREATE/DROP INDEX statement")
	}
	switch statements[0].(type) {
	case *sqldb.AlterTableStmt, *sqldb.CreateIndexStmt, *sqldb.DropIndexStmt:
		return database.Exec("EXPLAIN " + query)
	default:
		return nil, fmt.Errorf(".plan requires one ALTER or CREATE/DROP INDEX statement")
	}
}

func printSchemaChangePlan(query string) {
	plan, err := planSchemaChange(currentDB, query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Plan error: %v\n", err)
		if cliCfg.bail {
			exitCLI(1)
		}
		return
	}
	cliPrint(formatSQLResult(plan))
}
