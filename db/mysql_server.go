package db

import (
	"context"
	"fmt"

	"slopdb/sql"
)

// MySQLReadOnly reports whether replication admission currently permits writes.
// Standalone databases have no admission guard and are writable. A cancelled
// inspection remains an error rather than an apparently healthy read-only node.
func (d *Database) MySQLReadOnly(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	guard := d.replication.Load()
	if guard == nil || guard.Check == nil {
		return false, nil
	}
	err := guard.Check(ctx)
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	return err != nil, nil
}

var mysqlServerStateKey = []byte("\x00mysql_server_state_v1")

// MySQLServerState is protocol administration metadata. It shares ordinary
// storage durability and backup, but is never exposed as a user SQL table.
func (d *Database) MySQLServerState() ([]byte, error) {
	return d.MySQLServerStateContext(context.Background())
}

func (d *Database) MySQLServerStateContext(ctx context.Context) ([]byte, error) {
	if err := d.execution.RLockContext(ctx); err != nil {
		return nil, err
	}
	defer d.execution.RUnlock()
	if d.closed.Load() {
		return nil, fmt.Errorf("database closed")
	}
	v, found, err := d.currentStorage.Load().Get(mysqlServerStateKey)
	if err != nil || !found {
		return nil, err
	}
	return v, nil
}

func (d *Database) SetMySQLServerState(ctx context.Context, data []byte) error {
	if err := d.execution.RLockContext(ctx); err != nil {
		return err
	}
	defer d.execution.RUnlock()
	if d.closed.Load() {
		return fmt.Errorf("database closed")
	}
	if err := d.checkReplication(ctx); err != nil {
		return err
	}
	return d.currentStorage.Load().Put(mysqlServerStateKey, data)
}

// OpenMySQLDatabase is retained for source compatibility. SQL databases now
// share one physical LSM; use CREATE DATABASE and a session's USE statement.
func (d *Database) OpenMySQLDatabase(ctx context.Context, name string, create bool) (*Database, error) {
	return nil, fmt.Errorf("physical MySQL child databases were removed; use CREATE DATABASE and a session USE statement")
}

// RecordMySQLProtocolResult accounts for server-side SQL helpers that do not
// execute an engine statement (for example SHOW VARIABLES and SELECT VERSION).
func (s *Session) RecordMySQLProtocolResult(hasColumns bool, changes int64, rows uint64) {
	s.gate.Lock()
	defer s.gate.Unlock()
	if s.closed || s.closing.Load() || s.db.closed.Load() {
		return
	}
	var statement sql.Statement = &sql.UpdateStmt{}
	if hasColumns {
		statement = &sql.SelectStmt{}
	}
	s.schema.RecordMySQLFunctionResult(statement, changes, rows)
}
