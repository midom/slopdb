package db

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"

	"slopdb/storage"
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
	if err := d.execution.RLockContext(context.Background()); err != nil {
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

// OpenMySQLDatabase opens a separately isolated database owned by the protocol
// server. Additional databases are standalone instances, never aliases for the
// default database's tables. Name hashing avoids path traversal and case issues.
func (d *Database) OpenMySQLDatabase(ctx context.Context, name string, create bool) (*Database, error) {
	if err := d.execution.RLockContext(ctx); err != nil {
		return nil, err
	}
	defer d.execution.RUnlock()
	if d.closed.Load() {
		return nil, fmt.Errorf("database closed")
	}
	if d.replication.Load() != nil {
		return nil, fmt.Errorf("additional MySQL databases require a standalone server")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	hash := sha256.Sum256([]byte(name))
	path := filepath.Join(d.dir, "mysql-databases", hex.EncodeToString(hash[:]))
	options := storage.Options(*d.opt)
	options.CreateIfNotExist = create
	options.ErrorIfExists = false
	return Open(path, &options)
}
