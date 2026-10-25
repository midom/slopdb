package db

import "context"

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
