package db

import (
	"context"
	"fmt"
)

// PageSize reports the effective uncompressed SSTable data-block target.
func (db *Database) PageSize() int64 { return db.currentStorage.Load().PageSize() }

// SetPageSize changes and persists the target for future flush/compaction
// writers. Existing SSTables remain readable. VACUUM rewrites existing data.
// Page-size changes are physical configuration and reject explicit transactions
// in the default session; use SQL PRAGMA page_size with independent sessions.
func (db *Database) SetPageSize(size int64) error {
	return db.SetPageSizeContext(context.Background(), size)
}
func (db *Database) SetPageSizeContext(ctx context.Context, size int64) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA page_size=%d", size))
	return err
}
