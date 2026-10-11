package db

import (
	"context"
	"fmt"

	"slopdb/sql"
	"slopdb/storage"
)

// BackupContext writes a consistent, durable backup to a new database directory
// while other sessions continue reading and writing. The backup includes only
// committed state at one snapshot, including catalog and secondary indexes.
// Restore by opening the destination with Open. Existing destinations are never
// overwritten, and an incomplete backup is never published.
func (db *Database) BackupContext(ctx context.Context, destination string) error {
	ctx, cancel, _, err := db.queryContext(ctx, ExecOptions{})
	if err != nil {
		return err
	}
	defer cancel()
	session, err := db.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()
	if err = session.gate.LockContext(ctx); err != nil {
		return err
	}
	defer session.gate.Unlock()
	if err = db.execution.RLockContext(ctx); err != nil {
		return err
	}
	defer db.execution.RUnlock()
	db.mu.Lock()
	if db.closed.Load() {
		db.mu.Unlock()
		return fmt.Errorf("database closed")
	}
	session.activeCancel = cancel
	db.mu.Unlock()
	defer func() { db.mu.Lock(); session.activeCancel = nil; db.mu.Unlock() }()
	return db.lsm.Backup(ctx, destination)
}
func (db *Database) Backup(destination string) error {
	return db.BackupContext(context.Background(), destination)
}

type IntegrityReport struct {
	Storage storage.IntegrityReport
	SQL     sql.IntegrityReport
}

// CheckIntegrityContext validates current visible data and immutable SSTables.
// It reports corruption without repairing it. Other sessions may keep writing.
func (db *Database) CheckIntegrityContext(ctx context.Context) (report IntegrityReport, err error) {
	ctx, cancel, _, err := db.queryContext(ctx, ExecOptions{})
	if err != nil {
		return report, err
	}
	defer cancel()
	session, err := db.NewSession()
	if err != nil {
		return report, err
	}
	defer session.Close()
	if err = session.gate.LockContext(ctx); err != nil {
		return report, err
	}
	defer session.gate.Unlock()
	if err = db.execution.RLockContext(ctx); err != nil {
		return report, err
	}
	defer db.execution.RUnlock()
	db.mu.Lock()
	if db.closed.Load() {
		db.mu.Unlock()
		return report, fmt.Errorf("database closed")
	}
	session.activeCancel = cancel
	db.mu.Unlock()
	defer func() { db.mu.Lock(); session.activeCancel = nil; db.mu.Unlock() }()
	session.schema.SetContext(ctx)
	// This private diagnostic session uses MVCC, never transactional row locks.
	session.schema.SetLockHandler(nil)
	session.schema.SetLockGroupHandler(nil)
	if err = session.schema.StartStatement(); err != nil {
		return report, err
	}
	defer session.schema.EndStatement()
	report.SQL, err = session.schema.CheckIntegrity()
	if err != nil {
		return report, err
	}
	report.Storage, err = db.lsm.CheckIntegrity(ctx)
	return report, err
}
func (db *Database) CheckIntegrity() (IntegrityReport, error) {
	return db.CheckIntegrityContext(context.Background())
}
