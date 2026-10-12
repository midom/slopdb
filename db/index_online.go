package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"slopdb/sql"
)

func onlineIndexStatement(schema *sql.Schema, stmt sql.Statement) bool {
	if schema.InTransaction() {
		return false
	}
	switch a := stmt.(type) {
	case *sql.CreateIndexStmt:
		return true
	case *sql.AlterTableStmt:
		return a.Operation == "ADD_INDEX" && (a.ObjectKind == "" || a.ObjectKind == "TABLE")
	}
	return false
}

func (db *Database) acquireOnlineCatalog(ctx context.Context, schema *sql.Schema, exclusive bool, lockTimeout time.Duration, deadline *time.Time, abortAfter *time.Duration) error {
	request := sql.LockRequest{Resource: "catalog", Exclusive: exclusive}
	if err := db.locks.acquire(schema, request); err != nil {
		var waiting *lockWait
		if !errors.As(err, &waiting) {
			return err
		}
		if lockTimeout == 0 {
			return db.lockDiagnosticError(schema, request, fmt.Errorf("%w: %w", ErrLockTimeout, context.DeadlineExceeded))
		}
		if deadline.IsZero() {
			*deadline = time.Now().Add(lockTimeout)
		}
		return db.waitForLockPolicy(ctx, schema, request, *deadline, abortAfter)
	}
	return nil
}

func (db *Database) execOnlineIndex(ctx context.Context, schema *sql.Schema, statement sql.Statement, lockTimeout time.Duration, deadline *time.Time) (result *ExecResult, resultErr error) {
	stmt, eligible, err := schema.OnlineCreateIndex(statement)
	if err != nil {
		return nil, err
	}
	if !eligible {
		return nil, fmt.Errorf("statement is not eligible for online index creation")
	}
	if stmt == nil {
		return &ExecResult{}, nil
	}
	policy, err := indexPublicationPolicy(ctx, schema, statement)
	if err != nil {
		return nil, err
	}
	build, err := schema.StartOnlineIndex(stmt)
	if err != nil {
		return nil, fmt.Errorf("CREATE INDEX error: %w", err)
	}
	if build == nil {
		return &ExecResult{}, nil
	}
	defer func() {
		if err := build.Close(); err != nil {
			status := "index build aborted; index cleanup pending"
			if resultErr == nil {
				status = "CREATE INDEX committed; index cleanup pending"
			}
			resultErr = errors.Join(resultErr, fmt.Errorf("%s: %w", status, err))
		}
	}()
	schema.EndStatement() // The build owns its own pinned scan snapshot.
	if err = schema.ScanOnlineIndex(build); err != nil {
		return nil, fmt.Errorf("CREATE INDEX error: %w", err)
	}
	if db.onlineIndexHook != nil {
		db.onlineIndexHook("scanned")
	}
	if err = schema.CatchUpOnlineIndex(build); err != nil {
		return nil, fmt.Errorf("CREATE INDEX error: %w", err)
	}
	if db.onlineIndexHook != nil {
		db.onlineIndexHook("caught-up")
	}
	for {
		// Release shared ownership before waiting for exclusive publication. Two
		// builders must never deadlock by retaining mutually incompatible upgrades.
		// Finish revalidates the table/collation after this gap before publication.
		db.locks.release(schema)
		if db.onlineIndexHook != nil {
			db.onlineIndexHook("publication-gap")
		}
		if err = db.acquireOnlineCatalog(ctx, schema, true, lockTimeout, deadline, policy); err != nil {
			return nil, err
		}
		if err = schema.StartStatement(); err != nil {
			return nil, err
		}
		err = schema.ExecuteStatement(func() error { return schema.FinishOnlineIndex(stmt, build) })
		schema.EndStatement()
		if !errors.Is(err, sql.ErrOnlineIndexCatchUp) {
			break
		}
		if db.onlineIndexHook != nil {
			db.onlineIndexHook("catch-up-retry")
		}
		db.locks.release(schema)
		if err = db.acquireOnlineCatalog(ctx, schema, false, lockTimeout, deadline, nil); err != nil {
			return nil, err
		}
		if err = schema.CatchUpOnlineIndex(build); err != nil {
			return nil, err
		}
	}
	if err != nil {
		return nil, fmt.Errorf("CREATE INDEX error: %w", err)
	}
	return &ExecResult{}, nil
}
