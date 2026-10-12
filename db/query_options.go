package db

import (
	"context"
	"fmt"
	"time"

	"slopdb/sql"
)

// ExecOptions overrides query defaults and enables optional index publication
// behavior for one SQL call. Nil query settings inherit the database default.
// A pointer to zero disables the query
// timeout for Timeout, or fails immediately on contention for LockTimeout.
// A caller's context deadline always applies, even when Timeout is overridden.
type ExecOptions struct {
	MemoryBytes *int64 // per-operator budget; nil inherits, zero selects 64 MiB
	Timeout     *time.Duration
	LockTimeout *time.Duration
	// IndexPublicationAbortAfter enables rollback of blocking explicit
	// transactions after this publication wait. Nil disables; zero is immediate.
	IndexPublicationAbortAfter *time.Duration
}

func (db *Database) queryContext(ctx context.Context, options ExecOptions) (context.Context, context.CancelFunc, time.Duration, error) {
	queryTimeout, lockTimeout := db.opt.QueryTimeout, db.opt.LockTimeout
	if options.Timeout != nil {
		queryTimeout = *options.Timeout
	}
	if options.LockTimeout != nil {
		lockTimeout = *options.LockTimeout
	}
	if queryTimeout < 0 || lockTimeout < 0 {
		return nil, nil, 0, fmt.Errorf("timeouts must not be negative")
	}
	if options.IndexPublicationAbortAfter != nil && *options.IndexPublicationAbortAfter < 0 {
		return nil, nil, 0, fmt.Errorf("index publication abort threshold must not be negative")
	}
	budget := db.opt.QueryMemoryBytes
	if options.MemoryBytes != nil {
		budget = *options.MemoryBytes
	}
	if budget < 0 {
		return nil, nil, 0, fmt.Errorf("query memory budget must not be negative")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = sql.WithQueryMemory(ctx, budget)
	if options.IndexPublicationAbortAfter != nil {
		ctx = context.WithValue(ctx, publicationAbortContextKey{}, *options.IndexPublicationAbortAfter)
	}
	var cancel context.CancelFunc
	if queryTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, queryTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	return ctx, cancel, lockTimeout, nil
}
