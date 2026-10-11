package db

import (
	"context"
	"fmt"
	"time"
)

// ExecOptions overrides the defaults from storage.Options for one SQL call.
// Nil inherits the database default. A pointer to zero disables the query
// timeout for Timeout, or fails immediately on contention for LockTimeout.
// A caller's context deadline always applies, even when Timeout is overridden.
type ExecOptions struct {
	Timeout     *time.Duration
	LockTimeout *time.Duration
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
	if ctx == nil {
		ctx = context.Background()
	}
	var cancel context.CancelFunc
	if queryTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, queryTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	return ctx, cancel, lockTimeout, nil
}
