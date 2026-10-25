package sql

import (
	"context"
	"time"
)

type statementTimeKey struct{}

// WithStatementTime keeps one logical statement's timestamp stable across lock
// retries and streamed input. It does not replace transaction or wall clocks.
func WithStatementTime(ctx context.Context, t time.Time) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, statementTimeKey{}, t.UTC())
}
func statementTime(ctx context.Context) time.Time {
	if ctx != nil {
		if t, ok := ctx.Value(statementTimeKey{}).(time.Time); ok && !t.IsZero() {
			return t
		}
	}
	return time.Now().UTC()
}
