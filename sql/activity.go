package sql

import (
	"context"
	"time"
)

// ActivityQueryLimit bounds retained source text without expanding bound values.
const ActivityQueryLimit = 64 << 10

type ActivityState string

const (
	ActivityIdle                     ActivityState = "idle"
	ActivityActive                   ActivityState = "active"
	ActivityIdleInTransaction        ActivityState = "idle in transaction"
	ActivityIdleInTransactionAborted ActivityState = "idle in transaction (aborted)"
)

// SessionActivity is an owned, point-in-time view of a connection. SessionID is
// the native lock owner; ID is the stable, globally allocated public backend ID.
type SessionActivity struct {
	SessionID, ID                                        uint64
	Dialect                                              Dialect
	User, Database, ApplicationName, ClientAddress, Host string
	ClientPort                                           int
	StartedAt, QueryStart, StateChange, TransactionStart time.Time
	State                                                ActivityState
	Query                                                string
	Waiting                                              bool
	WaitKind, WaitResource                               string
}

type ActivityIdentity struct {
	BackendID                                            uint32
	Dialect                                              Dialect
	User, Database, ApplicationName, ClientAddress, Host string
	ClientPort                                           int
}
type ActivitySnapshotProvider func(context.Context) ([]SessionActivity, error)

func (s *Schema) SetActivitySnapshot(provider ActivitySnapshotProvider) {
	s.activitySnapshot = provider
}

type activityQueryKey struct{}

// WithActivityQuery preserves protocol source text through SQL rewrites.
func WithActivityQuery(ctx context.Context, query string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, activityQueryKey{}, query)
}
func ActivityQueryFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	q, ok := ctx.Value(activityQueryKey{}).(string)
	return q, ok
}
