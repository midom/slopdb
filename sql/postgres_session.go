package sql

import "context"

type PostgresSessionOptions struct {
	Database, User string
	BackendPID     uint32
	Function       func(context.Context, string, []Value) (Value, bool, error)
}
type postgresSessionKey struct{}

func WithPostgresSession(ctx context.Context, options PostgresSessionOptions) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = WithSessionContext(ctx, SessionContext{Dialect: DialectPostgreSQL, Database: options.Database, User: options.User, BackendID: uint64(options.BackendPID), Function: options.Function})
	return context.WithValue(ctx, postgresSessionKey{}, options)
}
func PostgresSession(ctx context.Context) (PostgresSessionOptions, bool) {
	if common, ok := SessionContextFrom(ctx); ok && common.Dialect != DialectPostgreSQL {
		return PostgresSessionOptions{}, false
	}
	if ctx == nil {
		return PostgresSessionOptions{}, false
	}
	options, ok := ctx.Value(postgresSessionKey{}).(PostgresSessionOptions)
	return options, ok
}
