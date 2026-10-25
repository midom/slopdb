package sql

import "context"

// SessionFunction is the protocol adapter for connection-local operations.
// Implementations must not re-enter an executing/locked SQL session.
type SessionFunction func(context.Context, string, []Value) (Value, bool, error)

// SessionContext carries shared connection identity and callback facilities.
// Dialect-specific settings (SQL_MODE, warnings, etc.) remain extension options.
type SystemVariableGetter func(context.Context, string, bool) (Value, error)

type SessionContext struct {
	Dialect        Dialect
	Database, User string
	BackendID      uint64
	SystemVariable SystemVariableGetter
	Function       SessionFunction
}
type sessionContextKey struct{}

func WithSessionContext(ctx context.Context, options SessionContext) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, sessionContextKey{}, options)
}
func SessionContextFrom(ctx context.Context) (SessionContext, bool) {
	if ctx == nil {
		return SessionContext{}, false
	}
	options, ok := ctx.Value(sessionContextKey{}).(SessionContext)
	return options, ok
}
func CallSessionFunction(ctx context.Context, name string, args []Value) (Value, bool, error) {
	if options, ok := SessionContextFrom(ctx); ok && options.Function != nil {
		return options.Function(ctx, name, args)
	}
	return Value{}, false, nil
}
