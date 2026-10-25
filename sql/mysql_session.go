package sql

import "context"

// MySQLSessionOptions supplies connection-specific behavior without changing
// process globals or retaining a network connection on a persistent Schema.
// A caller must keep Function safe for the lifetime of the executing call.
type MySQLSessionOptions struct {
	Database          string
	Schemas           []string
	GroupConcatMaxLen int64
	SQLMode           string
	Function          func(context.Context, string, []Value) (Value, bool, error)
	SystemTables      map[string]*ResultSet
}

type mysqlSessionKey struct{}

func WithMySQLSession(ctx context.Context, options MySQLSessionOptions) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, mysqlSessionKey{}, options)
}

func MySQLFunction(ctx context.Context, name string, args []Value) (Value, bool, error) {
	if ctx != nil {
		if options, ok := ctx.Value(mysqlSessionKey{}).(MySQLSessionOptions); ok && options.Function != nil {
			return options.Function(ctx, name, args)
		}
	}
	return Value{}, false, nil
}

func MySQLGroupConcatMaxLen(ctx context.Context) int64 {
	if ctx != nil {
		if options, ok := ctx.Value(mysqlSessionKey{}).(MySQLSessionOptions); ok {
			return options.GroupConcatMaxLen
		}
	}
	return 0
}

func MySQLSession(ctx context.Context) (MySQLSessionOptions, bool) {
	if ctx == nil {
		return MySQLSessionOptions{}, false
	}
	options, ok := ctx.Value(mysqlSessionKey{}).(MySQLSessionOptions)
	return options, ok
}
