package db

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync/atomic"

	"slopdb/sql"
)

var ErrStmtClosed = errors.New("prepared statement closed")

// Stmt is a parsed, parameterized SQL statement with reusable execution plans. Database statements can be
// shared by concurrent sessions and transactions on that database. Prepare on
// a Session or Tx instead to bind the statement to that execution context.
// Close rejects new executions; an execution already started may finish.
type Stmt struct {
	generation uint64
	db         *Database
	session    *Session
	bound      bool
	query      *sql.PreparedQuery
	closed     atomic.Bool
}

func (db *Database) Prepare(query string) (*Stmt, error) {
	return db.PrepareContext(context.Background(), query)
}

// PrepareContext parses exactly one statement. Anonymous '?' placeholders bind
// values, not SQL text or identifiers. Execution compiles and reuses eligible
// operator templates, rebinding parameters and invalidating metadata on schema
// or statistics changes.
func (db *Database) PrepareContext(ctx context.Context, query string) (*Stmt, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if db.closed.Load() {
		return nil, errors.New("database closed")
	}
	p, err := sql.PrepareWithOptions(query, db.ParseOptions())
	if err != nil {
		return nil, fmt.Errorf("prepare: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if db.closed.Load() {
		return nil, errors.New("database closed")
	}
	return &Stmt{db: db, query: p, generation: db.generation.Load()}, nil
}

func (s *Session) Prepare(query string) (*Stmt, error) {
	return s.PrepareContext(context.Background(), query)
}

func (s *Session) PrepareContext(ctx context.Context, query string) (*Stmt, error) {
	s.db.mu.Lock()
	closed := s.closed || s.closing.Load()
	s.db.mu.Unlock()
	if closed {
		return nil, ErrSessionClosed
	}
	p, err := s.db.PrepareContext(ctx, query)
	if err == nil {
		p.session = s
	}
	return p, err
}

func (tx *Tx) Prepare(query string) (*Stmt, error) {
	return tx.PrepareContext(context.Background(), query)
}

func (tx *Tx) PrepareContext(ctx context.Context, query string) (*Stmt, error) {
	if err := tx.session.terminalTransactionError(); err != nil {
		return nil, err
	}
	p, err := tx.session.PrepareContext(ctx, query)
	if errors.Is(err, ErrSessionClosed) {
		if aborted := tx.session.terminalTransactionError(); aborted != nil {
			return nil, aborted
		}
		return nil, ErrTxDone
	}
	if err == nil {
		p.bound = true
	}
	return p, err
}

// PlanStats reports compiled plan builds and reuse across this statement's
// private execution trees. Pool eviction or concurrent first executions may
// require more than one build; no parameter values or results are cached.
func (s *Stmt) PlanStats() sql.PreparedPlanStats { return s.query.PlanStats() }

func (s *Stmt) NumInput() int { return s.query.NumInput() }
func (s *Stmt) Close() error  { s.closed.Store(true); return nil }

func (s *Stmt) Exec(args ...any) (*ExecResult, error) {
	return s.ExecContext(context.Background(), args...)
}

func (s *Stmt) ExecContext(ctx context.Context, args ...any) (*ExecResult, error) {
	return s.ExecContextWithOptions(ctx, ExecOptions{}, args...)
}

func (s *Stmt) ExecWithOptions(options ExecOptions, args ...any) (*ExecResult, error) {
	return s.ExecContextWithOptions(context.Background(), options, args...)
}

func (s *Stmt) ExecContextWithOptions(ctx context.Context, options ExecOptions, args ...any) (result *ExecResult, resultErr error) {
	if s.session != nil {
		return s.session.execPrepared(ctx, s, options, s.bound, args)
	}
	ctx, cancel, lockTimeout, err := s.db.queryContext(ctx, options)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if err := s.db.defaultGate.LockContext(ctx); err != nil {
		return nil, err
	}
	defer s.db.defaultGate.Unlock()
	if err := s.db.execution.RLockContextForReader(ctx, s.db.executionReader); err != nil {
		return nil, err
	}
	defer s.db.execution.RUnlockForReader(s.db.executionReader)
	s.db.mu.Lock()
	if s.db.closed.Load() {
		s.db.mu.Unlock()
		return nil, errors.New("database closed")
	}
	s.db.defaultCancel = cancel
	s.db.mu.Unlock()
	defer func() { s.db.mu.Lock(); s.db.defaultCancel = nil; s.db.mu.Unlock() }()
	ctx, abortCancel := managedCancellation(ctx)
	defer abortCancel(nil)
	defer func() { resultErr = s.db.finishManagedCall(s.db.schema, ctx, resultErr, false) }()
	if err := s.db.beginManagedCall(s.db.schema, abortCancel); err != nil {
		return nil, err
	}
	b, err := s.bind(s.db, nil, false, args)
	if err != nil {
		return nil, err
	}
	defer b.Release()
	return s.db.execStatements(ctx, b.Statements(), s.db.schema, false, lockTimeout)
}

func (tx *Tx) ExecPrepared(stmt *Stmt, args ...any) (*ExecResult, error) {
	return tx.ExecPreparedContext(context.Background(), stmt, args...)
}

func (tx *Tx) ExecPreparedContext(ctx context.Context, stmt *Stmt, args ...any) (*ExecResult, error) {
	return tx.ExecPreparedContextWithOptions(ctx, stmt, ExecOptions{}, args...)
}

func (tx *Tx) ExecPreparedContextWithOptions(ctx context.Context, stmt *Stmt, options ExecOptions, args ...any) (*ExecResult, error) {
	return tx.session.execPrepared(ctx, stmt, options, true, args)
}

func (s *Session) ExecPrepared(stmt *Stmt, args ...any) (*ExecResult, error) {
	return s.ExecPreparedContext(context.Background(), stmt, args...)
}

// ExecPreparedInto executes stmt into caller-owned storage. On entry, the
// caller gives up all previous contents of dst, including its row maps and
// BLOB buffers. Do not share dst or retain aliases to its contents across calls.
// A prepared point SELECT reuses these buffers; other statements replace them.
// On error, dst's contents are unspecified. Execution and cancellation follow
// the same rules as ExecPrepared.
func (s *Session) ExecPreparedInto(dst *ExecResult, stmt *Stmt, args ...any) (*ExecResult, error) {
	return s.ExecPreparedContextInto(context.Background(), dst, stmt, args...)
}

func (s *Session) ExecPreparedContextInto(ctx context.Context, dst *ExecResult, stmt *Stmt, args ...any) (*ExecResult, error) {
	return s.ExecPreparedContextWithOptionsInto(ctx, dst, stmt, ExecOptions{}, args...)
}

// ExecPreparedContextWithOptionsInto adds per-call options to ExecPreparedInto.
func (s *Session) ExecPreparedContextWithOptionsInto(ctx context.Context, dst *ExecResult, stmt *Stmt, options ExecOptions, args ...any) (*ExecResult, error) {
	if dst == nil {
		return nil, errors.New("nil result destination")
	}
	return s.execPreparedInto(ctx, stmt, options, false, args, dst)
}

func (s *Session) ExecPreparedContext(ctx context.Context, stmt *Stmt, args ...any) (*ExecResult, error) {
	return s.ExecPreparedContextWithOptions(ctx, stmt, ExecOptions{}, args...)
}

func (s *Session) ExecPreparedContextWithOptions(ctx context.Context, stmt *Stmt, options ExecOptions, args ...any) (*ExecResult, error) {
	return s.execPrepared(ctx, stmt, options, false, args)
}

func (s *Session) execPrepared(ctx context.Context, stmt *Stmt, options ExecOptions, bound bool, args []any) (*ExecResult, error) {
	return s.execPreparedInto(ctx, stmt, options, bound, args, nil)
}

func (s *Session) execPreparedInto(ctx context.Context, stmt *Stmt, options ExecOptions, bound bool, args []any, dst *ExecResult) (*ExecResult, error) {
	ctx, cancel, lockTimeout, err := s.db.queryContext(ctx, options)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if err := s.gate.LockContext(ctx); err != nil {
		return nil, err
	}
	defer s.gate.Unlock()
	b, err := stmt.bind(s.db, s, bound, args)
	if err != nil {
		return nil, err
	}
	defer b.Release()
	return s.execParsedInto(ctx, b.Statements(), cancel, bound, lockTimeout, dst)
}

func (s *Stmt) bind(database *Database, session *Session, bound bool, args []any) (*sql.BoundQuery, error) {
	if s == nil {
		return nil, errors.New("nil prepared statement")
	}
	if s.generation != s.db.generation.Load() {
		return nil, ErrStmtClosed
	}
	if s.closed.Load() {
		return nil, ErrStmtClosed
	}
	if s.db != database {
		return nil, errors.New("prepared statement belongs to a different database")
	}
	if s.session != nil && (s.session != session || s.bound && !bound) {
		return nil, errors.New("prepared statement belongs to a different session or transaction")
	}
	if len(args) != s.NumInput() {
		return nil, fmt.Errorf("expected %d arguments, got %d", s.NumInput(), len(args))
	}
	var local [8]sql.Value
	var values []sql.Value
	if len(args) <= len(local) {
		values = local[:len(args)]
	} else {
		values = make([]sql.Value, len(args))
	}
	for i, arg := range args {
		v, err := argumentValue(arg)
		if err != nil {
			return nil, fmt.Errorf("argument %d: %w", i+1, err)
		}
		values[i] = v
	}
	return s.query.Bind(values)
}

func argumentValue(arg any) (sql.Value, error) {
	switch v := arg.(type) {
	case int:
		return sql.NewIntValue(int64(v)), nil
	case int64:
		return sql.NewIntValue(v), nil
	case string:
		return sql.NewTextValue(v), nil
	case bool:
		if v {
			return sql.NewIntValue(1), nil
		}
		return sql.NewIntValue(0), nil
	case nil:
		return sql.NewNullValue(), nil
	case sql.Value:
		if v.Null {
			return sql.NewNullValue(), nil
		}
		switch v.Type {
		case sql.ColInteger, sql.ColText:
			return v, nil
		case sql.ColReal:
			if !math.IsNaN(v.Real) && !math.IsInf(v.Real, 0) {
				return v, nil
			}
		case sql.ColBlob:
			v.Blob = append([]byte(nil), v.Blob...)
			return v, nil
		}
		return sql.Value{}, fmt.Errorf("invalid SQL value")
	case []byte:
		if v == nil {
			return sql.NewNullValue(), nil
		}
		return sql.NewBlobValue(append([]byte(nil), v...)), nil
	}
	v := reflect.ValueOf(arg)
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return sql.NewIntValue(v.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if v.Uint() <= math.MaxInt64 {
			return sql.NewIntValue(int64(v.Uint())), nil
		}
	case reflect.Float32, reflect.Float64:
		f := v.Float()
		if !math.IsNaN(f) && !math.IsInf(f, 0) {
			return sql.NewRealValue(f), nil
		}
	case reflect.String:
		return sql.NewTextValue(v.String()), nil
	case reflect.Bool:
		if v.Bool() {
			return sql.NewIntValue(1), nil
		}
		return sql.NewIntValue(0), nil
	}
	return sql.Value{}, fmt.Errorf("unsupported or out-of-range argument %T", arg)
}
