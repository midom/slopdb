package db

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sync"
	"time"

	"slopdb/sql"
)

// Streaming Rows holds a statement snapshot until exhaustion, cancellation or Close.
// Next and Scan should be used by one goroutine; Close is safe concurrently.
// Close every result, even when stopping early. A session cannot execute another
// statement until its Rows is closed. Database.Query uses an independent session.
type Rows struct {
	mu                   sync.Mutex
	ctx                  context.Context
	columns              []sql.ColumnDef
	cursor               *sql.RowCursor
	buffered             []map[string]sql.Value
	position             int
	delivered            uint64
	current              map[string]sql.Value
	err                  error
	closed, materialized bool
	cancelled            bool
	finish               func()
	stopCancellation     func() bool
}

func (db *Database) Query(query string) (*Rows, error) {
	return db.QueryContext(context.Background(), query)
}
func (db *Database) QueryContext(ctx context.Context, query string) (*Rows, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := db.defaultGate.LockContext(ctx); err != nil {
		return nil, err
	}
	path := db.schema.SessionSearchPath()
	db.defaultGate.Unlock()
	ctx = sql.WithNamespaceSearchPath(ctx, path)
	s, err := db.newSessionContext(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := s.queryContext(ctx, query, false, true)
	if err != nil {
		_ = s.Close()
	}
	return rows, err
}
func (s *Session) Query(query string) (*Rows, error) {
	return s.QueryContext(context.Background(), query)
}
func (s *Session) QueryContext(ctx context.Context, query string) (*Rows, error) {
	return s.queryContext(ctx, query, false, false)
}

// QueryParsedContext executes a SELECT already parsed with this session's SQL
// syntax options. query supplies activity text only. The caller must exclusively
// own statement until Rows.Close and must not modify it during execution.
// Admission, snapshots, catalog resolution and cancellation match QueryContext.
func (s *Session) QueryParsedContext(ctx context.Context, query string, statement sql.Statement) (*Rows, error) {
	if statement == nil {
		return nil, errors.New("nil parsed statement")
	}
	input := parsedRead{statement: statement}
	return s.queryRowsParsed(ctx, query, nil, nil, false, false, &input)
}

// QueryParsedContextInto executes a parsed SELECT with one admission, timeout
// and snapshot. A small catalog-proven primary-key read returns result == dst
// and nil Rows; other queries return nil result and streaming Rows, leaving dst
// unchanged.
// Close returned Rows. query supplies activity text only. The caller exclusively
// owns statement until completion/Rows.Close; dst follows ExecPreparedInto's
// ownership rules. No query answer or SQL text is cached.
func (s *Session) QueryParsedContextInto(ctx context.Context, dst *ExecResult, query string, statement *sql.SelectStmt) (*ExecResult, *Rows, error) {
	if dst == nil {
		return nil, nil, errors.New("nil result destination")
	}
	if statement == nil {
		return nil, nil, errors.New("nil parsed statement")
	}
	input := parsedRead{statement: statement, destination: dst}
	rows, err := s.queryRowsParsed(ctx, query, nil, nil, false, false, &input)
	return input.result, rows, err
}

type parsedRead struct {
	statement   sql.Statement
	destination *ExecResult
	result      *ExecResult
}

func (tx *Tx) Query(query string) (*Rows, error) { return tx.QueryContext(context.Background(), query) }
func (tx *Tx) QueryContext(ctx context.Context, query string) (*Rows, error) {
	return tx.session.queryContext(ctx, query, true, false)
}

func (s *Session) queryContext(ctx context.Context, query string, bound, owned bool) (*Rows, error) {
	return s.queryRows(ctx, query, nil, nil, bound, owned)
}
func (s *Stmt) Query(args ...any) (*Rows, error) {
	return s.QueryContext(context.Background(), args...)
}
func (s *Stmt) QueryContext(ctx context.Context, args ...any) (*Rows, error) {
	if s == nil {
		return nil, errors.New("nil prepared statement")
	}
	if s.session != nil {
		return s.session.queryRows(ctx, "", s, args, s.bound, false)
	}
	session, err := s.db.newSessionContext(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := session.queryRows(ctx, "", s, args, false, true)
	if err != nil {
		_ = session.Close()
	}
	return rows, err
}
func (s *Session) queryRows(ctx context.Context, query string, prepared *Stmt, args []any, bound, owned bool) (*Rows, error) {
	return s.queryRowsParsed(ctx, query, prepared, args, bound, owned, nil)
}

func (s *Session) queryRowsParsed(ctx context.Context, query string, prepared *Stmt, args []any, bound, owned bool, input *parsedRead) (*Rows, error) {
	ctx, cancel, lockTimeout, err := s.db.queryContext(ctx, ExecOptions{})
	if err != nil {
		return nil, err
	}
	if err := s.terminalTransactionError(); err != nil {
		cancel()
		return nil, err
	}
	ctx, abortCancel := managedCancellation(ctx)
	parentCancel := cancel
	cancel = func() { abortCancel(nil); parentCancel() }
	if err = s.gate.LockContext(ctx); err != nil {
		cancel()
		return nil, err
	}
	if err = s.db.execution.RLockContextForReader(ctx, s.executionReader); err != nil {
		s.gate.Unlock()
		cancel()
		return nil, err
	}
	source := query
	if prepared != nil {
		source = prepared.text
	}
	finishActivity := s.db.beginActivity(s.schema, ctx, source)
	var binding *sql.BoundQuery
	var rows *Rows
	var resultStatement sql.Statement = &sql.SelectStmt{}
	managedStarted := false
	successful := false
	held := s.db.locks.snapshot(s.schema)
	release := func() {
		if successful && rows != nil && rows.err == nil {
			s.schema.RecordMySQLFunctionResult(resultStatement, 0, rows.delivered)
		}
		s.schema.EndStatement()
		if binding != nil {
			binding.Release()
			binding = nil
		}
		if !successful || !s.schema.InTransaction() {
			s.db.locks.restore(s.schema, held)
		}
		if managedStarted {
			if rows != nil {
				rows.err = s.db.finishManagedCall(s.schema, ctx, rows.err, true)
				_ = s.finishTransactionAbort(rows.err)
			} else {
				err = s.db.finishManagedCall(s.schema, ctx, err, false)
				_ = s.finishTransactionAbort(err)
			}
		}
		finishActivity.end()
		s.clearActive()
		s.db.execution.RUnlockForReader(s.executionReader)
		s.gate.Unlock()
		cancel()
		if owned {
			_ = s.Close()
		}
	}
	if err = s.registerActive(cancel); err != nil {
		release()
		if bound {
			if aborted := s.terminalTransactionError(); aborted != nil {
				return nil, aborted
			}
			return nil, ErrTxDone
		}
		return nil, err
	}
	managedStarted = true
	if err = s.db.beginManagedCall(s.schema, abortCancel); err != nil {
		release()
		return nil, err
	}
	if bound && !s.schema.InTransaction() {
		release()
		return nil, ErrTxDone
	}
	var statements []sql.Statement
	if input != nil {
		statements = []sql.Statement{input.statement}
	} else if prepared == nil {
		statements, err = sql.ParseWithOptions(query, s.effectiveParseOptions())
	} else {
		binding, err = prepared.bind(s.db, s, bound, args)
		if err == nil {
			statements = binding.Statements()
		}
	}
	if err != nil {
		release()
		return nil, err
	}
	if len(statements) != 1 {
		release()
		return nil, errors.New("Query requires exactly one SELECT statement")
	}
	switch statements[0].(type) {
	case *sql.SelectStmt, *sql.UnionStmt:
	default:
		release()
		return nil, errors.New("Query requires a SELECT statement")
	}
	ctx = withSessionReadBarrier(ctx, s.schema)
	if err = s.db.checkStatementReplication(ctx, statements, s.schema.Dialect()); err != nil {
		release()
		return nil, err
	}
	s.schema.SetContext(s.db.inspectionContext(readStatementContext(ctx, s.schema, statements[0]), statements[0], s.schema.Dialect()))
	deadline := time.Now().Add(lockTimeout)
	for {
		err = s.db.statementCatalogLock(s.schema, statements[0])
		if err == nil {
			break
		}
		var waiting *lockWait
		if !errors.As(err, &waiting) {
			release()
			return nil, err
		}
		if lockTimeout == 0 {
			release()
			return nil, s.db.lockDiagnosticError(s.schema, waiting.request, fmt.Errorf("%w: %w", ErrLockTimeout, context.DeadlineExceeded))
		}
		if err = s.db.waitForLock(ctx, s.schema, waiting.request, deadline); err != nil {
			if errors.Is(err, ErrDeadlock) {
				_ = s.schema.Rollback()
			}
			release()
			return nil, err
		}
	}
	s.schema.StatementReadView(statements[0])
	if err = s.schema.StartStatement(); err != nil {
		release()
		return nil, err
	}
	statements[0], err = s.schema.ResolveNamespaces(statements[0])
	if err != nil {
		release()
		return nil, err
	}
	resultStatement = statements[0]
	if input != nil && input.destination != nil {
		dst := input.destination
		s.selectTarget = sql.ResultSet{Columns: dst.Columns, Rows: dst.Rows}
		defer func() { s.selectTarget = sql.ResultSet{} }()
		result, handled, readErr := s.schema.TrySelectReadInto(statements[0].(*sql.SelectStmt), &s.selectTarget)
		if readErr != nil {
			err = readErr
			release()
			return nil, err
		}
		if handled {
			if err = ctx.Err(); err != nil {
				release()
				return nil, err
			}
			*dst = ExecResult{Columns: result.Columns, Rows: result.Rows, Changes: int64(len(result.Rows))}
			s.schema.RecordMySQLFunctionResult(resultStatement, dst.Changes, uint64(len(dst.Rows)))
			successful = true
			release()
			if err == nil {
				input.result = dst
			}
			return nil, err
		}
	}
	rows = &Rows{ctx: ctx, finish: release}
	if selectStmt, ok := statements[0].(*sql.SelectStmt); ok {
		var handled bool
		rows.cursor, handled, err = s.schema.OpenRowCursor(selectStmt)
		if err != nil {
			release()
			return nil, err
		}
		if handled {
			rows.columns = rows.cursor.Columns()
			// Typed catalog projections already carry complete declaration
			// metadata from this execution view. Describe only shapes that still
			// require expression inference/validation; never describe after the
			// view is released or after concurrent catalog changes.
			if !rows.cursor.ColumnsComplete() {
				columns, e := s.schema.Describe(statements[0])
				if e != nil {
					release()
					return nil, e
				}
				if len(columns) == len(rows.columns) {
					for i := range columns {
						columns[i].Name = rows.columns[i].Name
						columns[i].DisplayName = rows.columns[i].DisplayName
					}
					rows.columns = columns
				}
			}
		}
	}
	if rows.cursor == nil {
		result, execErr := s.db.execStatements(context.WithValue(ctx, reuseReadSnapshotKey{}, true), statements, s.schema, bound, lockTimeout)
		if execErr != nil {
			release()
			return nil, execErr
		}
		rows.columns = append([]sql.ColumnDef(nil), result.Columns...)
		rows.buffered = result.Rows
		rows.materialized = true
	}
	successful = true
	// Register only after initialization. Cancellation releases idle cursors too,
	// so Close on the database never depends on the consumer calling Next again.
	rows.mu.Lock()
	rows.stopCancellation = context.AfterFunc(ctx, func() {
		rows.mu.Lock()
		if !rows.closed {
			rows.err = context.Cause(ctx)
			rows.closeLocked()
		}
		rows.mu.Unlock()
	})
	rows.mu.Unlock()
	return rows, nil
}

func (r *Rows) Columns() []sql.ColumnDef {
	r.mu.Lock()
	defer r.mu.Unlock()
	columns := append([]sql.ColumnDef(nil), r.columns...)
	for i := range columns {
		columns[i].Default = nil
		columns[i].Check = nil
		columns[i].RefColumns = append([]string(nil), columns[i].RefColumns...)
		columns[i].EnumValues = slices.Clone(columns[i].EnumValues)
		columns[i].ConstraintNames = maps.Clone(columns[i].ConstraintNames)
	}
	return columns
}

// Materialized reports whether this query required the blocking executor.
func (r *Rows) Materialized() bool { return r.materialized }
func (r *Rows) Next() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	if err := r.ctx.Err(); err != nil {
		r.err = context.Cause(r.ctx)
		r.closeLocked()
		return false
	}
	var row map[string]sql.Value
	var ok bool
	if r.cursor != nil {
		row, ok, r.err = r.cursor.Next()
	} else if r.position < len(r.buffered) {
		row = r.buffered[r.position]
		r.buffered[r.position] = nil
		r.position++
		ok = true
	}
	if !ok {
		r.closeLocked()
		return false
	}
	r.current = row
	r.delivered++
	return true
}
func (r *Rows) Err() error   { r.mu.Lock(); defer r.mu.Unlock(); return r.err }
func (r *Rows) Close() error { r.mu.Lock(); defer r.mu.Unlock(); r.closeLocked(); return nil }
func (r *Rows) closeLocked() {
	if r.closed {
		return
	}
	r.closed = true
	r.cancelled = r.ctx.Err() != nil
	if r.stopCancellation != nil {
		r.stopCancellation()
	}
	if r.cursor != nil {
		r.cursor.Close()
	}
	r.buffered = nil
	r.current = nil
	if r.finish != nil {
		r.finish()
		r.finish = nil
	}
}

// Values returns caller-owned values in projection order. BLOBs are copied.
func (r *Rows) Values() ([]sql.Value, error) {
	return r.ValuesInto(nil)
}

// ValuesInto returns caller-owned values in projection order, reusing dst and
// each destination BLOB's capacity when possible. Like Values, results never
// alias the cursor's rows or storage pages, and empty BLOBs remain non-NULL.
// The caller gives up all prior contents of dst, including aliases to its
// values and BLOB buffers. Keep dst and its individual buffers exclusively
// owned by this call; use Values for independent values retained across calls.
func (r *Rows) ValuesInto(dst []sql.Value) ([]sql.Value, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.current == nil || r.closed {
		return nil, errors.New("Rows.Values requires a successful Next")
	}
	if len(dst) > len(r.columns) {
		clear(dst[len(r.columns):])
	}
	if cap(dst) < len(r.columns) {
		dst = make([]sql.Value, len(r.columns))
	} else {
		dst = dst[:len(r.columns)]
	}
	for i, col := range r.columns {
		value := r.current[col.Name]
		if value.Type == sql.ColBlob && !value.Null {
			buffer := dst[i].Blob
			if buffer == nil {
				buffer = []byte{}
			}
			value.Blob = append(buffer[:0], value.Blob...)
		}
		dst[i] = value
	}
	return dst, nil
}
func ownedValue(v sql.Value) sql.Value {
	if v.Type == sql.ColBlob && !v.Null {
		v.Blob = append([]byte{}, v.Blob...)
	}
	return v
}

// Scan accepts *sql.Value, *int64, *int, *float64, *string, *[]byte, *bool,
// *any and database/sql.Scanner destinations. NULL requires *sql.Value, *any,
// *[]byte or a Scanner (such as sql.NullInt64). It never narrows an integer or
// silently stringifies a value with a different SQL type.
func (r *Rows) Scan(dest ...any) error {
	values, err := r.Values()
	if err != nil {
		return err
	}
	if len(dest) != len(values) {
		return fmt.Errorf("Scan expected %d destinations, got %d", len(values), len(dest))
	}
	for i, v := range values {
		if err = scanValue(dest[i], v); err != nil {
			return fmt.Errorf("Scan column %d (%s): %w", i+1, r.columns[i].ResultName(), err)
		}
	}
	return nil
}
func scanValue(dest any, v sql.Value) error {
	if dest == nil || reflect.ValueOf(dest).Kind() == reflect.Ptr && reflect.ValueOf(dest).IsNil() {
		return errors.New("nil Scan destination")
	}
	var primitive any
	if !v.Null {
		switch v.Type {
		case sql.ColInteger:
			primitive = v.Int
		case sql.ColUnsigned:
			primitive = v.Uint
		case sql.ColDecimal, sql.ColJSON:
			primitive = v.Text
		case sql.ColReal:
			primitive = v.Real
		case sql.ColText:
			primitive = v.Text
		case sql.ColBlob:
			primitive = append([]byte{}, v.Blob...)
		}
	}
	if scanner, ok := dest.(stdsql.Scanner); ok {
		return scanner.Scan(primitive)
	}
	switch d := dest.(type) {
	case *sql.Value:
		if d == nil {
			break
		}
		*d = ownedValue(v)
		return nil
	case *any:
		if d == nil {
			break
		}
		*d = primitive
		return nil
	case *[]byte:
		if d == nil {
			break
		}
		if v.Null {
			*d = nil
			return nil
		}
		if v.Type == sql.ColBlob {
			*d = append([]byte{}, v.Blob...)
			return nil
		}
	case *uint64:
		if d != nil && !v.Null {
			if v.Type == sql.ColUnsigned {
				*d = v.Uint
				return nil
			}
			if v.Type == sql.ColInteger && v.Int >= 0 {
				*d = uint64(v.Int)
				return nil
			}
		}
	case *int64:
		if d != nil && !v.Null && v.Type == sql.ColInteger {
			*d = v.Int
			return nil
		}
	case *int:
		if d != nil && !v.Null && v.Type == sql.ColInteger && int64(int(v.Int)) == v.Int {
			*d = int(v.Int)
			return nil
		}
	case *float64:
		if d != nil && !v.Null {
			if v.Type == sql.ColReal {
				*d = v.Real
				return nil
			}
			if v.Type == sql.ColInteger && v.Int >= -(1<<53) && v.Int <= (1<<53) {
				*d = float64(v.Int)
				return nil
			}
		}
	case *string:
		if d != nil && !v.Null && (v.Type == sql.ColText || v.Type == sql.ColDecimal || v.Type == sql.ColJSON) {
			*d = v.Text
			return nil
		}
	case *bool:
		if d != nil && !v.Null && v.Type == sql.ColInteger && (v.Int == 0 || v.Int == 1) {
			*d = v.Int == 1
			return nil
		}
	}
	return fmt.Errorf("cannot scan SQL %s (NULL=%v) into %T", v.Type, v.Null, dest)
}

// OnCancel connects a streaming consumer's blocking I/O to session/query
// cancellation. Normal exhaustion, explicit Close and SQL evaluation errors do
// not invoke fn. The returned function unregisters and joins an active callback;
// callers should defer it before releasing external I/O resources.
func (r *Rows) OnCancel(fn func()) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(r.ctx, func() {
		defer close(done)
		r.mu.Lock()
		cancelled := r.cancelled || !r.closed
		r.mu.Unlock()
		if cancelled && fn != nil {
			fn()
		}
	})
	var once sync.Once
	return func() {
		once.Do(func() {
			if !stop() {
				<-done
			}
		})
	}
}
