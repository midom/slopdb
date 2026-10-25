package driver

import (
	"context"
	stdsql "database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"

	"slopdb/db"
	nativesql "slopdb/sql"
)

var errRowsActive = errors.New("slopdb: close active rows before using the connection")
var errStatementClosed = errors.New("slopdb: prepared statement is closed")

type conn struct {
	mu                 sync.Mutex
	database           *db.Database
	session            *db.Session
	generation         uint64
	closed, bad, dirty bool
	active             *rows
	transaction        *transaction
	statements         map[*statement]struct{}
	release            func() error
}

func (c *conn) ready(ctx context.Context) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	if c.closed || c.bad {
		return sqldriver.ErrBadConn
	}
	if c.active != nil {
		return errRowsActive
	}
	return nil
}
func (c *conn) observed(e error) error {
	if errors.Is(e, db.ErrSessionClosed) {
		c.bad = true
	}
	return e
}
func (c *conn) Prepare(query string) (sqldriver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}
func (c *conn) PrepareContext(ctx context.Context, query string) (sqldriver.Stmt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.ready(ctx); e != nil {
		return nil, e
	}
	return c.prepare(ctx, query)
}
func (c *conn) prepare(ctx context.Context, query string) (*statement, error) {
	native, e := c.session.PrepareContext(ctx, query)
	if e != nil {
		return nil, c.observed(e)
	}
	selectOnly, e := selectQuery(query, c.session)
	if e != nil {
		_ = native.Close()
		return nil, e
	}
	stmt := &statement{connection: c, query: query, native: native, generation: c.generation, inputs: native.NumInput(), selectOnly: selectOnly}
	c.statements[stmt] = struct{}{}
	return stmt, nil
}
func (c *conn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	active := c.active
	c.active = nil
	session := c.session
	release := c.release
	c.release = nil
	for statement := range c.statements {
		statement.closed = true
		_ = statement.native.Close()
	}
	c.statements = nil
	c.mu.Unlock()
	// Rows.Close releases the native session gate and then updates this connection.
	// Never retain the connection mutex while invoking it.
	if active != nil {
		_ = active.Close()
	}
	e := session.Close()
	if release != nil {
		e = errors.Join(e, release())
	}
	return e
}
func (c *conn) Ping(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.ready(ctx); e != nil {
		return e
	}
	_, e := c.session.ExecContext(ctx, "SELECT 1")
	return c.observed(e)
}
func (c *conn) IsValid() bool { c.mu.Lock(); defer c.mu.Unlock(); return !c.closed && !c.bad }
func (c *conn) ResetSession(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.ready(ctx); e != nil {
		return e
	}
	if !c.dirty {
		return nil
	}
	if e := c.session.Close(); e != nil {
		c.bad = true
		return e
	}
	session, e := c.database.NewSession()
	if e != nil {
		c.bad = true
		return e
	}
	c.session = session
	c.generation++
	c.dirty = false
	c.transaction = nil
	return nil
}
func (c *conn) Begin() (sqldriver.Tx, error) {
	return c.BeginTx(context.Background(), sqldriver.TxOptions{})
}
func (c *conn) BeginTx(ctx context.Context, options sqldriver.TxOptions) (sqldriver.Tx, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.ready(ctx); e != nil {
		return nil, e
	}
	if options.ReadOnly {
		return nil, errors.New("slopdb: read-only transactions are unsupported")
	}
	level := ""
	switch stdsql.IsolationLevel(options.Isolation) {
	case stdsql.LevelDefault:
	case stdsql.LevelReadCommitted:
		level = "READ COMMITTED"
	case stdsql.LevelRepeatableRead:
		level = "REPEATABLE READ"
	default:
		return nil, fmt.Errorf("slopdb: unsupported isolation level %d", options.Isolation)
	}
	if c.transaction != nil || c.session.InTransaction() {
		return nil, errors.New("slopdb: transaction already active")
	}
	command := "BEGIN"
	if level != "" {
		command += " ISOLATION LEVEL " + level
	}
	c.dirty = true
	if _, e := c.session.ExecContext(ctx, command); e != nil {
		return nil, c.observed(e)
	}
	tx := &transaction{connection: c}
	c.transaction = tx
	return tx, nil
}
func (c *conn) ExecContext(ctx context.Context, query string, arguments []sqldriver.NamedValue) (sqldriver.Result, error) {
	args, e := argumentValues(arguments)
	if e != nil {
		return nil, e
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e = c.ready(ctx); e != nil {
		return nil, e
	}
	c.dirty = true
	var result *db.ExecResult
	if len(args) == 0 {
		result, e = c.session.ExecContext(ctx, query)
	} else {
		var statement *db.Stmt
		statement, e = c.session.PrepareContext(ctx, query)
		if e == nil {
			defer statement.Close()
			result, e = c.session.ExecPreparedContext(ctx, statement, args...)
		}
	}
	if e != nil {
		return nil, c.observed(e)
	}
	return commandResult{result.LastID, result.LastIDUnsigned, result.Changes}, nil
}
func (c *conn) QueryContext(ctx context.Context, query string, arguments []sqldriver.NamedValue) (sqldriver.Rows, error) {
	args, e := argumentValues(arguments)
	if e != nil {
		return nil, e
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e = c.ready(ctx); e != nil {
		return nil, e
	}
	if len(args) == 0 {
		return c.queryText(ctx, query)
	}
	statement, e := c.prepare(ctx, query)
	if e != nil {
		return nil, e
	}
	result, e := statement.queryRows(ctx, args)
	if e != nil {
		statement.close()
		return nil, e
	}
	result.temporary = statement
	return result, nil
}
func (c *conn) CheckNamedValue(value *sqldriver.NamedValue) error { return checkNamedValue(value) }

type commandResult struct {
	last     int64
	unsigned uint64
	affected int64
}

func (r commandResult) LastInsertId() (int64, error) {
	if r.unsigned > math.MaxInt64 {
		return 0, errors.New("slopdb: generated unsigned ID exceeds int64")
	}
	if r.unsigned != 0 {
		return int64(r.unsigned), nil
	}
	return r.last, nil
}
func (r commandResult) RowsAffected() (int64, error) { return r.affected, nil }

type transaction struct {
	connection *conn
	done       bool
}

func (t *transaction) Commit() error   { return t.finish("COMMIT") }
func (t *transaction) Rollback() error { return t.finish("ROLLBACK") }
func (t *transaction) finish(command string) error {
	c := t.connection
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.done || c.transaction != t {
		return stdsql.ErrTxDone
	}
	if e := c.ready(context.Background()); e != nil {
		return e
	}
	t.done = true
	c.transaction = nil
	c.dirty = true
	_, e := c.session.ExecContext(context.Background(), command)
	// database/sql considers the transaction finished even when COMMIT fails.
	// Discard its private writes before a pinned sql.Conn can be used again.
	if e != nil && command == "COMMIT" {
		_, rollbackErr := c.session.ExecContext(context.Background(), "ROLLBACK")
		if rollbackErr != nil {
			c.bad = true
			e = errors.Join(e, rollbackErr)
		}
	}
	if e != nil && command == "ROLLBACK" {
		c.bad = true
	}
	return c.observed(e)
}

type statement struct {
	connection         *conn
	query              string
	native             *db.Stmt
	generation         uint64
	inputs             int
	selectOnly, closed bool
}

func (s *statement) NumInput() int { return s.inputs }
func (s *statement) Close() error {
	s.connection.mu.Lock()
	defer s.connection.mu.Unlock()
	return s.close()
}
func (s *statement) close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	delete(s.connection.statements, s)
	return s.native.Close()
}
func (s *statement) ensure(ctx context.Context) error {
	if s.closed {
		return errStatementClosed
	}
	if s.generation == s.connection.generation {
		return nil
	}
	native, e := s.connection.session.PrepareContext(ctx, s.query)
	if e != nil {
		return e
	}
	_ = s.native.Close()
	s.native = native
	s.generation = s.connection.generation
	return nil
}
func (s *statement) Exec(values []sqldriver.Value) (sqldriver.Result, error) {
	return s.ExecContext(context.Background(), namedValues(values))
}
func (s *statement) ExecContext(ctx context.Context, values []sqldriver.NamedValue) (sqldriver.Result, error) {
	args, e := argumentValues(values)
	if e != nil {
		return nil, e
	}
	c := s.connection
	c.mu.Lock()
	defer c.mu.Unlock()
	if e = c.ready(ctx); e != nil {
		return nil, e
	}
	if e = s.ensure(ctx); e != nil {
		return nil, c.observed(e)
	}
	c.dirty = true
	result, e := c.session.ExecPreparedContext(ctx, s.native, args...)
	if e != nil {
		return nil, c.observed(e)
	}
	return commandResult{result.LastID, result.LastIDUnsigned, result.Changes}, nil
}
func (s *statement) Query(values []sqldriver.Value) (sqldriver.Rows, error) {
	return s.QueryContext(context.Background(), namedValues(values))
}
func (s *statement) QueryContext(ctx context.Context, values []sqldriver.NamedValue) (sqldriver.Rows, error) {
	args, e := argumentValues(values)
	if e != nil {
		return nil, e
	}
	c := s.connection
	c.mu.Lock()
	defer c.mu.Unlock()
	if e = c.ready(ctx); e != nil {
		return nil, e
	}
	return s.queryRows(ctx, args)
}
func (s *statement) queryRows(ctx context.Context, args []any) (*rows, error) {
	c := s.connection
	if e := s.ensure(ctx); e != nil {
		return nil, c.observed(e)
	}
	if s.selectOnly {
		native, e := s.native.QueryContext(ctx, args...)
		return c.streamingRows(ctx, native, e)
	}
	c.dirty = true
	native, e := c.session.ExecPreparedContext(ctx, s.native, args...)
	return c.bufferedRows(ctx, native, e)
}
func (s *statement) CheckNamedValue(value *sqldriver.NamedValue) error { return checkNamedValue(value) }

var (
	_ sqldriver.Conn               = (*conn)(nil)
	_ sqldriver.ConnPrepareContext = (*conn)(nil)
	_ sqldriver.ExecerContext      = (*conn)(nil)
	_ sqldriver.QueryerContext     = (*conn)(nil)
	_ sqldriver.ConnBeginTx        = (*conn)(nil)
	_ sqldriver.Pinger             = (*conn)(nil)
	_ sqldriver.SessionResetter    = (*conn)(nil)
	_ sqldriver.Validator          = (*conn)(nil)
	_ sqldriver.NamedValueChecker  = (*conn)(nil)
	_ sqldriver.StmtExecContext    = (*statement)(nil)
	_ sqldriver.StmtQueryContext   = (*statement)(nil)
)

// Native Prepare already validates a single statement. SELECT necessarily
// produces a SelectStmt/UnionStmt, so ordinary direct queries need no second
// parse or metadata AST clone merely to choose the Rows API.
func selectQuery(query string, session *db.Session) (bool, error) {
	first := nativesql.NewTokenizer(query).Next()
	if first.Type == nativesql.TokKeyword && strings.EqualFold(first.Value, "SELECT") {
		return true, nil
	}
	if first.Type != nativesql.TokLParen && !(first.Type == nativesql.TokKeyword && strings.EqualFold(first.Value, "WITH")) {
		return false, nil
	}
	parsed, e := nativesql.PrepareWithOptions(query, session.ParseOptions())
	if e != nil {
		return false, e
	}
	switch parsed.MetadataStatement().(type) {
	case *nativesql.SelectStmt, *nativesql.UnionStmt:
		return true, nil
	}
	return false, nil
}

// A plain query must not allocate a PreparedQuery's plan-counter shards or
// sync.Pool. The native Query API validates SELECTs as one statement itself.
func (c *conn) queryText(ctx context.Context, query string) (*rows, error) {
	first := nativesql.NewTokenizer(query).Next()
	selectOnly := first.Type == nativesql.TokKeyword && strings.EqualFold(first.Value, "SELECT")
	if !selectOnly {
		// Exec accepts multiple statements; the Rows API deliberately does not.
		// This also distinguishes WITH ... SELECT from WITH ... INSERT RETURNING.
		statements, e := nativesql.ParseWithOptions(query, c.session.ParseOptions())
		if e != nil {
			return nil, e
		}
		if len(statements) != 1 {
			return nil, errors.New("slopdb: Query requires exactly one statement")
		}
		switch statements[0].(type) {
		case *nativesql.SelectStmt, *nativesql.UnionStmt:
			selectOnly = true
		}
	}
	if selectOnly {
		native, e := c.session.QueryContext(ctx, query)
		return c.streamingRows(ctx, native, e)
	}
	c.dirty = true
	native, e := c.session.ExecContext(ctx, query)
	return c.bufferedRows(ctx, native, e)
}
func (c *conn) streamingRows(ctx context.Context, native *db.Rows, err error) (*rows, error) {
	if err != nil {
		c.dirty = true
		return nil, c.observed(err)
	}
	// Streaming cursors admit physical relations and safe expressions only;
	// views and session-mutating functions take the conservative dirty fallback.
	c.dirty = c.dirty || native.Materialized()
	result := newNativeRows(ctx, c, native)
	c.active = result
	return result, nil
}
func (c *conn) bufferedRows(ctx context.Context, native *db.ExecResult, err error) (*rows, error) {
	if err != nil {
		return nil, c.observed(err)
	}
	result := newBufferedRows(ctx, c, native)
	c.active = result
	return result, nil
}
