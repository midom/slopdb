package db

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"slopdb/sql"
)

// BulkSource returns the next row's bindings, or io.EOF. It runs synchronously
// while the session is executing and must honor ctx; it must not re-enter that
// session. Returned buffers may be reused on the next call.
type BulkSource func(context.Context) ([]any, error)

// BulkInsert executes a streaming INSERT as one atomic SQL statement in a new
// session using the database's default dialect and namespace path.
func (d *Database) BulkInsert(ctx context.Context, table string, columns []string, next BulkSource) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := d.defaultGate.LockContext(ctx); err != nil {
		return 0, err
	}
	path, dialect := d.schema.SessionSearchPath(), d.schema.Dialect()
	d.defaultGate.Unlock()
	ctx = sql.WithNamespaceSearchPath(ctx, path)
	s, err := d.newSessionContext(ctx)
	if err != nil {
		return 0, err
	}
	defer s.Close()
	if err = s.SetDialect(dialect); err != nil {
		return 0, err
	}
	return s.BulkInsert(ctx, table, columns, next)
}

// BulkInsert executes one INSERT in this session. An existing transaction keeps
// its earlier writes on failure; outside a transaction the import commits once.
// Constraints, triggers, indexes and defaults have ordinary statement semantics,
// including immediate foreign keys to rows later in the same import.
// Input buffers are bounded to a row. Consumed bindings are replayed from a
// private temporary spool on lock retries, without repeating source callbacks.
// Native transaction writes still consume memory proportional to the import.
func (s *Session) BulkInsert(ctx context.Context, table string, columns []string, next BulkSource) (int64, error) {
	return s.bulkInsert(ctx, table, columns, next, false)
}
func (tx *Tx) BulkInsert(ctx context.Context, table string, columns []string, next BulkSource) (int64, error) {
	return tx.session.bulkInsert(ctx, table, columns, next, true)
}

// BulkInsertResult exposes ordinary mutation/commit metadata to wire adapters.
func (s *Session) BulkInsertResult(ctx context.Context, table string, columns []string, next BulkSource) (*ExecResult, error) {
	_, r, e := s.bulkApply(ctx, table, columns, next, false)
	return r, e
}
func (tx *Tx) BulkInsertResult(ctx context.Context, table string, columns []string, next BulkSource) (*ExecResult, error) {
	_, r, e := tx.session.bulkApply(ctx, table, columns, next, true)
	return r, e
}
func (s *Session) bulkInsert(ctx context.Context, table string, columns []string, next BulkSource, bound bool) (int64, error) {
	n, _, e := s.bulkApply(ctx, table, columns, next, bound)
	return n, e
}
func (s *Session) bulkApply(ctx context.Context, table string, columns []string, next BulkSource, bound bool) (int64, *ExecResult, error) {
	if next == nil {
		return 0, nil, fmt.Errorf("nil bulk source")
	}
	if len(columns) == 0 {
		return 0, nil, fmt.Errorf("bulk insert requires explicit columns")
	}
	names := make([]string, len(columns))
	seen := map[string]bool{}
	for i, n := range columns {
		n = strings.ToLower(n)
		if seen[n] {
			return 0, nil, fmt.Errorf("duplicate bulk column %s", n)
		}
		seen[n] = true
		names[i] = n
	}
	ctx, cancel, lockTimeout, err := s.db.queryContext(ctx, ExecOptions{})
	if err != nil {
		return 0, nil, err
	}
	defer cancel()
	if err = s.gate.LockContext(ctx); err != nil {
		return 0, nil, err
	}
	defer s.gate.Unlock()
	defer s.db.beginActivity(s.schema, ctx, "INSERT INTO "+table)()
	ctx = sql.WithStatementTime(ctx, sql.StatementTime(ctx))
	replay := &bulkReplay{next: next, parent: s.db.opt.QueryTempDir, width: len(names)}
	defer replay.close()
	stmt := &sql.InsertStmt{Table: strings.ToLower(table), ColumnNames: names, Source: &sql.RowSource{Reset: replay.reset, Next: replay.read}}
	result, err := s.execParsed(ctx, []sql.Statement{stmt}, cancel, bound, lockTimeout)
	if err != nil {
		var input *bulkInputError
		if errors.As(err, &input) {
			return 0, nil, fmt.Errorf("bulk source row %d: %w", replay.produced+1, input.cause)
		}
		return 0, nil, err
	}
	return replay.position, result, nil
}

// One gob stream retains every exported typed-value field, including transient
// dialect annotations and exact numeric provenance. ReadAt keeps retries from
// disturbing the append cursor; resets never rewind the caller's input stream.
type bulkReplay struct {
	next               BulkSource
	parent             string
	width              int
	file               *os.File
	encoder            *gob.Encoder
	decoder            *gob.Decoder
	produced, position int64
	terminal           error
}

func (r *bulkReplay) close() {
	if r.file != nil {
		name := r.file.Name()
		_ = r.file.Close()
		_ = os.Remove(name)
	}
}
func (r *bulkReplay) reset() error {
	r.position = 0
	if r.file != nil {
		r.decoder = gob.NewDecoder(io.NewSectionReader(r.file, 0, 1<<63-1))
	}
	return nil
}
func (r *bulkReplay) read(ctx context.Context) ([]sql.Value, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.position < r.produced {
		var values []sql.Value
		if err := r.decoder.Decode(&values); err != nil {
			return nil, fmt.Errorf("bulk replay row %d: %w", r.position+1, err)
		}
		r.position++
		return values, nil
	}
	if r.terminal != nil {
		return nil, r.terminal
	}
	values, err := r.next(ctx)
	if err != nil {
		if err != io.EOF {
			err = &bulkInputError{err}
		}
		r.terminal = err
		return nil, err
	}
	if len(values) != r.width {
		return nil, fmt.Errorf("bulk row %d expected %d values, got %d", r.produced+1, r.width, len(values))
	}
	typed := make([]sql.Value, len(values))
	for i, v := range values {
		x, e := argumentValue(v)
		if e != nil {
			return nil, fmt.Errorf("bulk row %d column %d: %w", r.produced+1, i+1, e)
		}
		typed[i] = x
	}
	if r.file == nil {
		r.file, err = os.CreateTemp(r.parent, "slopdb-bulk-*")
		if err != nil {
			return nil, err
		}
		r.encoder = gob.NewEncoder(r.file)
	}
	if err = r.encoder.Encode(typed); err != nil {
		return nil, fmt.Errorf("bulk spool row %d: %w", r.produced+1, err)
	}
	r.produced++
	r.position++
	return typed, nil
}

// External callback errors must not impersonate executor retry sentinels. The
// public API unwraps them after statement rollback and lifecycle cleanup.
type bulkInputError struct{ cause error }

func (e *bulkInputError) Error() string { return e.cause.Error() }
