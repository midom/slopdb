package db

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"slopdb/sql"
)

// ErrConflict identifies a transaction validation failure requiring a retry.
var ErrConflict = sql.ErrConflict

var (
	ErrTxDone        = errors.New("transaction already committed or rolled back")
	ErrSessionClosed = errors.New("session closed")
)

// Session is an independent SQL connection to an open Database. BEGIN, COMMIT,
// ROLLBACK and savepoints affect only this session. All transactions use READ
// COMMITTED: each statement sees current committed data and its own writes.
// Calls are safe from multiple goroutines, but callers sharing a session also
// share its transaction. Use separate sessions for independent transactions.
// Sessions execute concurrently using MVCC statement snapshots. Writers hold
// row and unique-value locks until commit or rollback; commits publish one atomic
// storage batch. Catalog changes take an exclusive logical catalog lock.
type Session struct {
	db                *Database
	schema            *sql.Schema
	closed            bool
	closing           atomic.Bool
	gate              contextMutex
	activeMu          sync.Mutex
	activeCancel      context.CancelFunc
	transactionHandle bool
	abortError        atomic.Pointer[TransactionAbortedError]
	executionReader   uint64
	pointTarget       sql.PointUpdate // private prepared execution; session gate protects it
	selectTarget      sql.ResultSet   // private destination view; session gate protects it
}

func (db *Database) NewSession() (*Session, error) {
	return db.newSessionContext(context.Background())
}

func (db *Database) newSessionContext(ctx context.Context) (*Session, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if db.replication.Load() != nil {
		if err := db.execution.RLockContext(ctx); err != nil {
			return nil, err
		}
		defer db.execution.RUnlock()
		if err := db.checkReplicationSession(ctx); err != nil {
			return nil, err
		}
	}
	if db.closed.Load() {
		return nil, errors.New("database closed")
	}
	// Catalog copying can wait for metadata publication. Keep that work outside
	// the lifecycle mutex so active queries can register cancellation promptly.
	session := &Session{db: db, schema: db.schema.NewSession()}
	db.installLocks(session.schema)
	session.executionReader = db.locks.state(session.schema).id
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed.Load() {
		return nil, errors.New("database closed")
	}
	db.sessions[session] = struct{}{}
	return session, nil
}

// Cancellation registration is private to this session. Publishing first and
// then checking the closing flags ensures concurrent Close either sees this
// callback or the registering operation cancels itself before execution.
func (s *Session) registerActive(cancel context.CancelFunc) error {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	s.activeCancel = cancel
	if s.closing.Load() || s.db.closed.Load() {
		cancel()
		s.activeCancel = nil
		return ErrSessionClosed
	}
	return nil
}
func (s *Session) clearActive() {
	s.activeMu.Lock()
	s.activeCancel = nil
	s.activeMu.Unlock()
}
func (s *Session) cancelActive() {
	s.activeMu.Lock()
	if s.activeCancel != nil {
		s.activeCancel()
	}
	s.activeMu.Unlock()
}

func (s *Session) Exec(query string) (*ExecResult, error) {
	return s.ExecContext(context.Background(), query)
}

func (s *Session) ExecContext(ctx context.Context, query string) (*ExecResult, error) {
	return s.ExecContextWithOptions(ctx, query, ExecOptions{})
}

func (s *Session) ExecWithOptions(query string, options ExecOptions) (*ExecResult, error) {
	return s.ExecContextWithOptions(context.Background(), query, options)
}

func (s *Session) ExecContextWithOptions(ctx context.Context, query string, options ExecOptions) (*ExecResult, error) {
	ctx, cancel, lockTimeout, err := s.db.queryContext(ctx, options)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if err := s.gate.LockContext(ctx); err != nil {
		return nil, err
	}
	defer s.gate.Unlock()
	statements, parseErr := sql.ParseWithOptions(query, s.db.ParseOptions())
	if parseErr != nil {
		return nil, fmt.Errorf("parse error: %w", parseErr)
	}
	return s.execParsed(ctx, statements, cancel, false, lockTimeout)
}

func (s *Session) execParsed(ctx context.Context, statements []sql.Statement, cancel context.CancelFunc, bound bool, lockTimeout time.Duration) (result *ExecResult, resultErr error) {
	return s.execParsedInto(ctx, statements, cancel, bound, lockTimeout, nil)
}

func (s *Session) execParsedInto(ctx context.Context, statements []sql.Statement, cancel context.CancelFunc, bound bool, lockTimeout time.Duration, dst *ExecResult) (result *ExecResult, resultErr error) {
	if dst != nil {
		defer func() { s.selectTarget = sql.ResultSet{} }()
	}
	if err := s.terminalTransactionError(); err != nil {
		return nil, err
	}
	ctx, abortCancel := managedCancellation(ctx)
	defer abortCancel(nil)
	if err := s.db.execution.RLockContextForReader(ctx, s.executionReader); err != nil {
		return nil, err
	}
	defer s.db.execution.RUnlockForReader(s.executionReader)
	if err := s.registerActive(cancel); err != nil {
		if bound {
			if aborted := s.terminalTransactionError(); aborted != nil {
				return nil, aborted
			}
			return nil, ErrTxDone
		}
		return nil, err
	}
	defer s.clearActive()
	defer func() {
		resultErr = s.db.finishManagedCall(s.schema, ctx, resultErr, false)
		resultErr = s.finishTransactionAbort(resultErr)
		if bound && !s.schema.InTransaction() {
			s.db.mu.Lock()
			s.closeLocked()
			s.db.mu.Unlock()
		}
	}()
	if err := s.db.beginManagedCall(s.schema, abortCancel); err != nil {
		return nil, err
	}
	if !hasReadBarrierSetting(statements) {
		if err := s.db.checkStatementReplication(withSessionReadBarrier(ctx, s.schema), statements); err != nil {
			return nil, err
		}
	}
	if bound && !s.schema.InTransaction() {
		return nil, ErrTxDone
	}
	if !bound {
		if result, handled, err := s.tryPointUpdate(ctx, statements, dst); handled {

			return result, err
		}
	}
	result, err := s.db.execAdmittedStatementsInto(ctx, statements, s.schema, bound, lockTimeout, dst, &s.selectTarget)
	return result, err
}

// Close discards this session's uncommitted writes. It does not close the database.
func (s *Session) Close() error {
	s.closing.Store(true)
	s.cancelActive()
	s.gate.Lock()
	defer s.gate.Unlock()
	_ = s.db.execution.RLockContextForReader(context.Background(), s.executionReader)
	defer s.db.execution.RUnlockForReader(s.executionReader)
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	s.closeLocked()
	return nil
}

func (s *Session) closeLocked() {
	if !s.closed {
		s.closing.Store(true)
		s.cancelActive()
		_ = s.schema.Rollback()
		s.schema.DiscardTemporaryTables()
		s.db.locks.release(s.schema)
		s.db.locks.forget(s.schema)
		s.closed = true
		delete(s.db.sessions, s)
	}
}

// Tx owns a private session for one transaction. A deadlock victim is rolled
// back; retry the entire transaction using Database.Begin. A lock timeout or
// cancelled statement preserves earlier writes and locks until explicit rollback.
type Tx struct {
	session *Session
}

func (db *Database) Begin() (*Tx, error) {
	return db.BeginContext(context.Background())
}

// BeginContext checks ctx while starting the transaction. Subsequent operations
// use the contexts passed to ExecContext or CommitContext.
func (db *Database) BeginContext(ctx context.Context) (*Tx, error) {
	ctx, cancel, lockTimeout, err := db.queryContext(ctx, ExecOptions{})
	if err != nil {
		return nil, err
	}
	defer cancel()
	session, err := db.NewSession()
	if err != nil {
		return nil, err
	}
	session.transactionHandle = true
	session.gate.Lock()
	_, err = session.execParsed(ctx, []sql.Statement{&sql.BeginStmt{}}, cancel, false, lockTimeout)
	session.gate.Unlock()
	if err != nil {
		session.Close()
		return nil, err
	}
	return &Tx{session: session}, nil
}

func (tx *Tx) Exec(query string) (*ExecResult, error) {
	return tx.ExecContext(context.Background(), query)
}

func (tx *Tx) ExecContext(ctx context.Context, query string) (*ExecResult, error) {
	return tx.ExecContextWithOptions(ctx, query, ExecOptions{})
}

func (tx *Tx) ExecWithOptions(query string, options ExecOptions) (*ExecResult, error) {
	return tx.ExecContextWithOptions(context.Background(), query, options)
}

func (tx *Tx) ExecContextWithOptions(ctx context.Context, query string, options ExecOptions) (*ExecResult, error) {
	s := tx.session
	ctx, cancel, lockTimeout, err := s.db.queryContext(ctx, options)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if err := s.gate.LockContext(ctx); err != nil {
		return nil, err
	}
	defer s.gate.Unlock()
	statements, err := sql.ParseWithOptions(query, s.db.ParseOptions())
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}
	return s.execParsed(ctx, statements, cancel, true, lockTimeout)
}

func (tx *Tx) Commit() error {
	return tx.CommitContext(context.Background())
}

func (tx *Tx) CommitContext(ctx context.Context) error {
	_, err := tx.ExecContext(ctx, "COMMIT")
	return err
}

func (tx *Tx) Rollback() error {
	s := tx.session
	if err := s.terminalTransactionError(); err != nil {
		return err
	}
	s.closing.Store(true)
	s.cancelActive()
	s.gate.Lock()
	defer s.gate.Unlock()
	_ = s.db.execution.RLockContextForReader(context.Background(), s.executionReader)
	defer s.db.execution.RUnlockForReader(s.executionReader)
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	if s.closed || !s.schema.InTransaction() {
		if err := s.terminalTransactionError(); err != nil {
			return err
		}
		return ErrTxDone
	}
	err := s.schema.Rollback()
	s.closeLocked()
	return err
}

// CommitDurable waits for the committed prefix to be stable on a voting majority
// when replication is asynchronous. An error may follow an asynchronous commit;
// it does not imply that the transaction rolled back.
func (tx *Tx) CommitDurable() error { return tx.CommitDurableContext(context.Background()) }
func (tx *Tx) CommitDurableContext(ctx context.Context) error {
	_, err := tx.ExecContext(ctx, "COMMIT DURABLE")
	return err
}
