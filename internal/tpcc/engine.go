package tpcc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"slopdb/db"
	"slopdb/storage"
)

type Statement interface{ Close() error }
type Transaction interface {
	ExecContext(context.Context, string) (*db.ExecResult, error)
	ExecPreparedContext(context.Context, Statement, ...any) (*db.ExecResult, error)
	ExecPrepared(Statement, ...any) (*db.ExecResult, error)
	Commit() error
	Rollback() error
}
type Database interface {
	Exec(string) (*db.ExecResult, error)
	Prepare(string) (Statement, error)
	SetupWorkers(int, []string, bool) error
	BeginContext(context.Context, int, bool) (Transaction, error)
	Close() error
	Info() map[string]any
}

type slopDatabase struct {
	*db.Database
	config          Config
	workers         []*slopWorker
	begin, rollback *db.Stmt
	commit          Statement
}

// Each worker owns its session and scratch; transaction handles retain separate
// completion state so a stale handle cannot act on a later transaction.
type slopWorker struct {
	active  atomic.Bool
	dirty   bool
	session *db.Session
	results map[Statement]*db.ExecResult
}
type slopTransaction struct {
	mu       sync.Mutex
	*db.Tx   // fallback for callers which have not configured worker sessions
	worker   *slopWorker
	database *slopDatabase
	done     bool
}

func OpenSlopDB(c Config) (Database, error) {
	if c.Engine != "slopdb" {
		return nil, fmt.Errorf("engine %q requires cmd/tpcc/compare", c.Engine)
	}
	o := storage.DefaultOptions()
	o.PageSize = c.PageSize
	o.DisableWALSync = !c.WALFsync
	o.LockTimeout = c.LockTimeout
	o.DeadlockDetection = true
	d, err := db.Open(c.Dir, o)
	if err != nil {
		return nil, err
	}
	return &slopDatabase{Database: d, config: c}, nil
}
func (d *slopDatabase) Prepare(q string) (Statement, error) {
	s, e := d.Database.Prepare(q)
	if e == nil && q == "COMMIT" {
		d.commit = s
	}
	return s, e
}
func (d *slopDatabase) SetupWorkers(count int, _ []string, _ bool) error {
	if len(d.workers) != 0 {
		return errors.New("TPC-C workers already configured")
	}
	var err error
	d.begin, err = d.Database.Prepare("BEGIN")
	if err != nil {
		return err
	}
	d.rollback, err = d.Database.Prepare("ROLLBACK")
	if err != nil {
		return err
	}
	for i := 0; i < count; i++ {
		session, e := d.Database.NewSession()
		if e != nil {
			return e
		}
		d.workers = append(d.workers, &slopWorker{session: session, results: make(map[Statement]*db.ExecResult)})
	}
	return nil
}
func (d *slopDatabase) Close() error {
	var err error
	for _, w := range d.workers {
		if w.session != nil {
			err = errors.Join(err, w.session.Close())
		}
	}
	if d.begin != nil {
		err = errors.Join(err, d.begin.Close())
	}
	if d.rollback != nil {
		err = errors.Join(err, d.rollback.Close())
	}
	return errors.Join(err, d.Database.Close())
}
func (d *slopDatabase) BeginContext(ctx context.Context, worker int, _ bool) (Transaction, error) {
	if len(d.workers) == 0 {
		tx, err := d.Database.BeginContext(ctx)
		if err != nil {
			return nil, err
		}
		return &slopTransaction{Tx: tx}, nil
	}
	if worker < 0 || worker >= len(d.workers) {
		return nil, fmt.Errorf("invalid worker %d", worker)
	}
	w := d.workers[worker]
	if !w.active.CompareAndSwap(false, true) {
		return nil, errors.New("TPC-C worker already has an active transaction")
	}
	if w.dirty || w.session == nil {
		if w.session != nil {
			if err := w.session.Close(); err != nil {
				w.active.Store(false)
				return nil, err
			}
		}
		var err error
		w.session, err = d.Database.NewSession()
		if err != nil {
			w.active.Store(false)
			return nil, err
		}
		w.dirty = false
	}
	if _, err := w.session.ExecPreparedContext(ctx, d.begin); err != nil {
		_ = w.session.Close()
		w.session = nil
		w.dirty = true
		w.active.Store(false)
		return nil, err
	}
	return &slopTransaction{worker: w, database: d}, nil
}
func (d *slopDatabase) Info() map[string]any {
	return map[string]any{"engine": "slopdb", "wal": true, "wal_fsync": d.config.WALFsync, "deadlock_detection": true, "transaction_mode": "READ COMMITTED, row locking", "storage": "disk", "session_per_worker": true, "prepared_result_reuse": d.config.Prepared, "page_size_bytes": d.PageSize(), "page_size_kind": "uncompressed SSTable data-block target", "zstd_window_bytes": storage.SSTableZstdWindowSizeForBlock(d.PageSize())}
}

// BenchmarkMetrics samples counters at workload boundaries without requiring
// native adapters to implement SlopDB-specific storage instrumentation.
func (d *slopDatabase) BenchmarkMetrics() map[string]int64 {
	s := d.Stats()
	var fileBytes int64
	for _, level := range s.Levels {
		fileBytes += level.TotalSize
	}
	return map[string]int64{
		"write_groups": s.WriteGroups, "write_batches": s.WriteBatches,
		"completed_compactions":   s.CompletedCompactions,
		"compaction_input_bytes":  s.CompactionInputBytes,
		"compaction_output_bytes": s.CompactionOutputBytes,
		"completed_flushes":       s.CompletedFlushes, "flush_output_bytes": s.FlushOutputBytes,
		"sstable_bytes": fileBytes, "block_cache_hits": s.BlockCacheHits, "block_cache_misses": s.BlockCacheMisses,
	}
}
func (t *slopTransaction) ExecContext(ctx context.Context, query string) (*db.ExecResult, error) {
	return t.execContext(ctx, query, false)
}
func (t *slopTransaction) execContextWorkload(ctx context.Context, query string) (*db.ExecResult, error) {
	return t.execContext(ctx, query, true)
}
func (t *slopTransaction) execContext(ctx context.Context, query string, trusted bool) (*db.ExecResult, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.Tx != nil {
		return t.Tx.ExecContext(ctx, query)
	}
	if t.done {
		return nil, db.ErrTxDone
	}
	if !trusted {
		t.worker.dirty = true
	}
	result, err := t.worker.session.ExecContext(ctx, query)
	if err == nil && ((!trusted && !t.worker.session.InTransaction()) || strings.EqualFold(strings.TrimSpace(query), "COMMIT") || strings.EqualFold(strings.TrimSpace(query), "ROLLBACK")) {
		t.done = true
		t.worker.active.Store(false)
	}
	return result, err
}
func (t *slopTransaction) ExecPreparedContext(ctx context.Context, s Statement, args ...any) (*db.ExecResult, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.Tx != nil {
		return t.Tx.ExecPreparedContext(ctx, s.(*db.Stmt), args...)
	}
	if t.done {
		return nil, db.ErrTxDone
	}
	if s != t.database.commit {
		t.worker.dirty = true
	}
	result, err := t.worker.session.ExecPreparedContext(ctx, s.(*db.Stmt), args...)
	if err == nil && !t.worker.session.InTransaction() {
		t.done = true
		t.worker.active.Store(false)
	}
	return result, err
}

// Only the workload helper opts into scratch results. Ordinary Transaction
// calls continue returning independently owned results to their callers.
func (t *slopTransaction) execPreparedContextReusable(ctx context.Context, s Statement, args ...any) (*db.ExecResult, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.Tx != nil {
		return t.Tx.ExecPreparedContext(ctx, s.(*db.Stmt), args...)
	}
	if t.done {
		return nil, db.ErrTxDone
	}
	dst := t.worker.results[s]
	if dst == nil {
		dst = &db.ExecResult{}
		t.worker.results[s] = dst
	}
	result, err := t.worker.session.ExecPreparedContextInto(ctx, dst, s.(*db.Stmt), args...)
	if err == nil && s == t.database.commit {
		t.done = true
		t.worker.active.Store(false)
	}
	return result, err
}
func (t *slopTransaction) ExecPrepared(s Statement, args ...any) (*db.ExecResult, error) {
	return t.ExecPreparedContext(context.Background(), s, args...)
}
func (t *slopTransaction) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.Tx != nil {
		return t.Tx.Commit()
	}
	if t.done {
		return db.ErrTxDone
	}
	_, err := t.worker.session.ExecContext(context.Background(), "COMMIT")
	if err == nil {
		t.done = true
		t.worker.active.Store(false)
	}
	return err
}
func (t *slopTransaction) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.Tx != nil {
		return t.Tx.Rollback()
	}
	if t.done {
		return db.ErrTxDone
	}
	_, err := t.worker.session.ExecPreparedContext(context.Background(), t.database.rollback)
	if err != nil {
		_ = t.worker.session.Close()
		t.worker.session = nil
		t.worker.dirty = true
	}
	t.done = true
	t.worker.active.Store(false)
	return err
}
