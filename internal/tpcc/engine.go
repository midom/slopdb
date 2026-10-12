package tpcc

import (
	"context"
	"fmt"

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
	config Config
}
type slopTransaction struct{ *db.Tx }

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
	return &slopDatabase{d, c}, nil
}
func (d *slopDatabase) Prepare(q string) (Statement, error)    { return d.Database.Prepare(q) }
func (d *slopDatabase) SetupWorkers(int, []string, bool) error { return nil }
func (d *slopDatabase) BeginContext(ctx context.Context, _ int, _ bool) (Transaction, error) {
	tx, err := d.Database.BeginContext(ctx)
	if err != nil {
		return nil, err
	}
	return &slopTransaction{tx}, nil
}
func (d *slopDatabase) Info() map[string]any {
	return map[string]any{"engine": "slopdb", "wal": true, "wal_fsync": d.config.WALFsync, "deadlock_detection": true, "transaction_mode": "READ COMMITTED, row locking", "storage": "disk", "page_size_bytes": d.PageSize(), "page_size_kind": "uncompressed SSTable data-block target", "zstd_window_bytes": storage.SSTableZstdWindowSize}
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
func (t *slopTransaction) ExecPreparedContext(ctx context.Context, s Statement, args ...any) (*db.ExecResult, error) {
	return t.Tx.ExecPreparedContext(ctx, s.(*db.Stmt), args...)
}
func (t *slopTransaction) ExecPrepared(s Statement, args ...any) (*db.ExecResult, error) {
	return t.Tx.ExecPrepared(s.(*db.Stmt), args...)
}
