package main

/*
#cgo LDFLAGS: -ldl
#include <dlfcn.h>
static long long skipped_wal_syncs() {
 unsigned long long (*fn)(void)=dlsym(RTLD_DEFAULT,"slopdb_tpcc_wal_sync_skipped");
 return fn ? (long long)fn() : -1;
}
*/
import "C"

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
	sqlite "github.com/mattn/go-sqlite3"
	"slopdb/db"
	"slopdb/internal/tpcc"
	slopsql "slopdb/sql"
)

type nativeStatement struct{ query string }

func (*nativeStatement) Close() error { return nil }

type nativeWorker struct {
	conn       *stdsql.Conn
	statements map[string]*stdsql.Stmt
}
type nativeDatabase struct {
	database *stdsql.DB
	config   tpcc.Config
	workers  []*nativeWorker
	version  string
	settings map[string]any
}
type nativeTransaction struct {
	worker *nativeWorker
	engine string
	done   bool
}

func openEngine(c tpcc.Config) (tpcc.Database, error) {
	if c.Engine == "slopdb" {
		return tpcc.OpenSlopDB(c)
	}
	d := &nativeDatabase{config: c, settings: map[string]any{"engine": c.Engine, "storage": "disk", "prepared_per_worker": c.Prepared}}
	var err error
	switch c.Engine {
	case "sqlite":
		syncMode := "OFF"
		if c.WALFsync {
			syncMode = "FULL"
		}
		u := url.URL{Scheme: "file", Path: filepath.Join(c.Dir, "benchmark.sqlite")}
		q := u.Query()
		q.Set("_journal_mode", "WAL")
		q.Set("_synchronous", syncMode)
		q.Set("_busy_timeout", fmt.Sprint(c.LockTimeout.Milliseconds()))
		u.RawQuery = q.Encode()
		d.database, err = stdsql.Open("sqlite3", u.String())
		d.settings["driver"] = "github.com/mattn/go-sqlite3 v1.14.43"
		d.settings["journal_mode"] = "wal"
		d.settings["synchronous"] = syncMode
		d.settings["busy_timeout_ms"] = c.LockTimeout.Milliseconds()
		d.settings["transaction_mode"] = "BEGIN IMMEDIATE for writers; BEGIN for read-only transactions"
		d.settings["wal_fsync"] = c.WALFsync
	case "duckdb":
		path := filepath.Join(c.Dir, "benchmark.duckdb")
		if !c.WALFsync && (C.skipped_wal_syncs() < 0 || os.Getenv("SLOPDB_TPCC_WAL_FILE") != path+".wal") {
			return nil, errors.New("DuckDB without WAL fsync requires the benchmark-only preload wrapper and SLOPDB_TPCC_WAL_FILE=<database>.wal; see README")
		}
		d.database, err = stdsql.Open("duckdb", path+"?threads=1&memory_limit=1GB")
		d.settings["driver"] = "github.com/duckdb/duckdb-go/v2 v2.10506.0"
		d.settings["threads"] = 1
		d.settings["memory_limit"] = "1GB"
		d.settings["transaction_mode"] = "snapshot isolation; optimistic write conflict retries"
		d.settings["wal_fsync"] = c.WALFsync
		if !c.WALFsync {
			d.settings["wal_fsync_mode"] = "benchmark-only LD_PRELOAD bypass for exact WAL path; checkpoint sync unchanged"
		}
	default:
		return nil, fmt.Errorf("unknown engine %q", c.Engine)
	}
	if err != nil {
		return nil, err
	}
	d.database.SetMaxOpenConns(c.Workers + 1)
	d.database.SetMaxIdleConns(c.Workers + 1)
	fail := func(err error) (tpcc.Database, error) { d.database.Close(); return nil, err }
	if err = d.database.Ping(); err != nil {
		return fail(err)
	}
	versionQuery := "SELECT sqlite_version()"
	if c.Engine == "duckdb" {
		versionQuery = "SELECT version()"
	}
	if err = d.database.QueryRow(versionQuery).Scan(&d.version); err != nil {
		return fail(err)
	}
	if c.Engine == "sqlite" {
		var journal string
		var synchronous int
		if err = d.database.QueryRow("PRAGMA journal_mode").Scan(&journal); err != nil {
			return fail(err)
		}
		if err = d.database.QueryRow("PRAGMA synchronous").Scan(&synchronous); err != nil {
			return fail(err)
		}
		if journal != "wal" || (!c.WALFsync && synchronous != 0) || (c.WALFsync && synchronous != 2) {
			return fail(fmt.Errorf("unexpected SQLite settings: journal=%s sync=%d", journal, synchronous))
		}
	}
	return d, nil
}
func (d *nativeDatabase) Info() map[string]any {
	info := make(map[string]any, len(d.settings)+2)
	for k, v := range d.settings {
		info[k] = v
	}
	info["version"] = d.version
	if d.config.Engine == "duckdb" && !d.config.WALFsync {
		info["wal_fsync_calls_skipped"] = int64(C.skipped_wal_syncs())
	}
	return info
}
func nativeQuery(query, engine string) string {
	query = strings.TrimSuffix(query, " FOR UPDATE SKIP LOCKED")
	query = strings.TrimSuffix(query, " FOR UPDATE")
	if engine == "duckdb" && strings.HasPrefix(query, "CREATE TABLE ") {
		query = strings.ReplaceAll(query, "INTEGER", "BIGINT")
	}
	return query
}
func (d *nativeDatabase) Exec(q string) (*db.ExecResult, error) {
	return nativeExec(context.Background(), d.database, nativeQuery(q, d.config.Engine))
}
func (d *nativeDatabase) Prepare(q string) (tpcc.Statement, error) { return &nativeStatement{q}, nil }
func (d *nativeDatabase) SetupWorkers(count int, queries []string, prepared bool) error {
	for i := 0; i < count; i++ {
		conn, err := d.database.Conn(context.Background())
		if err != nil {
			return err
		}
		w := &nativeWorker{conn: conn, statements: make(map[string]*stdsql.Stmt)}
		d.workers = append(d.workers, w)
		if prepared {
			for _, q := range queries {
				stmt, err := conn.PrepareContext(context.Background(), nativeQuery(q, d.config.Engine))
				if err != nil {
					return fmt.Errorf("prepare %s: %w", q, err)
				}
				w.statements[q] = stmt
			}
		}
	}
	return nil
}
func (d *nativeDatabase) BeginContext(ctx context.Context, worker int, readOnly bool) (tpcc.Transaction, error) {
	w := d.workers[worker]
	begin := "BEGIN"
	if d.config.Engine == "sqlite" && !readOnly {
		begin = "BEGIN IMMEDIATE"
	}
	if _, err := w.conn.ExecContext(ctx, begin); err != nil {
		return nil, nativeError(err, d.config.Engine)
	}
	return &nativeTransaction{worker: w, engine: d.config.Engine}, nil
}
func (d *nativeDatabase) Close() error {
	var errs []error
	for _, w := range d.workers {
		for _, s := range w.statements {
			errs = append(errs, s.Close())
		}
		errs = append(errs, w.conn.Close())
	}
	d.workers = nil
	errs = append(errs, d.database.Close())
	return errors.Join(errs...)
}
func (t *nativeTransaction) ExecContext(ctx context.Context, q string) (*db.ExecResult, error) {
	if t.done {
		return nil, db.ErrTxDone
	}
	r, err := nativeExec(ctx, t.worker.conn, nativeQuery(q, t.engine))
	return r, nativeError(err, t.engine)
}
func (t *nativeTransaction) ExecPreparedContext(ctx context.Context, s tpcc.Statement, args ...any) (*db.ExecResult, error) {
	if t.done {
		return nil, db.ErrTxDone
	}
	q := s.(*nativeStatement).query
	if q == "COMMIT" {
		err := t.Commit()
		return &db.ExecResult{}, err
	}
	statement := t.worker.statements[q]
	if statement == nil {
		return nil, fmt.Errorf("unprepared native query %s", q)
	}
	var r *db.ExecResult
	var err error
	if strings.HasPrefix(q, "SELECT ") {
		rows, e := statement.QueryContext(ctx, args...)
		if e != nil {
			err = e
		} else {
			r, err = readRows(rows)
		}
	} else {
		result, e := statement.ExecContext(ctx, args...)
		if e != nil {
			err = e
		} else {
			var n int64
			n, err = result.RowsAffected()
			r = &db.ExecResult{Changes: n}
		}
	}
	return r, nativeError(err, t.engine)
}
func (t *nativeTransaction) ExecPrepared(s tpcc.Statement, args ...any) (*db.ExecResult, error) {
	return t.ExecPreparedContext(context.Background(), s, args...)
}
func (t *nativeTransaction) Commit() error {
	if t.done {
		return db.ErrTxDone
	}
	_, err := t.worker.conn.ExecContext(context.Background(), "COMMIT")
	if err == nil {
		t.done = true
	}
	return nativeError(err, t.engine)
}
func (t *nativeTransaction) Rollback() error {
	if t.done {
		return db.ErrTxDone
	}
	_, err := t.worker.conn.ExecContext(context.Background(), "ROLLBACK")
	t.done = true
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "no transaction is active") {
		return db.ErrTxDone
	}
	return err
}

type sqlExecutor interface {
	QueryContext(context.Context, string, ...any) (*stdsql.Rows, error)
	ExecContext(context.Context, string, ...any) (stdsql.Result, error)
}

func nativeExec(ctx context.Context, e sqlExecutor, q string) (*db.ExecResult, error) {
	if strings.HasPrefix(q, "SELECT ") {
		rows, err := e.QueryContext(ctx, q)
		if err != nil {
			return nil, err
		}
		return readRows(rows)
	}
	result, err := e.ExecContext(ctx, q)
	if err != nil {
		return nil, err
	}
	n, err := result.RowsAffected()
	return &db.ExecResult{Changes: n}, err
}
func readRows(rows *stdsql.Rows) (*db.ExecResult, error) {
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	r := &db.ExecResult{}
	for _, c := range cols {
		r.Columns = append(r.Columns, slopsql.ColumnDef{Name: c})
	}
	values := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range values {
		ptrs[i] = &values[i]
	}
	for rows.Next() {
		if err = rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]slopsql.Value, len(cols))
		for i, c := range cols {
			v := values[i]
			var x slopsql.Value
			switch v := v.(type) {
			case nil:
				x = slopsql.NewNullValue()
			case int64:
				x = slopsql.NewIntValue(v)
			case *big.Int:
				if !v.IsInt64() {
					return nil, errors.New("aggregate exceeds int64")
				}
				x = slopsql.NewIntValue(v.Int64())
			case float64:
				if math.IsNaN(v) || math.IsInf(v, 0) {
					return nil, errors.New("nonfinite result")
				}
				x = slopsql.NewRealValue(v)
			case string:
				x = slopsql.NewTextValue(v)
			default:
				return nil, fmt.Errorf("unsupported native result type %T", v)
			}
			row[c] = x
		}
		r.Rows = append(r.Rows, row)
	}
	return r, rows.Err()
}
func nativeError(err error, engine string) error {
	if err == nil {
		return nil
	}
	if engine == "sqlite" {
		var e sqlite.Error
		if errors.As(err, &e) && (e.Code == sqlite.ErrBusy || e.Code == sqlite.ErrLocked) {
			return fmt.Errorf("%w: %v", db.ErrLockTimeout, err)
		}
	}
	if engine == "duckdb" {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "transactioncontext error") && strings.Contains(msg, "conflict") {
			return fmt.Errorf("%w: %v", db.ErrConflict, err)
		}
	}
	return err
}
