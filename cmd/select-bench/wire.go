package main

import (
	"bytes"
	"context"
	gosql "database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq"
	"slopdb/db"
	"slopdb/mysqlserver"
	"slopdb/postgresserver"
)

const wireQueryPrefix = "SELECT id,v,payload FROM small_rows WHERE id="

// Each database/sql reader owns typed scan destinations. Embedded and TCP
// backends use the same path without an ExecResult/map adapter.
type sqlPoolReader struct {
	pool      *gosql.DB
	statement *gosql.Stmt
	literal   []byte
	id, value int64
	data      []byte
	rows      int
}

func (r *sqlPoolReader) openRows(ctx context.Context, id int64) (*gosql.Rows, error) {
	if r.statement != nil {
		return r.statement.QueryContext(ctx, id)
	}
	r.literal = strconv.AppendInt(r.literal[:len(wireQueryPrefix)], id, 10)
	// Text has no bound arguments. In particular, the MySQL driver must not
	// silently replace this path with a prepare/execute/close sequence.
	return r.pool.QueryContext(ctx, string(r.literal))
}

func (r *sqlPoolReader) scanRows(rows *gosql.Rows, id int64) error {
	r.rows = 0
	var scanErr error
	for rows.Next() {
		r.rows++
		if err := rows.Scan(&r.id, &r.value, &r.data); err != nil && scanErr == nil {
			scanErr = err
		}
	}
	// Drain and close before the next operation, including malformed answers.
	err := errors.Join(scanErr, rows.Err(), rows.Close())
	if err != nil {
		return err
	}
	if r.rows != 1 {
		return fmt.Errorf("id %d: got %d rows", id, r.rows)
	}
	return nil
}

func (r *sqlPoolReader) query(id int64) error {
	r.rows = 0
	rows, err := r.openRows(context.Background(), id)
	if err != nil {
		return err
	}
	return r.scanRows(rows, id)
}

func (r *sqlPoolReader) validate(id int64, expected []byte) error {
	if len(expected) != payloadBytes {
		return fmt.Errorf("invalid validation buffer size %d", len(expected))
	}
	payload(id, expected)
	if r.rows != 1 || r.id != id || r.value != id*17 || !bytes.Equal(r.data, expected) {
		return fmt.Errorf("id %d: incorrect database/sql result", id)
	}
	return nil
}

type benchmarkWireServer interface {
	Serve(net.Listener) error
	Close() error
}

// With no Endpoint, both TCP endpoints live in this process and the harness
// counts client, database/sql, protocol and engine CPU/allocations together.
func newWireBackend(c config, d *db.Database) (_ *benchmarkBackend, err error) {
	if c.Workers < 1 {
		return nil, fmt.Errorf("wire backend requires at least one worker")
	}
	if c.QueryMode != "text" && c.QueryMode != "prepared" {
		return nil, fmt.Errorf("unsupported wire query mode %q", c.QueryMode)
	}
	if c.API != "mysql" && c.API != "pq" && c.API != "pgx" {
		return nil, fmt.Errorf("unsupported wire API %q", c.API)
	}
	address := c.Endpoint
	var server benchmarkWireServer
	var served chan error
	if address != "" {
		host, port, splitErr := net.SplitHostPort(address)
		n, portErr := strconv.Atoi(port)
		if splitErr != nil || host == "" || portErr != nil || n < 1 || n > 65535 {
			return nil, fmt.Errorf("invalid TCP endpoint %q", address)
		}
	} else {
		switch c.API {
		case "mysql":
			server, err = mysqlserver.New(d, mysqlserver.Config{})
		case "pq", "pgx":
			server, err = postgresserver.New(d, postgresserver.Config{})
		}
		if err != nil {
			return nil, err
		}
		listener, listenErr := net.Listen("tcp", "127.0.0.1:0")
		if listenErr != nil {
			return nil, errors.Join(listenErr, server.Close())
		}
		address = listener.Addr().String()
		served = make(chan error, 1)
		go func() { served <- server.Serve(listener) }()
	}
	var pool *gosql.DB
	var poolClose func() error
	var once sync.Once
	var closeErr error
	backend := &benchmarkBackend{}
	backend.close = func() error {
		once.Do(func() {
			// Close clients before stopping the server, and join Serve before the
			// harness closes its Database. The server never owns that Database.
			if poolClose != nil {
				closeErr = errors.Join(closeErr, poolClose())
			} else if pool != nil {
				closeErr = errors.Join(closeErr, pool.Close())
			}
			if server != nil {
				closeErr = errors.Join(closeErr, server.Close())
				serveErr := <-served
				if !errors.Is(serveErr, net.ErrClosed) {
					closeErr = errors.Join(closeErr, serveErr)
				}
			}
		})
		return closeErr
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, backend.close())
		}
	}()
	switch c.API {
	case "mysql":
		options := mysql.NewConfig()
		options.User, options.DBName, options.Net, options.Addr = "slopdb", "slopdb", "tcp", address
		options.Timeout = 5 * time.Second
		pool, err = gosql.Open("mysql", options.FormatDSN())
	case "pq":
		pool, err = gosql.Open("postgres", "postgresql://slopdb@"+address+"/slopdb?sslmode=disable&connect_timeout=5")
	case "pgx":
		var options *pgx.ConnConfig
		options, err = pgx.ParseConfig("postgresql://slopdb@" + address + "/slopdb?sslmode=disable&connect_timeout=5")
		if err == nil {
			if c.QueryMode == "text" {
				options.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
				options.StatementCacheCapacity, options.DescriptionCacheCapacity = 0, 0
			} else {
				options.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement
			}
			pool = stdlib.OpenDB(*options)
		}
	}
	if err != nil {
		return nil, err
	}
	sqlBackend, err := newSQLPoolBackend(c, pool)
	if err != nil {
		return nil, err
	}
	poolClose = sqlBackend.close
	backend.readers = sqlBackend.readers
	backend.poolStats = sqlBackend.poolStats
	return backend, nil
}

// newSQLPoolBackend owns pool setup and cleanup for both embedded and wire
// database/sql drivers, so their timed scan and validation paths are identical.
func newSQLPoolBackend(c config, pool *gosql.DB) (_ *benchmarkBackend, err error) {
	if pool == nil {
		return nil, fmt.Errorf("nil SQL pool")
	}
	var statement *gosql.Stmt
	var once sync.Once
	var closeErr error
	backend := &benchmarkBackend{}
	backend.close = func() error {
		once.Do(func() {
			closeErr = pool.Close()
			if statement != nil {
				closeErr = errors.Join(closeErr, statement.Close())
			}
		})
		return closeErr
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, backend.close())
		}
	}()
	if c.Workers < 1 {
		return nil, fmt.Errorf("SQL pool backend requires at least one worker")
	}
	if c.QueryMode != "text" && c.QueryMode != "prepared" {
		return nil, fmt.Errorf("unsupported SQL pool query mode %q", c.QueryMode)
	}
	pool.SetMaxOpenConns(c.Workers)
	pool.SetMaxIdleConns(c.Workers)
	backend.poolStats = func() poolCounters {
		stats := pool.Stats()
		return poolCounters{WaitCount: stats.WaitCount, WaitSeconds: stats.WaitDuration.Seconds(), OpenConnections: stats.OpenConnections, InUse: stats.InUse}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if c.QueryMode == "prepared" {
		text := query
		if c.API == "pq" || c.API == "pgx" {
			text = "SELECT id,v,payload FROM small_rows WHERE id=$1"
		}
		statement, err = pool.PrepareContext(ctx, text)
		if err != nil {
			return nil, err
		}
	}
	backend.readers = make([]benchmarkReader, c.Workers)
	readers := make([]*sqlPoolReader, c.Workers)
	for i := range readers {
		reader := &sqlPoolReader{pool: pool, statement: statement, literal: make([]byte, len(wireQueryPrefix), len(wireQueryPrefix)+20)}
		copy(reader.literal, wireQueryPrefix)
		readers[i] = reader
		backend.readers[i] = reader
	}
	// Hold each Rows open until every worker has checked out a connection. This
	// establishes the full pool and lazily prepares the shared Stmt on every
	// physical connection before measurement. No connections stay pinned afterward.
	opened := make([]*gosql.Rows, 0, c.Workers)
	for _, reader := range readers {
		var rows *gosql.Rows
		rows, err = reader.openRows(ctx, 1)
		if err != nil {
			for _, prior := range opened {
				err = errors.Join(err, prior.Close())
			}
			return nil, err
		}
		opened = append(opened, rows)
	}
	expected := make([]byte, payloadBytes)
	for i, rows := range opened {
		scanErr := readers[i].scanRows(rows, 1)
		if scanErr == nil {
			scanErr = readers[i].validate(1, expected)
		}
		err = errors.Join(err, scanErr)
	}
	if err != nil {
		return nil, err
	}
	return backend, nil
}
