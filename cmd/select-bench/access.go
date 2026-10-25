package main

import (
	"fmt"
	"slopdb/db"
	"strconv"
)

// Each reader owns its decoding state; pooled driver connections stay managed
// by database/sql. Correctness checks run after the measured query lifetime.
type benchmarkReader interface {
	query(id int64) error
	validate(id int64, expected []byte) error
}
type poolCounters struct {
	WaitCount              int64
	WaitSeconds            float64
	OpenConnections, InUse int
}
type benchmarkBackend struct {
	readers   []benchmarkReader
	close     func() error
	poolStats func() poolCounters
}
type nativeReader struct {
	session     *db.Session
	statement   *db.Stmt
	reuse, text bool
	destination db.ExecResult
	result      *db.ExecResult
	textBuffer  []byte
}

func (r *nativeReader) query(id int64) (err error) {
	if r.text {
		r.textBuffer = append(r.textBuffer[:0], "SELECT id,v,payload FROM small_rows WHERE id="...)
		r.textBuffer = strconv.AppendInt(r.textBuffer, id, 10)
		r.result, err = r.session.Exec(string(r.textBuffer))
	} else {
		r.result, err = execute(r.session, r.statement, &r.destination, r.reuse, id)
	}
	return err
}
func (r *nativeReader) validate(id int64, expected []byte) error {
	return check(r.result, id, expected)
}
func accessConfig(c config) (config, error) {
	if c.API == "" {
		c.API = "native"
	}
	if c.QueryMode == "" {
		c.QueryMode = "prepared"
	}
	switch c.API {
	case "native", "embedded", "mysql", "pq", "pgx":
	default:
		return c, fmt.Errorf("unknown API %q", c.API)
	}
	if c.QueryMode != "text" && c.QueryMode != "prepared" {
		return c, fmt.Errorf("unknown query mode %q", c.QueryMode)
	}
	if c.ReuseResults && (c.API != "native" || c.QueryMode != "prepared") {
		return c, fmt.Errorf("reuse-results requires native prepared execution")
	}
	if c.Endpoint != "" && (c.API == "native" || c.API == "embedded") {
		return c, fmt.Errorf("external endpoint requires mysql, pq or pgx")
	}
	return c, nil
}
func apiMode(c config) string {
	if c.API == "native" {
		if c.QueryMode == "text" {
			return "Session.Exec"
		}
		if c.ReuseResults {
			return "ExecPreparedInto"
		}
		return "ExecPrepared"
	}
	return "database/sql:" + c.API + ":" + c.QueryMode
}
func newNativeBackend(c config, d *db.Database) (backend *benchmarkBackend, err error) {
	backend = &benchmarkBackend{}
	owned := backend
	var statement *db.Stmt
	closeBackend := func() error {
		var first error
		for _, reader := range owned.readers {
			if e := reader.(*nativeReader).session.Close(); e != nil && first == nil {
				first = e
			}
		}
		if statement != nil {
			if e := statement.Close(); e != nil && first == nil {
				first = e
			}
		}
		return first
	}
	backend.close = closeBackend
	defer func() {
		if err != nil {
			_ = closeBackend()
		}
	}()
	if c.QueryMode == "prepared" {
		statement, err = d.Prepare(query)
		if err != nil {
			return nil, err
		}
	}
	for i := 0; i < c.Workers; i++ {
		session, e := d.NewSession()
		if e != nil {
			return nil, e
		}
		backend.readers = append(backend.readers, &nativeReader{session: session, statement: statement, reuse: c.ReuseResults, text: c.QueryMode == "text", textBuffer: make([]byte, 0, 96)})
	}
	return backend, nil
}
