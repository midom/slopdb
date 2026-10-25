package driver

import (
	"context"
	sqldriver "database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"

	"slopdb/db"
	nativesql "slopdb/sql"
)

type rows struct {
	mu         sync.Mutex
	connection *conn
	ctx        context.Context
	native     *db.Rows
	columns    []nativesql.ColumnDef
	names      []string
	buffered   []map[string]nativesql.Value
	position   int
	closed     bool
	temporary  *statement
}

func newNativeRows(ctx context.Context, c *conn, native *db.Rows) *rows {
	return initRows(&rows{ctx: ctx, connection: c, native: native, columns: native.Columns()})
}
func newBufferedRows(ctx context.Context, c *conn, result *db.ExecResult) *rows {
	return initRows(&rows{ctx: ctx, connection: c, columns: result.Columns, buffered: result.Rows})
}
func initRows(r *rows) *rows {
	r.names = make([]string, len(r.columns))
	for i, c := range r.columns {
		r.names[i] = c.DisplayName
		if r.names[i] == "" {
			r.names[i] = c.Name
		}
	}
	return r
}
func (r *rows) Columns() []string { return r.names }
func (r *rows) Close() error      { r.mu.Lock(); defer r.mu.Unlock(); return r.close() }
func (r *rows) close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	var err error
	if r.native != nil {
		err = errors.Join(r.native.Close(), r.native.Err())
		r.native = nil
	}
	r.buffered = nil
	c := r.connection
	c.mu.Lock()
	if c.active == r {
		c.active = nil
	}
	if r.temporary != nil {
		err = errors.Join(err, r.temporary.close())
		r.temporary = nil
	}
	c.mu.Unlock()
	return err
}
func (r *rows) Next(destination []sqldriver.Value) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return io.EOF
	}
	if e := r.ctx.Err(); e != nil {
		_ = r.close()
		return e
	}
	if len(destination) != len(r.columns) {
		_ = r.close()
		return errors.New("slopdb: result destination column count mismatch")
	}
	if r.native != nil {
		if !r.native.Next() {
			e := r.native.Err()
			closeErr := r.close()
			if e != nil {
				return e
			}
			if closeErr != nil {
				return closeErr
			}
			return io.EOF
		}
		values, e := r.native.Values()
		if e != nil {
			_ = r.close()
			if r.ctx.Err() != nil {
				return r.ctx.Err()
			}
			return e
		}
		for i, value := range values {
			destination[i], e = resultValue(value)
			if e != nil {
				_ = r.close()
				return e
			}
		}
		return nil
	}
	if r.position >= len(r.buffered) {
		_ = r.close()
		return io.EOF
	}
	row := r.buffered[r.position]
	r.buffered[r.position] = nil
	r.position++
	for i, col := range r.columns {
		value, e := resultValue(row[col.Name])
		if e != nil {
			_ = r.close()
			return e
		}
		destination[i] = value
	}
	return nil
}
func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	name := r.columns[index].DeclaredType()
	if i := strings.IndexByte(name, '('); i >= 0 {
		if j := strings.IndexByte(name[i:], ')'); j >= 0 {
			name = name[:i] + name[i+j+1:]
		}
	}
	return name
}
func (r *rows) ColumnTypeLength(index int) (int64, bool) {
	c := r.columns[index]
	if c.Length > 0 {
		return c.Length, true
	}
	return 0, false
}
func (r *rows) ColumnTypeNullable(index int) (bool, bool) { return !r.columns[index].NotNull, true }
func (r *rows) ColumnTypePrecisionScale(index int) (int64, int64, bool) {
	c := r.columns[index]
	if c.ColType == nativesql.ColDecimal && c.Precision > 0 {
		return int64(c.Precision), int64(c.Scale), true
	}
	return 0, 0, false
}
func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	c := r.columns[index]
	typ := c.LogicalType()
	if typ.Kind == nativesql.TypeBoolean {
		return reflect.TypeOf(false)
	}
	if typ.Unsigned || c.ColType == nativesql.ColUnsigned {
		return reflect.TypeOf("")
	}
	switch c.ColType {
	case nativesql.ColInteger:
		return reflect.TypeOf(int64(0))
	case nativesql.ColReal:
		return reflect.TypeOf(float64(0))
	case nativesql.ColBlob:
		return reflect.TypeOf([]byte{})
	case nativesql.ColText, nativesql.ColDecimal, nativesql.ColJSON:
		return reflect.TypeOf("")
	}
	return reflect.TypeOf((*any)(nil)).Elem()
}

var (
	_ sqldriver.Rows                           = (*rows)(nil)
	_ sqldriver.RowsColumnTypeDatabaseTypeName = (*rows)(nil)
	_ sqldriver.RowsColumnTypeLength           = (*rows)(nil)
	_ sqldriver.RowsColumnTypeNullable         = (*rows)(nil)
	_ sqldriver.RowsColumnTypePrecisionScale   = (*rows)(nil)
	_ sqldriver.RowsColumnTypeScanType         = (*rows)(nil)
)
