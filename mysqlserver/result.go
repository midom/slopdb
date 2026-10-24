package mysqlserver

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"

	"slopdb/db"
	"slopdb/sql"
)

type protocolError struct {
	code           uint16
	state, message string
}

func (e *protocolError) Error() string { return e.message }
func (c *client) status() uint16 {
	var status uint16
	if c.autocommit {
		status |= 2
	}
	if c.session != nil && c.session.InTransaction() {
		status |= 1
	}
	return status
}
func (c *client) ok(result *db.ExecResult) error {
	var changes, id uint64
	if result != nil {
		if result.Changes > 0 {
			changes = uint64(result.Changes)
		}
		if result.LastID > 0 {
			id = uint64(result.LastID)
		}
	}
	p := appendLen([]byte{0}, changes)
	p = appendLen(p, id)
	p = append16(p, c.status())
	p = append16(p, 0)
	return c.writer.write(p)
}
func (c *client) eof() error { return c.writer.write(append16([]byte{0xfe, 0, 0}, c.status())) }
func (c *client) writeError(err error) error {
	pErr := &protocolError{1105, "HY000", err.Error()}
	var explicit *protocolError
	var parse *sql.ParseError
	switch {
	case errors.As(err, &explicit):
		pErr = explicit
	case errors.Is(err, db.ErrDeadlock), errors.Is(err, db.ErrConflict), errors.Is(err, db.ErrTransactionAborted):
		pErr.code = 1213
		pErr.state = "40001"
	case errors.Is(err, db.ErrLockTimeout):
		pErr.code = 1205
	case errors.Is(err, db.ErrLockNotAvailable):
		pErr.code = 3572
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		pErr.code = 1317
		pErr.state = "70100"
	case errors.As(err, &parse):
		pErr.code = 1064
		pErr.state = "42000"
	default:
		message := strings.ToLower(err.Error())
		switch {
		case strings.Contains(message, "unique constraint"), strings.Contains(message, "duplicate primary"), strings.Contains(message, "duplicate key"):
			pErr.code = 1062
			pErr.state = "23000"
		case strings.Contains(message, "not null"):
			pErr.code = 1048
			pErr.state = "23000"
		case strings.Contains(message, "foreign key"):
			pErr.code = 1452
			pErr.state = "23000"
		case strings.Contains(message, "table") && (strings.Contains(message, "not found") || strings.Contains(message, "does not exist")):
			pErr.code = 1146
			pErr.state = "42S02"
		}
	}
	p := append16([]byte{0xff}, pErr.code)
	p = append(p, '#')
	p = append(p, pErr.state...)
	p = append(p, pErr.message...)
	return c.writer.write(p)
}

const (
	typeLongLong byte = 8
	typeDouble   byte = 5
	typeBlob     byte = 252
	typeString   byte = 253
)

func wireType(col sql.ColumnDef) byte {
	switch col.ColType {
	case sql.ColInteger:
		return typeLongLong
	case sql.ColReal:
		return typeDouble
	case sql.ColBlob:
		return typeBlob
	default:
		return typeString
	}
}
func columnPacket(col sql.ColumnDef, database string) []byte {
	p := appendString(nil, []byte("def"))
	p = appendString(p, []byte(database))
	p = appendString(p, nil)
	p = appendString(p, nil)
	p = appendString(p, []byte(col.Name))
	p = appendString(p, nil)
	p = append(p, 12)
	charset := uint16(45)
	var flags uint16
	if col.ColType != sql.ColText || col.Binary {
		charset = 63
		flags |= 128
	}
	if col.NotNull {
		flags |= 1
	}
	if col.PrimaryKey {
		flags |= 2
	}
	if col.Unique {
		flags |= 4
	}
	if col.AutoIncrement {
		flags |= 512
	}
	p = append16(p, charset)
	size := uint32(math.MaxUint32)
	if col.ColType == sql.ColInteger {
		size = 20
	}
	if col.ColType == sql.ColReal {
		size = 22
	}
	if col.Length > 0 && col.Length <= math.MaxUint32/4 {
		size = uint32(col.Length)
		if col.ColType == sql.ColText {
			size *= 4
		}
	}
	p = append32(p, size)
	p = append(p, wireType(col))
	p = append16(p, flags)
	decimals := byte(0)
	if col.ColType == sql.ColReal {
		decimals = 31
	}
	return append(p, decimals, 0, 0)
}
func valueBytes(v sql.Value) []byte {
	switch v.Type {
	case sql.ColInteger:
		return strconv.AppendInt(nil, v.Int, 10)
	case sql.ColReal:
		return strconv.AppendFloat(nil, v.Real, 'g', -1, 64)
	case sql.ColBlob:
		return v.Blob
	default:
		return []byte(v.Text)
	}
}
func (c *client) result(result *db.ExecResult, binaryRows bool) error {
	if result == nil || len(result.Columns) == 0 {
		return c.ok(result)
	}
	// SQL expression columns can contain different types in different rows.
	// Use a string wire type for mixed columns to avoid truncation or rounding.
	columns := append([]sql.ColumnDef(nil), result.Columns...)
	for i, col := range columns {
		seen := false
		for _, row := range result.Rows {
			v, ok := row[col.Name]
			if !ok || v.Null {
				continue
			}
			if !seen {
				columns[i].ColType = v.Type
				seen = true
			} else if columns[i].ColType != v.Type {
				columns[i].ColType = sql.ColText
				columns[i].NotNull = false
				break
			}
		}
	}
	if err := c.writer.write(appendLen(nil, uint64(len(columns)))); err != nil {
		return err
	}
	for _, col := range columns {
		if err := c.writer.write(columnPacket(col, c.selected)); err != nil {
			return err
		}
	}
	if err := c.eof(); err != nil {
		return err
	}
	for _, row := range result.Rows {
		var p []byte
		nullBytes := (len(columns) + 9) / 8
		if binaryRows {
			p = make([]byte, 1+nullBytes)
		}
		for i, col := range columns {
			v, ok := row[col.Name]
			if !ok || v.Null {
				if binaryRows {
					p[1+(i+2)/8] |= 1 << ((i + 2) % 8)
				} else {
					p = append(p, 0xfb)
				}
				continue
			}
			if binaryRows {
				switch wireType(col) {
				case typeLongLong:
					p = append64(p, uint64(v.Int))
					continue
				case typeDouble:
					p = append64(p, math.Float64bits(v.Real))
					continue
				}
			}
			p = appendString(p, valueBytes(v))
		}
		if err := c.writer.write(p); err != nil {
			return err
		}
	}
	return c.eof()
}
