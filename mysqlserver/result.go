package mysqlserver

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

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
	position := ""
	if result != nil {
		position = result.CommitPosition
		if result.Changes > 0 {
			changes = uint64(result.Changes)
		}
		if result.LastID > 0 {
			id = uint64(result.LastID)
		}
	}
	p := appendLen([]byte{0}, changes)
	p = appendLen(p, id)
	status := c.status()
	if position != "" && c.capabilities&clientSessionTrack != 0 {
		status |= 1 << 14
	}
	p = append16(p, status)
	p = append16(p, c.warningCount())
	info := ""
	if position != "" {
		info = "slopdb_commit_position=" + position
	}
	if c.capabilities&clientSessionTrack != 0 {
		p = appendString(p, []byte(info))
		if position != "" {
			variables := appendString(nil, []byte("slopdb_commit_position"))
			variables = appendString(variables, []byte(position))
			state := appendString([]byte{0}, variables) // SESSION_TRACK_SYSTEM_VARIABLES
			p = appendString(p, state)
		}
	} else {
		p = append(p, info...)
	}
	return c.writer.write(p)
}
func (c *client) eof() error {
	return c.writer.write(append16(append16([]byte{0xfe}, c.warningCount()), c.status()))
}
func (c *client) writeError(err error) error {
	pErr := &protocolError{1105, "HY000", err.Error()}
	var explicit *protocolError
	var parse *sql.ParseError
	switch {
	case errors.Is(err, db.ErrDurabilityUnconfirmed):
		pErr.state = "40003"
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
	typeTiny        byte = 1
	typeShort       byte = 2
	typeLong        byte = 3
	typeFloat       byte = 4
	typeDouble      byte = 5
	typeTimestamp   byte = 7
	typeLongLong    byte = 8
	typeDate        byte = 10
	typeDatetime    byte = 12
	typeBlob        byte = 252
	typeString      byte = 253
	typeFixedString byte = 254
)

func wireType(col sql.ColumnDef) byte {
	switch col.ColType {
	case sql.ColInteger:
		// Legacy native INTEGER columns retain their full int64 domain.
		if col.MySQL || col.Unsigned || (col.Declared != "" && col.Declared != "INTEGER") {
			switch col.IntegerBits {
			case 8:
				return typeTiny
			case 16:
				return typeShort
			case 24, 32:
				return typeLong
			}
		}
		return typeLongLong
	case sql.ColReal:
		if col.Declared == "FLOAT" {
			return typeFloat
		}
		return typeDouble
	case sql.ColBlob:
		if col.FixedBinary {
			return typeFixedString
		}
		return typeBlob
	default:
		switch col.Declared {
		case "TIMESTAMP":
			return typeTimestamp
		case "DATETIME":
			return typeDatetime
		case "DATE":
			return typeDate
		case "ENUM":
			return typeFixedString
		case "TINYTEXT", "MEDIUMTEXT", "LONGTEXT":
			return typeBlob
		}
		return typeString
	}
}

// Temporal binary rows use MySQL's structured calendar encoding, while ordinary
// text result rows continue to expose the stored canonical SQL string.
func appendTemporal(p []byte, value string, kind byte) []byte {
	if strings.HasPrefix(value, "0000-00-00") {
		return append(p, 0)
	}
	layout := "2006-01-02 15:04:05"
	if kind == typeDate {
		layout = "2006-01-02"
	}
	date, err := time.Parse(layout, value)
	if err != nil {
		return append(p, 0)
	} // Stored temporal values are validated by SQL.
	size := byte(7)
	if kind == typeDate {
		size = 4
	} else if date.Nanosecond() != 0 {
		size = 11
	}
	p = append(p, size)
	p = append16(p, uint16(date.Year()))
	p = append(p, byte(date.Month()), byte(date.Day()))
	if size > 4 {
		p = append(p, byte(date.Hour()), byte(date.Minute()), byte(date.Second()))
	}
	if size == 11 {
		p = append32(p, uint32(date.Nanosecond()/1000))
	}
	return p
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
	if col.Unsigned {
		flags |= 32
	}
	if col.ColType == sql.ColBlob {
		flags |= 16
	}
	if col.Declared == "ENUM" {
		flags |= 256
	}
	if col.AutoIncrement || col.MySQLAutoIncrement {
		flags |= 512
	}
	p = append16(p, charset)
	size := uint32(math.MaxUint32)
	if col.ColType == sql.ColInteger {
		size = 20
		switch wireType(col) {
		case typeTiny:
			size = 4
		case typeShort:
			size = 6
		case typeLong:
			size = 11
		}
		if col.Unsigned && wireType(col) != typeLongLong {
			size--
		}
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
				case typeTiny:
					p = append(p, byte(v.Int))
					continue
				case typeShort:
					p = append16(p, uint16(v.Int))
					continue
				case typeLong:
					p = append32(p, uint32(v.Int))
					continue
				case typeFloat:
					p = append32(p, math.Float32bits(float32(v.Real)))
					continue
				case typeTimestamp, typeDatetime, typeDate:
					p = appendTemporal(p, v.Text, wireType(col))
					continue
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
