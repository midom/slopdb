package mysqlserver

import (
	"encoding/binary"
	"fmt"
	"math"

	"slopdb/db"
	"slopdb/sql"
)

type prepared struct {
	stmt       *db.Stmt
	query      string
	columns    []sql.ColumnDef
	schemaPath []string
	types      []uint16
	long       map[uint16][]byte
	longErr    error
}

func unknownStatement() error { return &protocolError{1243, "HY000", "unknown prepared statement"} }
func (c *client) prepare(query string) error {
	if len(c.statements) >= c.server.config.MaxPreparedStatements {
		return c.writeError(&protocolError{1461, "42000", "too many prepared statements"})
	}
	stmt, err := c.session.PrepareContext(c.ctx, query)
	if err != nil {
		c.session.TrackActivity(c.ctx, query)()
		return c.writeError(err)
	}
	if stmt.NumInput() > math.MaxUint16 {
		_ = stmt.Close()
		return c.writeError(&protocolError{1390, "HY000", "too many placeholders"})
	}
	_, path, err := c.session.NamespaceSettings(c.ctx)
	if err != nil {
		_ = stmt.Close()
		return c.writeError(err)
	}
	columns, err := c.session.DescribePrepared(c.mysqlContext(), stmt)
	if err != nil {
		_ = stmt.Close()
		return c.writeError(err)
	}
	if len(columns) > math.MaxUint16 {
		_ = stmt.Close()
		return c.writeError(fmt.Errorf("too many result columns"))
	}
	for {
		c.nextStmt++
		if c.nextStmt != 0 && c.statements[c.nextStmt] == nil {
			break
		}
	}
	c.statements[c.nextStmt] = &prepared{stmt: stmt, query: query, columns: columns, schemaPath: path}
	// Catalog and AST metadata is available before execution; no user SQL ran.
	p := append32([]byte{0}, c.nextStmt)
	p = append16(p, uint16(len(columns)))
	p = append16(p, uint16(stmt.NumInput()))
	p = append(p, 0)
	p = append16(p, c.warningCount())
	if c.capabilities&clientOptionalMetadata != 0 {
		p = append(p, c.resultMetadataFlag())
	}
	if err := c.writer.write(p); err != nil {
		return err
	}
	if c.sendResultMetadata() {
		for i := 0; i < stmt.NumInput(); i++ {
			if err := c.writeColumnDefinition(sql.ColumnDef{Name: "?", ColType: sql.ColText}, ""); err != nil {
				return err
			}
		}
	}
	if stmt.NumInput() > 0 {
		if err := c.resultMetadataEnd(c.status()); err != nil {
			return err
		}
	}
	if c.sendResultMetadata() {
		for _, col := range columns {
			if err := c.writeColumnDefinition(col, c.selected); err != nil {
				return err
			}
		}
	}
	if len(columns) > 0 {
		return c.resultMetadataEnd(c.status())
	}
	return nil
}
func (c *client) longData(p []byte) error {
	// COM_STMT_SEND_LONG_DATA has no response. Defer errors until EXECUTE.
	if len(p) < 6 {
		return nil
	}
	s := c.statements[binary.LittleEndian.Uint32(p)]
	if s == nil {
		return nil
	}
	param := binary.LittleEndian.Uint16(p[4:])
	if int(param) >= s.stmt.NumInput() {
		s.longErr = errMalformed
		return nil
	}
	if s.longErr != nil {
		return nil
	}
	total := len(p) - 6
	for _, b := range s.long {
		total += len(b)
	}
	if total > c.server.config.MaxPacketBytes {
		s.longErr = fmt.Errorf("long parameter data exceeds packet limit")
		return nil
	}
	if s.long == nil {
		s.long = make(map[uint16][]byte)
	}
	if s.long[param] == nil {
		s.long[param] = make([]byte, 0)
	}
	s.long[param] = append(s.long[param], p[6:]...)
	return nil
}
func (c *client) executePrepared(p []byte) error {
	if len(p) < 9 {
		return c.writeError(errMalformed)
	}
	s := c.statements[binary.LittleEndian.Uint32(p)]
	if s == nil {
		return c.writeError(unknownStatement())
	}
	defer func() { s.long = nil; s.longErr = nil }()
	if s.longErr != nil {
		return c.writeError(s.longErr)
	}
	if p[4] != 0 || binary.LittleEndian.Uint32(p[5:]) != 1 {
		return c.writeError(&protocolError{1235, "42000", "prepared cursors and repeated execution are not supported"})
	}
	args, err := decodeBindings(s, p[9:])
	if err != nil {
		return c.writeError(err)
	}
	c.preparedPath = s.schemaPath
	defer func() { c.preparedPath = nil }()
	result, err := c.query(s.query, s.stmt, args)
	if err != nil {
		return c.writeError(err)
	}
	return c.result(result, true)
}
func decodeBindings(s *prepared, p []byte) ([]any, error) {
	n := s.stmt.NumInput()
	if n == 0 {
		if len(p) != 0 {
			return nil, errMalformed
		}
		return nil, nil
	}
	d := decoder{data: p}
	nulls := d.take((n + 7) / 8)
	fresh := d.u8()
	if fresh > 1 {
		return nil, errMalformed
	}
	if fresh == 1 {
		types := make([]uint16, n)
		for i := range types {
			types[i] = d.u16()
		}
		if d.err != nil {
			return nil, d.err
		}
		s.types = types
	}
	if d.err != nil {
		return nil, d.err
	}
	if len(s.types) != n {
		return nil, errMalformed
	}
	args := make([]any, n)
	for i := range args {
		if nulls[i/8]&(1<<(i%8)) != 0 {
			continue
		}
		if value, ok := s.long[uint16(i)]; ok {
			switch byte(s.types[i]) {
			case 16, 249, 250, 251, 252, 255:
				args[i] = value
			default:
				args[i] = string(value)
			}
			continue
		}
		value, err := decodeValue(&d, s.types[i])
		if err != nil {
			return nil, err
		}
		args[i] = value
	}
	if d.err != nil {
		return nil, d.err
	}
	if len(d.data) != 0 {
		return nil, errMalformed
	}
	return args, nil
}
func decodeValue(d *decoder, typ uint16) (any, error) {
	unsigned := typ&0x8000 != 0
	switch byte(typ) {
	case 6:
		return nil, nil // MYSQL_TYPE_NULL
	case 1:
		v := d.u8()
		if unsigned {
			return int64(v), d.err
		}
		return int64(int8(v)), d.err
	case 2, 13:
		v := d.u16()
		if unsigned {
			return int64(v), d.err
		}
		return int64(int16(v)), d.err
	case 3, 9:
		v := d.u32()
		if unsigned {
			return int64(v), d.err
		}
		return int64(int32(v)), d.err
	case 8:
		v := d.u64()
		if unsigned {
			return sql.NewUnsignedValue(v), d.err
		}
		return int64(v), d.err
	case 4:
		return float64(math.Float32frombits(d.u32())), d.err
	case 5:
		return math.Float64frombits(d.u64()), d.err
	case 0, 15, 16, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255:
		value := d.str()
		switch byte(typ) {
		case 16, 249, 250, 251, 252, 255:
			return value, d.err
		case 0, 246:
			if d.err != nil {
				return nil, d.err
			}
			return sql.NewDecimalValue(string(value))
		case 245:
			if d.err != nil {
				return nil, d.err
			}
			return sql.NewJSONValue(string(value))
		default:
			return string(value), d.err
		}
	case 7, 10, 11, 12:
		return decodeTemporal(d, byte(typ))
	default:
		return nil, fmt.Errorf("unsupported MySQL parameter type %d", byte(typ))
	}
}
func decodeTemporal(d *decoder, typ byte) (any, error) {
	size := int(d.u8())
	value := d.take(size)
	if d.err != nil {
		return nil, d.err
	}
	v := decoder{data: value}
	if typ == 11 { // TIME
		if size == 0 {
			return "00:00:00", nil
		}
		if size != 8 && size != 12 {
			return nil, errMalformed
		}
		negative := v.u8()
		days := v.u32()
		hours, minutes, seconds := v.u8(), v.u8(), v.u8()
		sign := ""
		if negative != 0 {
			sign = "-"
		}
		text := fmt.Sprintf("%s%02d:%02d:%02d", sign, uint64(days)*24+uint64(hours), minutes, seconds)
		if size == 12 {
			text += fmt.Sprintf(".%06d", v.u32())
		}
		return text, v.err
	}
	if size == 0 {
		if typ == 10 {
			return "0000-00-00", nil
		}
		return "0000-00-00 00:00:00", nil
	}
	if size != 4 && size != 7 && size != 11 {
		return nil, errMalformed
	}
	text := fmt.Sprintf("%04d-%02d-%02d", v.u16(), v.u8(), v.u8())
	if size >= 7 {
		text += fmt.Sprintf(" %02d:%02d:%02d", v.u8(), v.u8(), v.u8())
	} else if typ != 10 {
		text += " 00:00:00"
	}
	if size == 11 {
		text += fmt.Sprintf(".%06d", v.u32())
	}
	return text, v.err
}
