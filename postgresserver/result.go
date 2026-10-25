package postgresserver

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgproto3"
	"slopdb/db"
	"slopdb/sql"
)

type protocolError struct{ code, message string }

func (e *protocolError) Error() string { return e.message }
func (c *client) ready() error {
	state := byte('I')
	if c.explicitTransaction || c.session != nil && c.session.InTransaction() {
		state = 'T'
		if c.failed {
			state = 'E'
		}
	} else {
		c.failed = false
		c.clearPortals()
	}
	return c.send(&pgproto3.ReadyForQuery{TxStatus: state})
}
func (c *client) writeError(err error) error {
	code := "XX000"
	var p *protocolError
	var parse *sql.ParseError
	switch {
	case errors.Is(err, db.ErrDurabilityUnconfirmed):
		code = "40003"
	case errors.Is(err, errMalformed):
		code = "08P01"
	case errors.As(err, &p):
		code = p.code
	case errors.As(err, &parse):
		code = "42601"
	case errors.Is(err, db.ErrDeadlock):
		code = "40P01"
	case errors.Is(err, db.ErrConflict), errors.Is(err, db.ErrTransactionAborted):
		code = "40001"
	case errors.Is(err, db.ErrLockTimeout), errors.Is(err, db.ErrLockNotAvailable):
		code = "55P03"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		code = "57014"
	default:
		message := strings.ToLower(err.Error())
		switch {
		case strings.Contains(message, "unique constraint"):
			code = "23505"
		case strings.Contains(message, "not null"):
			code = "23502"
		case strings.Contains(message, "foreign key"):
			code = "23503"
		case strings.Contains(message, "check constraint"):
			code = "23514"
		case strings.Contains(message, "does not exist") && strings.Contains(message, "table"):
			code = "42P01"
		case strings.Contains(message, "division by zero"):
			code = "22012"
		case strings.Contains(message, "overflow") || strings.Contains(message, "out of range"):
			code = "22003"
		case strings.Contains(message, "invalid input syntax"):
			code = "22P02"
		case strings.Contains(message, "datatype mismatch"):
			code = "42804"
		case strings.Contains(message, "parse") || strings.Contains(message, "expected"):
			code = "42601"
		}
	}
	if c.explicitTransaction || c.session != nil && c.session.InTransaction() {
		c.failed = true
	}
	response := &pgproto3.ErrorResponse{Severity: "ERROR", SeverityUnlocalized: "ERROR", Code: code, Message: err.Error()}
	if parse != nil && c.reportPositions {
		response.Position = int32(len([]rune(c.currentSQL[:min(parse.ByteOffset, len(c.currentSQL))])) + 1)
	}
	return c.send(response)
}
func fieldDescription(fields []field, formats []int16) *pgproto3.RowDescription {
	descriptions := make([]pgproto3.FieldDescription, len(fields))
	for i, f := range fields {
		size := int16(-1)
		switch f.oid {
		case oidBool:
			size = 1
		case oidInt2:
			size = 2
		case oidInt4, oidFloat4, oidDate, 26, 2205, 2206, 4089:
			size = 4
		case oidInt8, oidFloat8, oidTime, oidTimestamp, oidTimestamptz:
			size = 8
		case oidInterval, oidUUID:
			size = 16
		case oidTimetz:
			size = 12
		}
		descriptions[i] = pgproto3.FieldDescription{Name: []byte(f.name), DataTypeOID: f.oid, DataTypeSize: size, TypeModifier: -1, Format: formatAt(formats, i)}
	}
	return &pgproto3.RowDescription{Fields: descriptions}
}
func formatAt(formats []int16, index int) int16 {
	if len(formats) == 0 {
		return 0
	}
	if len(formats) == 1 {
		return formats[0]
	}
	return formats[index]
}
func validateFormats(formats []int16, n int) error {
	if len(formats) > 1 && len(formats) != n {
		return &protocolError{"08P01", "format count does not match column or parameter count"}
	}
	for _, v := range formats {
		if v != 0 && v != 1 {
			return errMalformed
		}
	}
	return nil
}
func valueText(v sql.Value) []byte {
	switch v.Type {
	case sql.ColInteger:
		return strconv.AppendInt(nil, v.Int, 10)
	case sql.ColUnsigned:
		return strconv.AppendUint(nil, v.Uint, 10)
	case sql.ColReal:
		return strconv.AppendFloat(nil, v.Real, 'g', -1, 64)
	case sql.ColBlob:
		return []byte("\\x" + hex.EncodeToString(v.Blob))
	default:
		return []byte(v.Text)
	}
}
func validateValue(v sql.Value, oid uint32) error {
	if v.Null {
		return nil
	}
	bad := &protocolError{"0A000", fmt.Sprintf("result type changed (%s, OID %d); prepare the query again or use text projection", v.Type.String(), oid)}
	switch oid {
	case oidBool:
		if v.Type != sql.ColInteger || (v.Int != 0 && v.Int != 1) {
			return bad
		}
	case oidInt2, oidInt4, oidInt8:
		if v.Type != sql.ColInteger {
			return bad
		}
		if oid == oidInt2 && (v.Int < math.MinInt16 || v.Int > math.MaxInt16) || oid == oidInt4 && (v.Int < math.MinInt32 || v.Int > math.MaxInt32) {
			return &protocolError{"22003", "integer out of range"}
		}
	case oidFloat4, oidFloat8:
		if v.Type != sql.ColReal && v.Type != sql.ColInteger && v.Type != sql.ColUnsigned && v.Type != sql.ColDecimal {
			return bad
		}
	case oidBytea:
		if v.Type != sql.ColBlob {
			return bad
		}
	case oidNumeric:
		if v.Type != sql.ColInteger && v.Type != sql.ColUnsigned && v.Type != sql.ColDecimal {
			return bad
		}
	case oidJSON, oidJSONB:
		if v.Type != sql.ColJSON {
			return bad
		}
	}
	return nil
}
func wireValue(v sql.Value, oid uint32, binaryFormat bool) ([]byte, error) {
	if !v.Null && (oid == oidFloat4 || oid == oidFloat8) && v.Type != sql.ColReal {
		target := "DOUBLE PRECISION"
		if oid == oidFloat4 {
			target = "REAL"
		}
		converted, err := sql.CastPostgresValue(v, target)
		if err != nil {
			return nil, err
		}
		v = converted
	}
	if v.Null {
		return nil, nil
	}
	if err := validateValue(v, oid); err != nil {
		return nil, err
	}
	bad := func() ([]byte, error) {
		return nil, &protocolError{"0A000", "result type changed; prepare the query again or use text projection"}
	}
	switch oid {
	case 26, 2205, 2206, 4089:
		if v.Type != sql.ColInteger || v.Int < 0 || v.Int > math.MaxUint32 {
			return nil, &protocolError{"22003", "OID out of range"}
		}
		if binaryFormat {
			return binary.BigEndian.AppendUint32(nil, uint32(v.Int)), nil
		}
		return strconv.AppendInt(nil, v.Int, 10), nil
	case oidUUID, oidDate, oidTime, oidTimestamp, oidTimestamptz, oidInterval, oidTimetz:
		return encodePostgresTypedValue(v, oid, binaryFormat)
	case oidNumeric:
		text := string(valueText(v))
		if binaryFormat {
			return encodeNumeric(text)
		}
		return []byte(text), nil
	case oidJSON, oidJSONB:
		if oid == oidJSONB && binaryFormat {
			return append([]byte{1}, []byte(v.Text)...), nil
		}
		return []byte(v.Text), nil
	case oidBool:
		if v.Type != sql.ColInteger || (v.Int != 0 && v.Int != 1) {
			return bad()
		}
		if binaryFormat {
			return []byte{byte(v.Int)}, nil
		}
		if v.Int == 1 {
			return []byte("t"), nil
		}
		return []byte("f"), nil
	case oidInt2, oidInt4, oidInt8:
		if v.Type != sql.ColInteger {
			return bad()
		}
		if oid == oidInt2 && (v.Int < math.MinInt16 || v.Int > math.MaxInt16) || oid == oidInt4 && (v.Int < math.MinInt32 || v.Int > math.MaxInt32) {
			return nil, &protocolError{"22003", "integer out of range"}
		}
		if !binaryFormat {
			return strconv.AppendInt(nil, v.Int, 10), nil
		}
		switch oid {
		case oidInt2:
			return binary.BigEndian.AppendUint16(nil, uint16(v.Int)), nil
		case oidInt4:
			return binary.BigEndian.AppendUint32(nil, uint32(v.Int)), nil
		default:
			return binary.BigEndian.AppendUint64(nil, uint64(v.Int)), nil
		}
	case oidFloat4, oidFloat8:
		if v.Type != sql.ColReal {
			return bad()
		}
		if !binaryFormat {
			return strconv.AppendFloat(nil, v.Real, 'g', -1, 64), nil
		}
		if oid == oidFloat4 {
			return binary.BigEndian.AppendUint32(nil, math.Float32bits(float32(v.Real))), nil
		}
		return binary.BigEndian.AppendUint64(nil, math.Float64bits(v.Real)), nil
	case oidBytea:
		if v.Type != sql.ColBlob {
			return bad()
		}
		if binaryFormat {
			return append([]byte{}, v.Blob...), nil
		}
		return valueText(v), nil
	default:
		return valueText(v), nil
	}
}
func resultFields(result *db.ExecResult) []field {
	fields := make([]field, len(result.Columns))
	for i, col := range result.Columns {
		oid := nativeOID(col.ColType)
		if col.SemanticType != sql.TypeUnknown {
			oid = sql.PostgresOID(col.LogicalType())
			if oid == 0 {
				oid = oidText
			}
			fields[i] = field{col.ResultName(), oid}
			continue
		}
		if col.Postgres {
			if typed := sql.PostgresColumnOID(col); typed != 0 {
				oid = typed
				fields[i] = field{col.ResultName(), oid}
				continue
			}
		}
		seen := false
		for _, row := range result.Rows {
			v := row[col.Name]
			if v.Null {
				if typed := sql.PostgresValueOID(v); typed != 0 && !seen {
					oid, seen = typed, true
				}
				continue
			}
			if !seen {
				oid = nativeOID(v.Type)
				if typed := sql.PostgresValueOID(v); typed != 0 {
					oid = typed
				}
				seen = true
			} else if typed := sql.PostgresValueOID(v); typed != 0 && oid != typed || typed == 0 && oid != nativeOID(v.Type) {
				oid = oidText
				break
			}
		}
		fields[i] = field{col.ResultName(), oid}
	}
	return fields
}
func (c *client) row(row map[string]sql.Value, columns []sql.ColumnDef, fields []field, formats []int16) error {
	values := make([][]byte, len(fields))
	for i, f := range fields {
		v, ok := row[columns[i].Name]
		if !ok {
			v = sql.NewNullValue()
		}
		value, err := wireValue(v, f.oid, formatAt(formats, i) == 1)
		if err != nil {
			return err
		}
		values[i] = value
	}
	return c.send(&pgproto3.DataRow{Values: values})
}
func completion(kind string, result *db.ExecResult) string {
	if result == nil {
		return kind
	}
	switch kind {
	case "INSERT", "REPLACE":
		return fmt.Sprintf("INSERT 0 %d", result.Changes)
	case "UPDATE", "DELETE":
		return fmt.Sprintf("%s %d", kind, result.Changes)
	case "SELECT", "WITH", "EXPLAIN":
		return fmt.Sprintf("SELECT %d", len(result.Rows))
	case "START":
		return "BEGIN"
	case "END":
		return "COMMIT"
	}
	return kind
}
func decodeValue(data []byte, oid uint32, binaryFormat bool) (any, error) {
	if data == nil {
		return nil, nil
	}
	textual := !binaryFormat || oid == oidText || oid == oidVarchar || oid == 19 || oid == oidJSON || oid == oidJSONB
	if textual && (!utf8.Valid(data) || bytesContainZero(data)) {
		return nil, &protocolError{"22021", "invalid byte sequence for PostgreSQL UTF8 text"}
	}
	invalid := func() (any, error) { return nil, &protocolError{"22P02", "invalid PostgreSQL parameter value"} }
	if oid == 26 || oid == 2205 || oid == 2206 || oid == 4089 {
		var n uint64
		var err error
		if binaryFormat {
			if len(data) != 4 {
				return invalid()
			}
			n = uint64(binary.BigEndian.Uint32(data))
		} else {
			n, err = strconv.ParseUint(string(data), 10, 32)
		}
		if err != nil {
			return invalid()
		}
		return sql.Value{Type: sql.ColInteger, Int: int64(n), SemanticType: sql.TypeObjectID}, nil
	}
	if postgresOIDName(oid) != "" {
		return decodePostgresTypedValue(data, oid, binaryFormat)
	}
	if binaryFormat {
		switch oid {
		case oidNumeric:
			return decodeNumeric(data)
		case oidJSON:
			return sql.NewJSONValue(string(data))
		case oidJSONB:
			if len(data) == 0 || data[0] != 1 {
				return invalid()
			}
			return sql.NewJSONValue(string(data[1:]))
		case oidBool:
			if len(data) != 1 || data[0] > 1 {
				return invalid()
			}
			return sql.Value{Type: sql.ColInteger, Int: int64(data[0]), SemanticType: sql.TypeBoolean}, nil
		case oidInt2:
			if len(data) != 2 {
				return invalid()
			}
			return int64(int16(binary.BigEndian.Uint16(data))), nil
		case oidInt4:
			if len(data) != 4 {
				return invalid()
			}
			return int64(int32(binary.BigEndian.Uint32(data))), nil
		case oidInt8:
			if len(data) != 8 {
				return invalid()
			}
			return int64(binary.BigEndian.Uint64(data)), nil
		case oidFloat4:
			if len(data) != 4 {
				return invalid()
			}
			v := float64(math.Float32frombits(binary.BigEndian.Uint32(data)))
			if math.IsInf(v, 0) || math.IsNaN(v) {
				return invalid()
			}
			return v, nil
		case oidFloat8:
			if len(data) != 8 {
				return invalid()
			}
			v := math.Float64frombits(binary.BigEndian.Uint64(data))
			if math.IsInf(v, 0) || math.IsNaN(v) {
				return invalid()
			}
			return v, nil
		case oidBytea:
			return append([]byte{}, data...), nil
		case oidText, oidVarchar, 19:
			return string(data), nil
		default:
			return nil, &protocolError{"0A000", "unsupported binary parameter type"}
		}
	}
	switch oid {
	case oidNumeric:
		return sql.NewDecimalValue(string(data))
	case oidJSON, oidJSONB:
		return sql.NewJSONValue(string(data))
	case oidBool:
		v, e := sql.CastPostgresValue(sql.NewTextValue(string(data)), "BOOLEAN")
		if e != nil {
			return invalid()
		}
		return v, nil
	case oidInt2, oidInt4, oidInt8:
		bits := 64
		if oid == oidInt2 {
			bits = 16
		}
		if oid == oidInt4 {
			bits = 32
		}
		v, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, bits)
		if err != nil {
			return invalid()
		}
		return v, nil
	case oidFloat4, oidFloat8:
		bits := 64
		if oid == oidFloat4 {
			bits = 32
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(string(data)), bits)
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			return invalid()
		}
		return v, nil
	case oidBytea:
		v, e := sql.ParsePostgresBytea(string(data))
		if e != nil {
			return invalid()
		}
		return v, nil
	default:
		return string(data), nil
	}
}

func bytesContainZero(data []byte) bool {
	for _, b := range data {
		if b == 0 {
			return true
		}
	}
	return false
}

// parameterValue retains semantic identity through the protocol-neutral binder;
// raw codecs remain independently testable using ordinary Go scalar values.
func parameterValue(decoded any, oid uint32) (sql.Value, error) {
	var v sql.Value
	switch x := decoded.(type) {
	case nil:
		v = sql.NewNullValue()
	case sql.Value:
		v = x
	case int64:
		v = sql.NewIntValue(x)
	case float64:
		v = sql.NewRealValue(x)
	case string:
		v = sql.NewTextValue(x)
	case []byte:
		v = sql.Value{Type: sql.ColBlob, Blob: x}
	default:
		return sql.Value{}, &protocolError{"22P02", "unsupported decoded parameter type"}
	}
	return v.WithSemanticType(sql.PostgresTypeDefinition(oid).LogicalType().Kind), nil
}
