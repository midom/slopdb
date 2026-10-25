package sql

import (
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// PostgresColumnOID describes the declared PostgreSQL type without changing the
// native row encoding. Zero means the column has no PostgreSQL declaration.
func PostgresColumnOID(col ColumnDef) uint32 {
	name := strings.ToUpper(strings.TrimSpace(col.Declared))
	if i := strings.IndexByte(name, '('); i >= 0 {
		name = name[:i]
	}
	switch name {
	case "BOOL", "BOOLEAN":
		return 16
	case "BYTEA":
		return 17
	case "BIGINT", "INT8", "BIGSERIAL", "SERIAL8":
		return 20
	case "SMALLINT", "INT2", "SMALLSERIAL", "SERIAL2":
		return 21
	case "INT", "INTEGER", "INT4", "SERIAL", "SERIAL4":
		return 23
	case "TEXT":
		return 25
	case "REAL", "FLOAT4":
		return 700
	case "DOUBLE", "DOUBLE PRECISION", "FLOAT8":
		return 701
	case "VARCHAR", "CHARACTER VARYING":
		return 1043
	case "DATE":
		return 1082
	case "TIME", "TIME WITHOUT TIME ZONE":
		return 1083
	case "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE":
		return 1114
	case "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE":
		return 1184
	case "INTERVAL":
		return 1186
	case "TIMETZ", "TIME WITH TIME ZONE":
		return 1266
	case "NUMERIC", "DECIMAL":
		return 1700
	case "UUID":
		return 2950
	case "JSON":
		return 114
	case "JSONB":
		return 3802
	}
	return 0
}
func PostgresValueOID(v Value) uint32 { return PostgresColumnOID(ColumnDef{Declared: v.PGType}) }

func postgresColumnDefinition(col *ColumnDef) {
	col.Postgres = true
	if col.Declared == "" {
		col.Declared = col.ColType.String()
	}
	switch strings.ToUpper(col.Declared) {
	case "INTEGER", "INT", "INT4", "SERIAL", "SERIAL4":
		col.IntegerBits = 32
	case "SMALLINT", "INT2", "SMALLSERIAL", "SERIAL2":
		col.IntegerBits = 16
	case "BIGINT", "INT8", "BIGSERIAL", "SERIAL8":
		col.IntegerBits = 64
	}
}
func normalizePostgresColumnValue(v Value, col ColumnDef) (Value, error) {
	if v.Null {
		v.PGType = col.Declared
		return v, nil
	}
	if col.ColType == ColDecimal && col.Precision > 0 {
		r, e := normalizeDecimal(v, col)
		r.PGType = col.Declared
		return r, e
	}
	r, err := CastPostgresValue(v, col.Declared)
	if err != nil {
		return NewNullValue(), fmt.Errorf("column %s: %w", col.Name, err)
	}
	r.Binary = col.Binary
	return r, nil
}

// CastPostgresValue performs the conversion used by PostgreSQL casts and typed
// parameter decoding. PGType is transient; tables persist their declaration in
// the catalog and restore it when rows are decoded.
func CastPostgresValue(v Value, target string) (Value, error) {
	name := strings.ToUpper(strings.TrimSpace(target))
	if strings.HasPrefix(name, "PG_CATALOG.") {
		name = strings.TrimPrefix(name, "PG_CATALOG.")
	}
	if v.Null {
		v.PGType = name
		return v, nil
	}
	oid := PostgresColumnOID(ColumnDef{Declared: name})
	result := v
	var err error
	invalid := func() (Value, error) {
		return NewNullValue(), fmt.Errorf("invalid input syntax for type %s: %q", name, v.String())
	}
	switch oid {
	case 16:
		switch v.Type {
		case ColInteger:
			if v.Int == 0 {
				result = NewIntValue(0)
			} else {
				result = NewIntValue(1)
			}
		case ColText:
			text := strings.ToLower(strings.TrimSpace(v.Text))
			yes, no := false, false
			for _, word := range []string{"true", "yes", "on", "1"} {
				if text != "" && strings.HasPrefix(word, text) {
					yes = true
				}
			}
			for _, word := range []string{"false", "no", "off", "0"} {
				if text != "" && strings.HasPrefix(word, text) {
					no = true
				}
			}
			if yes == no {
				return invalid()
			}
			if yes {
				result = NewIntValue(1)
			} else {
				result = NewIntValue(0)
			}
		default:
			return invalid()
		}
	case 20, 21, 23:
		if v.Type == ColText {
			n, e := strconv.ParseInt(strings.TrimSpace(v.Text), 10, 64)
			if e != nil {
				return invalid()
			}
			result = NewIntValue(n)
		} else if v.Type == ColReal {
			rounded := math.RoundToEven(v.Real)
			if math.IsInf(rounded, 0) || math.IsNaN(rounded) || rounded < math.MinInt64 || rounded >= 9223372036854775808.0 {
				return invalid()
			}
			result = NewIntValue(int64(rounded))
		} else {
			result, err = castSQLValue(v, "BIGINT")
		}
		if err == nil && (oid == 21 && (result.Int < math.MinInt16 || result.Int > math.MaxInt16) || oid == 23 && (result.Int < math.MinInt32 || result.Int > math.MaxInt32)) {
			return NewNullValue(), fmt.Errorf("%s out of range", name)
		}
	case 700, 701:
		result, err = castSQLValue(v, "DOUBLE PRECISION")
		if err == nil && oid == 700 {
			f := float32(result.Real)
			if math.IsInf(float64(f), 0) && !math.IsInf(result.Real, 0) {
				return NewNullValue(), fmt.Errorf("REAL out of range")
			}
			result.Real = float64(f)
		}
	case 2950:
		if v.Type == ColBlob && len(v.Blob) == 16 {
			result = NewBlobValue(append([]byte(nil), v.Blob...))
			break
		}
		text := strings.TrimSpace(v.String())
		text = strings.TrimPrefix(strings.TrimSuffix(text, "}"), "{")
		text = strings.ReplaceAll(text, "-", "")
		raw, e := hex.DecodeString(text)
		if e != nil || len(raw) != 16 {
			return invalid()
		}
		result = NewBlobValue(raw)
	case 1082, 1083, 1114, 1184, 1266:
		result, err = postgresTemporalValue(v, name, oid)
	case 17:
		if v.Type == ColBlob {
			result = NewBlobValue(append([]byte(nil), v.Blob...))
		} else {
			result = NewBlobValue([]byte(v.String()))
		}
	case 25, 1043:
		if v.PGType == "BOOLEAN" || v.PGType == "BOOL" {
			if v.Int != 0 {
				result = NewTextValue("true")
			} else {
				result = NewTextValue("false")
			}
		} else if PostgresValueOID(v) == 2950 && len(v.Blob) == 16 {
			text := hex.EncodeToString(v.Blob)
			result = NewTextValue(text[:8] + "-" + text[8:12] + "-" + text[12:16] + "-" + text[16:20] + "-" + text[20:])
		} else {
			result = NewTextValue(v.String())
		}
	default:
		result, err = castSQLValue(v, name)
	}
	if err != nil {
		return NewNullValue(), err
	}
	result.PGType = name
	return result, nil
}

func postgresTemporalValue(v Value, name string, oid uint32) (Value, error) {
	text := strings.TrimSpace(v.String())
	if (oid == 1082 || oid == 1114 || oid == 1184) && (text == "infinity" || text == "-infinity") {
		return NewTextValue(text), nil
	}
	layouts := []string{}
	switch oid {
	case 1082:
		layouts = []string{"2006-01-02", "2006-1-2"}
	case 1083:
		layouts = []string{"15:04:05.999999999", "15:04"}
	case 1266:
		layouts = []string{"15:04:05.999999999Z07:00", "15:04:05.999999999Z07", "15:04Z07:00"}
	default:
		layouts = []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999Z07", "2006-01-02 15:04:05.999999999-07:00:00", "2006-01-02 15:04:05.999999999", "2006-01-02 15:04", "2006-01-02"}
	}
	for _, layout := range layouts {
		t, e := time.Parse(layout, text)
		if e != nil {
			continue
		}
		t = t.Round(time.Microsecond)
		switch oid {
		case 1082:
			return NewTextValue(t.Format("2006-01-02")), nil
		case 1083:
			return NewTextValue(t.Format("15:04:05.999999")), nil
		case 1266:
			return NewTextValue(t.Format("15:04:05.999999-07:00")), nil
		case 1114:
			return NewTextValue(t.Format("2006-01-02 15:04:05.999999")), nil
		case 1184:
			return NewTextValue(t.UTC().Format("2006-01-02 15:04:05.999999") + "+00"), nil
		}
	}
	return NewNullValue(), fmt.Errorf("invalid input syntax for type %s: %q", name, text)
}
