package sql

import (
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// PostgresColumnOID describes the declared PostgreSQL type without changing the
// native row encoding. Zero means the column has no PostgreSQL declaration.
func PostgresColumnOID(col ColumnDef) uint32 {
	if col.Unsigned && col.ColType == ColInteger {
		return 1700
	}
	name := strings.ToUpper(strings.TrimSpace(col.Declared))
	if i := strings.IndexByte(name, '('); i >= 0 {
		if j := strings.IndexByte(name[i:], ')'); j >= 0 {
			name = name[:i] + name[i+j+1:]
		}
	}
	switch name {
	case "NAME":
		return 19
	case "OID", "REGCLASS", "REGTYPE", "REGNAMESPACE":
		switch name {
		case "OID":
			return 26
		case "REGCLASS":
			return 2205
		case "REGTYPE":
			return 2206
		default:
			return 4089
		}
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
	if col.ColType == ColDecimal && !col.TypeModifiers {
		col.Precision, col.Scale = 0, 0
	}
	if col.Declared == "" {
		col.Declared = col.ColType.String()
		if col.ColType == ColText && col.Length > 0 {
			col.Declared = "VARCHAR"
		}
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
	if col.Unsigned {
		copy := col
		copy.Postgres = false
		r, e := normalizeColumnValue(v, copy)
		r.PGType = "NUMERIC"
		return r, e
	}
	if v.Null {
		v.PGType = col.Declared
		return v, nil
	}
	if col.ColType == ColDecimal && col.Precision > 0 {
		r, e := normalizeDecimal(v, col)
		r.PGType = col.Declared
		return r, e
	}
	target := col.Declared
	if col.TypeModifiers && col.TemporalPrecision >= 0 {
		switch PostgresColumnOID(col) {
		case 1083, 1114, 1184, 1266:
			target = postgresTemporalTypeModifier(target, col.TemporalPrecision)
		}
	}
	r, err := CastPostgresValue(v, target)
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
	case 20, 21, 23, 26, 2205, 2206, 4089:
		if v.NumericText != "" {
			v, err = NewDecimalValue(v.NumericText)
			if err != nil {
				return NewNullValue(), err
			}
		}
		if v.Type == ColDecimal {
			n, scale, e := valueDecimal(v)
			if e != nil {
				return NewNullValue(), e
			}
			den := decimalPower(scale)
			q, rem := new(big.Int), new(big.Int)
			q.QuoRem(n, den, rem)
			if new(big.Int).Lsh(new(big.Int).Abs(rem), 1).Cmp(den) >= 0 {
				q.Add(q, big.NewInt(int64(n.Sign())))
			}
			if !q.IsInt64() {
				return NewNullValue(), fmt.Errorf("%s out of range", name)
			}
			v = NewIntValue(q.Int64())
		}
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
		if err == nil && (oid == 26 || oid == 2205 || oid == 2206 || oid == 4089) && (result.Int < 0 || result.Int > math.MaxUint32) {
			return NewNullValue(), fmt.Errorf("%s out of range", name)
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
		if strings.HasPrefix(text, "{") && strings.HasSuffix(text, "}") {
			text = text[1 : len(text)-1]
		}
		digits := strings.Builder{}
		previousHyphen := false
		for _, c := range text {
			if c == '-' {
				if previousHyphen || digits.Len() == 0 || digits.Len() >= 32 || digits.Len()%4 != 0 {
					return invalid()
				}
				previousHyphen = true
				continue
			}
			if !strings.ContainsRune("0123456789abcdefABCDEF", c) {
				return invalid()
			}
			digits.WriteRune(c)
			previousHyphen = false
		}
		raw, e := hex.DecodeString(digits.String())
		if e != nil || len(raw) != 16 || previousHyphen {
			return invalid()
		}
		result = NewBlobValue(raw)
	case 1186:
		m, d, u, e := ParsePostgresInterval(v.String())
		if e != nil {
			return NewNullValue(), e
		}
		result = NewTextValue(FormatPostgresInterval(m, d, u))
	case 1082, 1083, 1114, 1184, 1266:
		result, err = postgresTemporalValue(v, name, oid)
	case 17:
		if v.Type == ColBlob {
			result = NewBlobValue(append([]byte(nil), v.Blob...))
		} else {
			raw, e := ParsePostgresBytea(v.String())
			if e != nil {
				return NewNullValue(), e
			}
			result = NewBlobValue(raw)
		}
	case 19, 25, 1043:
		if typ, length, ok := varyingCastType(name); ok && typ == ColText {
			result, err = CastPostgresValue(v, "TEXT")
			if err != nil {
				return NewNullValue(), err
			}
			chars := []rune(result.Text)
			if int64(len(chars)) > length {
				result.Text = string(chars[:int(length)])
			}
			break
		}
		if v.PGType == "BOOLEAN" || v.PGType == "BOOL" {
			if v.Int != 0 {
				result = NewTextValue("true")
			} else {
				result = NewTextValue("false")
			}
		} else if PostgresValueOID(v) == 17 && v.Type == ColBlob {
			result = NewTextValue("\\x" + hex.EncodeToString(v.Blob))
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
	if oid == 19 && len(result.Text) > 63 {
		result.Text = result.Text[:63]
		for !utf8.ValidString(result.Text) {
			result.Text = result.Text[:len(result.Text)-1]
		}
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
		layouts = []string{"2006-01-02", "2006-1-2", "2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999Z07"}
	case 1083:
		if text == "24:00" || text == "24:00:00" || strings.HasPrefix(text, "24:00:00.") && strings.Trim(text[len("24:00:00."):], "0") == "" {
			return NewTextValue("24:00:00"), nil
		}
		layouts = []string{"15:04:05.999999999", "15:04"}
	case 1266:
		layouts = []string{"15:04:05.999999999Z07:00", "15:04:05.999999999Z07", "15:04Z07:00"}
	default:
		layouts = []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999Z07", "2006-01-02 15:04:05.999999999-07:00:00", "2006-01-02 15:04:05.999999999", "2006-01-02 15:04", "2006-01-02"}
	}
	precision := 6
	if i := strings.IndexByte(name, '('); i >= 0 {
		if _, e := fmt.Sscanf(name[i+1:], "%d", &precision); e != nil || precision < 0 || precision > 6 {
			return NewNullValue(), fmt.Errorf("invalid time precision")
		}
	}
	for _, layout := range layouts {
		t, e := time.Parse(layout, text)
		if e != nil {
			continue
		}
		t = t.Round(time.Duration(math.Pow10(9 - precision)))
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

func postgresTemporalTypeModifier(name string, precision int) string {
	if strings.Contains(name, "(") {
		return name
	}
	if i := strings.IndexByte(name, ' '); i >= 0 {
		return fmt.Sprintf("%s(%d)%s", name[:i], precision, name[i:])
	}
	return fmt.Sprintf("%s(%d)", name, precision)
}
func assignmentAffinity(v Value, col ColumnDef) Value {
	if col.Postgres {
		return v
	}
	return applyAffinity(v, col.ColType)
}

func postgresDescribeType(name string) ColumnDef {
	c := describeType(name)
	if PostgresColumnOID(ColumnDef{Declared: name}) != 0 {
		c.Declared = strings.ToUpper(name)
	}
	postgresColumnDefinition(&c)
	return c
}
