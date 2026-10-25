package sql

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

var mysqlNumericPrefix = regexp.MustCompile(`^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?`)

type mysqlIgnoreKey struct{}

// IGNORE downgrades assignment failures and duplicate-key errors to warnings,
// preserving the other connection options (including NO_AUTO_VALUE_ON_ZERO).
func mysqlIgnoreContext(ctx context.Context) context.Context {
	options, _ := MySQLSession(ctx)
	modes := strings.Split(options.SQLMode, ",")
	kept := modes[:0]
	for _, mode := range modes {
		if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(mode)), "STRICT_") {
			kept = append(kept, mode)
		}
	}
	options.SQLMode = strings.Join(kept, ",")
	return context.WithValue(WithMySQLSession(ctx, options), mysqlIgnoreKey{}, true)
}

func (s *Schema) mysqlNonStrict() bool {
	options, ok := MySQLSession(s.ctx)
	return ok && !strings.Contains(strings.ToUpper(options.SQLMode), "STRICT_")
}
func (s *Schema) mysqlWarning(code uint16, message string) {
	if options, ok := MySQLSession(s.ctx); ok && options.Warning != nil {
		options.Warning(code, message)
	}
}
func (s *Schema) mysqlAssignmentValue(v Value, col ColumnDef) (Value, error) {
	if v.Null {
		if !col.NotNull || col.AutoIncrement || !s.mysqlNonStrict() {
			return v, nil
		}
		s.mysqlWarning(1048, "Column "+col.Name+" cannot be null")
		switch col.ColType {
		case ColInteger:
			v = NewIntValue(0)
		case ColReal:
			v = NewRealValue(0)
		case ColDecimal:
			v = NewIntValue(0)
		case ColBlob:
			v = NewBlobValue(nil)
		default:
			v = NewTextValue("")
			if len(col.EnumValues) > 0 {
				v = NewTextValue(col.EnumValues[0])
			}
		}
	}
	if col.ColType == ColInteger {
		return s.mysqlIntegerAssignment(v, col)
	}
	if col.ColType == ColDecimal {
		result, err := normalizeDecimal(v, col)
		if err != nil && s.mysqlNonStrict() {
			s.mysqlWarning(1264, "Out of range or incorrect DECIMAL value for column "+col.Name)
			n, _, parseErr := valueDecimal(v)
			if parseErr != nil {
				return normalizeDecimal(NewIntValue(0), col)
			}
			limit := new(big.Int).Sub(decimalPower(col.Precision), big.NewInt(1))
			if n.Sign() < 0 && !col.Unsigned {
				limit.Neg(limit)
			} else if n.Sign() < 0 {
				limit.SetInt64(0)
			}
			return Value{Type: ColDecimal, Text: decimalText(limit, col.Scale)}, nil
		}
		return result, err
	}
	nonstrict := s.mysqlNonStrict()
	warn := func(code uint16, message string) { s.mysqlWarning(code, message+" for column "+col.Name) }
	if col.ColType == ColInteger || col.ColType == ColReal {
		if v.EnumValue {
			v = NewIntValue(v.EnumOrdinal)
		}
		if v.Type == ColText {
			text := strings.TrimSpace(v.Text)
			if i, err := strconv.ParseInt(text, 10, 64); err == nil {
				v = NewIntValue(i)
			} else if f, err := strconv.ParseFloat(text, 64); err == nil {
				v = NewRealValue(f)
			} else if nonstrict {
				prefix := mysqlNumericPrefix.FindString(text)
				f := float64(0)
				if prefix != "" {
					f, _ = strconv.ParseFloat(prefix, 64)
				}
				v = NewRealValue(f)
				warn(1366, "Incorrect numeric value")
			}
		}
		if col.ColType == ColInteger && v.Type == ColReal {
			if math.IsNaN(v.Real) || math.IsInf(v.Real, 0) {
				return v, fmt.Errorf("numeric value out of range for %s", col.Name)
			}
			f := math.Round(v.Real)
			if f >= 9223372036854775808.0 || f < math.MinInt64 {
				if !nonstrict {
					return v, fmt.Errorf("integer value out of range for %s", col.Name)
				}
				if f < 0 {
					v = NewIntValue(math.MinInt64)
				} else {
					v = NewIntValue(math.MaxInt64)
				}
				warn(1264, "Out of range value")
			} else {
				v = NewIntValue(int64(f))
			}
		}
		if nonstrict && v.Type == ColInteger {
			min, max := int64(math.MinInt64), int64(math.MaxInt64)
			if col.IntegerBits > 0 && col.IntegerBits < 64 {
				max = (int64(1) << (col.IntegerBits - 1)) - 1
				min = -(int64(1) << (col.IntegerBits - 1))
				if col.Unsigned {
					min = 0
					max = (int64(1) << col.IntegerBits) - 1
				}
			} else if col.Unsigned {
				min = 0
			}
			if v.Int < min {
				v.Int = min
				warn(1264, "Out of range value")
			}
			if v.Int > max {
				v.Int = max
				warn(1264, "Out of range value")
			}
		}
		if nonstrict && col.Unsigned && v.Type == ColReal && v.Real < 0 {
			v.Real = 0
			warn(1264, "Out of range value")
		}
		if nonstrict && col.Declared == "FLOAT" {
			f := toFloat(v)
			if math.Abs(f) > math.MaxFloat32 {
				v = NewRealValue(math.Copysign(math.MaxFloat32, f))
				warn(1264, "Out of range value")
			}
		}
	}
	if nonstrict && len(col.EnumValues) > 0 {
		valid := false
		if v.Type == ColInteger {
			valid = v.Int >= 1 && v.Int <= int64(len(col.EnumValues))
		} else if v.Type == ColText {
			for _, x := range col.EnumValues {
				valid = valid || v.Text == x
			}
			valid = valid || v.EnumValue && v.EnumOrdinal == 0 && v.Text == ""
			if ordinal, err := strconv.ParseInt(v.Text, 10, 64); err == nil && ordinal >= 1 && ordinal <= int64(len(col.EnumValues)) {
				valid = true
			}
		}
		if !valid {
			v = NewTextValue("")
			v.EnumValue = true
			warn(1265, "Data truncated")
		}
	}
	if nonstrict && col.Length > 0 {
		if col.ColType == ColBlob && (v.Type == ColBlob || v.Type == ColText) {
			data := v.Blob
			if v.Type == ColText {
				data = []byte(v.Text)
			}
			if int64(len(data)) > col.Length {
				v = NewBlobValue(copyBytes(data[:int(col.Length)]))
				warn(1265, "Data truncated")
			}
		}
		if col.ColType == ColText && v.Type == ColText {
			if mysqlTextByteLength(col) {
				if int64(len(v.Text)) > col.Length {
					cutoff := int(col.Length)
					for cutoff > 0 && !utf8.RuneStart(v.Text[cutoff]) {
						cutoff--
					}
					v.Text = v.Text[:cutoff]
					warn(1265, "Data truncated")
				}
			} else if int64(utf8.RuneCountInString(v.Text)) > col.Length {
				v.Text = string([]rune(v.Text)[:int(col.Length)])
				warn(1265, "Data truncated")
			}
		}
	}
	if nonstrict && (col.Declared == "TIMESTAMP" || col.Declared == "DATETIME" || col.Declared == "DATE") {
		if _, err := normalizeColumnValue(v, col); err != nil {
			if col.Declared == "DATE" {
				v = NewTextValue("0000-00-00")
			} else {
				v = NewTextValue("0000-00-00 00:00:00")
			}
			warn(1292, "Incorrect datetime value")
		}
	}
	return v, nil
}
func mysqlTextByteLength(col ColumnDef) bool {
	return col.Declared == "TINYTEXT" || col.Declared == "MEDIUMTEXT" || col.Declared == "LONGTEXT"
}
