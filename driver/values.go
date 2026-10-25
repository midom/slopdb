package driver

import (
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"

	nativesql "slopdb/sql"
)

func checkNamedValue(value *sqldriver.NamedValue) error {
	if value.Name != "" {
		return errors.New("slopdb: named parameters are unsupported; use positional ? parameters")
	}
	normalized, e := normalizeArgument(value.Value, 0)
	if e != nil {
		return e
	}
	value.Value = normalized
	return nil
}
func normalizeArgument(value any, depth int) (any, error) {
	if depth > 8 {
		return nil, errors.New("slopdb: recursive driver.Valuer")
	}
	switch v := value.(type) {
	case nil, nativesql.Value, uint64:
		return value, nil
	case *nativesql.Value:
		if v == nil {
			return nil, nil
		}
		return *v, nil
	case time.Time:
		return v.Format(time.RFC3339Nano), nil
	}
	if valuer, ok := value.(sqldriver.Valuer); ok {
		v := reflect.ValueOf(value)
		if v.Kind() == reflect.Ptr && v.IsNil() {
			return nil, nil
		}
		converted, e := valuer.Value()
		if e != nil {
			return nil, e
		}
		return normalizeArgument(converted, depth+1)
	}
	v := reflect.ValueOf(value)
	if v.IsValid() {
		switch v.Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return v.Uint(), nil
		}
	}
	return sqldriver.DefaultParameterConverter.ConvertValue(value)
}
func argumentValues(values []sqldriver.NamedValue) ([]any, error) {
	if len(values) == 0 {
		return nil, nil
	}
	arguments := make([]any, len(values))
	for i, v := range values {
		if v.Ordinal != i+1 {
			return nil, errors.New("slopdb: invalid parameter ordinal")
		}
		if e := checkNamedValue(&v); e != nil {
			return nil, e
		}
		arguments[i] = v.Value
	}
	return arguments, nil
}
func namedValues(values []sqldriver.Value) []sqldriver.NamedValue {
	out := make([]sqldriver.NamedValue, len(values))
	for i, v := range values {
		out[i] = sqldriver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return out
}
func resultValue(v nativesql.Value) (sqldriver.Value, error) {
	if v.Null {
		return nil, nil
	}
	typ := v.LogicalType()
	if typ.Kind == nativesql.TypeBoolean {
		return v.Int != 0, nil
	}
	switch v.Type {
	case nativesql.ColInteger:
		if typ.Unsigned {
			return strconv.FormatInt(v.Int, 10), nil
		}
		return v.Int, nil
	case nativesql.ColUnsigned:
		return strconv.FormatUint(v.Uint, 10), nil
	case nativesql.ColReal:
		if math.IsNaN(v.Real) || math.IsInf(v.Real, 0) {
			return nil, errors.New("slopdb: non-finite REAL")
		}
		return v.Real, nil
	case nativesql.ColText, nativesql.ColDecimal, nativesql.ColJSON:
		return v.Text, nil
	case nativesql.ColBlob:
		return v.Blob, nil
	}
	return nil, fmt.Errorf("slopdb: unsupported result type %d", v.Type)
}
