package sql

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Function families share validation, result metadata and optimizer volatility.
// The registry is immutable after package initialization.
type mysqlFunctionSpec struct {
	Min, Max  int // Max < 0 permits a variadic argument list.
	Return    ColumnType
	Metadata  func([]ColumnDef) ColumnDef
	Volatile  bool
	MySQLOnly bool // preserve an existing native function outside MySQL mode
	Eval      func(*Schema, []Value) (Value, error)
}

var mysqlFunctionRegistry map[string]mysqlFunctionSpec

func init() { mysqlFunctionRegistry = buildMySQLFunctionRegistry() }

func buildMySQLFunctionRegistry() map[string]mysqlFunctionSpec {
	out := make(map[string]mysqlFunctionSpec)
	for _, family := range []map[string]mysqlFunctionSpec{mysqlStringFunctions(), mysqlTemporalFunctions(), mysqlNumericFunctions(), mysqlUtilityFunctions(), mysqlSessionFunctions()} {
		for name, spec := range family {
			if _, exists := out[name]; exists {
				panic("duplicate MySQL function: " + name)
			}
			out[name] = spec
		}
	}
	return out
}

func validRegisteredMySQLFunction(name string, n int) (bool, bool) {
	spec, ok := mysqlFunctionRegistry[name]
	return ok && n >= spec.Min && (spec.Max < 0 || n <= spec.Max), ok
}

func (s *Schema) evalRegisteredMySQLFunction(name string, args []Value) (Value, bool, error) {
	spec, ok := mysqlFunctionRegistry[name]
	if !ok {
		return Value{}, false, nil
	}
	if len(args) < spec.Min || spec.Max >= 0 && len(args) > spec.Max {
		return NewNullValue(), true, fmt.Errorf("wrong number of arguments to %s", name)
	}
	value, err := spec.Eval(s, args)
	return value, true, err
}

func (s *Schema) describeRegisteredMySQLFunction(name string, args []ColumnDef) (ColumnDef, bool) {
	spec, ok := mysqlFunctionRegistry[name]
	if !ok || spec.MySQLOnly && !s.mysqlMode {
		return ColumnDef{}, false
	}
	if spec.Metadata != nil {
		return spec.Metadata(args), true
	}
	return ColumnDef{ColType: spec.Return, Unsigned: spec.Return == ColUnsigned}, true
}

func mysqlAnyNull(args []Value) bool {
	for _, v := range args {
		if v.Null {
			return true
		}
	}
	return false
}

// Check before allocating repeated/padded/generated output, including overflow.
func mysqlFunctionSize(s *Schema, n uint64) error {
	if n > uint64(s.memoryBudget()) {
		return fmt.Errorf("MySQL function result exceeds query memory budget")
	}
	return nil
}

// MySQL scalar functions accept a leading numeric prefix in string arguments.
func mysqlFunctionNumber(v Value) Value {
	if v.HexLiteral && v.Type == ColBlob {
		return mysqlHexNumber(v)
	}
	if v.Null || isNumeric(v) {
		return v
	}
	text := strings.TrimSpace(mysqlStringValue(v))
	i := 0
	if i < len(text) && (text[i] == '+' || text[i] == '-') {
		i++
	}
	digits := 0
	for i < len(text) && text[i] >= '0' && text[i] <= '9' {
		i++
		digits++
	}
	if i < len(text) && text[i] == '.' {
		i++
		for i < len(text) && text[i] >= '0' && text[i] <= '9' {
			i++
			digits++
		}
	}
	if digits == 0 {
		return NewIntValue(0)
	}
	if i < len(text) && (text[i] == 'e' || text[i] == 'E') {
		start := i
		i++
		if i < len(text) && (text[i] == '+' || text[i] == '-') {
			i++
		}
		exponent := i
		for i < len(text) && text[i] >= '0' && text[i] <= '9' {
			i++
		}
		if exponent == i {
			i = start
		}
	}
	value, err := NewDecimalValue(text[:i])
	if err != nil {
		return NewIntValue(0)
	}
	return value
}

func mysqlFunctionInt(v Value) int64 {
	if v.HexLiteral && v.Type == ColBlob {
		v = mysqlHexNumber(v)
	}
	switch v.Type {
	case ColInteger, ColUnsigned:
		return integerValue(v)
	case ColReal:
		return integerValue(NewRealValue(math.RoundToEven(v.Real)))
	case ColDecimal:
		n, scale, err := valueDecimal(v)
		if err != nil {
			return 0
		}
		n = roundDecimal(n, scale, 0)
		return integerValue(Value{Type: ColDecimal, Text: n.String()})
	default:
		text := strings.TrimSpace(mysqlStringValue(v))
		i := 0
		if i < len(text) && (text[i] == '+' || text[i] == '-') {
			i++
		}
		start := i
		for i < len(text) && text[i] >= '0' && text[i] <= '9' {
			i++
		}
		if i == start {
			return 0
		}
		n, err := strconv.ParseInt(text[:i], 10, 64)
		if err != nil && !strings.Contains(err.Error(), "value out of range") {
			return 0
		}
		return n
	}
}

func mysqlHexNumber(v Value) Value {
	if len(v.Blob) > 8 {
		return NewUnsignedValue(0)
	}
	var n uint64
	for _, b := range v.Blob {
		n = n<<8 | uint64(b)
	}
	return NewUnsignedValue(n)
}
