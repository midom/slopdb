package sql

import (
	"math"
	"strconv"
	"strings"
)

var mysqlFunctionRegistry map[string]FunctionSpec

func buildMySQLFunctionRegistry() map[string]FunctionSpec {
	return buildFunctionRegistry(DialectMySQL, mysqlStringFunctions(), mysqlTemporalFunctions(), mysqlNumericFunctions(), mysqlUtilityFunctions(), mysqlSessionFunctions())
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
