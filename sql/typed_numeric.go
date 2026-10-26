package sql

import "fmt"

// The scalar fast paths borrow operands only for this instruction. Other
// numeric types and coercions retain the shared SQL arithmetic/comparison path.
func plainTypedInteger(value *Value) bool {
	return value.Type == ColInteger && !value.EnumValue && !value.HexLiteral
}

func typedIntegerCompare(dst, left, right *Value, op string, postgres bool) bool {
	var result bool
	if left.Null || right.Null {
		switch op {
		case "IS":
			result = left.Null && right.Null
		case "IS NOT":
			result = !(left.Null && right.Null)
		case "=", "<>", "<", "<=", ">", ">=":
			*dst = Value{Null: true}
			if postgres {
				dst.SemanticType = TypeBoolean
			}
			return true
		default:
			return false
		}
	} else {
		if !plainTypedInteger(left) || !plainTypedInteger(right) {
			return false
		}
		a, b := left.Int, right.Int
		switch op {
		case "=", "IS":
			result = a == b
		case "<>", "IS NOT":
			result = a != b
		case "<":
			result = a < b
		case "<=":
			result = a <= b
		case ">":
			result = a > b
		case ">=":
			result = a >= b
		default:
			return false
		}
	}
	*dst = Value{}
	if result {
		dst.Int = 1
	}
	if postgres {
		dst.SemanticType = TypeBoolean
	}
	return true
}

func typedIntegerModulo(dst, left, right *Value) (bool, error) {
	if left.Null || right.Null {
		*dst = Value{Null: true}
		return true, nil
	}
	if !plainTypedInteger(left) || !plainTypedInteger(right) {
		return false, nil
	}
	a, b := left.Int, right.Int
	if b == 0 {
		return true, fmt.Errorf("division by zero")
	}
	// Go's MinInt64 % -1 is zero, matching the existing SQL integer path.
	*dst = Value{Int: a % b}
	return true, nil
}
