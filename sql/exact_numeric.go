package sql

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
)

const maxDecimalDigits = 4096

// Extended values own immutable strings. Arithmetic uses private big.Ints;
// copying a Value never shares a mutable arbitrary-precision accumulator.
func NewUnsignedValue(n uint64) Value { return Value{Type: ColUnsigned, Uint: n} }

func negateExactValue(v Value) (Value, error) {
	if v.Type == ColUnsigned {
		if v.Uint > uint64(math.MaxInt64)+1 {
			return NewNullValue(), fmt.Errorf("integer overflow")
		}
		return NewIntValue(-int64(v.Uint)), nil
	}
	n, s, err := valueDecimal(v)
	if err != nil {
		return NewNullValue(), err
	}
	n.Neg(n)
	return Value{Type: ColDecimal, Text: decimalText(n, s)}, nil
}

func (p *Parser) parseDecimalParameters() (precision, scale int, err error) {
	precision = 10
	if p.cur.Type != TokLParen {
		return
	}
	p.advance()
	if p.cur.Type != TokInt {
		return 0, 0, fmt.Errorf("expected DECIMAL precision")
	}
	precision, err = strconv.Atoi(p.cur.Value)
	if err != nil {
		return
	}
	p.advance()
	if p.cur.Type == TokComma {
		p.advance()
		if p.cur.Type != TokInt {
			return 0, 0, fmt.Errorf("expected DECIMAL scale")
		}
		scale, err = strconv.Atoi(p.cur.Value)
		if err != nil {
			return
		}
		p.advance()
	}
	if err = p.expect(TokRParen); err != nil {
		return
	}
	if precision < 1 || precision > 65 || scale < 0 || scale > 30 || scale > precision {
		err = fmt.Errorf("DECIMAL requires precision 1..65 and scale 0..min(30,precision)")
	}
	return
}

func NewDecimalValue(text string) (Value, error) {
	n, scale, err := parseDecimal(text)
	if err != nil {
		return NewNullValue(), err
	}
	return Value{Type: ColDecimal, Text: decimalText(n, scale)}, nil
}

func parseDecimal(text string) (*big.Int, int, error) {
	text = strings.TrimSpace(text)
	if len(text) == 0 || len(text) > maxDecimalDigits+32 {
		return nil, 0, fmt.Errorf("invalid decimal value")
	}
	sign := ""
	if text[0] == '-' || text[0] == '+' {
		sign, text = text[:1], text[1:]
	}
	exponent := 0
	if pos := strings.IndexAny(text, "eE"); pos >= 0 {
		var err error
		exponent, err = strconv.Atoi(text[pos+1:])
		if err != nil || exponent < -maxDecimalDigits || exponent > maxDecimalDigits {
			return nil, 0, fmt.Errorf("decimal exponent out of range")
		}
		text = text[:pos]
	}
	scale := 0
	if pos := strings.IndexByte(text, '.'); pos >= 0 {
		scale = len(text) - pos - 1
		text = text[:pos] + text[pos+1:]
	}
	if len(text) == 0 || len(text) > maxDecimalDigits {
		return nil, 0, fmt.Errorf("invalid decimal value")
	}
	for _, c := range text {
		if c < '0' || c > '9' {
			return nil, 0, fmt.Errorf("invalid decimal value")
		}
	}
	scale -= exponent
	if scale < 0 {
		text += strings.Repeat("0", -scale)
		scale = 0
	}
	if len(text) > maxDecimalDigits || scale > maxDecimalDigits {
		return nil, 0, fmt.Errorf("decimal value out of range")
	}
	n, ok := new(big.Int).SetString(sign+text, 10)
	if !ok {
		return nil, 0, fmt.Errorf("invalid decimal value")
	}
	return n, scale, nil
}

func decimalText(n *big.Int, scale int) string {
	negative := n.Sign() < 0
	text := new(big.Int).Abs(n).String()
	if scale > 0 {
		if len(text) <= scale {
			text = strings.Repeat("0", scale-len(text)+1) + text
		}
		text = text[:len(text)-scale] + "." + text[len(text)-scale:]
	}
	if negative {
		text = "-" + text
	}
	return text
}

func decimalPower(n int) *big.Int { return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil) }

// roundDecimal applies half-away-from-zero, including negative ties.
func roundDecimal(n *big.Int, scale, target int) *big.Int {
	if scale <= target {
		return new(big.Int).Mul(n, decimalPower(target-scale))
	}
	factor := decimalPower(scale - target)
	q, r := new(big.Int), new(big.Int)
	q.QuoRem(n, factor, r)
	if new(big.Int).Lsh(new(big.Int).Abs(r), 1).Cmp(factor) >= 0 {
		if n.Sign() < 0 {
			q.Sub(q, big.NewInt(1))
		} else {
			q.Add(q, big.NewInt(1))
		}
	}
	return q
}

func valueDecimal(v Value) (*big.Int, int, error) {
	if v.EnumValue {
		return big.NewInt(v.EnumOrdinal), 0, nil
	}
	switch v.Type {
	case ColInteger:
		return big.NewInt(v.Int), 0, nil
	case ColUnsigned:
		return new(big.Int).SetUint64(v.Uint), 0, nil
	case ColDecimal:
		return parseDecimal(v.Text)
	case ColReal:
		text := v.NumericText
		if text == "" {
			text = strconv.FormatFloat(v.Real, 'g', -1, 64)
		}
		return parseDecimal(text)
	case ColText:
		return parseDecimal(v.Text)
	case ColBlob:
		return parseDecimal(string(v.Blob))
	}
	return nil, 0, fmt.Errorf("numeric value required")
}

func valueRational(v Value) *big.Rat {
	if v.Type == ColReal {
		return new(big.Rat).SetFloat64(v.Real)
	}
	n, s, err := valueDecimal(v)
	if err != nil {
		return new(big.Rat)
	}
	return new(big.Rat).SetFrac(n, decimalPower(s))
}

func exactNumericCompare(a, b Value) int { return valueRational(a).Cmp(valueRational(b)) }

func rationalDecimal(value *big.Rat, scale int) (Value, error) {
	numerator := new(big.Int).Mul(value.Num(), decimalPower(scale))
	q, r := new(big.Int), new(big.Int)
	q.QuoRem(numerator, value.Denom(), r)
	if new(big.Int).Lsh(new(big.Int).Abs(r), 1).Cmp(value.Denom()) >= 0 {
		if numerator.Sign() < 0 {
			q.Sub(q, big.NewInt(1))
		} else {
			q.Add(q, big.NewInt(1))
		}
	}
	if len(q.String()) > maxDecimalDigits {
		return NewNullValue(), fmt.Errorf("decimal value out of range")
	}
	return Value{Type: ColDecimal, Text: decimalText(q, scale)}, nil
}

func extendedArithmetic(op string, a, b Value) (Value, error) {
	if a.Type == ColReal || b.Type == ColReal {
		if a.Type != ColDecimal && b.Type != ColDecimal {
			left, right := toFloatVal(a), toFloatVal(b)
			var result float64
			switch op {
			case "+":
				result = left + right
			case "-":
				result = left - right
			case "*":
				result = left * right
			case "/":
				if right == 0 {
					return NewNullValue(), fmt.Errorf("division by zero")
				}
				result = left / right
			case "%":
				if right == 0 {
					return NewNullValue(), fmt.Errorf("division by zero")
				}
				result = math.Mod(left, right)
			}
			if math.IsNaN(result) || math.IsInf(result, 0) {
				return NewNullValue(), fmt.Errorf("numeric value out of range")
			}
			return NewRealValue(result), nil
		}
	}
	an, as, err := valueDecimal(a)
	if err != nil {
		return NewNullValue(), err
	}
	bn, bs, err := valueDecimal(b)
	if err != nil {
		return NewNullValue(), err
	}
	decimal := a.Type == ColDecimal || b.Type == ColDecimal || a.Type == ColReal || b.Type == ColReal
	scale := max(as, bs)
	left := new(big.Int).Mul(an, decimalPower(scale-as))
	right := new(big.Int).Mul(bn, decimalPower(scale-bs))
	n := new(big.Int)
	switch op {
	case "+":
		n.Add(left, right)
	case "-":
		n.Sub(left, right)
	case "*":
		n.Mul(an, bn)
		scale = as + bs
	case "/":
		if bn.Sign() == 0 {
			return NewNullValue(), fmt.Errorf("division by zero")
		}
		if decimal {
			target := min(max(as+4, bs), 30)
			numerator := new(big.Int).Mul(an, decimalPower(bs+target))
			denominator := new(big.Int).Mul(bn, decimalPower(as))
			q, r := new(big.Int), new(big.Int)
			q.QuoRem(numerator, denominator, r)
			if new(big.Int).Lsh(new(big.Int).Abs(r), 1).Cmp(new(big.Int).Abs(denominator)) >= 0 {
				if numerator.Sign()*denominator.Sign() < 0 {
					q.Sub(q, big.NewInt(1))
				} else {
					q.Add(q, big.NewInt(1))
				}
			}
			n = q
			scale = target
		} else {
			n.Quo(an, bn)
			scale = 0
		}
	case "%":
		if bn.Sign() == 0 {
			return NewNullValue(), fmt.Errorf("division by zero")
		}
		n.Rem(left, right)
	default:
		return NewNullValue(), fmt.Errorf("unsupported numeric operator %s", op)
	}
	if len(n.String()) > maxDecimalDigits || scale > maxDecimalDigits {
		return NewNullValue(), fmt.Errorf("decimal value out of range")
	}
	if decimal {
		return Value{Type: ColDecimal, Text: decimalText(n, scale)}, nil
	}
	if n.Sign() < 0 || n.BitLen() > 64 {
		return NewNullValue(), fmt.Errorf("unsigned integer overflow")
	}
	return NewUnsignedValue(n.Uint64()), nil
}

func normalizeDecimal(v Value, col ColumnDef) (Value, error) {
	n, s, err := valueDecimal(v)
	if err != nil {
		return NewNullValue(), fmt.Errorf("invalid DECIMAL value for %s: %w", col.Name, err)
	}
	n = roundDecimal(n, s, col.Scale)
	if len(new(big.Int).Abs(n).String()) > col.Precision || col.Unsigned && n.Sign() < 0 {
		return NewNullValue(), fmt.Errorf("DECIMAL value out of range for %s", col.Name)
	}
	return Value{Type: ColDecimal, Text: decimalText(n, col.Scale)}, nil
}

func (s *Schema) mysqlIntegerAssignment(v Value, col ColumnDef) (Value, error) {
	if v.Null {
		return v, nil
	}
	if v.Type == ColBlob {
		v = NewTextValue(string(v.Blob))
	}
	n, scale, err := valueDecimal(v)
	if err != nil {
		if !s.mysqlNonStrict() {
			return v, fmt.Errorf("invalid integer value for %s", col.Name)
		}
		prefix := mysqlNumericPrefix.FindString(strings.TrimSpace(mysqlStringValue(v)))
		if prefix == "" {
			prefix = "0"
		}
		n, scale, err = parseDecimal(prefix)
		if err != nil {
			return v, err
		}
		s.mysqlWarning(1366, "Incorrect integer value for column "+col.Name)
	}
	n = roundDecimal(n, scale, 0)
	bits := col.IntegerBits
	if bits == 0 {
		bits = 64
	}
	low, high := new(big.Int), new(big.Int)
	if col.Unsigned {
		high.Sub(new(big.Int).Lsh(big.NewInt(1), uint(bits)), big.NewInt(1))
	} else {
		high.Sub(new(big.Int).Lsh(big.NewInt(1), uint(bits-1)), big.NewInt(1))
		low.Neg(new(big.Int).Lsh(big.NewInt(1), uint(bits-1)))
	}
	if n.Cmp(low) < 0 || n.Cmp(high) > 0 {
		if !s.mysqlNonStrict() {
			return v, fmt.Errorf("integer value out of range for %s", col.Name)
		}
		if n.Cmp(low) < 0 {
			n = low
		} else {
			n = high
		}
		s.mysqlWarning(1264, "Out of range value for column "+col.Name)
	}
	if n.IsInt64() {
		return NewIntValue(n.Int64()), nil
	}
	return NewUnsignedValue(n.Uint64()), nil
}

// Decimal sortable bytes use sign, a biased scientific exponent and significant
// digits. Negative payload bytes are inverted; the terminator reverses prefix
// ordering too. Numerically equal scales produce identical keys.
func decimalSortKey(text string) []byte {
	n, scale, err := parseDecimal(text)
	if err != nil {
		return nil
	}
	if n.Sign() == 0 {
		return []byte{1}
	}
	negative := n.Sign() < 0
	digits := new(big.Int).Abs(n).String()
	exponent := len(digits) - scale
	digits = strings.TrimRight(digits, "0")
	payload := binary.BigEndian.AppendUint32(nil, uint32(int32(exponent))^(1<<31))
	payload = append(payload, digits...)
	payload = append(payload, 0)
	if negative {
		for i := range payload {
			payload[i] = ^payload[i]
		}
		return append([]byte{0}, payload...)
	}
	return append([]byte{2}, payload...)
}
