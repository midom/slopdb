package sql

import (
	"fmt"
	"math"
	"math/big"
	"strings"
)

func postgresMathFunctions() map[string]FunctionSpec {
	out := map[string]FunctionSpec{}
	add := func(name string, min, max int, typ ColumnType, eval func(*Schema, []Value) (Value, error)) {
		out[name] = FunctionSpec{Min: min, Max: max, Return: typ, Eval: eval}
	}
	for name, op := range map[string]func(float64) float64{
		"SQRT": math.Sqrt, "CBRT": math.Cbrt, "EXP": math.Exp, "LN": math.Log, "LOG10": math.Log10, "SIN": math.Sin, "COS": math.Cos, "TAN": math.Tan, "COT": func(x float64) float64 { return 1 / math.Tan(x) }, "ASIN": math.Asin, "ACOS": math.Acos, "ATAN": math.Atan, "SINH": math.Sinh, "COSH": math.Cosh, "TANH": math.Tanh, "ASINH": math.Asinh, "ACOSH": math.Acosh, "ATANH": math.Atanh, "DEGREES": func(x float64) float64 { return x * 180 / math.Pi }, "RADIANS": func(x float64) float64 { return x * math.Pi / 180 },
	} {
		name, op := name, op
		add(name, 1, 1, ColReal, func(s *Schema, a []Value) (Value, error) {
			if a[0].Null {
				return NewNullValue(), nil
			}
			x, e := postgresNumber(a[0])
			if e != nil {
				return NewNullValue(), e
			}
			return postgresMathResult(name, op(x))
		})
	}
	for name, op := range map[string]func(float64) float64{"SIND": math.Sin, "COSD": math.Cos, "TAND": math.Tan, "COTD": func(x float64) float64 { return 1 / math.Tan(x) }, "ASIND": math.Asin, "ACOSD": math.Acos, "ATAND": math.Atan} {
		name, op := name, op
		add(name, 1, 1, ColReal, func(s *Schema, a []Value) (Value, error) {
			if a[0].Null {
				return NewNullValue(), nil
			}
			x, e := postgresNumber(a[0])
			if e != nil {
				return NewNullValue(), e
			}
			if strings.HasPrefix(name, "A") {
				return postgresMathResult(name, op(x)*180/math.Pi)
			}
			x = math.Mod(x, 360)
			switch name {
			case "SIND":
				if x == 0 || math.Abs(x) == 180 {
					return NewRealValue(0), nil
				}
			case "COSD":
				if math.Abs(x) == 90 || math.Abs(x) == 270 {
					return NewRealValue(0), nil
				}
			case "TAND":
				if x == 0 || math.Abs(x) == 180 {
					return NewRealValue(0), nil
				}
				if math.Abs(x) == 90 {
					return NewNullValue(), fmt.Errorf("tangent is out of range")
				}
			case "COTD":
				if math.Abs(x) == 90 || math.Abs(x) == 270 {
					return NewRealValue(0), nil
				}
				if x == 0 || math.Abs(x) == 180 {
					return NewNullValue(), fmt.Errorf("cotangent is out of range")
				}
			}
			return postgresMathResult(name, op(x*math.Pi/180))
		})
	}
	add("PI", 0, 0, ColReal, func(*Schema, []Value) (Value, error) { return NewRealValue(math.Pi), nil })
	for _, name := range []string{"POWER", "POW", "ATAN2", "ATAN2D", "LOG"} {
		name := name
		min := 2
		if name == "LOG" {
			min = 1
		}
		add(name, min, 2, ColReal, func(s *Schema, a []Value) (Value, error) {
			if functionAnyNull(a) {
				return NewNullValue(), nil
			}
			x, e := postgresNumber(a[0])
			if e != nil {
				return NewNullValue(), e
			}
			if len(a) == 1 {
				return postgresMathResult(name, math.Log10(x))
			}
			y, e := postgresNumber(a[1])
			if e != nil {
				return NewNullValue(), e
			}
			switch name {
			case "LOG":
				if x == 1 || x <= 0 || y <= 0 {
					return NewNullValue(), fmt.Errorf("invalid logarithm argument")
				}
				return postgresMathResult(name, math.Log(y)/math.Log(x))
			case "ATAN2":
				return postgresMathResult(name, math.Atan2(x, y))
			case "ATAN2D":
				return postgresMathResult(name, math.Atan2(x, y)*180/math.Pi)
			default:
				return postgresMathResult(name, math.Pow(x, y))
			}
		})
	}
	for _, name := range []string{"ABS", "SIGN", "CEIL", "CEILING", "FLOOR", "ROUND", "TRUNC"} {
		name := name
		max := 1
		if name == "ROUND" || name == "TRUNC" {
			max = 2
		}
		add(name, 1, max, ColDecimal, func(s *Schema, a []Value) (Value, error) { return postgresExactUnary(s, name, a) })
		spec := out[name]
		spec.Metadata = func(a []ColumnDef) ColumnDef {
			if len(a) > 0 && a[0].ColType == ColReal {
				return ColumnDef{ColType: ColReal}
			}
			if name == "ABS" && len(a) > 0 {
				return a[0]
			}
			return ColumnDef{ColType: ColDecimal}
		}
		out[name] = spec
	}
	for _, name := range strings.Fields("SCALE MIN_SCALE TRIM_SCALE") {
		name := name
		typ := ColInteger
		if name == "TRIM_SCALE" {
			typ = ColDecimal
		}
		add(name, 1, 1, typ, func(s *Schema, a []Value) (Value, error) {
			if a[0].Null {
				return NewNullValue(), nil
			}
			n, scale, e := valueDecimal(a[0])
			if e != nil {
				return NewNullValue(), e
			}
			if name != "SCALE" {
				ten := big.NewInt(10)
				for scale > 0 {
					q, r := new(big.Int), new(big.Int)
					q.QuoRem(n, ten, r)
					if r.Sign() != 0 {
						break
					}
					n = q
					scale--
				}
			}
			if name == "TRIM_SCALE" {
				return NewDecimalValue(decimalText(n, scale))
			}
			return NewIntValue(int64(scale)), nil
		})
	}
	for _, name := range strings.Fields("GCD LCM MOD DIV") {
		name := name
		typ := ColDecimal
		add(name, 2, 2, typ, func(s *Schema, a []Value) (Value, error) {
			if functionAnyNull(a) {
				return NewNullValue(), nil
			}
			x, xs, e := valueDecimal(a[0])
			if e != nil {
				return NewNullValue(), e
			}
			y, ys, e := valueDecimal(a[1])
			if e != nil {
				return NewNullValue(), e
			}
			scale := xs
			if ys > scale {
				scale = ys
			}
			x.Mul(x, decimalPower(scale-xs))
			y.Mul(y, decimalPower(scale-ys))
			r := new(big.Int)
			switch name {
			case "GCD":
				r.GCD(nil, nil, x, y)
			case "LCM":
				if x.Sign() != 0 && y.Sign() != 0 {
					g := new(big.Int).GCD(nil, nil, x, y)
					r.Quo(x, g)
					r.Mul(r, y)
					r.Abs(r)
				}
			case "MOD":
				if y.Sign() == 0 {
					return NewNullValue(), fmt.Errorf("division by zero")
				}
				r.Rem(x, y)
			case "DIV":
				if y.Sign() == 0 {
					return NewNullValue(), fmt.Errorf("division by zero")
				}
				r.Quo(x, y)
				scale = 0
			}
			if name != "DIV" && a[0].Type == ColInteger && a[1].Type == ColInteger {
				if !r.IsInt64() {
					return NewNullValue(), fmt.Errorf("integer out of range")
				}
				return NewIntValue(r.Int64()), nil
			}
			return NewDecimalValue(decimalText(r, scale))
		})
		spec := out[name]
		spec.Metadata = func(a []ColumnDef) ColumnDef {
			if name != "DIV" && len(a) == 2 && a[0].ColType == ColInteger && a[1].ColType == ColInteger {
				return ColumnDef{ColType: ColInteger}
			}
			return ColumnDef{ColType: ColDecimal}
		}
		out[name] = spec
	}
	add("FACTORIAL", 1, 1, ColDecimal, func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		v, e := CastPostgresValue(a[0], "BIGINT")
		if e != nil {
			return NewNullValue(), e
		}
		if v.Int < 0 {
			return NewNullValue(), fmt.Errorf("factorial of negative number")
		}
		if v.Int > 100000 {
			return NewNullValue(), fmt.Errorf("factorial result exceeds numeric limit")
		}
		n := big.NewInt(1)
		for i := int64(2); i <= v.Int; i++ {
			if i%128 == 0 {
				if e = s.checkInterrupt(); e != nil {
					return NewNullValue(), e
				}
			}
			n.Mul(n, big.NewInt(i))
		}
		if e = functionResultSize(s, uint64(n.BitLen()/3+1)); e != nil {
			return NewNullValue(), e
		}
		return NewDecimalValue(n.String())
	})
	add("WIDTH_BUCKET", 4, 4, ColInteger, func(s *Schema, a []Value) (Value, error) {
		if functionAnyNull(a) {
			return NewNullValue(), nil
		}
		for _, v := range a[:3] {
			if _, e := postgresNumber(v); e != nil {
				return NewNullValue(), e
			}
		}
		count, e := CastPostgresValue(a[3], "INTEGER")
		if e != nil {
			return NewNullValue(), e
		}
		if count.Int <= 0 {
			return NewNullValue(), fmt.Errorf("bucket count must be positive")
		}
		x, low, high := valueRational(a[0]), valueRational(a[1]), valueRational(a[2])
		if low.Cmp(high) == 0 {
			return NewNullValue(), fmt.Errorf("bucket bounds must differ")
		}
		reverse := low.Cmp(high) > 0
		if (!reverse && x.Cmp(low) < 0) || (reverse && x.Cmp(low) > 0) {
			return NewIntValue(0), nil
		}
		if (!reverse && x.Cmp(high) >= 0) || (reverse && x.Cmp(high) <= 0) {
			return NewIntValue(count.Int + 1), nil
		}
		r := new(big.Rat).Sub(x, low)
		r.Quo(r, new(big.Rat).Sub(high, low))
		r.Mul(r, new(big.Rat).SetInt64(count.Int))
		q := new(big.Int).Quo(r.Num(), r.Denom())
		return NewIntValue(q.Int64() + 1), nil
	})
	return out
}
func postgresNumber(v Value) (float64, error) {
	if v.Type == ColText {
		r, e := CastPostgresValue(v, "DOUBLE PRECISION")
		if e != nil {
			return 0, e
		}
		v = r
	}
	if !isNumeric(v) {
		return 0, fmt.Errorf("numeric argument required")
	}
	x := toFloat(v)
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return 0, fmt.Errorf("nonfinite argument is unsupported")
	}
	return x, nil
}
func postgresMathResult(name string, x float64) (Value, error) {
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return NewNullValue(), fmt.Errorf("%s argument or result out of range", name)
	}
	return NewRealValue(x), nil
}
func postgresExactUnary(s *Schema, name string, a []Value) (Value, error) {
	if functionAnyNull(a) {
		return NewNullValue(), nil
	}
	v := a[0]
	if v.Type == ColReal {
		x := v.Real
		switch name {
		case "ABS":
			x = math.Abs(x)
		case "SIGN":
			if x > 0 {
				x = 1
			} else if x < 0 {
				x = -1
			}
		case "CEIL", "CEILING":
			x = math.Ceil(x)
		case "FLOOR":
			x = math.Floor(x)
		case "ROUND":
			if len(a) > 1 {
				return NewNullValue(), fmt.Errorf("round(double precision, integer) does not exist")
			}
			x = math.RoundToEven(x)
		case "TRUNC":
			if len(a) > 1 {
				return NewNullValue(), fmt.Errorf("trunc(double precision, integer) does not exist")
			}
			x = math.Trunc(x)
		}
		return postgresMathResult(name, x)
	}
	n, scale, e := valueDecimal(v)
	if e != nil {
		return NewNullValue(), e
	}
	target := 0
	if len(a) > 1 {
		d, e := CastPostgresValue(a[1], "INTEGER")
		if e != nil {
			return NewNullValue(), e
		}
		if d.Int < -100000 || d.Int > 100000 {
			return NewNullValue(), fmt.Errorf("numeric scale out of range")
		}
		target = int(d.Int)
	}
	switch name {
	case "ABS":
		n.Abs(n)
		if v.Type == ColInteger {
			if !n.IsInt64() {
				return NewNullValue(), fmt.Errorf("integer out of range")
			}
			return NewIntValue(n.Int64()), nil
		}
	case "SIGN":
		n.SetInt64(int64(n.Sign()))
		scale = 0
	case "ROUND":
		n = roundDecimal(n, scale, target)
		scale = target
	case "TRUNC":
		if target >= scale {
			n.Mul(n, decimalPower(target-scale))
		} else {
			n.Quo(n, decimalPower(scale-target))
		}
		scale = target
	case "CEIL", "CEILING", "FLOOR":
		q, r := new(big.Int), new(big.Int)
		q.QuoRem(n, decimalPower(scale), r)
		if r.Sign() != 0 {
			if name == "FLOOR" && n.Sign() < 0 {
				q.Sub(q, big.NewInt(1))
			} else if name != "FLOOR" && n.Sign() > 0 {
				q.Add(q, big.NewInt(1))
			}
		}
		n = q
		scale = 0
	}
	if scale < 0 {
		n.Mul(n, decimalPower(-scale))
		scale = 0
	}
	return NewDecimalValue(decimalText(n, scale))
}
