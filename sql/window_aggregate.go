package sql

import (
	"fmt"
	"math"
	"math/big"
)

// SQL frame endpoints are monotone over a sorted partition. Advance both edges
// once per input row instead of rescanning every overlapping frame. Integer
// sums stay exact while a compensated sum bounds error during REAL removal.
type windowAggregate struct {
	schema            *Schema
	name              string
	values            []Value
	lo, hi            int
	count, reals      int64
	integer           big.Int
	total, correction float64
	extrema           []int
}

func (s *Schema) newWindowAggregate(f *FuncCallExpr, p []windowRow) (*windowAggregate, error) {
	switch f.Name {
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
	default:
		return nil, nil
	}
	a := &windowAggregate{schema: s, name: f.Name, values: make([]Value, len(p))}
	star := false
	if c, ok := f.Args[0].(*ColumnRefExpr); ok {
		star = c.Name == "*"
	}
	for i, r := range p {
		if err := s.checkInterrupt(); err != nil {
			return nil, err
		}
		if star {
			a.values[i] = NewIntValue(1)
			continue
		}
		v, err := s.evalExpr(f.Args[0], r.row)
		if err != nil {
			return nil, err
		}
		a.values[i] = v
	}
	return a, nil
}
func (a *windowAggregate) accumulate(x float64) {
	t := a.total + x
	if math.Abs(a.total) >= math.Abs(x) {
		a.correction += (a.total - t) + x
	} else {
		a.correction += (x - t) + a.total
	}
	a.total = t
}
func (a *windowAggregate) change(v Value, add bool) error {
	if v.Null {
		return nil
	}
	direction := int64(1)
	if !add {
		direction = -1
	}
	a.count += direction
	if a.name != "SUM" && a.name != "AVG" {
		return nil
	}
	if v.Type != ColInteger && v.Type != ColReal {
		return fmt.Errorf("%s requires numeric arguments", a.name)
	}
	if v.Type == ColReal {
		a.reals += direction
	} else {
		var n big.Int
		n.SetInt64(v.Int)
		if add {
			a.integer.Add(&a.integer, &n)
		} else {
			a.integer.Sub(&a.integer, &n)
		}
	}
	a.accumulate(float64(direction) * toFloat(v))
	if a.count == 0 {
		a.total = 0
		a.correction = 0
	}
	return nil
}
func (a *windowAggregate) frame(lo, hi int) (Value, error) {
	if lo < a.lo || hi < a.hi {
		return NewNullValue(), fmt.Errorf("nonmonotone window frame")
	}
	// Remove old values before extending the right edge. Disjoint frames start
	// fresh, avoiding a transient range outside either requested frame.
	stop := lo
	if stop > a.hi {
		stop = a.hi
	}
	for i := a.lo; i < stop; i++ {
		if i%128 == 0 {
			if err := a.schema.checkInterrupt(); err != nil {
				return NewNullValue(), err
			}
		}
		if err := a.change(a.values[i], false); err != nil {
			return NewNullValue(), err
		}
	}
	a.lo = lo
	if a.hi < lo {
		a.hi = lo
	}
	for len(a.extrema) > 0 && a.extrema[0] < lo {
		a.extrema = a.extrema[1:]
	}
	for i := a.hi; i < hi; i++ {
		if i%128 == 0 {
			if err := a.schema.checkInterrupt(); err != nil {
				return NewNullValue(), err
			}
		}
		v := a.values[i]
		if err := a.change(v, true); err != nil {
			return NewNullValue(), err
		}
		if v.Null || (a.name != "MIN" && a.name != "MAX") {
			continue
		}
		for len(a.extrema) > 0 {
			c := a.schema.valueCompare(a.values[a.extrema[len(a.extrema)-1]], v)
			if a.name == "MIN" && c <= 0 || a.name == "MAX" && c >= 0 {
				break
			}
			a.extrema = a.extrema[:len(a.extrema)-1]
		}
		a.extrema = append(a.extrema, i)
	}
	a.hi = hi
	if a.name == "COUNT" {
		return NewIntValue(a.count), nil
	}
	if a.count == 0 {
		return NewNullValue(), nil
	}
	if a.name == "MIN" || a.name == "MAX" {
		return a.values[a.extrema[0]], nil
	}
	if a.name == "SUM" && a.reals == 0 {
		if !a.integer.IsInt64() {
			return NewNullValue(), fmt.Errorf("integer overflow")
		}
		return NewIntValue(a.integer.Int64()), nil
	}
	total := a.total + a.correction
	if a.name == "AVG" {
		total /= float64(a.count)
	}
	if math.IsInf(total, 0) || math.IsNaN(total) {
		return NewNullValue(), fmt.Errorf("numeric value out of range")
	}
	return NewRealValue(total), nil
}
