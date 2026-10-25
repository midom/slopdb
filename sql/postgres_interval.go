package sql

import (
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
)

// PostgreSQL intervals retain calendar months and days separately from elapsed
// microseconds. This representation survives binary protocol round trips.
func ParsePostgresInterval(text string) (months, days int32, micros int64, err error) {
	original := text
	text = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(text), "@"))
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return 0, 0, 0, fmt.Errorf("invalid input syntax for type INTERVAL: %q", original)
	}

	mon, day, us := new(big.Rat), new(big.Rat), new(big.Rat)
	round := func(r *big.Rat, even bool) *big.Int {
		q, rem := new(big.Int), new(big.Int)
		q.QuoRem(r.Num(), r.Denom(), rem)
		cmp := new(big.Int).Lsh(new(big.Int).Abs(rem), 1).Cmp(r.Denom())
		if cmp > 0 || cmp == 0 && even && q.Bit(0) == 1 {
			q.Add(q, big.NewInt(int64(r.Sign())))
		}
		return q
	}
	add := func(dst, n *big.Rat, multiplier int64) {
		value := new(big.Rat).Mul(n, new(big.Rat).SetInt64(multiplier))
		// PostgreSQL unit spellings round half microseconds toward zero.
		if dst == us {
			value.SetInt(round(value, false))
		}
		dst.Add(dst, value)
	}
	quantity := func(text string) (*big.Rat, bool) {
		if strings.ContainsAny(text, "/xXpP") {
			return nil, false
		}
		r, ok := new(big.Rat).SetString(text)
		return r, ok
	}
	ago := false
	for i := 0; i < len(fields); i++ {
		f := fields[i]
		if strings.EqualFold(f, "ago") && i == len(fields)-1 {
			ago = true
			break
		}
		if strings.Contains(f, ":") {
			sign := int64(1)
			if strings.HasPrefix(f, "-") {
				sign = -1
				f = f[1:]
			} else {
				f = strings.TrimPrefix(f, "+")
			}
			pieces := strings.Split(f, ":")
			if len(pieces) < 2 || len(pieces) > 3 {
				err = fmt.Errorf("invalid interval time")
				break
			}
			h, e := strconv.ParseInt(pieces[0], 10, 64)
			m, e2 := strconv.ParseInt(pieces[1], 10, 64)
			sec := new(big.Rat)
			ok := true
			if len(pieces) == 3 {
				sec, ok = quantity(pieces[2])
			}
			if e != nil || e2 != nil || h < 0 || m < 0 || m > 59 || !ok || sec.Sign() < 0 || sec.Cmp(big.NewRat(60, 1)) >= 0 {
				err = fmt.Errorf("invalid interval time")
				break
			}
			clock := new(big.Rat).Mul(new(big.Rat).SetInt64(h), big.NewRat(3600, 1))
			clock.Add(clock, new(big.Rat).Mul(new(big.Rat).SetInt64(m), big.NewRat(60, 1)))
			clock.Add(clock, sec)
			clock.Mul(clock, big.NewRat(sign*1e6, 1))
			us.Add(us, new(big.Rat).SetInt(round(clock, true)))
			continue
		}
		n, ok := quantity(f)
		if !ok {
			err = fmt.Errorf("invalid interval quantity")
			break
		}
		unit := "second"
		if i+1 < len(fields) {
			i++
			unit = strings.ToLower(fields[i])
		}
		if len(unit) > 2 {
			unit = strings.TrimSuffix(unit, "s")
		}
		switch unit {
		case "millennium", "millennia":
			mon.Add(mon, new(big.Rat).SetInt(round(new(big.Rat).Mul(n, big.NewRat(12000, 1)), true)))
		case "century", "centurie":
			mon.Add(mon, new(big.Rat).SetInt(round(new(big.Rat).Mul(n, big.NewRat(1200, 1)), true)))
		case "decade":
			mon.Add(mon, new(big.Rat).SetInt(round(new(big.Rat).Mul(n, big.NewRat(120, 1)), true)))
		case "year", "yr", "y":
			mon.Add(mon, new(big.Rat).SetInt(round(new(big.Rat).Mul(n, big.NewRat(12, 1)), true)))
		case "month", "mon":
			add(mon, n, 1)
		case "week", "w":
			add(day, n, 7)
		case "day", "d":
			add(day, n, 1)
		case "hour", "hr", "h":
			add(us, n, 3600e6)
		case "minute", "min", "m":
			add(us, n, 60e6)
		case "second", "sec", "s":
			add(us, n, 1e6)
		case "millisecond", "msec", "ms":
			add(us, n, 1000)
		case "microsecond", "usec", "us":
			add(us, n, 1)
		default:
			err = fmt.Errorf("invalid interval unit %s", unit)
		}
		if err != nil {
			break
		}
	}
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid input syntax for type INTERVAL: %q: %w", original, err)
	}
	if ago {
		mon.Neg(mon)
		day.Neg(day)
		us.Neg(us)
	}
	wholeMonths := new(big.Int).Quo(mon.Num(), mon.Denom())
	add(day, new(big.Rat).Sub(mon, new(big.Rat).SetInt(wholeMonths)), 30)
	wholeDays := new(big.Int).Quo(day.Num(), day.Denom())
	add(us, new(big.Rat).Sub(day, new(big.Rat).SetInt(wholeDays)), 86400e6)
	micro, rem := new(big.Int), new(big.Int)
	micro.QuoRem(us.Num(), us.Denom(), rem)
	cmp := new(big.Int).Lsh(new(big.Int).Abs(rem), 1).Cmp(us.Denom())
	if cmp > 0 || cmp == 0 && micro.Bit(0) == 1 {
		micro.Add(micro, big.NewInt(int64(us.Sign())))
	}
	if !wholeMonths.IsInt64() || wholeMonths.Int64() < math.MinInt32 || wholeMonths.Int64() > math.MaxInt32 || !wholeDays.IsInt64() || wholeDays.Int64() < math.MinInt32 || wholeDays.Int64() > math.MaxInt32 || !micro.IsInt64() {
		return 0, 0, 0, fmt.Errorf("INTERVAL out of range")
	}
	return int32(wholeMonths.Int64()), int32(wholeDays.Int64()), micro.Int64(), nil

}
func FormatPostgresInterval(months, days int32, micros int64) string {
	pieces := []string{}
	if months != 0 {
		pieces = append(pieces, fmt.Sprintf("%d mons", months))
	}
	if days != 0 {
		pieces = append(pieces, fmt.Sprintf("%d days", days))
	}
	sign := ""
	magnitude := uint64(micros)
	if micros < 0 {
		sign = "-"
		magnitude = uint64(-(micros + 1)) + 1
	}
	seconds := magnitude / 1e6
	fraction := magnitude % 1e6
	clock := fmt.Sprintf("%s%02d:%02d:%02d", sign, seconds/3600, (seconds/60)%60, seconds%60)
	if fraction > 0 {
		clock += strings.TrimRight(fmt.Sprintf(".%06d", fraction), "0")
	}
	if micros != 0 || len(pieces) == 0 {
		pieces = append(pieces, clock)
	}
	return strings.Join(pieces, " ")
}
