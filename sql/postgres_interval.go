package sql

import (
	"fmt"
	"math"
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
	var mon, day float64
	var us float64
	ago := false
	for i := 0; i < len(fields); i++ {
		f := fields[i]
		if strings.EqualFold(f, "ago") && i == len(fields)-1 {
			ago = true
			break
		}
		if strings.Contains(f, ":") {
			sign := 1.0
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
			sec := 0.0
			if len(pieces) == 3 {
				sec, e = strconv.ParseFloat(pieces[2], 64)
			}
			if e != nil || e2 != nil || m < 0 || m > 59 || sec < 0 || sec >= 60 {
				err = fmt.Errorf("invalid interval time")
				break
			}
			us += sign * (float64(h)*3600 + float64(m)*60 + sec) * 1e6
			continue
		}
		n, e := strconv.ParseFloat(f, 64)
		if e != nil || math.IsNaN(n) || math.IsInf(n, 0) {
			err = fmt.Errorf("invalid interval quantity")
			break
		}
		unit := "second"
		if i+1 < len(fields) {
			i++
			unit = strings.ToLower(fields[i])
		}
		unit = strings.TrimSuffix(unit, "s")
		switch unit {
		case "millennium", "millennia":
			mon += n * 12000
		case "century", "centurie":
			mon += n * 1200
		case "decade":
			mon += n * 120
		case "year", "yr", "y":
			mon += n * 12
		case "month", "mon":
			mon += n
		case "week", "w":
			day += n * 7
		case "day", "d":
			day += n
		case "hour", "hr", "h":
			us += n * 3600e6
		case "minute", "min", "m":
			us += n * 60e6
		case "second", "sec", "s":
			us += n * 1e6
		case "millisecond", "msec", "ms":
			us += n * 1000
		case "microsecond", "usec", "us":
			us += n
		default:
			err = fmt.Errorf("invalid interval unit %s", unit)
		}
		if err != nil {
			break
		}
	}
	day += (mon - math.Trunc(mon)) * 30
	us += (day - math.Trunc(day)) * 86400e6
	if ago {
		mon = -mon
		day = -day
		us = -us
	}
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid input syntax for type INTERVAL: %q: %w", original, err)
	}
	if mon < math.MinInt32 || mon > math.MaxInt32 || day < math.MinInt32 || day > math.MaxInt32 || us < math.MinInt64 || us >= 9223372036854775808.0 {
		return 0, 0, 0, fmt.Errorf("INTERVAL out of range")
	}
	return int32(mon), int32(day), int64(math.RoundToEven(us)), nil
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
