package sql

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// MySQL temporal functions use the endpoint's UTC session zone. Calendar
// arithmetic uses civil dates rather than time.Duration (which overflows after
// roughly 290 years), and signed TIME values are stored as integer microseconds.
const mysqlMaxTimeMicros int64 = (838*3600+59*60+59)*1000000 + 999999

type mysqlCivil struct {
	year, month, day int
	t                time.Time
	dateOnly         bool
	fsp              int
	valid            bool
	parsed           bool
}

func mysqlTemporalFunctions() map[string]mysqlFunctionSpec {
	out := make(map[string]mysqlFunctionSpec)
	add := func(names string, min, max int, ret ColumnType, volatile bool, fn func(*Schema, []Value) (Value, error)) {
		for _, name := range strings.Fields(names) {
			out[name] = mysqlFunctionSpec{Min: min, Max: max, Return: ret, Volatile: volatile, Eval: fn}
		}
	}
	for _, name := range strings.Fields("YEAR MONTH DAY DAYOFMONTH DAYOFWEEK DAYOFYEAR WEEKDAY QUARTER MONTHNAME DAYNAME LAST_DAY") {
		name := name
		ret := ColInteger
		if name == "MONTHNAME" || name == "DAYNAME" || name == "LAST_DAY" {
			ret = ColText
		}
		add(name, 1, 1, ret, false, func(s *Schema, a []Value) (Value, error) { return mysqlDatePart(name, a[0]), nil })
	}
	for _, name := range []string{"WEEK", "YEARWEEK", "WEEKOFYEAR"} {
		name := name
		max := 2
		if name == "WEEKOFYEAR" {
			max = 1
		}
		add(name, 1, max, ColInteger, false, func(s *Schema, a []Value) (Value, error) {
			if mysqlAnyNull(a) {
				return NewNullValue(), nil
			}
			d := mysqlParseCivil(a[0])
			if !d.valid {
				return NewNullValue(), nil
			}
			mode := 0
			if name == "WEEKOFYEAR" {
				mode = 3
			}
			if len(a) > 1 {
				mode = int(mysqlFunctionInt(a[1]) & 7)
			}
			y, w := mysqlWeek(d.t, mode, name == "YEARWEEK")
			if name == "YEARWEEK" {
				w += 100 * y
			}
			return NewIntValue(int64(w)), nil
		})
	}
	for _, name := range strings.Fields("HOUR MINUTE SECOND MICROSECOND TIME_TO_SEC") {
		name := name
		ret := ColInteger
		add(name, 1, 1, ret, false, func(s *Schema, a []Value) (Value, error) {
			if a[0].Null {
				return NewNullValue(), nil
			}
			n, _, ok := mysqlTimeValue(a[0])
			if !ok {
				return NewNullValue(), nil
			}
			u := n
			if u < 0 {
				u = -u
			}
			switch name {
			case "HOUR":
				return NewIntValue(u / 3600000000), nil
			case "MINUTE":
				return NewIntValue(u / 60000000 % 60), nil
			case "SECOND":
				return NewIntValue(u / 1000000 % 60), nil
			case "MICROSECOND":
				return NewIntValue(u % 1000000), nil
			default:
				return NewIntValue(n / 1000000), nil
			}
		})
	}
	add("SEC_TO_TIME", 1, 1, ColText, false, func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		n, fsp, ok := mysqlSecondsMicros(a[0])
		if !ok {
			return NewNullValue(), nil
		}
		n = mysqlClampTime(n)
		return NewTextValue(mysqlFormatTime(n, fsp)), nil
	})
	add("DATEDIFF", 2, 2, ColInteger, false, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		x, y := mysqlParseCivil(a[0]), mysqlParseCivil(a[1])
		if !x.valid || !y.valid {
			return NewNullValue(), nil
		}
		return NewIntValue(mysqlDayNumber(x.t) - mysqlDayNumber(y.t)), nil
	})
	for _, name := range []string{"ADDTIME", "SUBTIME", "TIMEDIFF"} {
		name := name
		add(name, 2, 2, ColText, false, func(s *Schema, a []Value) (Value, error) { return mysqlTimeArithmetic(name, a), nil })
	}
	add("MAKETIME", 3, 3, ColText, false, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		h, m := mysqlFunctionInt(a[0]), mysqlFunctionInt(a[1])
		sec, fsp, ok := mysqlSecondsMicros(a[2])
		if !ok || m < 0 || m > 59 || sec < 0 || sec >= 60000000 {
			return NewNullValue(), nil
		}
		negative := h < 0
		if h < 0 {
			if h == math.MinInt64 {
				h = 839
			} else {
				h = -h
			}
		}
		if h > 838 {
			h = 839
		}
		n := h*3600000000 + m*60000000 + sec
		if negative {
			n = -n
		}
		n = mysqlClampTime(n)
		return NewTextValue(mysqlFormatTime(n, fsp)), nil
	})
	add("MAKEDATE", 2, 2, ColText, false, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		y, n := mysqlFunctionInt(a[0]), mysqlFunctionInt(a[1])
		if y >= 0 && y < 100 {
			if y < 70 {
				y += 2000
			} else {
				y += 1900
			}
		}
		if y < 0 || y > 9999 || n <= 0 || n > 3660000 {
			return NewNullValue(), nil
		}
		t := time.Date(int(y), 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(n-1))
		if t.Year() > 9999 {
			return NewNullValue(), nil
		}
		return NewTextValue(t.Format("2006-01-02")), nil
	})
	for _, name := range []string{"TO_DAYS", "TO_SECONDS"} {
		name := name
		add(name, 1, 1, ColInteger, false, func(s *Schema, a []Value) (Value, error) {
			if a[0].Null {
				return NewNullValue(), nil
			}
			d := mysqlParseCivil(a[0])
			if !d.valid {
				return NewNullValue(), nil
			}
			n := mysqlDayNumber(d.t)
			if name == "TO_SECONDS" {
				n = n*86400 + int64(d.t.Hour()*3600+d.t.Minute()*60+d.t.Second())
			}
			return NewIntValue(n), nil
		})
	}
	add("FROM_DAYS", 1, 1, ColText, false, func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		n := mysqlFunctionInt(a[0])
		if n < 366 || n > 3652424 {
			return NewTextValue("0000-00-00"), nil
		}
		t := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(n-366))
		return NewTextValue(t.Format("2006-01-02")), nil
	})
	for _, name := range []string{"PERIOD_ADD", "PERIOD_DIFF"} {
		name := name
		add(name, 2, 2, ColInteger, false, func(s *Schema, a []Value) (Value, error) {
			if mysqlAnyNull(a) {
				return NewNullValue(), nil
			}
			x, ok := mysqlPeriod(mysqlFunctionInt(a[0]))
			if !ok {
				return NewNullValue(), nil
			}
			if name == "PERIOD_DIFF" {
				y, ok := mysqlPeriod(mysqlFunctionInt(a[1]))
				if !ok {
					return NewNullValue(), nil
				}
				return NewIntValue(x - y), nil
			}
			delta := mysqlFunctionInt(a[1])
			if delta > 1200000 || delta < -1200000 {
				return NewNullValue(), nil
			}
			n := x + delta
			if n < 0 {
				return NewNullValue(), nil
			}
			return NewIntValue(n/12*100 + n%12 + 1), nil
		})
	}
	for _, name := range strings.Fields("CURDATE CURRENT_DATE UTC_DATE CURTIME CURRENT_TIME UTC_TIME UTC_TIMESTAMP LOCALTIME LOCALTIMESTAMP") {
		name := name
		max := 1
		if strings.HasSuffix(name, "DATE") {
			max = 0
		}
		add(name, 0, max, ColText, true, func(s *Schema, a []Value) (Value, error) {
			if mysqlAnyNull(a) {
				return NewNullValue(), nil
			}
			fsp := 0
			if len(a) > 0 {
				fsp = int(mysqlFunctionInt(a[0]))
				if fsp < 0 || fsp > 6 {
					return NewNullValue(), fmt.Errorf("%s precision must be between 0 and 6", name)
				}
			}
			t := s.statementTime
			if t.IsZero() {
				t = time.Now().UTC()
			}
			t = t.UTC()
			if strings.HasSuffix(name, "DATE") {
				return NewTextValue(t.Format("2006-01-02")), nil
			}
			if strings.Contains(name, "TIMESTAMP") || name == "LOCALTIME" {
				return NewTextValue(mysqlFormatDate(t, false, fsp)), nil
			}
			return NewTextValue(mysqlFormatTime(int64(t.Hour()*3600+t.Minute()*60+t.Second())*1000000+int64(t.Nanosecond()/1000), fsp)), nil
		})
	}
	add("FROM_UNIXTIME", 1, 2, ColText, false, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		n, fsp, ok := mysqlSecondsMicros(a[0])
		if !ok || n < 0 || n/1000000 > 32536771199 {
			return NewNullValue(), nil
		}
		t := time.Unix(n/1000000, n%1000000*1000).UTC()
		if len(a) == 2 {
			return mysqlDateFormat(s, mysqlCivil{t: t, year: t.Year(), month: int(t.Month()), day: t.Day(), valid: true, fsp: fsp}, mysqlStringValue(a[1]), false), nil
		}
		return NewTextValue(mysqlFormatDate(t, false, fsp)), nil
	})
	for _, name := range []string{"DATE_FORMAT", "TIME_FORMAT"} {
		name := name
		add(name, 2, 2, ColText, false, func(s *Schema, a []Value) (Value, error) {
			if mysqlAnyNull(a) {
				return NewNullValue(), nil
			}
			d := mysqlParseCivil(a[0])
			if name == "TIME_FORMAT" {
				n, fsp, ok := mysqlTimeValue(a[0])
				if !ok {
					return NewNullValue(), nil
				}
				return mysqlTimeFormat(s, n, fsp, mysqlStringValue(a[1])), nil
			}
			if !d.parsed || !d.valid && d.month != 0 && d.day != 0 || d.year == 0 && d.month == 0 && d.day == 0 {
				return NewNullValue(), nil
			}
			return mysqlDateFormat(s, d, mysqlStringValue(a[1]), false), nil
		})
	}
	add("STR_TO_DATE", 2, 2, ColText, false, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		format, err := mysqlExpandDateFormat(s, mysqlStringValue(a[1]))
		if err != nil {
			return NewNullValue(), err
		}
		return mysqlStrToDate(mysqlStringValue(a[0]), format), nil
	})
	for _, name := range []string{"DATE_ADD", "DATE_SUB", "ADDDATE", "SUBDATE"} {
		name := name
		add(name, 2, 3, ColText, false, func(s *Schema, a []Value) (Value, error) {
			if mysqlAnyNull(a) {
				return NewNullValue(), nil
			}
			unit := "DAY"
			if len(a) == 3 {
				unit = strings.ToUpper(mysqlStringValue(a[2]))
			}
			return mysqlDateAdd(a[0], a[1], unit, name == "DATE_SUB" || name == "SUBDATE"), nil
		})
	}
	add("TIMESTAMPADD", 3, 3, ColText, false, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		return mysqlDateAdd(a[2], a[1], strings.ToUpper(mysqlStringValue(a[0])), false), nil
	})
	add("TIMESTAMPDIFF", 3, 3, ColInteger, false, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		return mysqlTimestampDiff(strings.ToUpper(mysqlStringValue(a[0])), a[1], a[2]), nil
	})
	add("EXTRACT", 2, 2, ColInteger, false, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		return mysqlExtract(strings.ToUpper(mysqlStringValue(a[0])), a[1]), nil
	})
	return out
}

func mysqlParseCivil(value Value) mysqlCivil {
	text := strings.TrimSpace(mysqlStringValue(value))
	d := mysqlCivil{dateOnly: true}
	date, clock, hasClock := strings.Cut(text, " ")
	if !hasClock {
		date, clock, hasClock = strings.Cut(text, "T")
	}
	d.dateOnly = !hasClock
	if !strings.Contains(date, "-") {
		digits := date
		if len(digits) == 14 {
			date = digits[:8]
			clock = digits[8:10] + ":" + digits[10:12] + ":" + digits[12:14]
			hasClock = true
			d.dateOnly = false
		}
		if len(date) == 8 {
			date = date[:4] + "-" + date[4:6] + "-" + date[6:8]
		} else if len(date) == 6 {
			y, _ := strconv.Atoi(date[:2])
			if y < 70 {
				y += 2000
			} else {
				y += 1900
			}
			date = fmt.Sprintf("%04d-%s-%s", y, date[2:4], date[4:6])
		} else if date == "0" {
			date = "0000-00-00"
		}
	}
	parts := strings.Split(date, "-")
	if len(parts) != 3 {
		return d
	}
	var err error
	d.year, err = strconv.Atoi(parts[0])
	if err != nil {
		return mysqlCivil{}
	}
	d.month, err = strconv.Atoi(parts[1])
	if err != nil {
		return mysqlCivil{}
	}
	d.day, err = strconv.Atoi(parts[2])
	if err != nil {
		return mysqlCivil{}
	}
	if d.year < 0 || d.year > 9999 || d.month < 0 || d.month > 12 || d.day < 0 || d.day > 31 {
		return mysqlCivil{}
	}
	d.parsed = true
	h, m, sec, micro := 0, 0, 0, 0
	if hasClock {
		fields := strings.Split(clock, ":")
		if len(fields) != 3 {
			return mysqlCivil{}
		}
		h, err = strconv.Atoi(fields[0])
		if err != nil {
			return mysqlCivil{}
		}
		m, err = strconv.Atoi(fields[1])
		if err != nil {
			return mysqlCivil{}
		}
		seconds, fraction, _ := strings.Cut(fields[2], ".")
		sec, err = strconv.Atoi(seconds)
		if err != nil {
			return mysqlCivil{}
		}
		if len(fraction) > 0 {
			for _, r := range fraction {
				if r < '0' || r > '9' {
					return mysqlCivil{}
				}
			}
			d.fsp = min(len(fraction), 6)
			fraction = fraction[:d.fsp] + strings.Repeat("0", 6-d.fsp)
			micro, _ = strconv.Atoi(fraction)
		}
		if h < 0 || h > 23 || m < 0 || m > 59 || sec < 0 || sec > 59 {
			return mysqlCivil{}
		}
	}
	if d.month == 0 || d.day == 0 {
		d.t = time.Date(1, 1, 1, h, m, sec, micro*1000, time.UTC)
		return d
	}
	d.t = time.Date(d.year, time.Month(d.month), d.day, h, m, sec, micro*1000, time.UTC)
	d.valid = d.t.Year() == d.year && int(d.t.Month()) == d.month && d.t.Day() == d.day
	return d
}

func mysqlDatePart(name string, v Value) Value {
	if v.Null {
		return NewNullValue()
	}
	d := mysqlParseCivil(v)
	if !d.valid {
		if !d.parsed || d.year == 0 && d.month == 0 && d.day == 0 || d.month != 0 && d.day != 0 {
			return NewNullValue()
		}
		switch name {
		case "YEAR":
			return NewIntValue(int64(d.year))
		case "MONTH":
			return NewIntValue(int64(d.month))
		case "DAY", "DAYOFMONTH":
			return NewIntValue(int64(d.day))
		case "MONTHNAME":
			if d.month > 0 {
				return NewTextValue(time.Month(d.month).String())
			}
		case "QUARTER":
			return NewIntValue(int64((d.month + 2) / 3))
		case "LAST_DAY":
			if d.month > 0 {
				return NewTextValue(time.Date(d.year, time.Month(d.month)+1, 0, 0, 0, 0, 0, time.UTC).Format("2006-01-02"))
			}
		}
		return NewNullValue()
	}
	switch name {
	case "YEAR":
		return NewIntValue(int64(d.year))
	case "MONTH":
		return NewIntValue(int64(d.month))
	case "DAY", "DAYOFMONTH":
		return NewIntValue(int64(d.day))
	case "DAYOFWEEK":
		return NewIntValue(int64(d.t.Weekday()) + 1)
	case "DAYOFYEAR":
		return NewIntValue(int64(d.t.YearDay()))
	case "WEEKDAY":
		return NewIntValue(int64((int(d.t.Weekday()) + 6) % 7))
	case "QUARTER":
		return NewIntValue(int64((d.month + 2) / 3))
	case "MONTHNAME":
		return NewTextValue(d.t.Month().String())
	case "DAYNAME":
		return NewTextValue(d.t.Weekday().String())
	case "LAST_DAY":
		return NewTextValue(time.Date(d.year, time.Month(d.month)+1, 0, 0, 0, 0, 0, time.UTC).Format("2006-01-02"))
	}
	return NewNullValue()
}

func mysqlWeek(t time.Time, mode int, yearWeek bool) (int, int) {
	monday := mode&1 != 0
	one := mode&2 != 0 || yearWeek
	four := mode&4 != 0
	if monday {
		four = !four
	}
	start := func(y int) time.Time {
		jan := time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC)
		offset := int(jan.Weekday())
		if monday {
			offset = (offset + 6) % 7
		}
		first := jan.AddDate(0, 0, -offset)
		if four {
			if 7-offset < 4 {
				first = first.AddDate(0, 0, 7)
			}
		} else if offset != 0 {
			first = first.AddDate(0, 0, 7)
		}
		return first
	}
	y := t.Year()
	first := start(y)
	if t.Before(first) {
		if !one {
			return y, 0
		}
		y--
		first = start(y)
	} else if one && !t.Before(start(y+1)) {
		return y + 1, 1
	}
	return y, int((mysqlDayNumber(t)-mysqlDayNumber(first))/7) + 1
}
func mysqlDayNumber(t time.Time) int64 {
	y, m, d := int64(t.Year()), int64(t.Month()), int64(t.Day())
	sum := 365*y + 31*(m-1) + d
	if m <= 2 {
		y--
	} else {
		sum -= (m*4 + 23) / 10
	}
	return sum + y/4 - ((y/100+1)*3)/4
}
func mysqlPeriod(n int64) (int64, bool) {
	if n < 0 {
		return 0, false
	}
	if n == 0 {
		return 0, true
	}
	y, m := n/100, n%100
	if m < 1 || m > 12 {
		return 0, false
	}
	if y < 100 {
		if y < 70 {
			y += 2000
		} else {
			y += 1900
		}
	}
	return y*12 + m - 1, true
}
func mysqlFormatDate(t time.Time, dateOnly bool, fsp int) string {
	if dateOnly {
		return t.Format("2006-01-02")
	}
	text := t.Format("2006-01-02 15:04:05")
	if fsp > 0 {
		text += "." + fmt.Sprintf("%06d", t.Nanosecond()/1000)[:min(fsp, 6)]
	}
	return text
}
func mysqlDecimalMicros(n int64, fsp int) string {
	negative := n < 0
	if negative {
		n = -n
	}
	text := strconv.FormatInt(n/1000000, 10)
	if fsp > 0 {
		text += "." + fmt.Sprintf("%06d", n%1000000)[:min(fsp, 6)]
	}
	if negative {
		text = "-" + text
	}
	return text
}
func mysqlFormatTime(n int64, fsp int) string {
	negative := n < 0
	if negative {
		n = -n
	}
	text := fmt.Sprintf("%02d:%02d:%02d", n/3600000000, n/60000000%60, n/1000000%60)
	if fsp > 0 {
		text += "." + fmt.Sprintf("%06d", n%1000000)[:min(fsp, 6)]
	}
	if negative {
		text = "-" + text
	}
	return text
}
func mysqlSecondsMicros(v Value) (int64, int, bool) {
	n, scale, err := valueDecimal(mysqlFunctionNumber(v))
	if err != nil {
		return 0, 0, false
	}
	fsp := min(scale, 6)
	if v.Type == ColText || v.Type == ColBlob || v.Type == ColReal {
		fsp = 6
	}
	if scale < 6 {
		n.Mul(n, decimalPower(6-scale))
	} else if scale > 6 {
		n = roundDecimal(n, scale, 6)
	}
	if !n.IsInt64() {
		if n.Sign() < 0 {
			return math.MinInt64, fsp, true
		}
		return math.MaxInt64, fsp, true
	}
	return n.Int64(), fsp, true
}
func mysqlTimeValue(v Value) (int64, int, bool) {
	text := strings.TrimSpace(mysqlStringValue(v))
	if strings.Contains(text, "-") && len(text) >= 10 && text[4] == '-' {
		d := mysqlParseCivil(v)
		if !d.valid {
			return 0, 0, false
		}
		return int64(d.t.Hour()*3600+d.t.Minute()*60+d.t.Second())*1000000 + int64(d.t.Nanosecond()/1000), d.fsp, true
	}
	negative := strings.HasPrefix(text, "-")
	text = strings.TrimPrefix(strings.TrimPrefix(text, "-"), "+")
	days := int64(0)
	if left, right, ok := strings.Cut(text, " "); ok {
		n, e := strconv.ParseInt(left, 10, 64)
		if e != nil || n < 0 || n > 34 {
			return 0, 0, false
		}
		days = n
		text = right
	}
	whole, fraction, _ := strings.Cut(text, ".")
	fsp := min(len(fraction), 6)
	if len(fraction) > 0 {
		for _, r := range fraction {
			if r < '0' || r > '9' {
				return 0, 0, false
			}
		}
	}
	fraction = fraction[:fsp] + strings.Repeat("0", 6-fsp)
	micro := int64(0)
	if fraction != "" {
		micro, _ = strconv.ParseInt(fraction, 10, 64)
	}
	h, m, sec := int64(0), int64(0), int64(0)
	var e error
	parts := strings.Split(whole, ":")
	if len(parts) == 1 {
		if len(whole) == 0 || len(whole) > 7 {
			return 0, 0, false
		}
		raw, err := strconv.ParseInt(whole, 10, 64)
		if err != nil || raw < 0 {
			return 0, 0, false
		}
		sec = raw % 100
		m = raw / 100 % 100
		h = raw / 10000
	} else if len(parts) == 2 || len(parts) == 3 {
		h, e = strconv.ParseInt(parts[0], 10, 64)
		if e != nil {
			return 0, 0, false
		}
		m, e = strconv.ParseInt(parts[1], 10, 64)
		if e != nil {
			return 0, 0, false
		}
		if len(parts) == 3 {
			sec, e = strconv.ParseInt(parts[2], 10, 64)
			if e != nil {
				return 0, 0, false
			}
		}
	} else {
		return 0, 0, false
	}
	if h < 0 || h > 838 || m < 0 || m > 59 || sec < 0 || sec > 59 {
		return 0, 0, false
	}
	h += days * 24
	if h > 838 {
		return 0, 0, false
	}
	n := (h*3600+m*60+sec)*1000000 + micro
	if negative {
		n = -n
	}
	return n, fsp, true
}
func mysqlTimeArithmetic(name string, a []Value) Value {
	if mysqlAnyNull(a) {
		return NewNullValue()
	}
	left := mysqlParseCivil(a[0])
	rightDate := mysqlParseCivil(a[1])
	x, xf, xok := mysqlTimeValue(a[0])
	y, yf, yok := mysqlTimeValue(a[1])
	if !yok {
		return NewNullValue()
	}
	fsp := max(xf, yf)
	if name == "TIMEDIFF" {
		if left.valid && rightDate.valid && left.dateOnly && rightDate.dateOnly {
			return NewTextValue("00:00:00")
		}
		if left.valid && rightDate.valid && left.dateOnly != rightDate.dateOnly {
			return NewNullValue()
		}
		if left.valid != rightDate.valid {
			return NewNullValue()
		}
		if left.valid {
			x = (mysqlDayNumber(left.t)-mysqlDayNumber(rightDate.t))*86400000000 + int64(left.t.Hour()*3600+left.t.Minute()*60+left.t.Second())*1000000 + int64(left.t.Nanosecond()/1000)
			y = int64(rightDate.t.Hour()*3600+rightDate.t.Minute()*60+rightDate.t.Second())*1000000 + int64(rightDate.t.Nanosecond()/1000)
		} else if !xok {
			return NewNullValue()
		}
		x -= y
	} else if left.valid {
		if rightDate.valid {
			return NewNullValue()
		}
		if name == "SUBTIME" {
			y = -y
		}
		t := left.t.Add(time.Duration(y) * time.Microsecond)
		if t.Year() < 0 || t.Year() > 9999 {
			return NewNullValue()
		}
		return NewTextValue(mysqlFormatDate(t, false, max(left.fsp, yf)))
	} else {
		if !xok || rightDate.valid {
			return NewNullValue()
		}
		if name == "SUBTIME" {
			y = -y
		}
		x += y
	}
	x = mysqlClampTime(x)
	return NewTextValue(mysqlFormatTime(x, fsp))
}

// The SQL forms DATE_ADD(d, INTERVAL n DAY), EXTRACT(YEAR FROM d), and
// TIMESTAMPDIFF(DAY,a,b) lower to ordinary persistent function expressions.
func mysqlDateAdd(v, amount Value, unit string, negative bool) Value {
	d := mysqlParseCivil(v)
	if !d.valid {
		return NewNullValue()
	}
	unit = strings.TrimPrefix(unit, "SQL_TSI_")
	n := mysqlTemporalInteger(amount)
	if negative {
		if n == math.MinInt64 {
			return NewNullValue()
		}
		n = -n
	}
	t := d.t
	dateOnly := d.dateOnly
	fsp := d.fsp
	switch unit {
	case "YEAR", "QUARTER", "MONTH":
		if unit == "YEAR" {
			if n > 10000 || n < -10000 {
				return NewNullValue()
			}
			n *= 12
		} else if unit == "QUARTER" {
			if n > 40000 || n < -40000 {
				return NewNullValue()
			}
			n *= 3
		}
		if n > 120000 || n < -120000 {
			return NewNullValue()
		}
		month := int64(d.year)*12 + int64(d.month-1) + n
		if month < 0 || month >= 120000 {
			return NewNullValue()
		}
		year, mon := int(month/12), time.Month(month%12+1)
		day := min(d.day, time.Date(year, mon+1, 0, 0, 0, 0, 0, time.UTC).Day())
		t = time.Date(year, mon, day, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
	case "WEEK", "DAY":
		if unit == "WEEK" {
			if n > 530000 || n < -530000 {
				return NewNullValue()
			}
			n *= 7
		}
		if n > 3660000 || n < -3660000 {
			return NewNullValue()
		}
		t = t.AddDate(0, 0, int(n))
	case "HOUR", "MINUTE", "SECOND", "MICROSECOND":
		dateOnly = false
		factor := int64(1)
		switch unit {
		case "HOUR":
			factor = 3600000000
		case "MINUTE":
			factor = 60000000
		case "SECOND":
			factor = 1000000
		}
		if unit == "SECOND" {
			var ok bool
			n, fsp, ok = mysqlSecondsMicros(amount)
			if !ok {
				return NewNullValue()
			}
			if negative {
				if n == math.MinInt64 {
					return NewNullValue()
				}
				n = -n
			}
			factor = 1
			fsp = max(d.fsp, fsp)
			if fsp > 0 {
				fsp = 6
			}
		}
		if unit == "MICROSECOND" {
			fsp = 6
		}
		if n > math.MaxInt64/factor || n < math.MinInt64/factor {
			return NewNullValue()
		}
		micro := n * factor
		days := micro / 86400000000
		remainder := micro % 86400000000
		if days > 3660000 || days < -3660000 {
			return NewNullValue()
		}
		t = t.AddDate(0, 0, int(days)).Add(time.Duration(remainder) * time.Microsecond)
	default:
		months, micro, intervalFsp, ok := mysqlCompositeInterval(mysqlStringValue(amount), unit)
		if !ok {
			return NewNullValue()
		}
		if negative {
			months = -months
			micro = -micro
		}
		if months != 0 {
			return mysqlDateAdd(v, NewIntValue(months), "MONTH", false)
		}
		dateOnly = false
		fsp = max(fsp, intervalFsp)
		days := micro / 86400000000
		if days > 3660000 || days < -3660000 {
			return NewNullValue()
		}
		t = t.AddDate(0, 0, int(days)).Add(time.Duration(micro%86400000000) * time.Microsecond)
	}
	if t.Year() < 0 || t.Year() > 9999 {
		return NewNullValue()
	}
	return NewTextValue(mysqlFormatDate(t, dateOnly, fsp))
}
func mysqlCompositeInterval(text, unit string) (int64, int64, int, bool) {
	text = strings.TrimSpace(text)
	sign := int64(1)
	if strings.HasPrefix(text, "-") {
		sign = -1
		text = text[1:]
	} else {
		text = strings.TrimPrefix(text, "+")
	}
	if unit == "YEAR_MONTH" {
		p := strings.Split(text, "-")
		if len(p) != 2 {
			return 0, 0, 0, false
		}
		y, e := strconv.ParseInt(p[0], 10, 64)
		if e != nil || y < 0 || y > 10000 {
			return 0, 0, 0, false
		}
		m, e := strconv.ParseInt(p[1], 10, 64)
		if e != nil || m < 0 || m > 120000 {
			return 0, 0, 0, false
		}
		return sign * (12*y + m), 0, 0, true
	}
	multipliers := map[string][]int64{"DAY_HOUR": {86400000000, 3600000000}, "DAY_MINUTE": {86400000000, 3600000000, 60000000}, "DAY_SECOND": {86400000000, 3600000000, 60000000, 1000000}, "HOUR_MINUTE": {3600000000, 60000000}, "HOUR_SECOND": {3600000000, 60000000, 1000000}, "MINUTE_SECOND": {60000000, 1000000}, "DAY_MICROSECOND": {86400000000, 3600000000, 60000000, 1000000}, "HOUR_MICROSECOND": {3600000000, 60000000, 1000000}, "MINUTE_MICROSECOND": {60000000, 1000000}, "SECOND_MICROSECOND": {1000000}}
	factors, ok := multipliers[unit]
	if !ok {
		return 0, 0, 0, false
	}
	whole, fraction, hasFraction := strings.Cut(text, ".")
	if hasFraction && !strings.HasSuffix(unit, "MICROSECOND") {
		return 0, 0, 0, false
	}
	if !hasFraction && strings.HasSuffix(unit, "MICROSECOND") {
		// Composite microsecond intervals right-align the fields, including a
		// decimal fractional field: '1:2' means one second and .2 seconds.
		last := strings.LastIndexAny(whole, " :-")
		fraction, whole, hasFraction = whole[last+1:], whole[:max(last, 0)], true
	}
	fields := strings.FieldsFunc(whole, func(r rune) bool { return r == ' ' || r == ':' || r == '-' })
	if len(fields) > len(factors) || len(fields) == 0 && !hasFraction {
		return 0, 0, 0, false
	}
	factors = factors[len(factors)-len(fields):]
	var sum int64
	for i, f := range fields {
		n, e := strconv.ParseInt(f, 10, 64)
		if e != nil || n < 0 || n > math.MaxInt64/factors[i] {
			return 0, 0, 0, false
		}
		piece := n * factors[i]
		if sum > math.MaxInt64-piece {
			return 0, 0, 0, false
		}
		sum += piece
	}
	fsp := 0
	if hasFraction {
		for _, r := range fraction {
			if r < '0' || r > '9' {
				return 0, 0, 0, false
			}
		}
		fsp = min(len(fraction), 6)
		fraction = fraction[:fsp] + strings.Repeat("0", 6-fsp)
		micro, _ := strconv.ParseInt(fraction, 10, 64)
		if sum > math.MaxInt64-micro {
			return 0, 0, 0, false
		}
		sum += micro
		fsp = 6
	}
	return 0, sign * sum, fsp, true
}
func mysqlTimestampDiff(unit string, x, y Value) Value {
	a, b := mysqlParseCivil(x), mysqlParseCivil(y)
	if !a.valid || !b.valid {
		return NewNullValue()
	}
	unit = strings.TrimPrefix(unit, "SQL_TSI_")
	months := int64((b.year-a.year)*12 + b.month - a.month)
	sign := int64(1)
	first, second := a, b
	if b.t.Before(a.t) {
		sign = -1
		first, second = b, a
	}
	if unit == "YEAR" || unit == "QUARTER" || unit == "MONTH" {
		months = int64((second.year-first.year)*12 + second.month - first.month)
		if second.day < first.day || second.day == first.day && (second.t.Hour()*3600000000+second.t.Minute()*60000000+second.t.Second()*1000000+second.t.Nanosecond()/1000) < (first.t.Hour()*3600000000+first.t.Minute()*60000000+first.t.Second()*1000000+first.t.Nanosecond()/1000) {
			months--
		}
		months *= sign
		switch unit {
		case "YEAR":
			months /= 12
		case "QUARTER":
			months /= 3
		}
		return NewIntValue(months)
	}
	micro := (mysqlDayNumber(b.t)-mysqlDayNumber(a.t))*86400000000 + int64(b.t.Hour()*3600+b.t.Minute()*60+b.t.Second()-a.t.Hour()*3600-a.t.Minute()*60-a.t.Second())*1000000 + int64(b.t.Nanosecond()-a.t.Nanosecond())/1000
	var factor int64
	switch unit {
	case "WEEK":
		factor = 7 * 86400000000
	case "DAY":
		factor = 86400000000
	case "HOUR":
		factor = 3600000000
	case "MINUTE":
		factor = 60000000
	case "SECOND":
		factor = 1000000
	case "MICROSECOND":
		factor = 1
	default:
		return NewNullValue()
	}
	return NewIntValue(micro / factor)
}
func mysqlExtract(unit string, v Value) Value {
	switch unit {
	case "YEAR", "MONTH", "DAY", "QUARTER":
		return mysqlDatePart(unit, v)
	case "WEEK":
		d := mysqlParseCivil(v)
		if !d.valid {
			return NewNullValue()
		}
		_, w := mysqlWeek(d.t, 0, false)
		return NewIntValue(int64(w))
	case "YEAR_MONTH":
		d := mysqlParseCivil(v)
		if !d.valid {
			return NewNullValue()
		}
		return NewIntValue(int64(d.year*100 + d.month))
	}
	n, _, ok := mysqlTimeValue(v)
	if !ok {
		return NewNullValue()
	}
	sign := int64(1)
	if n < 0 {
		sign = -1
		n = -n
	}
	h, m, sec, micro := n/3600000000, n/60000000%60, n/1000000%60, n%1000000
	d := mysqlParseCivil(v)
	day := int64(0)
	if d.valid {
		day = int64(d.day)
	}
	var result int64
	switch unit {
	case "HOUR":
		result = h
	case "MINUTE":
		result = m
	case "SECOND":
		result = sec
	case "MICROSECOND":
		result = micro
	case "DAY_HOUR":
		result = day*100 + h
	case "DAY_MINUTE":
		result = day*10000 + h*100 + m
	case "DAY_SECOND":
		result = day*1000000 + h*10000 + m*100 + sec
	case "HOUR_MINUTE":
		result = h*100 + m
	case "HOUR_SECOND":
		result = h*10000 + m*100 + sec
	case "MINUTE_SECOND":
		result = m*100 + sec
	case "DAY_MICROSECOND":
		result = (day*1000000+h*10000+m*100+sec)*1000000 + micro
	case "HOUR_MICROSECOND":
		result = (h*10000+m*100+sec)*1000000 + micro
	case "MINUTE_MICROSECOND":
		result = (m*100+sec)*1000000 + micro
	case "SECOND_MICROSECOND":
		result = sec*1000000 + micro
	default:
		return NewNullValue()
	}
	return NewIntValue(sign * result)
}

func mysqlClampTime(n int64) int64 {
	// MySQL clips to the last whole second; a clipped value retains its declared
	// fractional precision but fills those fractional places with zero.
	maximum := (mysqlMaxTimeMicros / 1000000) * 1000000
	if n > maximum {
		return maximum
	}
	if n < -maximum {
		return -maximum
	}
	return n
}
func mysqlTemporalInteger(v Value) int64 {
	if !isNumeric(v) {
		return mysqlFunctionInt(v)
	}
	n, scale, err := valueDecimal(v)
	if err != nil {
		return 0
	}
	n = roundDecimal(n, scale, 0)
	if !n.IsInt64() {
		if n.Sign() < 0 {
			return math.MinInt64
		}
		return math.MaxInt64
	}
	return n.Int64()
}
