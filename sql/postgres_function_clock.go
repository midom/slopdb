package sql

import (
	"fmt"
	"math"
	"strings"
	"time"
)

func postgresClockFunctions() map[string]mysqlFunctionSpec {
	out := map[string]mysqlFunctionSpec{}
	add := func(name string, min, max int, typ ColumnType, decl string, volatile bool, eval func(*Schema, []Value) (Value, error)) {
		out[name] = mysqlFunctionSpec{Min: min, Max: max, Return: typ, Volatile: volatile, Metadata: func([]ColumnDef) ColumnDef { return ColumnDef{ColType: typ, Declared: decl, Postgres: true} }, Eval: func(s *Schema, a []Value) (Value, error) {
			v, e := eval(s, a)
			if decl != "" {
				v.PGType = decl
			}
			return v, e
		}}
	}
	for _, name := range strings.Fields("CLOCK_TIMESTAMP STATEMENT_TIMESTAMP TRANSACTION_TIMESTAMP NOW") {
		name := name
		add(name, 0, 0, ColText, "TIMESTAMPTZ", true, func(s *Schema, a []Value) (Value, error) {
			t := s.statementTime
			if t.IsZero() || name == "CLOCK_TIMESTAMP" {
				t = time.Now().UTC()
			}
			if (name == "NOW" || name == "TRANSACTION_TIMESTAMP") && s.inTx && !s.postgresTransactionTime.IsZero() {
				t = s.postgresTransactionTime
			}
			return NewTextValue(postgresTimeText(t, true)), nil
		})
	}
	add("TIMEOFDAY", 0, 0, ColText, "TEXT", true, func(*Schema, []Value) (Value, error) {
		return NewTextValue(time.Now().UTC().Format("Mon Jan 02 15:04:05.000000 2006 MST")), nil
	})
	for _, name := range strings.Fields("CURRENT_DATABASE CURRENT_SCHEMA CURRENT_USER SESSION_USER USER PG_BACKEND_PID VERSION") {
		name := name
		typ := ColText
		decl := "TEXT"
		if name == "PG_BACKEND_PID" {
			typ = ColInteger
			decl = "INTEGER"
		}
		add(name, 0, 0, typ, decl, true, func(s *Schema, a []Value) (Value, error) {
			session, _ := PostgresSession(s.ctx)
			switch name {
			case "CURRENT_DATABASE":
				return NewTextValue(session.Database), nil
			case "CURRENT_SCHEMA":
				for _, n := range s.SearchPath() {
					if s.namespaces[s.namespaceAlias(n)] {
						if n == "main" {
							n = "public"
						}
						return NewTextValue(n), nil
					}
				}
				return NewNullValue(), nil
			case "CURRENT_USER", "SESSION_USER", "USER":
				return NewTextValue(session.User), nil
			case "PG_BACKEND_PID":
				return NewIntValue(int64(session.BackendPID)), nil
			case "PG_CLIENT_ENCODING":
				return NewTextValue("UTF8"), nil
			default:
				return NewTextValue("PostgreSQL 16.0 compatible SlopDB (LSM)"), nil
			}
		})
	}
	add("PG_SLEEP", 1, 1, ColText, "TEXT", true, func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		seconds, e := postgresNumber(a[0])
		if e != nil {
			return NewNullValue(), e
		}
		if seconds <= 0 {
			return NewTextValue(""), nil
		}
		if seconds > float64(math.MaxInt64)/float64(time.Second) {
			return NewNullValue(), fmt.Errorf("sleep duration out of range")
		}
		timer := time.NewTimer(time.Duration(seconds * float64(time.Second)))
		defer timer.Stop()
		if s.ctx == nil {
			<-timer.C
			return NewTextValue(""), nil
		}
		select {
		case <-timer.C:
			return NewTextValue(""), nil
		case <-s.ctx.Done():
			return NewNullValue(), s.ctx.Err()
		}
	})
	add("TO_TIMESTAMP", 1, 2, ColText, "TIMESTAMPTZ", false, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		if len(a) == 1 {
			n, e := postgresNumber(a[0])
			if e != nil {
				return NewNullValue(), e
			}
			if math.Abs(n) > 253402300799 {
				return NewNullValue(), fmt.Errorf("timestamp out of supported range")
			}
			sec, fraction := math.Modf(n)
			t := time.Unix(int64(sec), int64(math.Round(fraction*1e6))*1000).UTC()
			return NewTextValue(postgresTimeText(t, true)), nil
		}
		t, e := postgresParseFormatted(postgresText(a[0]), postgresText(a[1]))
		if e != nil {
			return NewNullValue(), e
		}
		return NewTextValue(postgresTimeText(t, true)), nil
	})
	add("TO_DATE", 2, 2, ColText, "DATE", false, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		t, e := postgresParseFormatted(postgresText(a[0]), postgresText(a[1]))
		if e != nil {
			return NewNullValue(), e
		}
		return NewTextValue(t.Format("2006-01-02")), nil
	})
	add("TO_CHAR", 2, 2, ColText, "TEXT", false, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		t, e := postgresParseTime(a[0])
		if e != nil {
			return NewNullValue(), fmt.Errorf("to_char numeric/interval templates are not supported: %w", e)
		}
		text, e := postgresFormatTime(t, postgresText(a[1]))
		if e != nil {
			return NewNullValue(), e
		}
		return NewTextValue(text), nil
	})
	add("DATE_PART", 2, 2, ColReal, "DOUBLE PRECISION", false, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		t, e := postgresParseTime(a[1])
		if e != nil {
			return NewNullValue(), e
		}
		n, e := postgresDatePart(postgresText(a[0]), t)
		if e != nil {
			return NewNullValue(), e
		}
		return NewRealValue(n), nil
	})
	add("DATE_TRUNC", 2, 3, ColText, "TIMESTAMPTZ", false, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		t, e := postgresParseTime(a[1])
		if e != nil {
			return NewNullValue(), e
		}
		zoned := PostgresValueOID(a[1]) != 1114
		if len(a) > 2 {
			if !zoned {
				return NewNullValue(), fmt.Errorf("date_trunc with time zone requires timestamptz")
			}
			zone, e := time.LoadLocation(postgresText(a[2]))
			if e != nil {
				return NewNullValue(), e
			}
			t = t.In(zone)
		}
		t, e = postgresTruncateTime(postgresText(a[0]), t)
		if e != nil {
			return NewNullValue(), e
		}
		v := NewTextValue(postgresTimeText(t, zoned))
		if zoned {
			v.PGType = "TIMESTAMPTZ"
		} else {
			v.PGType = "TIMESTAMP"
		}
		return v, nil
	})
	spec := out["DATE_TRUNC"]
	spec.Metadata = func(a []ColumnDef) ColumnDef {
		decl := "TIMESTAMPTZ"
		if len(a) > 1 && PostgresColumnOID(a[1]) == 1114 {
			decl = "TIMESTAMP"
		}
		return ColumnDef{ColType: ColText, Declared: decl, Postgres: true}
	}
	fn := spec.Eval
	spec.Eval = func(s *Schema, a []Value) (Value, error) {
		v, e := fn(s, a)
		if len(a) > 1 && PostgresValueOID(a[1]) == 1114 {
			v.PGType = "TIMESTAMP"
		}
		return v, e
	}
	out["DATE_TRUNC"] = spec
	add("ISFINITE", 1, 1, ColInteger, "BOOLEAN", false, func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		if a[0].Text == "infinity" || a[0].Text == "-infinity" {
			return NewIntValue(0), nil
		}
		if _, e := postgresParseTime(a[0]); e != nil {
			return NewNullValue(), e
		}
		return NewIntValue(1), nil
	})
	add("MAKE_DATE", 3, 3, ColText, "DATE", false, func(s *Schema, a []Value) (Value, error) {
		t, e := postgresMakeTime(a)
		if e != nil || mysqlAnyNull(a) {
			return NewNullValue(), e
		}
		return NewTextValue(t.Format("2006-01-02")), nil
	})
	add("MAKE_TIME", 3, 3, ColText, "TIME", false, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		t, e := postgresMakeTime(append([]Value{NewIntValue(2000), NewIntValue(1), NewIntValue(1)}, a...))
		if e != nil {
			return NewNullValue(), e
		}
		return NewTextValue(t.Format("15:04:05.999999")), nil
	})
	add("MAKE_TIMESTAMP", 6, 6, ColText, "TIMESTAMP", false, func(s *Schema, a []Value) (Value, error) {
		t, e := postgresMakeTime(a)
		if e != nil || mysqlAnyNull(a) {
			return NewNullValue(), e
		}
		return NewTextValue(postgresTimeText(t, false)), nil
	})
	return out
}
func postgresTimeText(t time.Time, zoned bool) string {
	if zoned {
		return t.UTC().Format("2006-01-02 15:04:05.999999") + "+00"
	}
	return t.Format("2006-01-02 15:04:05.999999")
}
func postgresParseTime(v Value) (time.Time, error) {
	text := postgresText(v)
	for _, f := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999Z07", "2006-01-02 15:04:05.999999999", "2006-01-02 15:04", "2006-01-02", "15:04:05.999999999", "15:04"} {
		if t, e := time.Parse(f, text); e == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid or unsupported timestamp %q", text)
}
func postgresMakeTime(a []Value) (time.Time, error) {
	if mysqlAnyNull(a) {
		return time.Time{}, nil
	}
	nums := make([]int, len(a))
	for i, v := range a {
		if i == 5 {
			continue
		}
		n, e := CastPostgresValue(v, "INTEGER")
		if e != nil {
			return time.Time{}, e
		}
		nums[i] = int(n.Int)
	}
	if nums[0] < 1 || nums[0] > 9999 || nums[1] < 1 || nums[1] > 12 || nums[2] < 1 || nums[2] > 31 {
		return time.Time{}, fmt.Errorf("date field out of range")
	}
	hour, minute, second, nano := 0, 0, 0, 0
	if len(a) == 6 {
		hour, minute = nums[3], nums[4]
		sec, e := postgresNumber(a[5])
		if e != nil {
			return time.Time{}, e
		}
		if hour < 0 || hour > 23 || minute < 0 || minute > 59 || sec < 0 || sec >= 60 {
			return time.Time{}, fmt.Errorf("time field out of range")
		}
		second = int(sec)
		nano = int(math.Round((sec-float64(second))*1e6)) * 1000
	}
	t := time.Date(nums[0], time.Month(nums[1]), nums[2], hour, minute, second, nano, time.UTC)
	if t.Day() != nums[2] || int(t.Month()) != nums[1] {
		return time.Time{}, fmt.Errorf("date field out of range")
	}
	return t, nil
}
func postgresDatePart(field string, t time.Time) (float64, error) {
	_, offset := t.Zone()
	y, w := t.ISOWeek()
	second := float64(t.Second()) + float64(t.Nanosecond())/1e9
	switch strings.ToLower(field) {
	case "year", "years":
		return float64(t.Year()), nil
	case "month", "months":
		return float64(t.Month()), nil
	case "day", "days":
		return float64(t.Day()), nil
	case "hour", "hours":
		return float64(t.Hour()), nil
	case "minute", "minutes":
		return float64(t.Minute()), nil
	case "second", "seconds":
		return second, nil
	case "milliseconds":
		return second * 1e3, nil
	case "microseconds":
		return second * 1e6, nil
	case "epoch":
		return float64(t.Unix()) + float64(t.Nanosecond())/1e9, nil
	case "dow":
		return float64(t.Weekday()), nil
	case "isodow":
		return float64((int(t.Weekday())+6)%7 + 1), nil
	case "doy":
		return float64(t.YearDay()), nil
	case "week", "weeks":
		return float64(w), nil
	case "isoyear":
		return float64(y), nil
	case "quarter":
		return float64((int(t.Month()) + 2) / 3), nil
	case "decade":
		return float64(t.Year() / 10), nil
	case "century":
		return float64((t.Year() + 99) / 100), nil
	case "millennium":
		return float64((t.Year() + 999) / 1000), nil
	case "timezone":
		return float64(offset), nil
	case "timezone_hour":
		return float64(offset / 3600), nil
	case "timezone_minute":
		return float64(offset % 3600 / 60), nil
	}
	return 0, fmt.Errorf("unsupported timestamp field %q", field)
}
func postgresTruncateTime(field string, t time.Time) (time.Time, error) {
	y, m, d := t.Date()
	h, min, sec := t.Clock()
	ns := t.Nanosecond()
	switch strings.ToLower(field) {
	case "microseconds":
		ns = ns / 1000 * 1000
	case "milliseconds":
		ns = ns / 1000000 * 1000000
	case "second":
		ns = 0
	case "minute":
		sec, ns = 0, 0
	case "hour":
		min, sec, ns = 0, 0, 0
	case "day":
		h, min, sec, ns = 0, 0, 0, 0
	case "week":
		return postgresTruncateTime("day", t.AddDate(0, 0, -(int(t.Weekday())+6)%7))
	case "month":
		d, h, min, sec, ns = 1, 0, 0, 0, 0
	case "quarter":
		m = time.Month((int(m)-1)/3*3 + 1)
		d, h, min, sec, ns = 1, 0, 0, 0, 0
	case "year":
		m, d, h, min, sec, ns = 1, 1, 0, 0, 0, 0
	case "decade":
		y = y / 10 * 10
		m, d, h, min, sec, ns = 1, 1, 0, 0, 0, 0
	case "century":
		y = (y-1)/100*100 + 1
		m, d, h, min, sec, ns = 1, 1, 0, 0, 0, 0
	case "millennium":
		y = (y-1)/1000*1000 + 1
		m, d, h, min, sec, ns = 1, 1, 0, 0, 0, 0
	default:
		return time.Time{}, fmt.Errorf("unsupported date_trunc field %q", field)
	}
	return time.Date(y, m, d, h, min, sec, ns, t.Location()), nil
}
