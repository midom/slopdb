package sql

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

var postgresDateTokens = []string{"HH24", "HH12", "HH", "MI", "SSSSS", "SSSS", "SS", "MS", "US", "YYYY", "YYY", "YY", "Y", "IYYY", "IYY", "IY", "I", "MONTH", "Month", "month", "MON", "Mon", "mon", "MM", "DAY", "Day", "day", "DDD", "DD", "DY", "Dy", "dy", "IDDD", "ID", "D", "IW", "WW", "W", "CC", "J", "Q", "RM", "rm", "TZH", "TZM", "TZ", "tz", "OF", "A.M.", "P.M.", "a.m.", "p.m.", "AM", "PM", "am", "pm", "BC", "AD", "bc", "ad"}

func init() {
	sort.SliceStable(postgresDateTokens, func(i, j int) bool { return len(postgresDateTokens[i]) > len(postgresDateTokens[j]) })
}
func postgresDateToken(format string) (string, int) {
	for _, token := range postgresDateTokens {
		if len(format) >= len(token) && strings.EqualFold(format[:len(token)], token) {
			return format[:len(token)], len(token)
		}
	}
	return "", 0
}
func postgresFormatTime(t time.Time, format string) (string, error) {
	var out strings.Builder
	for i := 0; i < len(format); {
		if format[i] == '"' {
			i++
			for i < len(format) && format[i] != '"' {
				if format[i] == '\\' && i+1 < len(format) {
					i++
				}
				out.WriteByte(format[i])
				i++
			}
			if i >= len(format) {
				return "", fmt.Errorf("unterminated date format literal")
			}
			i++
			continue
		}
		fill := false
		if i+2 <= len(format) && strings.EqualFold(format[i:i+2], "FM") {
			fill = true
			i += 2
			if i == len(format) {
				break
			}
		}
		token, n := postgresDateToken(format[i:])
		if n == 0 {
			out.WriteByte(format[i])
			i++
			continue
		}
		i += n
		u := strings.ToUpper(token)
		year, week := t.ISOWeek()
		value := ""
		numeric := -1
		pad := 0
		switch u {
		case "YYYY":
			numeric, pad = t.Year(), 4
		case "YYY":
			numeric, pad = t.Year()%1000, 3
		case "YY":
			numeric, pad = t.Year()%100, 2
		case "Y":
			numeric = t.Year() % 10
		case "IYYY":
			numeric, pad = year, 4
		case "IYY":
			numeric, pad = year%1000, 3
		case "IY":
			numeric, pad = year%100, 2
		case "I":
			numeric = year % 10
		case "MM":
			numeric, pad = int(t.Month()), 2
		case "DD":
			numeric, pad = t.Day(), 2
		case "DDD":
			numeric, pad = t.YearDay(), 3
		case "IDDD":
			numeric, pad = (week-1)*7+(int(t.Weekday())+6)%7+1, 3
		case "D":
			numeric = int(t.Weekday()) + 1
		case "ID":
			numeric = (int(t.Weekday())+6)%7 + 1
		case "Q":
			numeric = (int(t.Month()) + 2) / 3
		case "HH24":
			numeric, pad = t.Hour(), 2
		case "HH12", "HH":
			numeric, pad = (t.Hour()+11)%12+1, 2
		case "MI":
			numeric, pad = t.Minute(), 2
		case "SS":
			numeric, pad = t.Second(), 2
		case "SSSS", "SSSSS":
			numeric = t.Hour()*3600 + t.Minute()*60 + t.Second()
		case "MS":
			numeric, pad = t.Nanosecond()/1000000, 3
		case "US":
			numeric, pad = t.Nanosecond()/1000, 6
		case "IW":
			numeric, pad = week, 2
		case "WW":
			numeric, pad = (t.YearDay()-1)/7+1, 2
		case "W":
			numeric = (t.Day()-1)/7 + 1
		case "CC":
			numeric, pad = (t.Year()+99)/100, 2
		case "J":
			numeric = int(t.UTC().Unix()/86400) + 2440588
		case "MONTH", "MON":
			value = t.Month().String()
			if u == "MON" {
				value = value[:3]
			} else if !fill {
				value += strings.Repeat(" ", 9-len(value))
			}
			value = postgresTokenCase(token, value)
		case "DAY", "DY":
			value = t.Weekday().String()
			if u == "DY" {
				value = value[:3]
			} else if !fill {
				value += strings.Repeat(" ", 9-len(value))
			}
			value = postgresTokenCase(token, value)
		case "RM":
			value = []string{"I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII"}[int(t.Month())-1]
			if !fill {
				value += strings.Repeat(" ", 4-len(value))
			}
			value = postgresTokenCase(token, value)
		case "AM", "PM", "A.M.", "P.M.":
			value = "AM"
			if t.Hour() >= 12 {
				value = "PM"
			}
			if strings.Contains(token, ".") {
				value = string(value[0]) + ".M."
			}
			value = postgresTokenCase(token, value)
		case "BC", "AD":
			value = "AD"
			if t.Year() < 1 {
				value = "BC"
			}
			value = postgresTokenCase(token, value)
		case "TZ":
			value, _ = t.Zone()
			value = postgresTokenCase(token, value)
		case "TZH":
			_, off := t.Zone()
			value = fmt.Sprintf("%+03d", off/3600)
		case "TZM":
			_, off := t.Zone()
			if off < 0 {
				off = -off
			}
			value = fmt.Sprintf("%02d", off%3600/60)
		case "OF":
			_, off := t.Zone()
			sign := "+"
			if off < 0 {
				sign = "-"
				off = -off
			}
			value = fmt.Sprintf("%s%02d", sign, off/3600)
			if off%3600 != 0 {
				value += fmt.Sprintf(":%02d", off%3600/60)
			}
		}
		if numeric >= 0 {
			value = strconv.Itoa(numeric)
			if !fill && len(value) < pad {
				value = strings.Repeat("0", pad-len(value)) + value
			}
		}
		if i+2 <= len(format) && strings.EqualFold(format[i:i+2], "TH") {
			suffix := "th"
			if numeric%100 < 11 || numeric%100 > 13 {
				switch numeric % 10 {
				case 1:
					suffix = "st"
				case 2:
					suffix = "nd"
				case 3:
					suffix = "rd"
				}
			}
			if format[i:i+2] == "TH" {
				suffix = strings.ToUpper(suffix)
			}
			value += suffix
			i += 2
		}
		out.WriteString(value)
	}
	return out.String(), nil
}
func postgresTokenCase(token, value string) string {
	if token == strings.ToLower(token) {
		return strings.ToLower(value)
	}
	if token == strings.ToUpper(token) {
		return strings.ToUpper(value)
	}
	if value == "" {
		return value
	}
	return strings.ToUpper(value[:1]) + strings.ToLower(value[1:])
}
func postgresParseFormatted(text, format string) (time.Time, error) {
	var layout strings.Builder
	for i := 0; i < len(format); {
		if format[i] == '"' {
			i++
			for i < len(format) && format[i] != '"' {
				layout.WriteByte(format[i])
				i++
			}
			if i >= len(format) {
				return time.Time{}, fmt.Errorf("unterminated date format literal")
			}
			i++
			continue
		}
		if i+2 <= len(format) && (strings.EqualFold(format[i:i+2], "FX") || strings.EqualFold(format[i:i+2], "FM")) {
			i += 2
			continue
		}
		token, n := postgresDateToken(format[i:])
		if n == 0 {
			if unicode.IsLetter(rune(format[i])) {
				layout.WriteByte(format[i])
			} else {
				layout.WriteByte(format[i])
			}
			i++
			continue
		}
		i += n
		switch strings.ToUpper(token) {
		case "YYYY":
			layout.WriteString("2006")
		case "YY":
			layout.WriteString("06")
		case "MM":
			layout.WriteString("01")
		case "MONTH":
			layout.WriteString("January")
		case "MON":
			layout.WriteString("Jan")
		case "DD":
			layout.WriteString("02")
		case "DDD":
			layout.WriteString("002")
		case "HH24":
			layout.WriteString("15")
		case "HH12", "HH":
			layout.WriteString("03")
		case "MI":
			layout.WriteString("04")
		case "SS":
			layout.WriteString("05")
		case "MS":
			layout.WriteString("000")
		case "US":
			layout.WriteString("000000")
		case "AM", "PM":
			layout.WriteString("PM")
		case "TZH":
			layout.WriteString("-07")
		case "TZM":
			layout.WriteString("00")
		default:
			return time.Time{}, fmt.Errorf("unsupported input date format token %q", token)
		}
	}
	t, e := time.Parse(layout.String(), text)
	if e != nil {
		return time.Time{}, e
	}
	if strings.Contains(strings.ToUpper(format), "YY") && !strings.Contains(strings.ToUpper(format), "YYYY") && t.Year() < 1970 {
		t = t.AddDate(100, 0, 0)
	}
	if t.Year() == 0 {
		t = t.AddDate(2000, 0, 0)
	}
	return t, nil
}
