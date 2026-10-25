package sql

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
)

func mysqlDateFormat(s *Schema, d mysqlCivil, format string, timeOnly bool) Value {
	var b strings.Builder
	for i := 0; i < len(format); i++ {
		piece := format[i : i+1]
		if format[i] == '%' && i+1 < len(format) {
			i++
			code := format[i]
			h := d.t.Hour()
			h12 := h % 12
			if h12 == 0 {
				h12 = 12
			}
			ampm := "AM"
			if h >= 12 {
				ampm = "PM"
			}
			switch code {
			case '%':
				piece = "%"
			case 'a':
				if !d.valid {
					return NewNullValue()
				}
				piece = d.t.Weekday().String()[:3]
			case 'b':
				if d.month == 0 {
					return NewNullValue()
				}
				piece = time.Month(d.month).String()[:3]
			case 'c':
				piece = strconv.Itoa(d.month)
			case 'D':
				suffix := "th"
				if d.day%100 < 11 || d.day%100 > 13 {
					switch d.day % 10 {
					case 1:
						suffix = "st"
					case 2:
						suffix = "nd"
					case 3:
						suffix = "rd"
					}
				}
				piece = strconv.Itoa(d.day) + suffix
			case 'd':
				piece = fmt.Sprintf("%02d", d.day)
			case 'e':
				piece = strconv.Itoa(d.day)
			case 'f':
				piece = fmt.Sprintf("%06d", d.t.Nanosecond()/1000)
			case 'H':
				piece = fmt.Sprintf("%02d", h)
			case 'h', 'I':
				piece = fmt.Sprintf("%02d", h12)
			case 'i':
				piece = fmt.Sprintf("%02d", d.t.Minute())
			case 'j':
				if !d.valid {
					return NewNullValue()
				}
				piece = fmt.Sprintf("%03d", d.t.YearDay())
			case 'k':
				piece = strconv.Itoa(h)
			case 'l':
				piece = strconv.Itoa(h12)
			case 'M':
				if d.month == 0 {
					return NewNullValue()
				}
				piece = time.Month(d.month).String()
			case 'm':
				piece = fmt.Sprintf("%02d", d.month)
			case 'p':
				piece = ampm
			case 'r':
				piece = fmt.Sprintf("%02d:%02d:%02d %s", h12, d.t.Minute(), d.t.Second(), ampm)
			case 'S', 's':
				piece = fmt.Sprintf("%02d", d.t.Second())
			case 'T':
				piece = fmt.Sprintf("%02d:%02d:%02d", h, d.t.Minute(), d.t.Second())
			case 'U', 'u', 'V', 'v':
				if !d.valid {
					return NewNullValue()
				}
				mode := 0
				switch code {
				case 'u':
					mode = 1
				case 'V':
					mode = 2
				case 'v':
					mode = 3
				}
				_, week := mysqlWeek(d.t, mode, false)
				piece = fmt.Sprintf("%02d", week)
			case 'W':
				if !d.valid {
					return NewNullValue()
				}
				piece = d.t.Weekday().String()
			case 'w':
				if !d.valid {
					return NewNullValue()
				}
				piece = strconv.Itoa(int(d.t.Weekday()))
			case 'X', 'x':
				if !d.valid {
					return NewNullValue()
				}
				mode := 2
				if code == 'x' {
					mode = 3
				}
				year, _ := mysqlWeek(d.t, mode, true)
				piece = fmt.Sprintf("%04d", year)
			case 'Y':
				piece = fmt.Sprintf("%04d", d.year)
			case 'y':
				piece = fmt.Sprintf("%02d", d.year%100)
			default:
				piece = string(code)
			}
		}
		if mysqlFunctionSize(s, uint64(b.Len())+uint64(len(piece))) != nil {
			return NewNullValue()
		}
		b.WriteString(piece)
	}
	return NewTextValue(b.String())
}
func mysqlTimeFormat(s *Schema, n int64, fsp int, format string) Value {
	negative := n < 0
	if negative {
		n = -n
	}
	hours := n / 3600000000
	minute := n / 60000000 % 60
	second := n / 1000000 % 60
	micro := n % 1000000
	var b strings.Builder
	if negative {
		b.WriteByte('-')
	}
	for i := 0; i < len(format); i++ {
		piece := format[i : i+1]
		if format[i] == '%' && i+1 < len(format) {
			i++
			h12 := hours % 12
			if h12 == 0 {
				h12 = 12
			}
			ampm := "AM"
			if hours%24 >= 12 {
				ampm = "PM"
			}
			switch format[i] {
			case '%':
				piece = "%"
			case 'f':
				piece = fmt.Sprintf("%06d", micro)
			case 'H':
				piece = fmt.Sprintf("%02d", hours)
			case 'k':
				piece = strconv.FormatInt(hours, 10)
			case 'h', 'I':
				piece = fmt.Sprintf("%02d", h12)
			case 'l':
				piece = strconv.FormatInt(h12, 10)
			case 'i':
				piece = fmt.Sprintf("%02d", minute)
			case 's', 'S':
				piece = fmt.Sprintf("%02d", second)
			case 'p':
				piece = ampm
			case 'r':
				piece = fmt.Sprintf("%02d:%02d:%02d %s", h12, minute, second, ampm)
			case 'T':
				piece = fmt.Sprintf("%02d:%02d:%02d", hours, minute, second)
			case 'a', 'b', 'c', 'D', 'd', 'e', 'j', 'M', 'm', 'U', 'u', 'V', 'v', 'W', 'w', 'X', 'x', 'Y', 'y':
				return NewNullValue()
			default:
				piece = string(format[i])
			}
		}
		if mysqlFunctionSize(s, uint64(b.Len())+uint64(len(piece))) != nil {
			return NewNullValue()
		}
		b.WriteString(piece)
	}
	return NewTextValue(b.String())
}

func mysqlStrToDate(input, format string) Value {
	// Expand compound clock directives before scanning. Every format character is
	// consumed once; inputs and parsing state remain statement-local.
	input = strings.TrimLeftFunc(input, unicode.IsSpace)
	pos := 0
	y, m, d, h, minute, sec, micro, fsp := 0, 0, 0, 0, 0, 0, 0, 0
	dateSeen, timeSeen, twelve := false, false, false
	ampm := ""
	yearDay := -1
	week, weekYear, weekday, weekMode := -1, -1, -1, 0
	number := func(width int) (int, bool) {
		start := pos
		for pos < len(input) && pos-start < width && input[pos] >= '0' && input[pos] <= '9' {
			pos++
		}
		if start == pos {
			return 0, false
		}
		n, e := strconv.Atoi(input[start:pos])
		return n, e == nil
	}
	word := func(names []string) (int, bool) {
		for i, name := range names {
			if len(input)-pos >= len(name) && strings.EqualFold(input[pos:pos+len(name)], name) {
				pos += len(name)
				return i, true
			}
		}
		return 0, false
	}
	months := []string{"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"}
	days := []string{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"}
	for i := 0; i < len(format); i++ {
		if unicode.IsSpace(rune(format[i])) {
			for pos < len(input) && unicode.IsSpace(rune(input[pos])) {
				pos++
			}
			continue
		}
		if format[i] != '%' {
			if pos >= len(input) || input[pos] != format[i] {
				return NewNullValue()
			}
			pos++
			continue
		}
		i++
		if i >= len(format) {
			return NewNullValue()
		}
		code := format[i]
		n, ok := 0, true
		switch code {
		case 'Y', 'y', 'X', 'x':
			width := 4
			if code == 'y' {
				width = 2
			}
			n, ok = number(width)
			if ok {
				if code == 'y' || code == 'Y' && n < 100 {
					if n < 70 {
						n += 2000
					} else {
						n += 1900
					}
				}
				if code == 'X' || code == 'x' {
					weekYear = n
					weekMode = 2
					if code == 'x' {
						weekMode = 3
					}
				} else {
					y = n
				}
				dateSeen = true
			}
		case 'm', 'c':
			m, ok = number(2)
			dateSeen = true
		case 'd', 'e', 'D':
			d, ok = number(2)
			dateSeen = true
			if code == 'D' && ok && pos+2 <= len(input) {
				suffix := strings.ToLower(input[pos : pos+2])
				if suffix == "st" || suffix == "nd" || suffix == "rd" || suffix == "th" {
					pos += 2
				}
			}
		case 'H', 'k':
			h, ok = number(2)
			timeSeen = true
		case 'h', 'I', 'l':
			h, ok = number(2)
			timeSeen = true
			twelve = true
		case 'i':
			minute, ok = number(2)
			timeSeen = true
		case 'S', 's':
			sec, ok = number(2)
			timeSeen = true
		case 'f':
			start := pos
			micro, ok = number(6)
			fsp = pos - start
			micro *= intPow10(6 - fsp)
			timeSeen = true
		case 'j':
			yearDay, ok = number(3)
			dateSeen = true
		case 'M', 'b':
			names := months
			if code == 'b' {
				names = make([]string, len(months))
				for j := range months {
					names[j] = months[j][:3]
				}
			}
			n, ok = word(names)
			m = n + 1
			dateSeen = true
		case 'W', 'a':
			names := days
			if code == 'a' {
				names = make([]string, len(days))
				for j := range days {
					names[j] = days[j][:3]
				}
			}
			weekday, ok = word(names)
			dateSeen = true
		case 'w':
			weekday, ok = number(1)
			dateSeen = true
		case 'U', 'u', 'V', 'v':
			week, ok = number(2)
			weekMode = 0
			switch code {
			case 'u':
				weekMode = 1
			case 'V':
				weekMode = 2
			case 'v':
				weekMode = 3
			}
			dateSeen = true
		case 'p':
			if pos+2 <= len(input) {
				ampm = strings.ToUpper(input[pos : pos+2])
				ok = ampm == "AM" || ampm == "PM"
				pos += 2
			} else {
				ok = false
			}
			timeSeen = true
		case '#':
			for pos < len(input) && input[pos] >= '0' && input[pos] <= '9' {
				pos++
			}
		case '@':
			for pos < len(input) && ((input[pos] >= 'A' && input[pos] <= 'Z') || (input[pos] >= 'a' && input[pos] <= 'z')) {
				pos++
			}
		case '.':
			for pos < len(input) && strings.ContainsRune("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~", rune(input[pos])) {
				pos++
			}
		default:
			if pos >= len(input) || input[pos] != code {
				ok = false
			} else {
				pos++
			}
		}
		if !ok {
			return NewNullValue()
		}
	}
	if twelve {
		if h < 1 || h > 12 {
			return NewNullValue()
		}
		h %= 12
		if ampm != "" {
			h %= 12
			if ampm == "PM" {
				h += 12
			}
		}
	}
	if ampm != "" && !twelve {
		return NewNullValue()
	}
	if y < 0 || y > 9999 || m < 0 || m > 12 || d < 0 || d > 31 || h < 0 || h > 23 || minute < 0 || minute > 59 || sec < 0 || sec > 59 {
		return NewNullValue()
	}
	if yearDay >= 0 {
		if yearDay < 1 || yearDay > 366 {
			return NewNullValue()
		}
		t := time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, yearDay-1)
		if t.Year() != y {
			return NewNullValue()
		}
		m = int(t.Month())
		d = t.Day()
	}
	if week >= 0 {
		if week > 53 || weekday < 0 || weekday > 6 {
			return NewNullValue()
		}
		targetYear := y
		if weekYear >= 0 {
			targetYear = weekYear
		}
		start := time.Date(targetYear-1, 12, 20, 0, 0, 0, 0, time.UTC)
		found := false
		for i := 0; i < 390; i++ {
			t := start.AddDate(0, 0, i)
			wy, w := mysqlWeek(t, weekMode, weekYear >= 0)
			if wy == targetYear && w == week && int(t.Weekday()) == weekday {
				y = t.Year()
				m = int(t.Month())
				d = t.Day()
				found = true
				break
			}
		}
		if !found {
			return NewNullValue()
		}
	}
	if dateSeen && (y == 0 && m == 0 && d == 0 || m == 0 || d == 0) {
		return NewNullValue()
	}
	if m > 0 && d > 0 {
		t := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)
		if t.Year() != y || int(t.Month()) != m || t.Day() != d {
			return NewNullValue()
		}
	}
	clock := fmt.Sprintf("%02d:%02d:%02d", h, minute, sec)
	if fsp > 0 {
		clock += "." + fmt.Sprintf("%06d", micro)
	}
	if !dateSeen && timeSeen {
		return NewTextValue(clock)
	}
	date := fmt.Sprintf("%04d-%02d-%02d", y, m, d)
	if timeSeen {
		date += " " + clock
	}
	return NewTextValue(date)
}

func mysqlExpandDateFormat(s *Schema, format string) (string, error) {
	var b strings.Builder
	for i := 0; i < len(format); i++ {
		piece := format[i : i+1]
		if format[i] == '%' && i+1 < len(format) {
			i++
			piece = format[i-1 : i+1]
			switch format[i] {
			case 'r':
				piece = "%h:%i:%s %p"
			case 'T':
				piece = "%H:%i:%s"
			}
		}
		if err := mysqlFunctionSize(s, uint64(b.Len())+uint64(len(piece))); err != nil {
			return "", err
		}
		b.WriteString(piece)
	}
	return b.String(), nil
}
func intPow10(n int) int {
	v := 1
	for ; n > 0; n-- {
		v *= 10
	}
	return v
}
