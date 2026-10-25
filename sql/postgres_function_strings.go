package sql

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

func postgresStringFunctions() map[string]mysqlFunctionSpec {
	out := map[string]mysqlFunctionSpec{}
	add := func(names string, min, max int, typ ColumnType, eval func(*Schema, []Value) (Value, error)) {
		for _, n := range strings.Fields(names) {
			spec := mysqlFunctionSpec{Min: min, Max: max, Return: typ, Eval: eval}
			if typ == ColInteger {
				spec.Metadata = func([]ColumnDef) ColumnDef {
					return ColumnDef{ColType: ColInteger, Declared: "INTEGER", Postgres: true}
				}
			}
			out[n] = spec
		}
	}
	add("CONCAT CONCAT_WS", 1, -1, ColText, nil)
	for _, name := range []string{"CONCAT", "CONCAT_WS"} {
		name := name
		spec := out[name]
		if name == "CONCAT_WS" {
			spec.Min = 2
		}
		spec.Eval = func(s *Schema, a []Value) (Value, error) {
			start := 0
			sep := ""
			if name == "CONCAT_WS" {
				if a[0].Null {
					return NewNullValue(), nil
				}
				sep = postgresText(a[0])
				start = 1
			}
			var parts []string
			var size uint64
			for _, v := range a[start:] {
				if !v.Null {
					text := postgresText(v)
					size += uint64(len(text))
					parts = append(parts, text)
				}
			}
			if len(parts) > 1 {
				size += uint64(len(parts)-1) * uint64(len(sep))
			}
			if err := mysqlFunctionSize(s, size); err != nil {
				return NewNullValue(), err
			}
			return NewTextValue(strings.Join(parts, sep)), nil
		}
		out[name] = spec
	}
	add("CHAR_LENGTH CHARACTER_LENGTH LENGTH OCTET_LENGTH BIT_LENGTH ASCII", 1, 1, ColInteger, nil)
	for _, name := range strings.Fields("CHAR_LENGTH CHARACTER_LENGTH LENGTH OCTET_LENGTH BIT_LENGTH ASCII") {
		name := name
		spec := out[name]
		spec.Eval = func(s *Schema, a []Value) (Value, error) {
			if a[0].Null {
				return NewNullValue(), nil
			}
			text := postgresText(a[0])
			if a[0].Type == ColBlob {
				text = string(a[0].Blob)
			}
			n := int64(len(text))
			switch name {
			case "LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH":
				if a[0].Type != ColBlob {
					n = int64(utf8.RuneCountInString(text))
				}
			case "BIT_LENGTH":
				n *= 8
			case "ASCII":
				n = 0
				if text != "" {
					r, _ := utf8.DecodeRuneInString(text)
					n = int64(r)
				}
			}
			return NewIntValue(n), nil
		}
		out[name] = spec
	}
	add("CHR", 1, 1, ColText, func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		n := mysqlFunctionInt(a[0])
		if n <= 0 || n > utf8.MaxRune || n >= 0xd800 && n <= 0xdfff {
			return NewNullValue(), fmt.Errorf("invalid Unicode code point")
		}
		return NewTextValue(string(rune(n))), nil
	})
	add("STRPOS", 2, 2, ColInteger, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		x, y := postgresText(a[0]), postgresText(a[1])
		n := strings.Index(x, y)
		if n < 0 {
			return NewIntValue(0), nil
		}
		return NewIntValue(int64(utf8.RuneCountInString(x[:n]) + 1)), nil
	})
	add("LEFT RIGHT REPEAT REVERSE INITCAP TRANSLATE SPLIT_PART LPAD RPAD", 1, 1, ColText, nil)
	for _, name := range strings.Fields("LEFT RIGHT REPEAT REVERSE INITCAP TRANSLATE SPLIT_PART LPAD RPAD") {
		name := name
		spec := out[name]
		switch name {
		case "LEFT", "RIGHT", "REPEAT":
			spec.Min, spec.Max = 2, 2
		case "TRANSLATE", "SPLIT_PART":
			spec.Min, spec.Max = 3, 3
		case "LPAD", "RPAD":
			spec.Min, spec.Max = 2, 3
		}
		spec.Eval = func(s *Schema, a []Value) (Value, error) { return postgresStringTransform(s, name, a) }
		out[name] = spec
	}
	add("BTRIM LTRIM RTRIM", 1, 2, ColText, nil)
	for _, name := range []string{"BTRIM", "LTRIM", "RTRIM"} {
		name := name
		spec := out[name]
		spec.Eval = func(s *Schema, a []Value) (Value, error) {
			if mysqlAnyNull(a) {
				return NewNullValue(), nil
			}
			cut := " "
			if len(a) > 1 {
				cut = postgresText(a[1])
			}
			text := postgresText(a[0])
			switch name {
			case "LTRIM":
				text = strings.TrimLeft(text, cut)
			case "RTRIM":
				text = strings.TrimRight(text, cut)
			default:
				text = strings.Trim(text, cut)
			}
			return NewTextValue(text), nil
		}
		out[name] = spec
	}
	add("QUOTE_IDENT QUOTE_LITERAL QUOTE_NULLABLE", 1, 1, ColText, nil)
	for _, name := range []string{"QUOTE_IDENT", "QUOTE_LITERAL", "QUOTE_NULLABLE"} {
		name := name
		spec := out[name]
		spec.Eval = func(s *Schema, a []Value) (Value, error) {
			if a[0].Null {
				if name == "QUOTE_NULLABLE" {
					return NewTextValue("NULL"), nil
				}
				return NewNullValue(), nil
			}
			text := postgresText(a[0])
			if name == "QUOTE_IDENT" {
				return NewTextValue(postgresQuoteIdent(text)), nil
			}
			return NewTextValue(postgresQuoteLiteral(text)), nil
		}
		out[name] = spec
	}
	add("FORMAT", 1, -1, ColText, postgresFormat)
	add("ENCODE", 2, 2, ColText, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		data := a[0].Blob
		if a[0].Type != ColBlob {
			data = []byte(postgresText(a[0]))
		}
		switch strings.ToLower(postgresText(a[1])) {
		case "hex":
			if err := mysqlFunctionSize(s, uint64(len(data))*2); err != nil {
				return NewNullValue(), err
			}
			return NewTextValue(hex.EncodeToString(data)), nil
		case "base64":
			n := base64.StdEncoding.EncodedLen(len(data))
			if err := mysqlFunctionSize(s, uint64(n+n/76)); err != nil {
				return NewNullValue(), err
			}
			text := base64.StdEncoding.EncodeToString(data)
			var b strings.Builder
			for len(text) > 76 {
				b.WriteString(text[:76])
				b.WriteByte('\n')
				text = text[76:]
			}
			b.WriteString(text)
			return NewTextValue(b.String()), nil
		case "escape":
			if err := mysqlFunctionSize(s, uint64(len(data))*4); err != nil {
				return NewNullValue(), err
			}
			var b strings.Builder
			for _, v := range data {
				if v == '\\' {
					b.WriteString("\\\\")
				} else if v == 0 || v >= 128 {
					fmt.Fprintf(&b, "\\%03o", v)
				} else {
					b.WriteByte(v)
				}
			}
			if err := mysqlFunctionSize(s, uint64(b.Len())); err != nil {
				return NewNullValue(), err
			}
			return NewTextValue(b.String()), nil
		default:
			return NewNullValue(), fmt.Errorf("unrecognized encoding")
		}
	})
	add("DECODE", 2, 2, ColBlob, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		text, format := postgresText(a[0]), strings.ToLower(postgresText(a[1]))
		if err := mysqlFunctionSize(s, uint64(len(text))); err != nil {
			return NewNullValue(), err
		}
		var data []byte
		var err error
		switch format {
		case "hex":
			text = strings.Join(strings.Fields(text), "")
			data, err = hex.DecodeString(text)
		case "base64":
			text = strings.Join(strings.Fields(text), "")
			data, err = base64.StdEncoding.DecodeString(text)
		case "escape":
			for i := 0; i < len(text); i++ {
				if text[i] != '\\' {
					data = append(data, text[i])
					continue
				}
				if i+1 < len(text) && text[i+1] == '\\' {
					data = append(data, '\\')
					i++
					continue
				}
				if i+3 >= len(text) {
					err = fmt.Errorf("invalid bytea escape")
					break
				}
				n, e := strconv.ParseUint(text[i+1:i+4], 8, 8)
				if e != nil {
					err = e
					break
				}
				data = append(data, byte(n))
				i += 3
			}
		default:
			err = fmt.Errorf("unrecognized encoding")
		}
		if err != nil {
			return NewNullValue(), err
		}
		if err := mysqlFunctionSize(s, uint64(len(data))); err != nil {
			return NewNullValue(), err
		}
		return NewBlobValue(data), nil
	})
	add("GET_BYTE", 2, 2, ColInteger, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		i := mysqlFunctionInt(a[1])
		if i < 0 || i >= int64(len(a[0].Blob)) {
			return NewNullValue(), fmt.Errorf("byte index out of range")
		}
		return NewIntValue(int64(a[0].Blob[i])), nil
	})
	add("SET_BYTE", 3, 3, ColBlob, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		i := mysqlFunctionInt(a[1])
		n := mysqlFunctionInt(a[2])
		if i < 0 || i >= int64(len(a[0].Blob)) {
			return NewNullValue(), fmt.Errorf("byte index out of range")
		}
		data := bytes.Clone(a[0].Blob)
		data[i] = byte(n)
		return NewBlobValue(data), nil
	})
	add("GET_BIT", 2, 2, ColInteger, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		i := mysqlFunctionInt(a[1])
		if i < 0 || i >= int64(len(a[0].Blob))*8 {
			return NewNullValue(), fmt.Errorf("bit index out of range")
		}
		return NewIntValue(int64(a[0].Blob[i/8] >> uint(i%8) & 1)), nil
	})
	add("SET_BIT", 3, 3, ColBlob, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		i, n := mysqlFunctionInt(a[1]), mysqlFunctionInt(a[2])
		if i < 0 || i >= int64(len(a[0].Blob))*8 || n < 0 || n > 1 {
			return NewNullValue(), fmt.Errorf("invalid bit index or value")
		}
		data := bytes.Clone(a[0].Blob)
		data[i/8] = data[i/8]&^(1<<uint(i%8)) | byte(n)<<uint(i%8)
		return NewBlobValue(data), nil
	})
	add("CONVERT_FROM", 2, 2, ColText, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		if !postgresUTF8Encoding(postgresText(a[1])) {
			return NewNullValue(), fmt.Errorf("unsupported character encoding")
		}
		data := a[0].Blob
		if a[0].Type != ColBlob {
			data = []byte(postgresText(a[0]))
		}
		if !utf8.Valid(data) || bytes.IndexByte(data, 0) >= 0 {
			return NewNullValue(), fmt.Errorf("invalid UTF-8 text")
		}
		return NewTextValue(string(data)), nil
	})
	add("CONVERT_TO", 2, 2, ColBlob, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		if !postgresUTF8Encoding(postgresText(a[1])) {
			return NewNullValue(), fmt.Errorf("unsupported character encoding")
		}
		return NewBlobValue([]byte(postgresText(a[0]))), nil
	})
	return out
}
func postgresUTF8Encoding(text string) bool {
	return strings.EqualFold(text, "UTF8") || strings.EqualFold(text, "UTF-8") || strings.EqualFold(text, "unicode")
}
func postgresText(v Value) string {
	if strings.EqualFold(v.PGType, "BOOLEAN") {
		if v.Int != 0 {
			return "t"
		}
		return "f"
	}
	if v.Type == ColBlob {
		return "\\x" + hex.EncodeToString(v.Blob)
	}
	return v.String()
}
func postgresStringTransform(s *Schema, name string, a []Value) (Value, error) {
	if mysqlAnyNull(a) {
		return NewNullValue(), nil
	}
	text := postgresText(a[0])
	r := []rune(text)
	n := int64(len(r))
	switch name {
	case "LEFT", "RIGHT":
		count := mysqlFunctionInt(a[1])
		if count < 0 {
			if count < -n {
				count = 0
			} else {
				count = n + count
			}
		}
		if count > n {
			count = n
		}
		if name == "LEFT" {
			return NewTextValue(string(r[:int(count)])), nil
		}
		return NewTextValue(string(r[len(r)-int(count):])), nil
	case "REPEAT":
		count := mysqlFunctionInt(a[1])
		if count <= 0 || text == "" {
			return NewTextValue(""), nil
		}
		if uint64(count) > uint64(s.memoryBudget())/uint64(len(text)) {
			return NewNullValue(), fmt.Errorf("function result exceeds memory budget")
		}
		return NewTextValue(strings.Repeat(text, int(count))), nil
	case "REVERSE":
		for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
			r[i], r[j] = r[j], r[i]
		}
		return NewTextValue(string(r)), nil
	case "INITCAP":
		begin := true
		for i, c := range r {
			if unicode.IsLetter(c) || unicode.IsDigit(c) {
				if begin {
					r[i] = unicode.ToUpper(c)
				} else {
					r[i] = unicode.ToLower(c)
				}
				begin = false
			} else {
				begin = true
			}
		}
		return NewTextValue(string(r)), nil
	case "TRANSLATE":
		from, to := []rune(postgresText(a[1])), []rune(postgresText(a[2]))
		var out strings.Builder
		for _, c := range r {
			i := -1
			for j, x := range from {
				if x == c {
					i = j
					break
				}
			}
			if i < 0 {
				out.WriteRune(c)
			} else if i < len(to) {
				out.WriteRune(to[i])
			}
		}
		return NewTextValue(out.String()), nil
	case "SPLIT_PART":
		delimiter := postgresText(a[1])
		count := mysqlFunctionInt(a[2])
		if count == 0 {
			return NewNullValue(), fmt.Errorf("field position must not be zero")
		}
		if delimiter == "" {
			if count == 1 || count == -1 {
				return NewTextValue(text), nil
			}
			return NewTextValue(""), nil
		}
		parts := strings.Split(text, delimiter)
		if count < 0 {
			count = int64(len(parts)) + count + 1
		}
		if count < 1 || count > int64(len(parts)) {
			return NewTextValue(""), nil
		}
		return NewTextValue(parts[count-1]), nil
	case "LPAD", "RPAD":
		count := mysqlFunctionInt(a[1])
		if count <= 0 {
			return NewTextValue(""), nil
		}
		if count <= n {
			return NewTextValue(string(r[:count])), nil
		}
		pad := []rune(" ")
		if len(a) > 2 {
			pad = []rune(postgresText(a[2]))
		}
		if len(pad) == 0 {
			return NewTextValue(text), nil
		}
		if err := mysqlFunctionSize(s, uint64(count)*4); err != nil {
			return NewNullValue(), err
		}
		out := make([]rune, 0, int(count))
		if name == "RPAD" {
			out = append(out, r...)
		}
		for i := int64(0); i < count-n; i++ {
			out = append(out, pad[i%int64(len(pad))])
		}
		if name == "LPAD" {
			out = append(out, r...)
		}
		return NewTextValue(string(out)), nil
	}
	return NewNullValue(), fmt.Errorf("unknown PostgreSQL string function")
}
func postgresQuoteIdent(text string) string {
	plain := text != ""
	for i, c := range text {
		if !(c == '_' || c >= 'a' && c <= 'z' || i > 0 && (c >= '0' && c <= '9' || c == '$')) {
			plain = false
			break
		}
	}
	if plain {
		if sqlKeyword := strings.ToUpper(text); postgresQuotedKeywords[strings.ToLower(sqlKeyword)] {
			plain = false
		}
	}
	if plain {
		return text
	}
	return `"` + strings.ReplaceAll(text, `"`, `""`) + `"`
}
func postgresQuoteLiteral(text string) string {
	escape := strings.Contains(text, "\\")
	text = strings.ReplaceAll(text, "'", "''")
	if escape {
		text = strings.ReplaceAll(text, "\\", "\\\\")
		return "E'" + text + "'"
	}
	return "'" + text + "'"
}
func postgresFormat(s *Schema, a []Value) (Value, error) {
	if a[0].Null {
		return NewNullValue(), nil
	}
	format := postgresText(a[0])
	var b strings.Builder
	next := 1
	for pos := 0; pos < len(format); {
		if format[pos] != '%' {
			b.WriteByte(format[pos])
			pos++
			continue
		}
		pos++
		if pos < len(format) && format[pos] == '%' {
			b.WriteByte('%')
			pos++
			continue
		}
		start := pos
		for pos < len(format) && format[pos] >= '0' && format[pos] <= '9' {
			pos++
		}
		index := 0
		if pos < len(format) && format[pos] == '$' {
			n, err := strconv.Atoi(format[start:pos])
			if err != nil || n < 1 {
				return NewNullValue(), fmt.Errorf("invalid format argument position")
			}
			index = n
			pos++
		} else {
			pos = start
		}
		left := false
		if pos < len(format) && format[pos] == '-' {
			left = true
			pos++
		}
		width := 0
		if pos < len(format) && format[pos] == '*' {
			pos++
			start = pos
			for pos < len(format) && format[pos] >= '0' && format[pos] <= '9' {
				pos++
			}
			windex := next
			if pos < len(format) && format[pos] == '$' {
				v, err := strconv.Atoi(format[start:pos])
				if err != nil || v < 1 {
					return NewNullValue(), fmt.Errorf("invalid format width")
				}
				windex = v
				pos++
			} else if pos != start {
				return NewNullValue(), fmt.Errorf("invalid format width")
			}
			if windex >= len(a) {
				return NewNullValue(), fmt.Errorf("too few arguments for format")
			}
			width = int(mysqlFunctionInt(a[windex]))
			if width < -int(s.memoryBudget()) || width > int(s.memoryBudget()) {
				return NewNullValue(), fmt.Errorf("format width exceeds memory budget")
			}
			next = windex + 1
			if width < 0 {
				left = true
				width = -width
			}
		} else {
			start = pos
			for pos < len(format) && format[pos] >= '0' && format[pos] <= '9' {
				pos++
			}
			if pos > start {
				n, err := strconv.Atoi(format[start:pos])
				if err != nil {
					return NewNullValue(), err
				}
				width = n
			}
		}
		if index == 0 {
			index = next
		}
		if index >= len(a) || index < 1 || pos >= len(format) {
			return NewNullValue(), fmt.Errorf("too few arguments for format")
		}
		next = index + 1
		v := a[index]
		var text string
		switch format[pos] {
		case 's':
			if !v.Null {
				text = postgresText(v)
			}
		case 'I':
			if v.Null {
				return NewNullValue(), fmt.Errorf("null values cannot be formatted as an SQL identifier")
			}
			text = postgresQuoteIdent(postgresText(v))
		case 'L':
			if v.Null {
				text = "NULL"
			} else {
				text = postgresQuoteLiteral(postgresText(v))
			}
		default:
			return NewNullValue(), fmt.Errorf("unsupported format conversion")
		}
		pos++
		pad := width - utf8.RuneCountInString(text)
		if pad < 0 {
			pad = 0
		}
		if err := mysqlFunctionSize(s, uint64(b.Len())+uint64(len(text))+uint64(pad)); err != nil {
			return NewNullValue(), err
		}
		if !left {
			b.WriteString(strings.Repeat(" ", pad))
		}
		b.WriteString(text)
		if left {
			b.WriteString(strings.Repeat(" ", pad))
		}
	}
	return NewTextValue(b.String()), nil
}

// PostgreSQL 16.15 pg_get_keywords() categories R, T and C require quoting.
var postgresQuotedKeywords = func() map[string]bool {
	out := map[string]bool{}
	for _, word := range strings.Fields(`all analyse analyze and any array as asc asymmetric authorization between bigint binary bit boolean both case cast char character check coalesce collate collation column concurrently constraint create cross current_catalog current_date current_role current_schema current_time current_timestamp current_user dec decimal default deferrable desc distinct do else end except exists extract false fetch float for foreign freeze from full grant greatest group grouping having ilike in initially inner inout int integer intersect interval into is isnull join json_array json_arrayagg json_object json_objectagg lateral leading least left like limit localtime localtimestamp national natural nchar none normalize not notnull null nullif numeric offset on only or order out outer overlaps overlay placing position precision primary real references returning right row select session_user setof similar smallint some substring symmetric system_user table tablesample then time timestamp to trailing treat trim true union unique user using values varchar variadic verbose when where window with xmlattributes xmlconcat xmlelement xmlexists xmlforest xmlnamespaces xmlparse xmlpi xmlroot xmlserialize xmltable`) {
		out[word] = true
	}
	return out
}()
