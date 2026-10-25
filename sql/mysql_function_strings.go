package sql

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

func mysqlStringFunctions() map[string]mysqlFunctionSpec {
	m := make(map[string]mysqlFunctionSpec)
	add := func(name string, min, max int, typ ColumnType, eval func(*Schema, []Value) (Value, error)) {
		m[name] = mysqlFunctionSpec{Min: min, Max: max, Return: typ, Eval: eval}
	}
	for _, name := range []string{"LEFT", "RIGHT", "REPEAT", "REVERSE", "LPAD", "RPAD", "INSERT", "SUBSTRING_INDEX", "LCASE", "UCASE", "MID"} {
		name := name
		min, max := 1, 1
		switch name {
		case "LEFT", "RIGHT", "REPEAT":
			min, max = 2, 2
		case "LPAD", "RPAD", "SUBSTRING_INDEX":
			min, max = 3, 3
		case "INSERT":
			min, max = 4, 4
		case "MID":
			min, max = 2, 3
		}
		add(name, min, max, ColText, func(s *Schema, a []Value) (Value, error) { return mysqlStringTransform(s, name, a) })
		spec := m[name]
		spec.Metadata = func(columns []ColumnDef) ColumnDef {
			if len(columns) > 0 && columns[0].ColType == ColBlob {
				return ColumnDef{ColType: ColBlob}
			}
			return ColumnDef{ColType: ColText}
		}
		m[name] = spec
	}
	add("CONCAT_WS", 2, -1, ColText, mysqlConcatWS)
	spec := m["CONCAT_WS"]
	spec.Metadata = mysqlStringMetadata
	m["CONCAT_WS"] = spec
	for _, name := range []string{"ASCII", "ORD", "CHAR_LENGTH", "CHARACTER_LENGTH", "OCTET_LENGTH", "BIT_LENGTH"} {
		name := name
		add(name, 1, 1, ColInteger, func(s *Schema, a []Value) (Value, error) {
			if a[0].Null {
				return NewNullValue(), nil
			}
			text := mysqlStringValue(a[0])
			n := int64(len(text))
			switch name {
			case "ASCII":
				n = 0
				if len(text) != 0 {
					n = int64(text[0])
				}
			case "ORD":
				n = 0
				_, width := utf8.DecodeRuneInString(text)
				if a[0].Type == ColBlob && len(text) > 0 {
					width = 1
				}
				for i := 0; i < width && i < len(text); i++ {
					n = n*256 + int64(text[i])
				}
			case "CHAR_LENGTH", "CHARACTER_LENGTH":
				if a[0].Type != ColBlob {
					n = int64(utf8.RuneCountInString(text))
				}
			case "BIT_LENGTH":
				n *= 8
			}
			return NewIntValue(n), nil
		})
	}
	add("SPACE", 1, 1, ColText, func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		count := mysqlStringCount(a[0])
		if count <= 0 {
			return NewTextValue(""), nil
		}
		if err := mysqlFunctionSize(s, uint64(count)); err != nil {
			return NewNullValue(), err
		}
		return NewTextValue(strings.Repeat(" ", int(count))), nil
	})
	add("LOCATE", 2, 3, ColInteger, mysqlLocate)
	add("POSITION", 2, 2, ColInteger, mysqlLocate)
	add("STRCMP", 2, 2, ColInteger, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		n := s.valueCompare(mysqlTextValue(a[0]), mysqlTextValue(a[1]))
		if n < 0 {
			n = -1
		}
		if n > 0 {
			n = 1
		}
		return NewIntValue(int64(n)), nil
	})
	add("FIELD", 2, -1, ColInteger, mysqlField)
	add("FIND_IN_SET", 2, 2, ColInteger, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		text := mysqlStringValue(a[1])
		if text == "" {
			return NewIntValue(0), nil
		}
		for i := 0; ; i++ {
			if i&63 == 0 {
				if err := s.checkInterrupt(); err != nil {
					return NewNullValue(), err
				}
			}
			item, rest, found := strings.Cut(text, ",")
			if s.valueCompare(mysqlTextValue(a[0]), mysqlTextValueLike(a[1], item)) == 0 {
				return NewIntValue(int64(i + 1)), nil
			}
			if !found {
				break
			}
			text = rest
		}
		return NewIntValue(0), nil
	})
	add("ELT", 2, -1, ColText, func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		n := mysqlStringCount(a[0])
		if n < 1 || n >= int64(len(a)) {
			return NewNullValue(), nil
		}
		if a[n].Null {
			return NewNullValue(), nil
		}
		return mysqlStringResult(mysqlStringValue(a[n]), mysqlHasBlob(a[1:])), nil
	})
	spec = m["ELT"]
	spec.Metadata = func(a []ColumnDef) ColumnDef {
		if len(a) > 0 {
			a = a[1:]
		}
		return mysqlStringMetadata(a)
	}
	m["ELT"] = spec
	add("CHAR", 1, -1, ColBlob, mysqlChar)
	add("__MYSQL_CHAR_UTF8", 1, -1, ColText, func(s *Schema, a []Value) (Value, error) {
		v, err := mysqlChar(s, a)
		if err != nil || v.Null {
			return v, err
		}
		if !utf8.Valid(v.Blob) {
			return NewNullValue(), nil
		}
		return NewTextValue(string(v.Blob)), nil
	})
	add("UNHEX", 1, 1, ColBlob, func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		text := mysqlStringValue(a[0])
		if len(text)%2 != 0 {
			text = "0" + text
		}
		if err := mysqlFunctionSize(s, uint64(len(text)/2)); err != nil {
			return NewNullValue(), err
		}
		data, err := hex.DecodeString(text)
		if err != nil {
			return NewNullValue(), nil
		}
		return NewBlobValue(data), nil
	})
	add("TO_BASE64", 1, 1, ColText, func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		text := mysqlStringValue(a[0])
		size := uint64(len(text)+2) / 3 * 4
		if size > 0 {
			size += (size - 1) / 76
		}
		if err := mysqlFunctionSize(s, size); err != nil {
			return NewNullValue(), err
		}
		encoded := base64.StdEncoding.EncodeToString([]byte(text))
		if len(encoded) <= 76 {
			return NewTextValue(encoded), nil
		}
		var out strings.Builder
		out.Grow(int(size))
		for len(encoded) > 76 {
			out.WriteString(encoded[:76])
			out.WriteByte('\n')
			encoded = encoded[76:]
		}
		out.WriteString(encoded)
		return NewTextValue(out.String()), nil
	})
	add("FROM_BASE64", 1, 1, ColBlob, func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		text := strings.Map(func(r rune) rune {
			if r == ' ' || r == '\t' || r == '\r' || r == '\n' {
				return -1
			}
			return r
		}, mysqlStringValue(a[0]))
		if err := mysqlFunctionSize(s, uint64(base64.StdEncoding.DecodedLen(len(text)))); err != nil {
			return NewNullValue(), err
		}
		data, err := base64.StdEncoding.DecodeString(text)
		if err != nil {
			return NewNullValue(), nil
		}
		return NewBlobValue(data), nil
	})
	add("MAKE_SET", 2, -1, ColText, mysqlMakeSet)
	add("EXPORT_SET", 3, 5, ColText, mysqlExportSet)
	for _, name := range []string{"MAKE_SET", "EXPORT_SET"} {
		spec := m[name]
		spec.Metadata = func(a []ColumnDef) ColumnDef {
			if len(a) > 0 {
				a = a[1:]
			}
			return mysqlStringMetadata(a)
		}
		m[name] = spec
	}
	add("SOUNDEX", 1, 1, ColText, mysqlSoundex)
	spec = m["SOUNDEX"]
	spec.Metadata = func(a []ColumnDef) ColumnDef {
		if len(a) > 0 && a[0].ColType == ColBlob {
			return ColumnDef{ColType: ColBlob}
		}
		return ColumnDef{ColType: ColText}
	}
	m["SOUNDEX"] = spec
	add("__MYSQL_TRIM", 3, 3, ColText, func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		text, trim, mode := mysqlStringValue(a[0]), mysqlStringValue(a[1]), a[2].Text
		if trim != "" {
			if mode == "BOTH" || mode == "LEADING" {
				for strings.HasPrefix(text, trim) {
					text = text[len(trim):]
				}
			}
			if mode == "BOTH" || mode == "TRAILING" {
				for strings.HasSuffix(text, trim) {
					text = text[:len(text)-len(trim)]
				}
			}
		}
		return mysqlStringResult(text, a[0].Type == ColBlob), nil
	})
	spec = m["__MYSQL_TRIM"]
	spec.Metadata = func(a []ColumnDef) ColumnDef {
		if len(a) > 0 && a[0].ColType == ColBlob {
			return ColumnDef{ColType: ColBlob}
		}
		return ColumnDef{ColType: ColText}
	}
	m["__MYSQL_TRIM"] = spec
	return m
}

func mysqlHasBlob(a []Value) bool {
	for _, v := range a {
		if v.Type == ColBlob && !v.Null {
			return true
		}
	}
	return false
}
func mysqlStringMetadata(a []ColumnDef) ColumnDef {
	for _, c := range a {
		if c.ColType == ColBlob {
			return ColumnDef{ColType: ColBlob}
		}
	}
	return ColumnDef{ColType: ColText}
}
func mysqlStringResult(text string, binary bool) Value {
	if binary {
		return NewBlobValue([]byte(text))
	}
	return NewTextValue(text)
}
func mysqlTextValue(v Value) Value { return mysqlTextValueLike(v, mysqlStringValue(v)) }
func mysqlTextValueLike(v Value, text string) Value {
	out := NewTextValue(text)
	out.Collation, out.Binary = v.Collation, v.Binary || v.Type == ColBlob
	return out
}
func mysqlStringCount(v Value) int64 {
	if !isNumeric(v) {
		return int64(mysqlStringIntegerBits(v))
	}
	v = mysqlFunctionNumber(v)
	if v.Type == ColReal {
		f := math.Round(v.Real)
		if f >= float64(math.MaxInt64) {
			return math.MaxInt64
		}
		if f <= float64(math.MinInt64) {
			return math.MinInt64
		}
		return int64(f)
	}
	if v.Type == ColDecimal {
		n, scale, err := valueDecimal(v)
		if err != nil {
			return 0
		}
		n = roundDecimal(n, scale, 0)
		if n.IsInt64() {
			return n.Int64()
		}
		if n.Sign() < 0 {
			return math.MinInt64
		}
		return math.MaxInt64
	}
	return integerValue(v)
}
func mysqlStringSlice(text string, start, end int64, binary bool) string {
	if binary {
		return text[int(start):int(end)]
	}
	first, last, position := 0, len(text), int64(0)
	for offset := range text {
		if position == start {
			first = offset
		}
		if position == end {
			last = offset
			break
		}
		position++
	}
	if start == end {
		return ""
	}
	return text[first:last]
}
func mysqlStringLength(text string, binary bool) int64 {
	if binary {
		return int64(len(text))
	}
	return int64(utf8.RuneCountInString(text))
}

func mysqlStringTransform(s *Schema, name string, a []Value) (Value, error) {
	if mysqlAnyNull(a) {
		return NewNullValue(), nil
	}
	text := mysqlStringValue(a[0])
	binary := a[0].Type == ColBlob
	length := mysqlStringLength(text, binary)
	result := text
	switch name {
	case "LEFT", "RIGHT":
		n := mysqlStringCount(a[1])
		if n < 0 {
			n = 0
		}
		if n > length {
			n = length
		}
		start := int64(0)
		if name == "RIGHT" {
			start = length - n
		}
		result = mysqlStringSlice(text, start, start+n, binary)
	case "MID":
		start := mysqlStringCount(a[1])
		n := length
		if len(a) == 3 {
			n = mysqlStringCount(a[2])
		}
		if start == 0 || start > length || start < -length || n <= 0 {
			result = ""
			break
		}
		if start < 0 {
			start += length
		} else {
			start--
		}
		if n > length-start {
			n = length - start
		}
		result = mysqlStringSlice(text, start, start+n, binary)
	case "REVERSE":
		if err := mysqlFunctionSize(s, uint64(len(text))*4); err != nil {
			return NewNullValue(), err
		}
		if binary {
			b := []byte(text)
			for l, r := 0, len(b)-1; l < r; l, r = l+1, r-1 {
				b[l], b[r] = b[r], b[l]
			}
			result = string(b)
		} else {
			r := []rune(text)
			for l, h := 0, len(r)-1; l < h; l, h = l+1, h-1 {
				r[l], r[h] = r[h], r[l]
			}
			result = string(r)
		}
	case "REPEAT":
		n := mysqlStringCount(a[1])
		if n <= 0 || len(text) == 0 {
			result = ""
			break
		}
		if uint64(n) > uint64(s.memoryBudget())/uint64(len(text)) {
			return NewNullValue(), mysqlFunctionSize(s, math.MaxUint64)
		}
		result = strings.Repeat(text, int(n))
	case "LCASE", "UCASE":
		if binary {
			break
		}
		if name == "LCASE" {
			result = strings.ToLower(text)
		} else {
			result = strings.ToUpper(text)
		}
	case "LPAD", "RPAD":
		length = mysqlStringLength(text, binary)
		n := mysqlStringCount(a[1])
		if n < 0 {
			return NewNullValue(), nil
		}
		if n <= length {
			result = mysqlStringSlice(text, 0, n, binary)
			break
		}
		pad := mysqlStringValue(a[2])
		if !binary && !utf8.ValidString(pad) {
			return NewNullValue(), fmt.Errorf("invalid UTF-8 in %s padding", name)
		}
		plen := mysqlStringLength(pad, binary)
		if plen == 0 {
			return NewNullValue(), nil
		}
		need := n - length
		copies := need / plen
		tail := mysqlStringSlice(pad, 0, need%plen, binary)
		if uint64(copies) > uint64(s.memoryBudget())/uint64(len(pad)) {
			return NewNullValue(), mysqlFunctionSize(s, math.MaxUint64)
		}
		size := uint64(len(text)) + uint64(copies)*uint64(len(pad)) + uint64(len(tail))
		if err := mysqlFunctionSize(s, size); err != nil {
			return NewNullValue(), err
		}
		padding := strings.Repeat(pad, int(copies)) + tail
		if name == "LPAD" {
			result = padding + text
		} else {
			result = text + padding
		}
	case "INSERT":
		length = mysqlStringLength(text, binary)
		pos, n := mysqlStringCount(a[1]), mysqlStringCount(a[2])
		if pos < 1 || pos > length {
			break
		}
		pos--
		if n < 0 || n > length-pos {
			n = length - pos
		}
		before, after := mysqlStringSlice(text, 0, pos, binary), mysqlStringSlice(text, pos+n, length, binary)
		inserted := mysqlStringValue(a[3])
		if !binary && !utf8.ValidString(inserted) {
			return NewNullValue(), fmt.Errorf("invalid UTF-8 in INSERT replacement")
		}
		if err := mysqlFunctionSize(s, uint64(len(before))+uint64(len(inserted))+uint64(len(after))); err != nil {
			return NewNullValue(), err
		}
		result = before + inserted + after
	case "SUBSTRING_INDEX":
		delim := mysqlStringValue(a[1])
		n := mysqlStringCount(a[2])
		if n == 0 || delim == "" {
			result = ""
			break
		}
		if n > 0 {
			offset := 0
			for ; n > 0; n-- {
				if n&63 == 0 {
					if err := s.checkInterrupt(); err != nil {
						return NewNullValue(), err
					}
				}
				at := strings.Index(text[offset:], delim)
				if at < 0 {
					return mysqlStringResult(text, binary), nil
				}
				at += offset
				if n == 1 {
					result = text[:at]
					break
				}
				offset = at + len(delim)
			}
		} else {
			end := len(text)
			for ; n < 0; n++ {
				if n&63 == 0 {
					if err := s.checkInterrupt(); err != nil {
						return NewNullValue(), err
					}
				}
				at := strings.LastIndex(text[:end], delim)
				if at < 0 {
					return mysqlStringResult(text, binary), nil
				}
				if n == -1 {
					result = text[at+len(delim):]
					break
				}
				end = at
			}
		}
	}
	if err := mysqlFunctionSize(s, uint64(len(result))); err != nil {
		return NewNullValue(), err
	}
	return mysqlStringResult(result, binary), nil
}
func mysqlConcatWS(s *Schema, a []Value) (Value, error) {
	if a[0].Null {
		return NewNullValue(), nil
	}
	separator := mysqlStringValue(a[0])
	parts := make([]string, 0, len(a)-1)
	size := uint64(0)
	for _, v := range a[1:] {
		if v.Null {
			continue
		}
		part := mysqlStringValue(v)
		if len(parts) > 0 {
			size += uint64(len(separator))
		}
		size += uint64(len(part))
		if err := mysqlFunctionSize(s, size); err != nil {
			return NewNullValue(), err
		}
		parts = append(parts, part)
	}
	return mysqlStringResult(strings.Join(parts, separator), mysqlHasBlob(a)), nil
}
func mysqlLocate(s *Schema, a []Value) (Value, error) {
	if mysqlAnyNull(a) {
		return NewNullValue(), nil
	}
	needle, haystack := mysqlStringValue(a[0]), mysqlStringValue(a[1])
	binary := a[0].Type == ColBlob || a[1].Type == ColBlob
	length := mysqlStringLength(haystack, binary)
	start := int64(1)
	if len(a) == 3 {
		start = mysqlStringCount(a[2])
	}
	if start < 1 || start > length+1 {
		return NewIntValue(0), nil
	}
	begin := mysqlStringSlice(haystack, 0, start-1, binary)
	suffix := haystack[len(begin):]
	original := suffix
	c := explicitValueCollation(a[0], a[1])
	if c == "" {
		c = s.collation
	}
	if !binary {
		if err := mysqlFunctionSize(s, uint64(len(needle))+uint64(len(suffix))); err != nil {
			return NewNullValue(), err
		}
		needle = mysqlStringSearchKey(c, needle)
		suffix = mysqlStringSearchKey(c, suffix)
	}
	at := strings.Index(suffix, needle)
	if at < 0 {
		return NewIntValue(0), nil
	}
	if binary {
		return NewIntValue(start + int64(at)), nil
	}
	if strings.HasPrefix(string(c), "NF") {
		// Map a normalized match back to the source's code-point position.
		// Binary search prefix boundaries avoids a quadratic prefix scan.
		count := utf8.RuneCountInString(original)
		var interrupted error
		position := sort.Search(count+1, func(i int) bool {
			if err := s.checkInterrupt(); err != nil {
				interrupted = err
				return true
			}
			prefix := mysqlStringSlice(original, 0, int64(i), false)
			return len(c.Key(prefix)) > at
		}) - 1
		if interrupted != nil {
			return NewNullValue(), interrupted
		}
		if position < 0 {
			position = 0
		}
		return NewIntValue(start + int64(position)), nil
	}
	return NewIntValue(start + int64(utf8.RuneCountInString(suffix[:at]))), nil
}

func mysqlStringSearchKey(c Collation, text string) string {
	if _, ok := mysqlCollation(string(c)); ok {
		if strings.HasSuffix(string(c), "_GENERAL_CI") {
			return mysqlGeneralCIKey(text)
		}
		return text
	}
	return c.Key(text)
}
func mysqlField(s *Schema, a []Value) (Value, error) {
	if a[0].Null {
		return NewIntValue(0), nil
	}
	allString, allNumeric := true, true
	for _, v := range a {
		if v.Null {
			continue
		}
		numeric := isNumeric(v)
		allNumeric = allNumeric && numeric
		allString = allString && !numeric
	}
	for i, v := range a[1:] {
		if v.Null {
			continue
		}
		equal := false
		if allString {
			equal = s.valueCompare(mysqlTextValue(a[0]), mysqlTextValue(v)) == 0
		} else if allNumeric {
			equal = s.valueCompare(a[0], v) == 0
		} else {
			equal = toFloat(mysqlFunctionNumber(a[0])) == toFloat(mysqlFunctionNumber(v))
		}
		if equal {
			return NewIntValue(int64(i + 1)), nil
		}
	}
	return NewIntValue(0), nil
}
func mysqlChar(s *Schema, a []Value) (Value, error) {
	if err := mysqlFunctionSize(s, uint64(len(a))*4); err != nil {
		return NewNullValue(), err
	}
	out := make([]byte, 0, len(a))
	for _, v := range a {
		if v.Null {
			continue
		}
		n := uint32(mysqlSetBits(v))
		width := 1
		if n > 0xffffff {
			width = 4
		} else if n > 0xffff {
			width = 3
		} else if n > 0xff {
			width = 2
		}
		for i := width - 1; i >= 0; i-- {
			out = append(out, byte(n>>uint(i*8)))
		}
	}
	return NewBlobValue(out), nil
}
func mysqlSetBits(v Value) uint64 {
	if v.Type == ColUnsigned {
		return v.Uint
	}
	if v.Type == ColInteger {
		return uint64(v.Int)
	}
	if !isNumeric(v) {
		return mysqlStringIntegerBits(v)
	}
	return uint64(mysqlStringCount(v))
}

// String val_int conversions consume an integer prefix; unlike numeric
// expressions, string fractions are truncated rather than rounded.
func mysqlStringIntegerBits(v Value) uint64 {
	text := strings.TrimSpace(mysqlStringValue(v))
	end := 0
	if end < len(text) && (text[end] == '+' || text[end] == '-') {
		end++
	}
	start := end
	for end < len(text) && text[end] >= '0' && text[end] <= '9' {
		end++
	}
	if start == end {
		return 0
	}
	n, ok := new(big.Int).SetString(text[:end], 10)
	if !ok {
		return 0
	}
	if n.Sign() < 0 {
		if !n.IsInt64() {
			return uint64(1) << 63
		}
		return uint64(n.Int64())
	}
	if !n.IsUint64() {
		return math.MaxUint64
	}
	return n.Uint64()
}
func mysqlMakeSet(s *Schema, a []Value) (Value, error) {
	if a[0].Null {
		return NewNullValue(), nil
	}
	bits := mysqlSetBits(a[0])
	parts := make([]string, 0)
	size := uint64(0)
	for i, v := range a[1:] {
		if i >= 64 {
			break
		}
		if bits&(uint64(1)<<uint(i)) == 0 || v.Null {
			continue
		}
		text := mysqlStringValue(v)
		size += uint64(len(text))
		if len(parts) > 0 {
			size++
		}
		if err := mysqlFunctionSize(s, size); err != nil {
			return NewNullValue(), err
		}
		parts = append(parts, text)
	}
	return mysqlStringResult(strings.Join(parts, ","), mysqlHasBlob(a[1:])), nil
}
func mysqlExportSet(s *Schema, a []Value) (Value, error) {
	if mysqlAnyNull(a) {
		return NewNullValue(), nil
	}
	bits := mysqlSetBits(a[0])
	on, off := mysqlStringValue(a[1]), mysqlStringValue(a[2])
	separator := ","
	if len(a) >= 4 {
		separator = mysqlStringValue(a[3])
	}
	count := int64(64)
	if len(a) == 5 {
		count = mysqlStringCount(a[4])
		if count < 0 || count > 64 {
			count = 64
		}
	}
	parts := make([]string, 0, count)
	size := uint64(0)
	for i := int64(0); i < count; i++ {
		text := off
		if bits&(uint64(1)<<uint(i)) != 0 {
			text = on
		}
		size += uint64(len(text))
		if i > 0 {
			size += uint64(len(separator))
		}
		if err := mysqlFunctionSize(s, size); err != nil {
			return NewNullValue(), err
		}
		parts = append(parts, text)
	}
	return mysqlStringResult(strings.Join(parts, separator), mysqlHasBlob(a[1:])), nil
}
func mysqlSoundex(s *Schema, a []Value) (Value, error) {
	if a[0].Null {
		return NewNullValue(), nil
	}
	text := mysqlStringValue(a[0])
	var out strings.Builder
	previous := byte(0)
	started := false
	code := func(r rune) byte {
		switch r {
		case 'B', 'F', 'P', 'V':
			return '1'
		case 'C', 'G', 'J', 'K', 'Q', 'S', 'X', 'Z':
			return '2'
		case 'D', 'T':
			return '3'
		case 'L':
			return '4'
		case 'M', 'N':
			return '5'
		case 'R':
			return '6'
		}
		return 0
	}
	for offset, r := range text {
		if offset&1023 == 0 {
			if err := s.checkInterrupt(); err != nil {
				return NewNullValue(), err
			}
		}
		if a[0].Type == ColBlob && r > 127 {
			continue
		}
		if !unicode.IsLetter(r) {
			continue
		}
		if r >= 'a' && r <= 'z' {
			r -= 'a' - 'A'
		}
		c := code(r)
		if !started {
			out.WriteRune(r)
			started = true
			previous = c
			continue
		}
		if r == 'H' || r == 'W' {
			continue
		}
		if c != 0 && c != previous {
			if err := mysqlFunctionSize(s, uint64(out.Len()+4)); err != nil {
				return NewNullValue(), err
			}
			out.WriteByte(c)
		}
		previous = c
	}
	if !started {
		return mysqlStringResult("", a[0].Type == ColBlob), nil
	}
	for utf8.RuneCountInString(out.String()) < 4 {
		out.WriteByte('0')
	}
	return mysqlStringResult(out.String(), a[0].Type == ColBlob), nil
}
