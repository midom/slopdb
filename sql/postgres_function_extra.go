package sql

import (
	"crypto/rand"
	"fmt"
	"math/bits"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

func postgresExtraFunctions() map[string]FunctionSpec {
	out := map[string]FunctionSpec{}
	add := func(name string, min, max int, typ string, fn func(*Schema, []Value) (Value, error)) {
		result := LogicalTypeFromDeclaration(typ).Column()
		out[name] = FunctionSpec{Min: min, Max: max, Return: result.ColType, Metadata: func([]ColumnDef) ColumnDef { return result }, Eval: fn}
	}
	add("STARTS_WITH", 2, 2, "BOOLEAN", func(s *Schema, a []Value) (Value, error) {
		if functionAnyNull(a) {
			return NewNullValue(), nil
		}
		return s.postgresBooleanValue(strings.HasPrefix(postgresText(a[0]), postgresText(a[1]))), nil
	})
	add("TO_HEX", 1, 1, "TEXT", func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		n, e := postgresFunctionInteger(a[0], true)
		if e != nil {
			return NewNullValue(), e
		}
		if a[0].LogicalType().Kind == TypeInt32 {
			return NewTextValue(strconv.FormatUint(uint64(uint32(n)), 16)), nil
		}
		return NewTextValue(strconv.FormatUint(uint64(n), 16)), nil
	})
	add("BIT_COUNT", 1, 1, "BIGINT", func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		var n int64
		for _, b := range a[0].Blob {
			n += int64(bits.OnesCount8(b))
		}
		return NewIntValue(n), nil
	})
	for _, name := range strings.Fields("SHA224 SHA256 SHA384 SHA512") {
		name := name
		add(name, 1, 1, "BYTEA", func(s *Schema, a []Value) (Value, error) {
			if a[0].Null {
				return NewNullValue(), nil
			}
			b := functionDigest(name, a[0].Blob)
			if e := functionResultSize(s, uint64(len(b))); e != nil {
				return NewNullValue(), e
			}
			return NewBlobValue(b), nil
		})
	}
	for _, name := range []string{"NORMALIZE", "IS_NORMALIZED"} {
		name := name
		typ := "TEXT"
		if name == "IS_NORMALIZED" {
			typ = "BOOLEAN"
		}
		add(name, 1, 2, typ, func(s *Schema, a []Value) (Value, error) {
			if functionAnyNull(a) {
				return NewNullValue(), nil
			}
			f := norm.NFC
			if len(a) > 1 {
				switch strings.ToUpper(postgresText(a[1])) {
				case "NFC":
				case "NFD":
					f = norm.NFD
				case "NFKC":
					f = norm.NFKC
				case "NFKD":
					f = norm.NFKD
				default:
					return NewNullValue(), fmt.Errorf("invalid Unicode normalization form")
				}
			}
			text := postgresText(a[0])
			if name == "IS_NORMALIZED" {
				return s.postgresBooleanValue(f.IsNormalString(text)), nil
			}
			var result strings.Builder
			var iterator norm.Iter
			iterator.InitString(f, text)
			for !iterator.Done() {
				part := iterator.Next()
				if e := functionResultSize(s, uint64(result.Len()+len(part))); e != nil {
					return NewNullValue(), e
				}
				result.Write(part)
			}
			return NewTextValue(result.String()), nil
		})
	}
	add("UNISTR", 1, 1, "TEXT", func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		return postgresUnistr(s, postgresText(a[0]))
	})
	for _, name := range []string{"NUM_NULLS", "NUM_NONNULLS"} {
		name := name
		add(name, 1, -1, "INTEGER", func(s *Schema, a []Value) (Value, error) {
			var n int64
			for _, v := range a {
				if v.Null == (name == "NUM_NULLS") {
					n++
				}
			}
			return NewIntValue(n), nil
		})
	}
	add("GEN_RANDOM_UUID", 0, 0, "UUID", func(s *Schema, _ []Value) (Value, error) {
		var id [16]byte
		if _, e := rand.Read(id[:]); e != nil {
			return NewNullValue(), e
		}
		id[6] = (id[6] & 15) | 64
		id[8] = (id[8] & 63) | 128
		return NewTextValue(functionUUIDText(id[:])).WithSemanticType(TypeUUID), nil
	})
	spec := out["GEN_RANDOM_UUID"]
	spec.Volatile = true
	out["GEN_RANDOM_UUID"] = spec
	return out
}

func postgresUnistr(s *Schema, text string) (Value, error) {
	var out strings.Builder
	var high rune
	for i := 0; i < len(text); {
		r, n := utf8.DecodeRuneInString(text[i:])
		i += n
		if r == '\\' {
			if i >= len(text) {
				return NewNullValue(), fmt.Errorf("invalid Unicode escape")
			}
			if text[i] == '\\' {
				i++
				r = '\\'
			} else {
				width := 4
				switch text[i] {
				case 'u':
					i++
				case '+':
					width = 6
					i++
				case 'U':
					width = 8
					i++
				}
				if len(text)-i < width {
					return NewNullValue(), fmt.Errorf("invalid Unicode escape")
				}
				code, e := strconv.ParseUint(text[i:i+width], 16, 32)
				if e != nil {
					return NewNullValue(), fmt.Errorf("invalid Unicode escape")
				}
				i += width
				r = rune(code)
			}
		}
		if high != 0 {
			if r < 0xdc00 || r > 0xdfff {
				return NewNullValue(), fmt.Errorf("invalid Unicode surrogate pair")
			}
			r = utf16.DecodeRune(high, r)
			high = 0
		} else if r >= 0xd800 && r <= 0xdbff {
			high = r
			continue
		} else if r >= 0xdc00 && r <= 0xdfff {
			return NewNullValue(), fmt.Errorf("invalid Unicode surrogate pair")
		}
		if r == 0 || !utf8.ValidRune(r) {
			return NewNullValue(), fmt.Errorf("invalid Unicode code point")
		}
		if e := functionResultSize(s, uint64(out.Len()+utf8.RuneLen(r))); e != nil {
			return NewNullValue(), e
		}
		out.WriteRune(r)
	}
	if high != 0 {
		return NewNullValue(), fmt.Errorf("invalid Unicode surrogate pair")
	}
	return NewTextValue(out.String()), nil
}
