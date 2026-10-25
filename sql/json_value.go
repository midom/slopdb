package sql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"unicode/utf8"
)

func NewJSONValue(text string) (Value, error) {
	if !utf8.ValidString(text) {
		return NewNullValue(), fmt.Errorf("invalid UTF-8 in JSON")
	}
	decoder := json.NewDecoder(bytes.NewBufferString(text))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		return NewNullValue(), fmt.Errorf("invalid JSON: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return NewNullValue(), fmt.Errorf("invalid JSON: trailing data")
	}
	if err := validateJSONDocument(document, 0); err != nil {
		return NewNullValue(), err
	}
	data, err := json.Marshal(document)
	if err != nil {
		return NewNullValue(), err
	}
	return Value{Type: ColJSON, Text: string(data)}, nil
}

func validateJSONDocument(document any, depth int) error {
	if depth > 100 {
		return fmt.Errorf("JSON nesting exceeds 100 levels")
	}
	switch value := document.(type) {
	case json.Number:
		_, _, err := parseDecimal(string(value))
		return err
	case []any:
		for _, child := range value {
			if err := validateJSONDocument(child, depth+1); err != nil {
				return err
			}
		}
	case map[string]any:
		for _, child := range value {
			if err := validateJSONDocument(child, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func jsonValueOrder(value any) int {
	switch value.(type) {
	case nil:
		return 0
	case json.Number:
		return 1
	case string:
		return 2
	case map[string]any:
		return 3
	case []any:
		return 4
	case bool:
		return 5
	}
	return 6
}
func jsonCompareDocuments(a, b any) int {
	ar, br := jsonValueOrder(a), jsonValueOrder(b)
	if ar < br {
		return -1
	}
	if ar > br {
		return 1
	}
	switch left := a.(type) {
	case json.Number:
		av, ae := NewDecimalValue(string(left))
		bv, be := NewDecimalValue(string(b.(json.Number)))
		if ae == nil && be == nil {
			return ValueCompare(av, bv)
		}
		return strings.Compare(string(left), string(b.(json.Number)))
	case string:
		return strings.Compare(left, b.(string))
	case bool:
		if left == b.(bool) {
			return 0
		}
		if !left {
			return -1
		}
		return 1
	case []any:
		right := b.([]any)
		for i := 0; i < min(len(left), len(right)); i++ {
			if c := jsonCompareDocuments(left[i], right[i]); c != 0 {
				return c
			}
		}
		return compareLengths(len(left), len(right))
	case map[string]any:
		right := b.(map[string]any)
		lk, rk := sortedJSONKeys(left), sortedJSONKeys(right)
		for i := 0; i < min(len(lk), len(rk)); i++ {
			if c := strings.Compare(lk[i], rk[i]); c != 0 {
				return c
			}
			if c := jsonCompareDocuments(left[lk[i]], right[rk[i]]); c != 0 {
				return c
			}
		}
		return compareLengths(len(lk), len(rk))
	}
	return 0
}
func compareLengths(a, b int) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
func sortedJSONKeys(object map[string]any) []string {
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
func compareJSONValues(a, b Value) int {
	convert := func(value Value) any {
		document, err := sqlJSONArgument(value)
		if err != nil {
			return value.String()
		}
		return document
	}
	return jsonCompareDocuments(convert(a), convert(b))
}
func jsonSortKey(text string) []byte {
	document, err := decodeJSONDocument(Value{Type: ColJSON, Text: text})
	if err != nil {
		return []byte(text)
	}
	var encode func(any) []byte
	encode = func(value any) []byte {
		out := []byte{byte(jsonValueOrder(value) + 1)}
		switch v := value.(type) {
		case json.Number:
			out = appendEscapedClustered(out, decimalSortKey(string(v)))
		case string:
			out = appendEscapedClustered(out, []byte(v))
		case bool:
			if v {
				out = append(out, 1)
			} else {
				out = append(out, 0)
			}
		case []any:
			for _, child := range v {
				out = appendEscapedClustered(out, encode(child))
			}
			out = append(out, 0, 0)
		case map[string]any:
			for _, key := range sortedJSONKeys(v) {
				out = appendEscapedClustered(out, []byte(key))
				out = appendEscapedClustered(out, encode(v[key]))
			}
			out = append(out, 0, 0)
		}
		return out
	}
	return encode(document)
}
