package sql

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func postgresJSONFunctions() map[string]mysqlFunctionSpec {
	out := map[string]mysqlFunctionSpec{}
	add := func(name string, min, max int, typ ColumnType, eval func(*Schema, []Value) (Value, error)) {
		spec := mysqlFunctionSpec{Min: min, Max: max, Return: typ, Eval: eval}
		if typ == ColJSON {
			kind := "JSON"
			if strings.HasPrefix(name, "JSONB") || name == "TO_JSONB" {
				kind = "JSONB"
			}
			spec.Metadata = func([]ColumnDef) ColumnDef { return ColumnDef{ColType: ColJSON, Declared: kind, Postgres: true} }
		}
		out[name] = spec
	}
	for _, name := range strings.Fields("JSON_BUILD_ARRAY JSONB_BUILD_ARRAY JSON_BUILD_OBJECT JSONB_BUILD_OBJECT") {
		name := name
		add(name, 0, -1, ColJSON, func(s *Schema, a []Value) (Value, error) {
			if strings.HasSuffix(name, "OBJECT") && len(a)%2 != 0 {
				return NewNullValue(), fmt.Errorf("argument list must have even number of elements")
			}
			var document any
			if strings.HasSuffix(name, "ARRAY") {
				v := make([]any, len(a))
				for i, x := range a {
					var e error
					v[i], e = postgresJSONArgument(x)
					if e != nil {
						return NewNullValue(), e
					}
				}
				document = v
			} else {
				v := map[string]any{}
				for i := 0; i < len(a); i += 2 {
					if a[i].Null {
						return NewNullValue(), fmt.Errorf("JSON object key must not be null")
					}
					if a[i].Type == ColJSON {
						return NewNullValue(), fmt.Errorf("JSON object key must be scalar")
					}
					x, e := postgresJSONArgument(a[i+1])
					if e != nil {
						return NewNullValue(), e
					}
					v[postgresText(a[i])] = x
				}
				document = v
			}
			return postgresJSONResult(s, document, strings.HasPrefix(name, "JSONB"))
		})
	}
	for _, name := range []string{"TO_JSON", "TO_JSONB"} {
		name := name
		add(name, 1, 1, ColJSON, func(s *Schema, a []Value) (Value, error) {
			if a[0].Null {
				return NewNullValue(), nil
			}
			v, e := postgresJSONArgument(a[0])
			if e != nil {
				return NewNullValue(), e
			}
			return postgresJSONResult(s, v, name == "TO_JSONB")
		})
	}
	for _, name := range []string{"JSON_TYPEOF", "JSONB_TYPEOF"} {
		add(name, 1, 1, ColText, func(s *Schema, a []Value) (Value, error) {
			if a[0].Null {
				return NewNullValue(), nil
			}
			v, e := decodeJSONDocument(a[0])
			if e != nil {
				return NewNullValue(), e
			}
			typ := "null"
			switch v.(type) {
			case map[string]any:
				typ = "object"
			case []any:
				typ = "array"
			case json.Number:
				typ = "number"
			case bool:
				typ = "boolean"
			case string:
				typ = "string"
			}
			return NewTextValue(typ), nil
		})
	}
	for _, name := range []string{"JSON_ARRAY_LENGTH", "JSONB_ARRAY_LENGTH"} {
		add(name, 1, 1, ColInteger, func(s *Schema, a []Value) (Value, error) {
			if a[0].Null {
				return NewNullValue(), nil
			}
			v, e := decodeJSONDocument(a[0])
			if e != nil {
				return NewNullValue(), e
			}
			arr, ok := v.([]any)
			if !ok {
				return NewNullValue(), fmt.Errorf("cannot get array length of a non-array")
			}
			return NewIntValue(int64(len(arr))), nil
		})
	}
	for _, name := range strings.Fields("JSON_EXTRACT_PATH JSONB_EXTRACT_PATH JSON_EXTRACT_PATH_TEXT JSONB_EXTRACT_PATH_TEXT") {
		name := name
		typ := ColJSON
		if strings.HasSuffix(name, "TEXT") {
			typ = ColText
		}
		add(name, 1, -1, typ, func(s *Schema, a []Value) (Value, error) {
			if mysqlAnyNull(a) {
				return NewNullValue(), nil
			}
			v, e := decodeJSONDocument(a[0])
			if e != nil {
				return NewNullValue(), e
			}
			for _, p := range a[1:] {
				switch x := v.(type) {
				case map[string]any:
					var ok bool
					v, ok = x[postgresText(p)]
					if !ok {
						return NewNullValue(), nil
					}
				case []any:
					i, e := strconv.ParseInt(postgresText(p), 10, 32)
					if e != nil {
						return NewNullValue(), nil
					}
					if i < 0 {
						i += int64(len(x))
					}
					if i < 0 || i >= int64(len(x)) {
						return NewNullValue(), nil
					}
					v = x[i]
				default:
					return NewNullValue(), nil
				}
			}
			if strings.HasSuffix(name, "TEXT") {
				if v == nil {
					return NewNullValue(), nil
				}
				if text, ok := v.(string); ok {
					return NewTextValue(text), nil
				}
				r, e := jsonResult(v)
				return NewTextValue(r.Text), e
			}
			return postgresJSONResult(s, v, strings.HasPrefix(name, "JSONB"))
		})
	}
	for _, name := range []string{"JSONB_SET", "JSONB_INSERT"} {
		name := name
		add(name, 3, 4, ColJSON, func(s *Schema, a []Value) (Value, error) {
			if mysqlAnyNull(a) {
				return NewNullValue(), nil
			}
			v, e := decodeJSONDocument(a[0])
			if e != nil {
				return NewNullValue(), e
			}
			parts, e := parsePGJSONPath(postgresText(a[1]))
			if e != nil {
				return NewNullValue(), e
			}
			replacement, e := decodeJSONDocument(a[2])
			if e != nil {
				return NewNullValue(), e
			}
			flag := name == "JSONB_SET"
			if len(a) > 3 {
				b, err := CastPostgresValue(a[3], "BOOLEAN")
				if err != nil {
					return NewNullValue(), err
				}
				flag = b.Int != 0
			}
			if len(parts) > 128 {
				return NewNullValue(), fmt.Errorf("JSON path exceeds nesting limit")
			}
			if len(parts) > 0 {
				switch v.(type) {
				case []any, map[string]any:
				default:
					return NewNullValue(), fmt.Errorf("cannot set path in scalar")
				}
			}
			v, e = postgresJSONModify(v, parts, replacement, name == "JSONB_INSERT", flag)
			if e != nil {
				return NewNullValue(), e
			}
			return postgresJSONResult(s, v, true)
		})
	}
	add("JSONB_STRIP_NULLS", 1, 1, ColJSON, func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		v, e := decodeJSONDocument(a[0])
		if e != nil {
			return NewNullValue(), e
		}
		return postgresJSONResult(s, postgresJSONStripNulls(v), true)
	})
	add("JSONB_PRETTY", 1, 1, ColText, func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		v, e := decodeJSONDocument(a[0])
		if e != nil {
			return NewNullValue(), e
		}
		data, e := json.MarshalIndent(v, "", "    ")
		if e != nil {
			return NewNullValue(), e
		}
		if e = mysqlFunctionSize(s, uint64(len(data))); e != nil {
			return NewNullValue(), e
		}
		return NewTextValue(string(data)), nil
	})
	return out
}
func postgresJSONArgument(v Value) (any, error) {
	if strings.EqualFold(v.PGType, "BOOLEAN") && !v.Null {
		return v.Int != 0, nil
	}
	if v.Type == ColBlob && !v.Null {
		return postgresText(v), nil
	}
	return sqlJSONArgument(v)
}
func postgresJSONResult(s *Schema, v any, binary bool) (Value, error) {
	r, e := jsonResult(v)
	if e != nil {
		return r, e
	}
	if e = mysqlFunctionSize(s, uint64(len(r.Text))); e != nil {
		return NewNullValue(), e
	}
	r.PGType = "JSON"
	if binary {
		r.PGType = "JSONB"
	}
	return r, nil
}
func postgresJSONStripNulls(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, v := range x {
			if v == nil {
				delete(x, k)
			} else {
				x[k] = postgresJSONStripNulls(v)
			}
		}
	case []any:
		for i, v := range x {
			x[i] = postgresJSONStripNulls(v)
		}
	}
	return v
}
func postgresJSONModify(v any, path []string, replacement any, insert, flag bool) (any, error) {
	if len(path) == 0 {
		return v, nil
	}
	last := len(path) == 1
	switch x := v.(type) {
	case map[string]any:
		old, exists := x[path[0]]
		if last {
			if insert && exists {
				return nil, fmt.Errorf("cannot replace existing key with jsonb_insert")
			}
			if exists || flag || insert {
				x[path[0]] = replacement
			}
			return x, nil
		}
		if !exists {
			return x, nil
		}
		child, e := postgresJSONModify(old, path[1:], replacement, insert, flag)
		if e != nil {
			return nil, e
		}
		x[path[0]] = child
		return x, nil
	case []any:
		i, e := strconv.ParseInt(path[0], 10, 32)
		if e != nil {
			return nil, fmt.Errorf("JSON path array element is not an integer")
		}
		n := int64(len(x))
		original := i
		if i < 0 {
			i += n
		}
		if !last {
			if i < 0 || i >= n {
				return x, nil
			}
			child, e := postgresJSONModify(x[i], path[1:], replacement, insert, flag)
			if e != nil {
				return nil, e
			}
			x[i] = child
			return x, nil
		}
		if !insert && i >= 0 && i < n {
			x[i] = replacement
			return x, nil
		}
		if !insert && !flag {
			return x, nil
		}
		if i < 0 {
			i = 0
		} else if i >= n {
			i = n
		} else if insert && flag {
			i++
		}
		if original < 0 && original < -n {
			i = 0
		}
		x = append(x, nil)
		copy(x[i+1:], x[i:])
		x[i] = replacement
		return x, nil
	default:
		return v, nil
	}
}
