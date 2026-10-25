package sql

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

var jsonFunctionArity = map[string][2]int{
	"JSON": {1, 1}, "JSONB": {1, 1}, "JSON_VALID": {1, 1}, "JSON_TYPE": {1, 1},
	"JSON_QUOTE": {1, 1}, "JSON_UNQUOTE": {1, 1}, "JSON_PRETTY": {1, 1},
	"JSON_ARRAY": {0, -1}, "JSON_OBJECT": {0, -1}, "JSON_EXTRACT": {2, -1},
	"JSON_VALUE": {2, 2}, "JSON_LENGTH": {1, 2}, "JSON_DEPTH": {1, 1}, "JSON_KEYS": {1, 2},
	"JSON_CONTAINS": {2, 3}, "JSON_CONTAINS_PATH": {3, -1}, "JSON_OVERLAPS": {2, 2},
	"JSON_SET": {3, -1}, "JSON_INSERT": {3, -1}, "JSON_REPLACE": {3, -1}, "JSON_REMOVE": {2, -1},
	"JSON_ARRAY_APPEND": {3, -1}, "JSON_ARRAY_INSERT": {3, -1},
	"JSON_MERGE_PATCH": {2, -1}, "JSON_MERGE_PRESERVE": {2, -1}, "JSON_MERGE": {2, -1},
	"JSON_STORAGE_SIZE": {1, 1}, "JSON_STORAGE_FREE": {1, 1},
	"__JSON_GET": {2, 2}, "__JSON_GET_TEXT": {2, 2}, "__JSON_PATH": {2, 2}, "__JSON_PATH_TEXT": {2, 2},
}

func validJSONFunction(name string, n int) (bool, bool) {
	limits, ok := jsonFunctionArity[name]
	if !ok {
		return false, false
	}
	valid := n >= limits[0] && (limits[1] < 0 || n <= limits[1])
	if name == "JSON_OBJECT" {
		valid = valid && n%2 == 0
	}
	switch name {
	case "JSON_SET", "JSON_INSERT", "JSON_REPLACE", "JSON_ARRAY_APPEND", "JSON_ARRAY_INSERT":
		valid = valid && n%2 == 1
	}
	return valid, true
}

func decodeJSONDocument(value Value) (any, error) {
	text := mysqlStringValue(value)
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	if _, err := NewJSONValue(text); err != nil {
		return nil, err
	}
	return document, nil
}

func jsonResult(document any) (Value, error) {
	data, err := json.Marshal(document)
	if err != nil {
		return NewNullValue(), err
	}
	return Value{Type: ColJSON, Text: string(data)}, nil
}

func sqlJSONArgument(v Value) (any, error) {
	if v.Null {
		return nil, nil
	}
	switch v.Type {
	case ColJSON:
		return decodeJSONDocument(v)
	case ColInteger, ColUnsigned, ColDecimal:
		return json.Number(v.String()), nil
	case ColReal:
		if math.IsInf(v.Real, 0) || math.IsNaN(v.Real) {
			return nil, fmt.Errorf("nonfinite JSON number")
		}
		if v.NumericText != "" {
			return json.Number(v.NumericText), nil
		}
		return json.Number(strconv.FormatFloat(v.Real, 'g', -1, 64)), nil
	case ColBlob:
		return string(v.Blob), nil
	default:
		return v.Text, nil
	}
}

type jsonPathLeg struct {
	key                              string
	index                            int
	array, wildcard, recursive, last bool
}

func parseJSONPath(path string, mutating bool) ([]jsonPathLeg, error) {
	if len(path) == 0 || path[0] != '$' {
		return nil, fmt.Errorf("JSON path must start with $")
	}
	var legs []jsonPathLeg
	for pos := 1; pos < len(path); {
		if len(legs) >= 128 {
			return nil, fmt.Errorf("JSON path exceeds nesting limit")
		}
		if strings.HasPrefix(path[pos:], "**") {
			if mutating {
				return nil, fmt.Errorf("wildcards are not allowed in modifying JSON paths")
			}
			legs = append(legs, jsonPathLeg{recursive: true})
			pos += 2
			continue
		}
		switch path[pos] {
		case '.':
			pos++
			if pos == len(path) {
				return nil, fmt.Errorf("incomplete JSON member path")
			}
			if path[pos] == '*' {
				if mutating {
					return nil, fmt.Errorf("wildcards are not allowed in modifying JSON paths")
				}
				legs = append(legs, jsonPathLeg{wildcard: true})
				pos++
				continue
			}
			var key string
			if path[pos] == '"' {
				start := pos
				pos++
				for pos < len(path) && path[pos] != '"' {
					if path[pos] == '\\' {
						pos++
					}
					pos++
				}
				if pos >= len(path) {
					return nil, fmt.Errorf("unterminated JSON member name")
				}
				pos++
				if err := json.Unmarshal([]byte(path[start:pos]), &key); err != nil {
					return nil, fmt.Errorf("invalid JSON member name")
				}
			} else {
				start := pos
				for pos < len(path) && path[pos] != '.' && path[pos] != '[' && path[pos] != '*' {
					pos++
				}
				key = path[start:pos]
				if key == "" || strings.ContainsAny(key, " \t\r\n]") {
					return nil, fmt.Errorf("invalid JSON member name")
				}
			}
			legs = append(legs, jsonPathLeg{key: key})
		case '[':
			end := strings.IndexByte(path[pos:], ']')
			if end < 0 {
				return nil, fmt.Errorf("unterminated JSON array path")
			}
			end += pos
			part := strings.TrimSpace(path[pos+1 : end])
			leg := jsonPathLeg{array: true}
			if part == "*" {
				if mutating {
					return nil, fmt.Errorf("wildcards are not allowed in modifying JSON paths")
				}
				leg.wildcard = true
			} else if strings.HasPrefix(part, "last") {
				leg.last = true
				rest := strings.TrimSpace(part[4:])
				if rest != "" {
					if rest[0] != '-' {
						return nil, fmt.Errorf("invalid JSON array path")
					}
					n, err := strconv.Atoi(strings.TrimSpace(rest[1:]))
					if err != nil || n < 0 {
						return nil, fmt.Errorf("invalid JSON array index")
					}
					leg.index = n
				}
			} else {
				n, err := strconv.Atoi(part)
				if err != nil || n < 0 {
					return nil, fmt.Errorf("invalid JSON array index")
				}
				leg.index = n
			}
			legs = append(legs, leg)
			pos = end + 1
		default:
			return nil, fmt.Errorf("invalid JSON path near %q", path[pos:])
		}
	}
	if len(legs) > 0 && legs[len(legs)-1].recursive {
		return nil, fmt.Errorf("recursive JSON path must have a following member")
	}
	return legs, nil
}

func jsonChildren(value any) []any {
	switch v := value.(type) {
	case []any:
		return v
	case map[string]any:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([]any, 0, len(keys))
		for _, k := range keys {
			out = append(out, v[k])
		}
		return out
	}
	return nil
}

func jsonPathValues(value any, legs []jsonPathLeg) []any {
	if len(legs) == 0 {
		return []any{value}
	}
	leg := legs[0]
	if leg.recursive {
		out := jsonPathValues(value, legs[1:])
		for _, child := range jsonChildren(value) {
			out = append(out, jsonPathValues(child, legs)...)
		}
		return out
	}
	if leg.wildcard {
		var out []any
		switch value.(type) {
		case []any:
			if !leg.array {
				return nil
			}
		case map[string]any:
			if leg.array {
				return nil
			}
		default:
			return nil
		}
		for _, child := range jsonChildren(value) {
			out = append(out, jsonPathValues(child, legs[1:])...)
		}
		return out
	}
	if leg.array {
		array, ok := value.([]any)
		if !ok {
			array = []any{value}
		}
		index := leg.index
		if leg.last {
			index = len(array) - 1 - index
		}
		if index < 0 || index >= len(array) {
			return nil
		}
		return jsonPathValues(array[index], legs[1:])
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	child, ok := object[leg.key]
	if !ok {
		return nil
	}
	return jsonPathValues(child, legs[1:])
}

func jsonPathHasWildcard(legs []jsonPathLeg) bool {
	for _, leg := range legs {
		if leg.wildcard || leg.recursive {
			return true
		}
	}
	return false
}

// Mutation walks one owned decoded document. Missing intermediate members are
// left alone; only an existing parent can receive a newly inserted final member.
func jsonModify(value any, legs []jsonPathLeg, replacement any, mode string) (any, bool, error) {
	if len(legs) == 0 {
		switch mode {
		case "JSON_REMOVE":
			return value, false, fmt.Errorf("cannot remove JSON document root")
		case "JSON_INSERT":
			return value, false, nil
		case "JSON_ARRAY_APPEND":
			array, ok := value.([]any)
			if !ok {
				array = []any{value}
			}
			return append(array, replacement), true, nil
		case "JSON_ARRAY_INSERT":
			return value, false, fmt.Errorf("JSON_ARRAY_INSERT requires an array index")
		}
		return replacement, true, nil
	}
	leg := legs[0]
	if leg.array {
		array, wasArray := value.([]any)
		if !wasArray {
			array = []any{value}
		}
		index := leg.index
		if leg.last {
			index = len(array) - 1 - index
		}
		if index < 0 {
			return value, false, nil
		}
		if mode == "JSON_ARRAY_INSERT" && len(legs) == 1 {
			if !wasArray {
				return value, false, nil
			}
			index = min(index, len(array))
			array = append(array, nil)
			copy(array[index+1:], array[index:])
			array[index] = replacement
			return array, true, nil
		}
		if index >= len(array) {
			if len(legs) > 1 || mode == "JSON_REPLACE" || mode == "JSON_REMOVE" || mode == "JSON_ARRAY_APPEND" {
				return value, false, nil
			}
			return append(array, replacement), true, nil
		}
		if len(legs) == 1 && mode == "JSON_REMOVE" {
			if !wasArray {
				return value, false, nil
			}
			return append(array[:index], array[index+1:]...), true, nil
		}
		child, changed, err := jsonModify(array[index], legs[1:], replacement, mode)
		if err != nil || !changed {
			return value, false, err
		}
		array[index] = child
		if !wasArray && index == 0 {
			return child, true, nil
		}
		return array, true, nil
	}
	object, ok := value.(map[string]any)
	if !ok {
		return value, false, nil
	}
	child, exists := object[leg.key]
	if !exists {
		if len(legs) > 1 || mode == "JSON_REPLACE" || mode == "JSON_REMOVE" || mode == "JSON_ARRAY_APPEND" || mode == "JSON_ARRAY_INSERT" {
			return value, false, nil
		}
		object[leg.key] = replacement
		return object, true, nil
	}
	if len(legs) == 1 && mode == "JSON_REMOVE" {
		delete(object, leg.key)
		return object, true, nil
	}
	changedValue, changed, err := jsonModify(child, legs[1:], replacement, mode)
	if changed {
		object[leg.key] = changedValue
	}
	return object, changed, err
}

func jsonScalarEqual(a, b any) bool {
	an, aok := a.(json.Number)
	bn, bok := b.(json.Number)
	if aok && bok {
		av, ae := NewDecimalValue(string(an))
		bv, be := NewDecimalValue(string(bn))
		return ae == nil && be == nil && ValueCompare(av, bv) == 0
	}
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return string(left) == string(right)
}

func jsonContains(target, candidate any) bool {
	if object, ok := candidate.(map[string]any); ok {
		other, ok := target.(map[string]any)
		if !ok {
			return false
		}
		for key, value := range object {
			child, exists := other[key]
			if !exists || !jsonContains(child, value) {
				return false
			}
		}
		return true
	}
	if values, ok := candidate.([]any); ok {
		other, ok := target.([]any)
		if !ok {
			return false
		}
		for _, value := range values {
			found := false
			for _, child := range other {
				if jsonContains(child, value) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}
	if array, ok := target.([]any); ok {
		for _, child := range array {
			if jsonContains(child, candidate) {
				return true
			}
		}
		return false
	}
	return jsonScalarEqual(target, candidate)
}

func jsonMergePatch(left, right any) any {
	patch, ok := right.(map[string]any)
	if !ok {
		return right
	}
	object, ok := left.(map[string]any)
	if !ok {
		object = map[string]any{}
	}
	for key, value := range patch {
		if value == nil {
			delete(object, key)
		} else {
			object[key] = jsonMergePatch(object[key], value)
		}
	}
	return object
}

func jsonMergePreserve(left, right any) any {
	a, aok := left.(map[string]any)
	b, bok := right.(map[string]any)
	if aok && bok {
		for key, value := range b {
			if old, ok := a[key]; ok {
				a[key] = jsonMergePreserve(old, value)
			} else {
				a[key] = value
			}
		}
		return a
	}
	array, aok := left.([]any)
	if !aok {
		array = []any{left}
	}
	values, bok := right.([]any)
	if !bok {
		values = []any{right}
	}
	return append(array, values...)
}

func jsonDepth(document any) int {
	depth := 1
	for _, child := range jsonChildren(document) {
		depth = max(depth, 1+jsonDepth(child))
	}
	return depth
}

func (s *Schema) evalJSONFunc(f *FuncCallExpr, row map[string]Value) (Value, bool, error) {
	name := strings.ToUpper(f.Name)
	valid, known := validJSONFunction(name, len(f.Args))
	if !known {
		return Value{}, false, nil
	}
	if !valid {
		return NewNullValue(), true, fmt.Errorf("wrong number of arguments to %s", name)
	}
	args := make([]Value, len(f.Args))
	for i, arg := range f.Args {
		value, err := s.evalExpr(arg, row)
		if err != nil {
			return NewNullValue(), true, err
		}
		args[i] = value
	}
	result, err := evaluateJSONFunction(name, args)
	return result, true, err
}

func evaluateJSONFunction(name string, args []Value) (Value, error) {
	if name == "JSON_ARRAY" || name == "JSON_OBJECT" {
		if name == "JSON_ARRAY" {
			array := make([]any, 0, len(args))
			for _, arg := range args {
				value, err := sqlJSONArgument(arg)
				if err != nil {
					return NewNullValue(), err
				}
				array = append(array, value)
			}
			return jsonResult(array)
		}
		object := map[string]any{}
		for i := 0; i < len(args); i += 2 {
			if args[i].Null {
				return NewNullValue(), fmt.Errorf("JSON_OBJECT keys cannot be NULL")
			}
			value, err := sqlJSONArgument(args[i+1])
			if err != nil {
				return NewNullValue(), err
			}
			object[mysqlStringValue(args[i])] = value
		}
		return jsonResult(object)
	}
	if args[0].Null {
		return NewNullValue(), nil
	}
	if name == "JSON_QUOTE" {
		data, _ := json.Marshal(mysqlStringValue(args[0]))
		return NewTextValue(string(data)), nil
	}
	if name == "JSON_UNQUOTE" {
		text := mysqlStringValue(args[0])
		if strings.HasPrefix(text, "\"") {
			var unquoted string
			if err := json.Unmarshal([]byte(text), &unquoted); err != nil {
				return NewNullValue(), fmt.Errorf("invalid JSON string")
			}
			return NewTextValue(unquoted), nil
		}
		return NewTextValue(text), nil
	}
	document, err := decodeJSONDocument(args[0])
	if name == "JSON_VALID" {
		if err == nil {
			return NewIntValue(1), nil
		}
		return NewIntValue(0), nil
	}
	if err != nil {
		return NewNullValue(), err
	}
	path := func(arg Value, mutate bool) ([]jsonPathLeg, error) {
		if arg.Null {
			return nil, fmt.Errorf("NULL JSON path")
		}
		return parseJSONPath(mysqlStringValue(arg), mutate)
	}
	switch name {
	case "JSON", "JSONB":
		return jsonResult(document)
	case "JSON_TYPE":
		kind := "NULL"
		switch value := document.(type) {
		case map[string]any:
			kind = "OBJECT"
		case []any:
			kind = "ARRAY"
		case string:
			kind = "STRING"
		case bool:
			kind = "BOOLEAN"
		case json.Number:
			kind = "INTEGER"
			if strings.ContainsAny(string(value), ".eE") {
				kind = "DOUBLE"
			}
		}
		return NewTextValue(kind), nil
	case "JSON_DEPTH":
		return NewIntValue(int64(jsonDepth(document))), nil
	case "JSON_STORAGE_SIZE":
		value, err := jsonResult(document)
		return NewIntValue(int64(len(value.Text))), err
	case "JSON_STORAGE_FREE":
		return NewIntValue(0), nil
	case "JSON_PRETTY":
		data, err := json.MarshalIndent(document, "", "  ")
		return NewTextValue(string(data)), err
	case "JSON_EXTRACT", "JSON_VALUE":
		var matches []any
		multiple := len(args) > 2
		for _, arg := range args[1:] {
			if arg.Null {
				return NewNullValue(), nil
			}
			legs, err := path(arg, false)
			if err != nil {
				return NewNullValue(), err
			}
			multiple = multiple || jsonPathHasWildcard(legs)
			matches = append(matches, jsonPathValues(document, legs)...)
		}
		if len(matches) == 0 {
			return NewNullValue(), nil
		}
		if name == "JSON_VALUE" {
			if matches[0] == nil {
				return NewNullValue(), nil
			}
			switch value := matches[0].(type) {
			case string:
				return NewTextValue(value), nil
			case json.Number:
				return NewTextValue(string(value)), nil
			case bool:
				return NewTextValue(strconv.FormatBool(value)), nil
			default:
				return NewNullValue(), nil
			}
		}
		if multiple {
			return jsonResult(matches)
		}
		return jsonResult(matches[0])
	case "__JSON_GET", "__JSON_GET_TEXT", "__JSON_PATH", "__JSON_PATH_TEXT":
		if args[1].Null {
			return NewNullValue(), nil
		}
		var legs []jsonPathLeg
		var pgParts []string
		pgPath := strings.HasPrefix(name, "__JSON_PATH")
		key := mysqlStringValue(args[1])
		if pgPath {
			var err error
			pgParts, err = parsePGJSONPath(key)
			if err != nil {
				return NewNullValue(), err
			}
		} else if strings.HasPrefix(key, "$") {
			var err error
			legs, err = parseJSONPath(key, false)
			if err != nil {
				return NewNullValue(), err
			}
		} else if args[1].Type == ColInteger || args[1].Type == ColUnsigned {
			array, ok := document.([]any)
			if !ok {
				return NewNullValue(), nil
			}
			index := args[1].Int
			if args[1].Type == ColUnsigned {
				if args[1].Uint > math.MaxInt64 {
					return NewNullValue(), nil
				}
				index = int64(args[1].Uint)
			}
			if index < 0 {
				index += int64(len(array))
			}
			if index < 0 || index >= int64(len(array)) {
				return NewNullValue(), nil
			}
			legs = []jsonPathLeg{{array: true, index: int(index)}}
		} else {
			legs = []jsonPathLeg{{key: key}}
		}
		matches := jsonPathValues(document, legs)
		if pgPath {
			matches = pgJSONPathValue(document, pgParts)
		}
		if len(matches) == 0 {
			return NewNullValue(), nil
		}
		if strings.HasSuffix(name, "_TEXT") {
			if matches[0] == nil {
				return NewNullValue(), nil
			}
			if text, ok := matches[0].(string); ok {
				return NewTextValue(text), nil
			}
			value, err := jsonResult(matches[0])
			return NewTextValue(value.Text), err
		}
		return jsonResult(matches[0])
	case "JSON_LENGTH", "JSON_KEYS":
		if len(args) > 1 {
			if args[1].Null {
				return NewNullValue(), nil
			}
			legs, err := path(args[1], false)
			if err != nil {
				return NewNullValue(), err
			}
			if jsonPathHasWildcard(legs) {
				return NewNullValue(), fmt.Errorf("wildcards are not allowed in this JSON path")
			}
			matches := jsonPathValues(document, legs)
			if len(matches) == 0 {
				return NewNullValue(), nil
			}
			document = matches[0]
		}
		if name == "JSON_KEYS" {
			object, ok := document.(map[string]any)
			if !ok {
				return NewNullValue(), nil
			}
			keys := make([]string, 0, len(object))
			for key := range object {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			return jsonResult(keys)
		}
		n := 1
		switch value := document.(type) {
		case []any:
			n = len(value)
		case map[string]any:
			n = len(value)
		}
		return NewIntValue(int64(n)), nil
	case "JSON_CONTAINS", "JSON_OVERLAPS":
		if args[1].Null {
			return NewNullValue(), nil
		}
		candidate, err := decodeJSONDocument(args[1])
		if err != nil {
			return NewNullValue(), err
		}
		if len(args) > 2 {
			if args[2].Null {
				return NewNullValue(), nil
			}
			legs, err := path(args[2], true)
			if err != nil {
				return NewNullValue(), err
			}
			matches := jsonPathValues(document, legs)
			if len(matches) == 0 {
				return NewNullValue(), nil
			}
			document = matches[0]
		}
		found := false
		if name == "JSON_CONTAINS" {
			found = jsonContains(document, candidate)
		} else {
			switch target := document.(type) {
			case map[string]any:
				if other, ok := candidate.(map[string]any); ok {
					for key, value := range target {
						if child, exists := other[key]; exists && jsonScalarEqual(value, child) {
							found = true
							break
						}
					}
				}
			case []any:
				array, ok := candidate.([]any)
				if !ok {
					array = []any{candidate}
				}
				for _, a := range target {
					for _, b := range array {
						if jsonScalarEqual(a, b) {
							found = true
						}
					}
				}
			default:
				if array, ok := candidate.([]any); ok {
					for _, b := range array {
						found = found || jsonScalarEqual(document, b)
					}
				} else {
					found = jsonScalarEqual(document, candidate)
				}
			}
		}
		if found {
			return NewIntValue(1), nil
		}
		return NewIntValue(0), nil
	case "JSON_CONTAINS_PATH":
		if args[1].Null {
			return NewNullValue(), nil
		}
		mode := strings.ToLower(mysqlStringValue(args[1]))
		if mode != "one" && mode != "all" {
			return NewNullValue(), fmt.Errorf("JSON_CONTAINS_PATH mode must be one or all")
		}
		found := mode == "all"
		for _, arg := range args[2:] {
			if arg.Null {
				return NewNullValue(), nil
			}
			legs, err := path(arg, false)
			if err != nil {
				return NewNullValue(), err
			}
			exists := len(jsonPathValues(document, legs)) > 0
			if mode == "one" {
				found = found || exists
			} else {
				found = found && exists
			}
		}
		if found {
			return NewIntValue(1), nil
		}
		return NewIntValue(0), nil
	case "JSON_SET", "JSON_INSERT", "JSON_REPLACE", "JSON_REMOVE", "JSON_ARRAY_APPEND", "JSON_ARRAY_INSERT":
		step := 2
		if name == "JSON_REMOVE" {
			step = 1
		}
		for i := 1; i < len(args); i += step {
			if args[i].Null {
				return NewNullValue(), nil
			}
			legs, err := path(args[i], true)
			if err != nil {
				return NewNullValue(), err
			}
			var replacement any
			if step == 2 {
				replacement, err = sqlJSONArgument(args[i+1])
				if err != nil {
					return NewNullValue(), err
				}
			}
			document, _, err = jsonModify(document, legs, replacement, name)
			if err != nil {
				return NewNullValue(), err
			}
		}
		return jsonResult(document)
	case "JSON_MERGE_PATCH", "JSON_MERGE_PRESERVE", "JSON_MERGE":
		for _, arg := range args[1:] {
			if arg.Null {
				return NewNullValue(), nil
			}
			right, err := decodeJSONDocument(arg)
			if err != nil {
				return NewNullValue(), err
			}
			if name == "JSON_MERGE_PATCH" {
				document = jsonMergePatch(document, right)
			} else {
				document = jsonMergePreserve(document, right)
			}
		}
		return jsonResult(document)
	}
	return NewNullValue(), fmt.Errorf("unsupported JSON function %s", name)
}
