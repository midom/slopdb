package sql

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// mysqlStringValue interprets binary strings as their bytes in SQL string
// operations. Value.String formats BLOBs for the native diagnostic interface.
func mysqlStringValue(value Value) string {
	if value.Type == ColBlob && !value.Null {
		return string(value.Blob)
	}
	return value.String()
}

// MySQL functions are evaluated in the ordinary expression tree. In particular,
// IF is lazy and session/clock functions are never marked deterministic.
func (s *Schema) evalMySQLFunc(e *FuncCallExpr, row map[string]Value) (Value, bool, error) {
	name := strings.ToUpper(e.Name)
	if name == "IF" {
		condition, err := s.evalCondition(e.Args[0], row)
		if err != nil {
			return NewNullValue(), true, err
		}
		if condition {
			v, err := s.evalExpr(e.Args[1], row)
			return v, true, err
		}
		v, err := s.evalExpr(e.Args[2], row)
		return v, true, err
	}
	switch name {
	case "GREATEST", "LEAST", "CONCAT", "SUBSTRING", "SYSDATE", "NOW", "CURRENT_TIMESTAMP", "UNIX_TIMESTAMP", "__MYSQL_CONVERT",
		"GET_LOCK", "RELEASE_LOCK", "IS_FREE_LOCK", "CURRENT_USER", "DATABASE", "VERSION", "CONNECTION_ID":
	default:
		return Value{}, false, nil
	}
	args := make([]Value, len(e.Args))
	for i, expr := range e.Args {
		var err error
		args[i], err = s.evalExpr(expr, row)
		if err != nil {
			return NewNullValue(), true, err
		}
	}
	if value, handled, err := MySQLFunction(s.ctx, name, args); handled || err != nil {
		return value, true, err
	}
	switch name {
	case "GREATEST", "LEAST":
		for _, arg := range args {
			if arg.Null {
				return NewNullValue(), true, nil
			}
		}
		best := args[0]
		allNumeric, anyReal := true, false
		for _, arg := range args {
			allNumeric = allNumeric && (arg.Type == ColInteger || arg.Type == ColReal)
			anyReal = anyReal || arg.Type == ColReal
		}
		for _, arg := range args[1:] {
			cmp := s.valueCompare(arg, best)
			if !allNumeric {
				cmp = strings.Compare(mysqlStringValue(arg), mysqlStringValue(best))
			}
			if name == "GREATEST" && cmp > 0 || name == "LEAST" && cmp < 0 {
				best = arg
			}
		}
		if allNumeric && anyReal {
			best = NewRealValue(toFloat(best))
		}
		return best, true, nil
	case "CONCAT":
		var text strings.Builder
		binary := false
		for _, arg := range args {
			if arg.Null {
				return NewNullValue(), true, nil
			}
			if arg.Type == ColBlob {
				text.Write(arg.Blob)
				binary = true
			} else {
				text.WriteString(arg.String())
			}
		}
		if binary {
			return NewBlobValue([]byte(text.String())), true, nil
		}
		return NewTextValue(text.String()), true, nil
	case "SUBSTRING":
		for _, arg := range args {
			if arg.Null {
				return NewNullValue(), true, nil
			}
		}
		if args[1].Type != ColInteger && args[1].Type != ColReal {
			return NewNullValue(), true, fmt.Errorf("SUBSTRING position must be numeric")
		}
		count := int64(utf8.RuneCountInString(args[0].String()))
		if args[0].Type == ColBlob {
			count = int64(len(args[0].Blob))
		}
		start := integerValue(args[1])
		length := count
		if len(args) == 3 {
			if args[2].Type != ColInteger && args[2].Type != ColReal {
				return NewNullValue(), true, fmt.Errorf("SUBSTRING length must be numeric")
			}
			length = integerValue(args[2])
		}
		if start == 0 || length <= 0 || start > count || start < -count {
			if args[0].Type == ColBlob {
				return NewBlobValue([]byte{}), true, nil
			}
			return NewTextValue(""), true, nil
		}
		if start > 0 {
			start--
		} else {
			start += count
		}
		end := count
		if length < count-start {
			end = start + length
		}
		if args[0].Type == ColBlob {
			return NewBlobValue(args[0].Blob[int(start):int(end)]), true, nil
		}
		return NewTextValue(string([]rune(args[0].String())[int(start):int(end)])), true, nil
	case "SYSDATE", "NOW", "CURRENT_TIMESTAMP":
		precision := int64(0)
		if len(args) != 0 {
			if args[0].Null {
				return NewNullValue(), true, nil
			}
			if args[0].Type != ColInteger {
				return NewNullValue(), true, fmt.Errorf("%s precision must be an integer", name)
			}
			precision = args[0].Int
		}
		if precision < 0 || precision > 6 {
			return NewNullValue(), true, fmt.Errorf("%s precision must be between 0 and 6", name)
		}
		now := time.Now().UTC()
		if name != "SYSDATE" && !s.statementTime.IsZero() {
			now = s.statementTime
		}
		text := now.Format("2006-01-02 15:04:05")
		if precision > 0 {
			text += "." + fmt.Sprintf("%06d", now.Nanosecond()/1000)[:precision]
		}
		return NewTextValue(text), true, nil
	case "UNIX_TIMESTAMP":
		if len(args) == 0 {
			return NewIntValue(time.Now().Unix()), true, nil
		}
		if args[0].Null {
			return NewNullValue(), true, nil
		}
		var parsed time.Time
		var err error
		for _, layout := range []string{"2006-01-02 15:04:05.999999999", "2006-01-02", "20060102150405", time.RFC3339Nano} {
			parsed, err = time.ParseInLocation(layout, mysqlStringValue(args[0]), time.UTC)
			if err == nil {
				break
			}
		}
		if err != nil {
			return NewIntValue(0), true, nil
		}
		if parsed.Nanosecond() != 0 {
			return NewRealValue(float64(parsed.Unix()) + float64(parsed.Nanosecond())/1e9), true, nil
		}
		return NewIntValue(parsed.Unix()), true, nil
	case "__MYSQL_CONVERT":
		if args[0].Null {
			return NewNullValue(), true, nil
		}
		charset := strings.ToLower(args[1].String())
		if charset == "binary" {
			value, err := castSQLValue(args[0], "BLOB")
			return value, true, err
		}
		text := args[0].String()
		if args[0].Type == ColBlob {
			text = string(args[0].Blob)
		}
		if !utf8.ValidString(text) {
			return NewNullValue(), true, fmt.Errorf("invalid UTF-8 in CONVERT")
		}
		if charset == "utf8" || charset == "utf8mb3" {
			for _, r := range text {
				if r > 0xffff {
					return NewNullValue(), true, fmt.Errorf("character outside utf8mb3 range")
				}
			}
		}
		if charset != "utf8" && charset != "utf8mb3" && charset != "utf8mb4" {
			return NewNullValue(), true, fmt.Errorf("unsupported character set: %s", charset)
		}
		return NewTextValue(text), true, nil
	default:
		return NewNullValue(), true, fmt.Errorf("%s requires a MySQL session", name)
	}
}

func mysqlCastTarget(target string) string {
	if strings.HasPrefix(strings.ToUpper(target), "BINARY(") {
		return "BLOB"
	}
	switch strings.ToUpper(target) {
	case "BINARY":
		return "BLOB"
	case "SIGNED", "SIGNED INTEGER":
		return "INTEGER"
	case "CHAR", "CHARACTER":
		return "TEXT"
	}
	return target
}

func validMySQLFunction(name string, n int) (bool, bool) {
	switch name {
	case "IF":
		return n == 3, true
	case "GREATEST", "LEAST":
		return n >= 2, true
	case "CONCAT":
		return n >= 1, true
	case "SUBSTRING":
		return n == 2 || n == 3, true
	case "SYSDATE", "NOW", "CURRENT_TIMESTAMP", "UNIX_TIMESTAMP":
		return n <= 1, true
	case "GET_LOCK", "__MYSQL_CONVERT":
		return n == 2, true
	case "RELEASE_LOCK", "IS_FREE_LOCK":
		return n == 1, true
	case "CURRENT_USER", "DATABASE", "VERSION", "CONNECTION_ID":
		return n == 0, true
	}
	return false, false
}

// Bound conversion for MySQL CAST(... AS BINARY(n)); zero padding is part of
// the conversion and does not expose scratch or retain aliases to its input.
func mysqlBinaryCast(value Value, target string) (Value, bool, error) {
	if !strings.HasPrefix(target, "BINARY(") || !strings.HasSuffix(target, ")") {
		return Value{}, false, nil
	}
	n, err := strconv.ParseInt(target[7:len(target)-1], 10, 64)
	if err != nil || n < 0 || n > 64<<20 {
		return NewNullValue(), true, fmt.Errorf("invalid BINARY cast length")
	}
	v, err := castSQLValue(value, "BLOB")
	if err != nil || v.Null {
		return v, true, err
	}
	v.Blob = append([]byte(nil), v.Blob...)
	if int64(len(v.Blob)) > n {
		v.Blob = v.Blob[:int(n)]
	}
	if int64(len(v.Blob)) < n {
		v.Blob = append(v.Blob, make([]byte, int(n)-len(v.Blob))...)
	}
	return v, true, nil
}
