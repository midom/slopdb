package mysqlserver

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"slopdb/sql"
)

// MySQL decimal2bin packs decimal digits in groups of nine, with the sign in
// the high bit of the first byte. It is distinct from decimal protocol text.
func binlogDecimal(text string, precision, scale int) ([]byte, error) {
	if precision < 1 || precision > 65 || scale < 0 || scale > precision {
		return nil, fmt.Errorf("invalid binlog DECIMAL(%d,%d)", precision, scale)
	}
	negative := strings.HasPrefix(text, "-")
	text = strings.TrimPrefix(text, "-")
	integer, fraction, _ := strings.Cut(text, ".")
	if len(integer) > precision-scale || len(fraction) > scale {
		return nil, fmt.Errorf("decimal row exceeds table metadata")
	}
	integer = strings.Repeat("0", precision-scale-len(integer)) + integer
	fraction += strings.Repeat("0", scale-len(fraction))
	bytesFor := []int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}
	var out []byte
	group := func(digits string) error {
		if len(digits) == 0 {
			return nil
		}
		n, err := strconv.ParseUint(digits, 10, 32)
		if err != nil {
			return err
		}
		width := bytesFor[len(digits)]
		for i := width - 1; i >= 0; i-- {
			out = append(out, byte(n>>uint(i*8)))
		}
		return nil
	}
	leading := len(integer) % 9
	if err := group(integer[:leading]); err != nil {
		return nil, err
	}
	for i := leading; i < len(integer); i += 9 {
		if err := group(integer[i : i+9]); err != nil {
			return nil, err
		}
	}
	for len(fraction) >= 9 {
		if err := group(fraction[:9]); err != nil {
			return nil, err
		}
		fraction = fraction[9:]
	}
	if err := group(fraction); err != nil {
		return nil, err
	}
	if negative {
		for i := range out {
			out[i] ^= 0xff
		}
	}
	if len(out) > 0 {
		out[0] ^= 0x80
	}
	return out, nil
}

// Binary JSON follows mysql/sql-common/json_binary.h. Large containers make
// offsets independent of value size; integers and opaque DECIMAL retain exact
// input instead of passing through float64.
func binlogJSON(text string) ([]byte, error) {
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return jsonBinaryValue(value, 0)
}
func jsonBinaryValue(value any, depth int) ([]byte, error) {
	if depth > 100 {
		return nil, fmt.Errorf("JSON nesting exceeds binlog limit")
	}
	switch v := value.(type) {
	case nil:
		return []byte{4, 0}, nil
	case bool:
		if v {
			return []byte{4, 1}, nil
		}
		return []byte{4, 2}, nil
	case string:
		p := []byte{12}
		p = binary.AppendUvarint(p, uint64(len(v)))
		return append(p, v...), nil
	case json.Number:
		text := v.String()
		if n, err := strconv.ParseInt(text, 10, 64); err == nil {
			return binary.LittleEndian.AppendUint64([]byte{9}, uint64(n)), nil
		}
		if n, err := strconv.ParseUint(text, 10, 64); err == nil {
			return binary.LittleEndian.AppendUint64([]byte{10}, n), nil
		}
		decimal, err := sql.NewDecimalValue(text)
		if err != nil {
			return nil, err
		}
		raw := strings.TrimPrefix(decimal.Text, "-")
		integer, fraction, _ := strings.Cut(raw, ".")
		precision := len(integer) + len(fraction)
		scale := len(fraction)
		if precision <= 65 && scale <= 30 {
			packed, err := binlogDecimal(decimal.Text, precision, scale)
			if err != nil {
				return nil, err
			}
			payload := append([]byte{byte(precision), byte(scale)}, packed...)
			p := binary.AppendUvarint([]byte{15, typeNewDecimal}, uint64(len(payload)))
			return append(p, payload...), nil
		}
		return nil, fmt.Errorf("exact JSON number outside MySQL binary representation")
	case []any:
		return jsonBinaryContainer(nil, v, depth+1)
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		// MySQL object lookup orders keys first by byte length, then lexically.
		sort.Slice(keys, func(i, j int) bool {
			if len(keys[i]) != len(keys[j]) {
				return len(keys[i]) < len(keys[j])
			}
			return keys[i] < keys[j]
		})
		values := make([]any, len(keys))
		for i, key := range keys {
			values[i] = v[key]
		}
		return jsonBinaryContainer(keys, values, depth+1)
	default:
		return nil, fmt.Errorf("unsupported JSON value %T", value)
	}
}
func jsonBinaryContainer(keys []string, values []any, depth int) ([]byte, error) {
	object := keys != nil
	header := 8 + 5*len(values)
	if object {
		header += 6 * len(keys)
	}
	body := make([]byte, header)
	binary.LittleEndian.PutUint32(body, uint32(len(values)))
	valueOffset := 8
	if object {
		valueOffset += 6 * len(keys)
	}
	for i, key := range keys {
		if len(key) > 65535 {
			return nil, fmt.Errorf("JSON object key exceeds MySQL binary limit")
		}
		entry := 8 + 6*i
		binary.LittleEndian.PutUint32(body[entry:], uint32(len(body)))
		binary.LittleEndian.PutUint16(body[entry+4:], uint16(len(key)))
		body = append(body, key...)
	}
	for i, value := range values {
		encoded, err := jsonBinaryValue(value, depth)
		if err != nil {
			return nil, err
		}
		entry := valueOffset + 5*i
		body[entry] = encoded[0]
		if encoded[0] == 4 {
			body[entry+1] = encoded[1]
		} else {
			binary.LittleEndian.PutUint32(body[entry+1:], uint32(len(body)))
			body = append(body, encoded[1:]...)
		}
	}
	if uint64(len(body)) > math.MaxUint32 {
		return nil, fmt.Errorf("JSON value too large")
	}
	binary.LittleEndian.PutUint32(body[4:], uint32(len(body)))
	kind := byte(3)
	if object {
		kind = 1
	}
	return append([]byte{kind}, body...), nil
}
