package postgresserver

import (
	"encoding/binary"
	"fmt"
	"slopdb/sql"
	"strconv"
	"strings"
)

// PostgreSQL numeric wire values are base-10000 digits with signed weight,
// sign, and display scale, independent of the in-memory SQL decimal encoding.
// https://doxygen.postgresql.org/backend_2utils_2adt_2numeric_8c.html (numeric_send/recv)
func encodeNumeric(text string) ([]byte, error) {
	value, err := sql.NewDecimalValue(text)
	if err != nil {
		return nil, err
	}
	text = value.Text
	sign := uint16(0)
	if strings.HasPrefix(text, "-") {
		sign = 0x4000
		text = text[1:]
	}
	integer, fraction, _ := strings.Cut(text, ".")
	scale := len(fraction)
	integer = strings.TrimLeft(integer, "0")
	groupsBefore := (len(integer) + 3) / 4
	padded := strings.Repeat("0", groupsBefore*4-len(integer)) + integer + fraction + strings.Repeat("0", (4-len(fraction)%4)%4)
	var digits []uint16
	for i := 0; i < len(padded); i += 4 {
		n, err := strconv.ParseUint(padded[i:i+4], 10, 16)
		if err != nil {
			return nil, err
		}
		digits = append(digits, uint16(n))
	}
	weight := groupsBefore - 1
	for len(digits) > 0 && digits[0] == 0 {
		digits = digits[1:]
		weight--
	}
	for len(digits) > 0 && digits[len(digits)-1] == 0 {
		digits = digits[:len(digits)-1]
	}
	if len(digits) == 0 {
		weight = 0
		sign = 0
	}
	if len(digits) > 32767 || weight > 32767 || weight < -32768 || scale > 16383 {
		return nil, fmt.Errorf("numeric wire value out of range")
	}
	out := binary.BigEndian.AppendUint16(nil, uint16(len(digits)))
	out = binary.BigEndian.AppendUint16(out, uint16(int16(weight)))
	out = binary.BigEndian.AppendUint16(out, sign)
	out = binary.BigEndian.AppendUint16(out, uint16(scale))
	for _, n := range digits {
		out = binary.BigEndian.AppendUint16(out, n)
	}
	return out, nil
}
func decodeNumeric(data []byte) (sql.Value, error) {
	invalid := func() (sql.Value, error) { return sql.Value{}, &protocolError{"22P03", "invalid binary numeric value"} }
	if len(data) < 8 {
		return invalid()
	}
	count := int(int16(binary.BigEndian.Uint16(data)))
	weight := int(int16(binary.BigEndian.Uint16(data[2:])))
	sign := binary.BigEndian.Uint16(data[4:])
	scale := int(binary.BigEndian.Uint16(data[6:]))
	if count < 0 || len(data) != 8+count*2 || count > 1025 || scale > 4096 || (sign != 0 && sign != 0x4000) {
		return invalid()
	}
	var digits strings.Builder
	for i := 0; i < count; i++ {
		n := binary.BigEndian.Uint16(data[8+i*2:])
		if n >= 10000 {
			return invalid()
		}
		fmt.Fprintf(&digits, "%04d", n)
	}
	text := digits.String()
	point := (weight + 1) * 4
	if count == 0 {
		text = "0"
		point = 1
	}
	if point > 4096 || point < -4096 {
		return invalid()
	}
	var integer, fraction string
	switch {
	case point <= 0:
		integer = "0"
		fraction = strings.Repeat("0", -point) + text
	case point >= len(text):
		integer = text + strings.Repeat("0", point-len(text))
	default:
		integer = text[:point]
		fraction = text[point:]
	}
	if len(fraction) > scale {
		if strings.Trim(fraction[scale:], "0") != "" {
			return invalid()
		}
		fraction = fraction[:scale]
	}
	if len(fraction) < scale {
		fraction += strings.Repeat("0", scale-len(fraction))
	}
	text = integer
	if scale > 0 {
		text += "." + fraction
	}
	if sign == 0x4000 {
		text = "-" + text
	}
	return sql.NewDecimalValue(text)
}
