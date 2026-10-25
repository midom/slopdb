package sql

import (
	"bytes"
	"compress/zlib"
	"crypto/aes"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"net/netip"
	"strconv"
	"strings"
	"time"
)

func mysqlUtilityFunctions() map[string]FunctionSpec {
	out := map[string]FunctionSpec{}
	add := func(names string, min, max int, typ ColumnType, volatile bool, eval func(*Schema, []Value) (Value, error)) {
		for _, name := range strings.Fields(names) {
			out[name] = FunctionSpec{Min: min, Max: max, Return: typ, Volatile: volatile, Eval: eval}
		}
	}
	add("MD5 SHA SHA1 SHA2", 1, 1, ColText, false, func(s *Schema, a []Value) (Value, error) { return NewNullValue(), nil })
	for _, name := range []string{"MD5", "SHA", "SHA1", "SHA2"} {
		name := name
		spec := out[name]
		if name == "SHA2" {
			spec.Min, spec.Max = 2, 2
		}
		spec.Eval = func(s *Schema, a []Value) (Value, error) {
			if functionAnyNull(a) {
				return NewNullValue(), nil
			}
			data := []byte(mysqlStringValue(a[0]))
			var digest []byte
			switch name {
			case "MD5":
				v := md5.Sum(data)
				digest = v[:]
			case "SHA", "SHA1":
				v := sha1.Sum(data)
				digest = v[:]
			case "SHA2":
				switch mysqlUtilityUint(a[1]) {
				case 0, 256:
					v := sha256.Sum256(data)
					digest = v[:]
				case 224:
					v := sha256.Sum224(data)
					digest = v[:]
				case 384:
					v := sha512.Sum384(data)
					digest = v[:]
				case 512:
					v := sha512.Sum512(data)
					digest = v[:]
				default:
					return NewNullValue(), nil
				}
			}
			return NewTextValue(hex.EncodeToString(digest)), nil
		}
		out[name] = spec
	}
	add("AES_ENCRYPT AES_DECRYPT", 2, 3, ColBlob, false, func(s *Schema, a []Value) (Value, error) { return NewNullValue(), nil })
	for _, name := range []string{"AES_ENCRYPT", "AES_DECRYPT"} {
		encrypt := name == "AES_ENCRYPT"
		spec := out[name]
		spec.Eval = func(s *Schema, a []Value) (Value, error) {
			if a[0].Null || a[1].Null {
				return NewNullValue(), nil
			}
			data := []byte(mysqlStringValue(a[0]))
			var key [16]byte
			for i, b := range []byte(mysqlStringValue(a[1])) {
				key[i%16] ^= b
			}
			cipher, err := aes.NewCipher(key[:])
			if err != nil {
				return NewNullValue(), err
			}
			if encrypt {
				pad := aes.BlockSize - len(data)%aes.BlockSize
				if err := functionResultSize(s, uint64(len(data))+uint64(pad)); err != nil {
					return NewNullValue(), err
				}
				result := make([]byte, len(data)+pad)
				copy(result, data)
				for i := len(data); i < len(result); i++ {
					result[i] = byte(pad)
				}
				for i := 0; i < len(result); i += aes.BlockSize {
					cipher.Encrypt(result[i:i+16], result[i:i+16])
				}
				return NewBlobValue(result), nil
			}
			if len(data) == 0 || len(data)%16 != 0 {
				return NewNullValue(), nil
			}
			if err := functionResultSize(s, uint64(len(data))); err != nil {
				return NewNullValue(), err
			}
			result := make([]byte, len(data))
			for i := 0; i < len(data); i += 16 {
				cipher.Decrypt(result[i:i+16], data[i:i+16])
			}
			pad := int(result[len(result)-1])
			if pad < 1 || pad > 16 || pad > len(result) {
				return NewNullValue(), nil
			}
			for _, b := range result[len(result)-pad:] {
				if int(b) != pad {
					return NewNullValue(), nil
				}
			}
			return NewBlobValue(result[:len(result)-pad]), nil
		}
		out[name] = spec
	}
	add("RANDOM_BYTES", 1, 1, ColBlob, true, func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		n := mysqlUtilityUint(a[0])
		if n < 1 || n > 1024 {
			return NewNullValue(), fmt.Errorf("RANDOM_BYTES length must be 1..1024")
		}
		data := make([]byte, int(n))
		_, err := rand.Read(data)
		return NewBlobValue(data), err
	})
	add("UUID", 0, 0, ColText, true, func(s *Schema, a []Value) (Value, error) {
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			return NewNullValue(), err
		}
		// RFC 4122 version 1 layout, with a random multicast node and clock sequence.
		ticks := uint64(time.Now().UnixNano()/100) + 122192928000000000
		binary.BigEndian.PutUint32(id[:4], uint32(ticks))
		binary.BigEndian.PutUint16(id[4:6], uint16(ticks>>32))
		binary.BigEndian.PutUint16(id[6:8], uint16(ticks>>48)&0xfff|0x1000)
		id[8] = id[8]&0x3f | 0x80
		id[10] |= 1
		return NewTextValue(mysqlUUIDText(id[:])), nil
	})
	add("IS_UUID", 1, 1, ColInteger, false, func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		_, ok := mysqlUUIDBytes(mysqlStringValue(a[0]))
		if ok {
			return NewIntValue(1), nil
		}
		return NewIntValue(0), nil
	})
	add("UUID_TO_BIN", 1, 2, ColBlob, false, func(s *Schema, a []Value) (Value, error) {
		if functionAnyNull(a) {
			return NewNullValue(), nil
		}
		id, ok := mysqlUUIDBytes(mysqlStringValue(a[0]))
		if !ok {
			return NewNullValue(), fmt.Errorf("invalid UUID string")
		}
		if len(a) == 2 && mysqlUtilityUint(a[1]) != 0 {
			id = mysqlUUIDSwap(id, false)
		}
		return NewBlobValue(id), nil
	})
	add("BIN_TO_UUID", 1, 2, ColText, false, func(s *Schema, a []Value) (Value, error) {
		if functionAnyNull(a) {
			return NewNullValue(), nil
		}
		id := []byte(mysqlStringValue(a[0]))
		if len(id) != 16 {
			return NewNullValue(), fmt.Errorf("BIN_TO_UUID requires 16 bytes")
		}
		if len(a) == 2 && mysqlUtilityUint(a[1]) != 0 {
			id = mysqlUUIDSwap(id, true)
		}
		return NewTextValue(mysqlUUIDText(id)), nil
	})
	add("INET_ATON", 1, 1, ColUnsigned, false, func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		parts := strings.Split(mysqlStringValue(a[0]), ".")
		if len(parts) > 4 {
			return NewNullValue(), nil
		}
		var ip uint64
		for i, part := range parts {
			if part == "" {
				return NewNullValue(), nil
			}
			for _, b := range part {
				if b < '0' || b > '9' {
					return NewNullValue(), nil
				}
			}
			n, err := strconv.ParseUint(part, 10, 32)
			bits := 8
			if i == len(parts)-1 {
				bits = 8 * (5 - len(parts))
			}
			if err != nil || n > uint64(1)<<bits-1 {
				return NewNullValue(), nil
			}
			if i == len(parts)-1 {
				ip = ip<<bits | n
			} else {
				ip = ip<<8 | n
			}
		}
		return NewUnsignedValue(ip), nil
	})
	add("INET_NTOA", 1, 1, ColText, false, func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		v := mysqlFunctionNumber(a[0])
		n, scale, err := valueDecimal(v)
		if err != nil {
			return NewNullValue(), nil
		}
		n = roundDecimal(n, scale, 0)
		if n.Sign() < 0 || !n.IsUint64() || n.Uint64() > math.MaxUint32 {
			return NewNullValue(), nil
		}
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(n.Uint64()))
		return NewTextValue(netip.AddrFrom4(b).String()), nil
	})
	add("INET6_ATON", 1, 1, ColBlob, false, func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		ip, err := netip.ParseAddr(mysqlStringValue(a[0]))
		if err != nil || ip.Zone() != "" {
			return NewNullValue(), nil
		}
		return NewBlobValue(append([]byte(nil), ip.AsSlice()...)), nil
	})
	add("INET6_NTOA", 1, 1, ColText, false, func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		b := []byte(mysqlStringValue(a[0]))
		ip, ok := netip.AddrFromSlice(b)
		if !ok {
			return NewNullValue(), nil
		}
		if len(b) == 16 && bytes.Equal(b[:12], make([]byte, 12)) && binary.BigEndian.Uint32(b[12:]) > 1 {
			var v [4]byte
			copy(v[:], b[12:])
			return NewTextValue("::" + netip.AddrFrom4(v).String()), nil
		}
		return NewTextValue(ip.String()), nil
	})
	add("IS_IPV4 IS_IPV6", 1, 1, ColInteger, false, func(s *Schema, a []Value) (Value, error) { return NewIntValue(0), nil })
	for _, name := range []string{"IS_IPV4", "IS_IPV6"} {
		is4 := name == "IS_IPV4"
		spec := out[name]
		spec.Eval = func(s *Schema, a []Value) (Value, error) {
			if a[0].Null {
				return NewIntValue(0), nil
			}
			ip, err := netip.ParseAddr(mysqlStringValue(a[0]))
			if err == nil && ip.Zone() == "" && ip.Is4() == is4 {
				return NewIntValue(1), nil
			}
			return NewIntValue(0), nil
		}
		out[name] = spec
	}
	add("IS_IPV4_COMPAT IS_IPV4_MAPPED", 1, 1, ColInteger, false, func(s *Schema, a []Value) (Value, error) { return NewIntValue(0), nil })
	for _, name := range []string{"IS_IPV4_COMPAT", "IS_IPV4_MAPPED"} {
		mapped := name == "IS_IPV4_MAPPED"
		spec := out[name]
		spec.Eval = func(s *Schema, a []Value) (Value, error) {
			if a[0].Null {
				return NewIntValue(0), nil
			}
			b := []byte(mysqlStringValue(a[0]))
			if len(b) != 16 {
				return NewIntValue(0), nil
			}
			if mapped {
				if bytes.Equal(b[:10], make([]byte, 10)) && b[10] == 255 && b[11] == 255 {
					return NewIntValue(1), nil
				}
			} else if bytes.Equal(b[:12], make([]byte, 12)) && binary.BigEndian.Uint32(b[12:]) > 1 {
				return NewIntValue(1), nil
			}
			return NewIntValue(0), nil
		}
		out[name] = spec
	}
	add("COMPRESS", 1, 1, ColBlob, false, mysqlCompress)
	add("UNCOMPRESS", 1, 1, ColBlob, false, mysqlUncompress)
	add("UNCOMPRESSED_LENGTH", 1, 1, ColInteger, false, func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		data := []byte(mysqlStringValue(a[0]))
		if len(data) <= 4 {
			return NewIntValue(0), nil
		}
		return NewIntValue(int64(binary.LittleEndian.Uint32(data) & 0x3fffffff)), nil
	})
	add("ISNULL", 1, 1, ColInteger, false, func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewIntValue(1), nil
		}
		return NewIntValue(0), nil
	})
	add("CHARSET COLLATION", 1, 1, ColText, false, func(s *Schema, a []Value) (Value, error) { return NewNullValue(), nil })
	for _, name := range []string{"CHARSET", "COLLATION"} {
		collation := name == "COLLATION"
		spec := out[name]
		spec.Eval = func(s *Schema, a []Value) (Value, error) {
			v := "binary"
			if !a[0].Null && (a[0].Type == ColText || a[0].Type == ColJSON) {
				v = "utf8mb4"
				if collation {
					v = "utf8mb4_bin"
					if a[0].Collation != "" {
						v = strings.ToLower(string(a[0].Collation))
					}
				}
			}
			return NewTextValue(v), nil
		}
		out[name] = spec
	}
	add("USER SESSION_USER SYSTEM_USER SCHEMA", 0, 0, ColText, true, func(s *Schema, a []Value) (Value, error) { return NewNullValue(), nil })
	for _, name := range []string{"USER", "SESSION_USER", "SYSTEM_USER", "SCHEMA"} {
		function := "USER"
		if name == "SCHEMA" {
			function = "DATABASE"
		}
		spec := out[name]
		spec.Eval = func(s *Schema, a []Value) (Value, error) {
			if value, ok, err := MySQLFunction(s.ctx, function, a); ok || err != nil {
				return value, err
			}
			if function == "DATABASE" {
				return NewTextValue(s.CurrentSchema()), nil
			}
			return NewNullValue(), fmt.Errorf("%s requires a MySQL connection", function)
		}
		out[name] = spec
	}
	add("SLEEP", 1, 1, ColInteger, true, func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), fmt.Errorf("invalid SLEEP duration")
		}
		seconds := toFloat(mysqlFunctionNumber(a[0]))
		if math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds < 0 || seconds > float64(math.MaxInt64)/1e9 {
			return NewNullValue(), fmt.Errorf("invalid SLEEP duration")
		}
		timer := time.NewTimer(time.Duration(seconds * 1e9))
		defer timer.Stop()
		if s.ctx == nil {
			<-timer.C
			return NewIntValue(0), nil
		}
		select {
		case <-timer.C:
			return NewIntValue(0), nil
		case <-s.ctx.Done():
			return NewNullValue(), s.ctx.Err()
		}
	})
	return out
}

func mysqlUtilityUint(v Value) uint64 {
	if v.HexLiteral {
		return mysqlHexNumber(v).Uint
	}
	if v.Type == ColUnsigned {
		return v.Uint
	}
	if isNumeric(v) {
		return uint64(mysqlFunctionInt(v))
	}
	text := strings.TrimSpace(mysqlStringValue(v))
	i := 0
	negative := false
	if i < len(text) && (text[i] == '+' || text[i] == '-') {
		negative = text[i] == '-'
		i++
	}
	start := i
	for i < len(text) && text[i] >= '0' && text[i] <= '9' {
		i++
	}
	if i == start {
		return 0
	}
	n, _ := strconv.ParseUint(text[start:i], 10, 64)
	if negative {
		return -n
	}
	return n
}
func mysqlUUIDBytes(text string) ([]byte, bool) {
	if len(text) == 38 && text[0] == '{' && text[37] == '}' {
		text = text[1:37]
	}
	if len(text) == 36 {
		if text[8] != '-' || text[13] != '-' || text[18] != '-' || text[23] != '-' {
			return nil, false
		}
		text = strings.ReplaceAll(text, "-", "")
	}
	if len(text) != 32 {
		return nil, false
	}
	data, err := hex.DecodeString(text)
	return data, err == nil
}
func mysqlUUIDText(b []byte) string {
	text := hex.EncodeToString(b)
	return text[:8] + "-" + text[8:12] + "-" + text[12:16] + "-" + text[16:20] + "-" + text[20:]
}
func mysqlUUIDSwap(b []byte, inverse bool) []byte {
	out := make([]byte, 16)
	if inverse {
		copy(out[:4], b[4:8])
		copy(out[4:6], b[2:4])
		copy(out[6:8], b[:2])
	} else {
		copy(out[:2], b[6:8])
		copy(out[2:4], b[4:6])
		copy(out[4:8], b[:4])
	}
	copy(out[8:], b[8:])
	return out
}
func mysqlCompress(s *Schema, a []Value) (Value, error) {
	if a[0].Null {
		return NewNullValue(), nil
	}
	data := []byte(mysqlStringValue(a[0]))
	if len(data) == 0 {
		return NewBlobValue([]byte{}), nil
	}
	if err := functionResultSize(s, uint64(len(data))+uint64(len(data)/100)+64); err != nil {
		return NewNullValue(), err
	}
	var b bytes.Buffer
	b.Write(binary.LittleEndian.AppendUint32(nil, uint32(len(data))))
	writer := zlib.NewWriter(&b)
	if _, err := writer.Write(data); err != nil {
		return NewNullValue(), err
	}
	if err := writer.Close(); err != nil {
		return NewNullValue(), err
	}
	if b.Bytes()[b.Len()-1] == ' ' {
		b.WriteByte('.')
	}
	return NewBlobValue(b.Bytes()), nil
}
func mysqlUncompress(s *Schema, a []Value) (Value, error) {
	if a[0].Null {
		return NewNullValue(), nil
	}
	data := []byte(mysqlStringValue(a[0]))
	if len(data) == 0 {
		return NewBlobValue([]byte{}), nil
	}
	if len(data) <= 4 {
		return NewNullValue(), nil
	}
	size := uint64(binary.LittleEndian.Uint32(data) & 0x3fffffff)
	reader, err := zlib.NewReader(bytes.NewReader(data[4:]))
	if err != nil {
		return NewNullValue(), nil
	}
	defer reader.Close()
	if err := functionResultSize(s, size); err != nil {
		return NewNullValue(), err
	}
	out, err := io.ReadAll(io.LimitReader(reader, int64(size)+1))
	if err != nil || uint64(len(out)) != size {
		return NewNullValue(), nil
	}
	return NewBlobValue(out), nil
}
