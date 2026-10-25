package postgresserver

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"unicode/utf8"

	"slopdb/sql"
)

var copySignature = []byte("PGCOPY\n\xff\r\n\x00")

type copyField struct {
	data   []byte
	quoted bool
}
type copyRecords struct {
	reader        *bufio.Reader
	command       *copyCommand
	limit         int
	binaryStarted bool
	ended         bool
	lineEnding    string
}

func (r *copyRecords) next(n int) ([]copyField, error) {
	if r.ended {
		return nil, io.EOF
	}
	if r.command.format == "binary" {
		return r.nextBinary(n)
	}
	var fields []copyField
	var f copyField
	total := 0
	quoted := false
	any := false
	appendByte := func(b byte) error {
		total++
		if total > r.limit {
			return &protocolError{"54000", "COPY record exceeds maximum packet size"}
		}
		f.data = append(f.data, b)
		return nil
	}
	for {
		b, err := r.reader.ReadByte()
		if err == io.EOF {
			if quoted {
				return nil, &protocolError{"22P04", "unterminated CSV quoted field"}
			}
			if !any && len(fields) == 0 && len(f.data) == 0 {
				return nil, io.EOF
			}
			fields = append(fields, f)
			if len(fields) == 1 && !f.quoted && bytes.Equal(f.data, []byte(`\.`)) {
				return nil, &protocolError{"22P04", "end-of-copy marker corrupt"}
			}
			return fields, nil
		}
		if err != nil {
			return nil, err
		}
		any = true
		c := r.command
		if c.format == "csv" {
			if quoted {
				if b == c.escape {
					next, e := r.reader.Peek(1)
					if e == nil && (next[0] == c.escape || next[0] == c.quote) {
						b, _ = r.reader.ReadByte()
						if e = appendByte(b); e != nil {
							return nil, e
						}
						continue
					}
				}
				if b == c.quote {
					quoted = false
					continue
				}
				if err = appendByte(b); err != nil {
					return nil, err
				}
				continue
			}
			if b == c.quote {
				quoted = true
				f.quoted = true
				continue
			}
		} else if b == '\\' {
			if err = appendByte(b); err != nil {
				return nil, err
			}
			next, e := r.reader.ReadByte()
			if e == io.EOF {
				f.data = f.data[:len(f.data)-1]
				fields = append(fields, f)
				return fields, nil
			}
			if e != nil {
				return nil, e
			}
			if err = appendByte(next); err != nil {
				return nil, err
			}
			continue
		}
		if b == c.delimiter {
			if len(fields) >= n {
				return nil, &protocolError{"22P04", "extra data after last COPY column"}
			}
			fields = append(fields, f)
			f = copyField{}
			continue
		}
		if b == '\n' || b == '\r' {
			ending := string(b)
			if b == '\r' {
				next, _ := r.reader.Peek(1)
				if len(next) > 0 && next[0] == '\n' {
					_, _ = r.reader.ReadByte()
					ending = "\r\n"
				}
			}
			if r.lineEnding != "" && r.lineEnding != ending {
				return nil, &protocolError{"22P04", "inconsistent line endings in COPY data"}
			}
			r.lineEnding = ending
			fields = append(fields, f)
			if len(fields) == 1 && !f.quoted && bytes.Equal(f.data, []byte(`\.`)) {
				r.ended = true
				_, err = io.Copy(io.Discard, r.reader)
				if err != nil {
					return nil, err
				}
				return nil, io.EOF
			}
			return fields, nil
		}
		if err = appendByte(b); err != nil {
			return nil, err
		}
	}
}
func (r *copyRecords) nextBinary(n int) ([]copyField, error) {
	if !r.binaryStarted {
		header := make([]byte, 19)
		if err := copyReadFull(r.reader, header); err != nil {
			return nil, err
		}
		if !bytes.Equal(header[:11], copySignature) {
			return nil, &protocolError{"22P04", "invalid COPY binary signature"}
		}
		if binary.BigEndian.Uint32(header[11:15])&0xffff0000 != 0 {
			return nil, &protocolError{"0A000", "unsupported COPY binary flags"}
		}
		size := binary.BigEndian.Uint32(header[15:19])
		if uint64(size) > uint64(r.limit) {
			return nil, &protocolError{"54000", "COPY binary extension too large"}
		}
		if _, err := io.CopyN(io.Discard, r.reader, int64(size)); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
		r.binaryStarted = true
	}
	var size [4]byte
	// PostgreSQL accepts CopyDone at a row boundary without a binary trailer;
	// pgx.CopyFrom deliberately uses that form. Its row-count read also treats
	// a final single byte as end of stream (verified against PostgreSQL16.15).
	if _, err := io.ReadFull(r.reader, size[:2]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			r.ended = true
			return nil, io.EOF
		}
		return nil, err
	}
	count := int(int16(binary.BigEndian.Uint16(size[:2])))
	if count == -1 {
		r.ended = true
		_, err := r.reader.ReadByte()
		if err == io.EOF {
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}
		return nil, &protocolError{"22P04", "data after COPY binary trailer"}
	}
	if count != n {
		return nil, &protocolError{"22P04", fmt.Sprintf("COPY row has %d fields, expected %d", count, n)}
	}
	fields := make([]copyField, n)
	total := 0
	for i := range fields {
		if err := copyReadFull(r.reader, size[:]); err != nil {
			return nil, err
		}
		length := int32(binary.BigEndian.Uint32(size[:]))
		if length == -1 {
			continue
		}
		if length < 0 || int64(length) > int64(r.limit-total) {
			return nil, &protocolError{"54000", "invalid or oversized COPY field"}
		}
		total += int(length)
		fields[i].data = make([]byte, int(length))
		if err := copyReadFull(r.reader, fields[i].data); err != nil {
			return nil, err
		}
	}
	return fields, nil
}
func copyUnescape(data []byte) []byte {
	out := make([]byte, 0, len(data))
	for i := 0; i < len(data); i++ {
		b := data[i]
		if b != '\\' || i+1 == len(data) {
			out = append(out, b)
			continue
		}
		i++
		b = data[i]
		switch b {
		case 'b':
			b = '\b'
		case 'f':
			b = '\f'
		case 'n':
			b = '\n'
		case 'r':
			b = '\r'
		case 't':
			b = '\t'
		case 'v':
			b = '\v'
		case 'x':
			v := 0
			digits := 0
			for i+1 < len(data) && digits < 2 {
				c := data[i+1]
				d := byte(255)
				if c >= '0' && c <= '9' {
					d = c - '0'
				}
				if c >= 'a' && c <= 'f' {
					d = c - 'a' + 10
				}
				if c >= 'A' && c <= 'F' {
					d = c - 'A' + 10
				}
				if d == 255 {
					break
				}
				i++
				v = v*16 + int(d)
				digits++
			}
			if digits > 0 {
				b = byte(v)
			}
		default:
			if b >= '0' && b <= '7' {
				v := int(b - '0')
				for digits := 1; digits < 3 && i+1 < len(data) && data[i+1] >= '0' && data[i+1] <= '7'; digits++ {
					i++
					v = v*8 + int(data[i]-'0')
				}
				b = byte(v)
			}
		}
		out = append(out, b)
	}
	return out
}
func (c *copyCommand) decode(f copyField, field field) (any, error) {
	data := f.data
	if c.format == "binary" {
		if data != nil && (field.oid == oidText || field.oid == oidVarchar || field.oid == oidJSON || field.oid == oidJSONB) {
			text := data
			if field.oid == oidJSONB && len(text) > 0 {
				text = text[1:]
			}
			if !utf8.Valid(text) || bytes.IndexByte(text, 0) >= 0 {
				return nil, &protocolError{"22021", "invalid UTF8 or zero byte in COPY text"}
			}
		}
		return decodeValue(data, field.oid, true)
	}
	name := field.name
	null := !f.quoted && string(data) == c.null
	if c.format == "csv" {
		if c.forceNotNull[name] {
			null = false
		}
		if c.forceNull[name] && f.quoted && string(data) == c.null {
			null = true
		}
	}
	if null {
		return sql.NewNullValue(), nil
	}
	if c.format == "text" {
		data = copyUnescape(data)
	}
	if !utf8.Valid(data) || bytes.IndexByte(data, 0) >= 0 {
		return sql.Value{}, &protocolError{"22021", "invalid UTF8 or zero byte in COPY text"}
	}
	if data == nil {
		data = []byte{}
	}
	return decodeValue(data, field.oid, false)
}
func (c *copyCommand) encode(values []sql.Value, fields []field) ([]byte, error) {
	var out []byte
	if c.format == "binary" {
		out = binary.BigEndian.AppendUint16(out, uint16(len(values)))
	}
	for i, v := range values {
		if c.format != "binary" && i > 0 {
			out = append(out, c.delimiter)
		}
		data, err := wireValue(v, fields[i].oid, c.format == "binary")
		if err != nil {
			return nil, err
		}
		if c.format == "binary" {
			if data == nil {
				out = binary.BigEndian.AppendUint32(out, ^uint32(0))
			} else {
				out = binary.BigEndian.AppendUint32(out, uint32(len(data)))
				out = append(out, data...)
			}
			continue
		}
		if data == nil {
			out = append(out, c.null...)
			continue
		}
		if c.format == "csv" {
			quoted := string(data) == c.null || bytes.IndexAny(data, "\r\n") >= 0 || bytes.IndexByte(data, c.delimiter) >= 0 || bytes.IndexByte(data, c.quote) >= 0 || c.forceQuote[fields[i].name] || c.forceQuote["*"] || bytes.Equal(data, []byte(`\.`))
			if quoted {
				out = append(out, c.quote)
			}
			for _, b := range data {
				if quoted && (b == c.quote || b == c.escape) {
					out = append(out, c.escape)
				}
				out = append(out, b)
			}
			if quoted {
				out = append(out, c.quote)
			}
		} else {
			for _, b := range data {
				switch b {
				case '\\':
					out = append(out, '\\', '\\')
				case '\b':
					out = append(out, '\\', 'b')
				case '\f':
					out = append(out, '\\', 'f')
				case '\n':
					out = append(out, '\\', 'n')
				case '\r':
					out = append(out, '\\', 'r')
				case '\t':
					out = append(out, '\\', 't')
				case '\v':
					out = append(out, '\\', 'v')
				default:
					if b == c.delimiter {
						out = append(out, '\\')
					}
					out = append(out, b)
				}
			}
		}
	}
	if c.format != "binary" {
		out = append(out, '\n')
	}
	return out, nil
}

func copyReadFull(reader io.Reader, out []byte) error {
	_, err := io.ReadFull(reader, out)
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}
