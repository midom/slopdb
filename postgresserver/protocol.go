package postgresserver

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/jackc/pgx/v5/pgproto3"
)

var errMalformed = errors.New("malformed PostgreSQL message")

func readBody(r io.Reader, limit int) ([]byte, error) {
	var size [4]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(size[:])
	if n < 4 || uint64(n-4) > uint64(limit) {
		return nil, errMalformed
	}
	body := make([]byte, int(n-4))
	_, err := io.ReadFull(r, body)
	return body, err
}
func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
func (c *client) send(m pgproto3.BackendMessage) error {
	b, err := m.Encode(nil)
	if err != nil {
		return err
	}
	return writeAll(c.conn, b)
}

type decoder struct {
	data []byte
	err  error
}

func (d *decoder) take(n int) []byte {
	if d.err != nil {
		return nil
	}
	if n < 0 || n > len(d.data) {
		d.err = errMalformed
		return nil
	}
	b := d.data[:n]
	d.data = d.data[n:]
	return b
}
func (d *decoder) u8() byte {
	b := d.take(1)
	if b == nil {
		return 0
	}
	return b[0]
}
func (d *decoder) u16() uint16 {
	b := d.take(2)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint16(b)
}
func (d *decoder) u32() uint32 {
	b := d.take(4)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}
func (d *decoder) nul() string {
	if d.err != nil {
		return ""
	}
	i := bytes.IndexByte(d.data, 0)
	if i < 0 {
		d.err = errMalformed
		return ""
	}
	b := d.take(i + 1)
	return string(b[:i])
}
func (d *decoder) end() error {
	if d.err != nil {
		return d.err
	}
	if len(d.data) != 0 {
		return errMalformed
	}
	return nil
}
func (d *decoder) formats() []int16 {
	n := int(d.u16())
	if n > len(d.data)/2 {
		d.err = errMalformed
		return nil
	}
	out := make([]int16, n)
	for i := range out {
		out[i] = int16(d.u16())
		if out[i] != 0 && out[i] != 1 {
			d.err = errMalformed
		}
	}
	return out
}

// Decode bounded messages ourselves: negative lengths and truncated arrays must
// fail before allocating or slicing. pgproto3 supplies backend message encoding.
func decodeMessage(kind byte, body []byte) (pgproto3.FrontendMessage, error) {
	d := decoder{data: body}
	var message pgproto3.FrontendMessage
	switch kind {
	case 'd':
		message = &pgproto3.CopyData{Data: d.take(len(d.data))}
	case 'c':
		message = &pgproto3.CopyDone{}
	case 'f':
		message = &pgproto3.CopyFail{Message: d.nul()}
	case 'Q':
		message = &pgproto3.Query{String: d.nul()}
	case 'P':
		m := &pgproto3.Parse{Name: d.nul(), Query: d.nul()}
		n := int(d.u16())
		if n > len(d.data)/4 {
			return nil, errMalformed
		}
		m.ParameterOIDs = make([]uint32, n)
		for i := range m.ParameterOIDs {
			m.ParameterOIDs[i] = d.u32()
		}
		message = m
	case 'B':
		m := &pgproto3.Bind{DestinationPortal: d.nul(), PreparedStatement: d.nul()}
		m.ParameterFormatCodes = d.formats()
		n := int(d.u16())
		if n > len(d.data)/4 {
			return nil, errMalformed
		}
		m.Parameters = make([][]byte, n)
		for i := range m.Parameters {
			size := int32(d.u32())
			if size == -1 {
				continue
			}
			if size < 0 {
				return nil, errMalformed
			}
			m.Parameters[i] = d.take(int(size))
		}
		m.ResultFormatCodes = d.formats()
		message = m
	case 'D':
		message = &pgproto3.Describe{ObjectType: d.u8(), Name: d.nul()}
	case 'E':
		message = &pgproto3.Execute{Portal: d.nul(), MaxRows: d.u32()}
	case 'C':
		message = &pgproto3.Close{ObjectType: d.u8(), Name: d.nul()}
	case 'S':
		message = &pgproto3.Sync{}
	case 'H':
		message = &pgproto3.Flush{}
	case 'X':
		message = &pgproto3.Terminate{}
	default:
		return nil, &protocolError{"0A000", "unsupported PostgreSQL message type"}
	}
	return message, d.end()
}
