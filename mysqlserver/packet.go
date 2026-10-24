package mysqlserver

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

const maxFrame = 1<<24 - 1

var errMalformed = errors.New("malformed MySQL packet")

// Each logical packet may span multiple 16 MiB frames, including a final
// zero-length frame when its length is an exact multiple of maxFrame.
func readPacket(r io.Reader, sequence byte, limit int) ([]byte, byte, error) {
	var data []byte
	for {
		var header [4]byte
		if _, err := io.ReadFull(r, header[:]); err != nil {
			return nil, sequence, err
		}
		size := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
		if header[3] != sequence {
			return nil, sequence, fmt.Errorf("invalid packet sequence: got %d, want %d", header[3], sequence)
		}
		sequence++
		if size > limit-len(data) {
			return nil, sequence, errors.New("MySQL packet exceeds configured limit")
		}
		start := len(data)
		data = append(data, make([]byte, size)...)
		if _, err := io.ReadFull(r, data[start:]); err != nil {
			return nil, sequence, err
		}
		if size < maxFrame {
			return data, sequence, nil
		}
	}
}

type packetWriter struct {
	conn     net.Conn
	sequence byte
}

func (w *packetWriter) write(data []byte) error {
	for {
		size := len(data)
		if size > maxFrame {
			size = maxFrame
		}
		header := []byte{byte(size), byte(size >> 8), byte(size >> 16), w.sequence}
		w.sequence++
		if err := writeAll(w.conn, header); err != nil {
			return err
		}
		if err := writeAll(w.conn, data[:size]); err != nil {
			return err
		}
		data = data[size:]
		if size < maxFrame {
			return nil
		}
	}
}
func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}
func append16(b []byte, n uint16) []byte { return binary.LittleEndian.AppendUint16(b, n) }
func append32(b []byte, n uint32) []byte { return binary.LittleEndian.AppendUint32(b, n) }
func append64(b []byte, n uint64) []byte { return binary.LittleEndian.AppendUint64(b, n) }
func appendLen(b []byte, n uint64) []byte {
	switch {
	case n < 251:
		return append(b, byte(n))
	case n < 1<<16:
		return append16(append(b, 0xfc), uint16(n))
	case n < 1<<24:
		return append(b, 0xfd, byte(n), byte(n>>8), byte(n>>16))
	default:
		return append64(append(b, 0xfe), n)
	}
}
func appendString(b []byte, s []byte) []byte { return append(appendLen(b, uint64(len(s))), s...) }

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
	return binary.LittleEndian.Uint16(b)
}
func (d *decoder) u32() uint32 {
	b := d.take(4)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint32(b)
}
func (d *decoder) u64() uint64 {
	b := d.take(8)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}
func (d *decoder) nul() string {
	if d.err != nil {
		return ""
	}
	for i, c := range d.data {
		if c == 0 {
			b := d.take(i + 1)
			return string(b[:i])
		}
	}
	d.err = errMalformed
	return ""
}
func (d *decoder) length() uint64 {
	switch n := d.u8(); n {
	case 0xfc:
		return uint64(d.u16())
	case 0xfd:
		b := d.take(3)
		if b == nil {
			return 0
		}
		return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16
	case 0xfe:
		return d.u64()
	case 0xfb, 0xff:
		d.err = errMalformed
		return 0
	default:
		return uint64(n)
	}
}
func (d *decoder) str() []byte {
	n := d.length()
	if n > uint64(len(d.data)) {
		d.err = errMalformed
		return nil
	}
	return d.take(int(n))
}
