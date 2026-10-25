package mysqlserver

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"net"
)

// compressedConn implements the MySQL compression layer after authentication.
// Read and Write state are independent: the command reader can detect a peer
// disconnect while execution waits, without racing the response sequence.
// Specification: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_compression_packet.html
// Both the outer and the ordinary packet sequence restart for each command;
// responses continue the incoming outer sequence independently of inner frames.
type compressedConn struct {
	net.Conn
	readSequence, writeSequence byte
	incoming                    []byte
	compressor                  *zlib.Writer
	encoded                     bytes.Buffer
}

func newCompressedConn(conn net.Conn) *compressedConn { return &compressedConn{Conn: conn} }
func (c *compressedConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for len(c.incoming) == 0 {
		var h [7]byte
		if _, err := io.ReadFull(c.Conn, h[:]); err != nil {
			return 0, err
		}
		size := int(h[0]) | int(h[1])<<8 | int(h[2])<<16
		uncompressed := int(h[4]) | int(h[5])<<8 | int(h[6])<<16
		if h[3] != c.readSequence {
			return 0, fmt.Errorf("invalid compressed packet sequence: got %d want %d", h[3], c.readSequence)
		}
		c.readSequence++
		if size == 0 {
			return 0, errMalformed
		}
		payload := make([]byte, size)
		if _, err := io.ReadFull(c.Conn, payload); err != nil {
			return 0, err
		}
		if uncompressed == 0 {
			c.incoming = payload
			continue
		}
		source := bytes.NewReader(payload)
		reader, err := zlib.NewReader(source)
		if err != nil {
			return 0, fmt.Errorf("invalid compressed packet: %w", err)
		}
		decoded, readErr := io.ReadAll(io.LimitReader(reader, int64(uncompressed)+1))
		closeErr := reader.Close()
		if readErr != nil {
			return 0, readErr
		}
		if closeErr != nil {
			return 0, closeErr
		}
		if len(decoded) != uncompressed || source.Len() != 0 {
			return 0, fmt.Errorf("compressed packet length mismatch")
		}
		c.incoming = decoded
	}
	n := copy(p, c.incoming)
	c.incoming = c.incoming[n:]
	return n, nil
}
func (c *compressedConn) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		n := len(p)
		if n > maxFrame {
			n = maxFrame
		}
		raw := p[:n]
		payload := raw
		uncompressed := 0
		if len(raw) >= 50 {
			c.encoded.Reset()
			if c.compressor == nil {
				var err error
				c.compressor, err = zlib.NewWriterLevel(&c.encoded, zlib.BestSpeed)
				if err != nil {
					return total, err
				}
			} else {
				c.compressor.Reset(&c.encoded)
			}
			if _, err := c.compressor.Write(raw); err != nil {
				return total, err
			}
			if err := c.compressor.Close(); err != nil {
				return total, err
			}
			if c.encoded.Len() < len(raw) {
				payload = c.encoded.Bytes()
				uncompressed = len(raw)
			}
		}
		size := len(payload)
		h := []byte{byte(size), byte(size >> 8), byte(size >> 16), c.writeSequence, byte(uncompressed), byte(uncompressed >> 8), byte(uncompressed >> 16)}
		c.writeSequence++
		if err := writeAll(c.Conn, h); err != nil {
			return total, err
		}
		if err := writeAll(c.Conn, payload); err != nil {
			return total, err
		}
		total += n
		p = p[n:]
	}
	return total, nil
}
