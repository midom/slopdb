package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"slopdb/db"
	"slopdb/mysqlserver"
)

const (
	leanOptionalMetadata uint32 = 1 << 25
	leanDeprecateEOF     uint32 = 1 << 24
	leanPacketLimit             = 1 << 20
	leanCapabilityFlags  uint32 = 1 | 4 | 8 | 1<<9 | 1<<13 | 1<<15 | 1<<19 | leanOptionalMetadata | leanDeprecateEOF
)

// leanMySQLReader is a deliberately fixture-specific protocol control, not a
// database/sql driver. The caller knows the three result types and addresses
// them by ordinal. It negotiates MySQL's optional metadata and deprecated EOF
// capabilities; NONE removes all column-definition packets, not just names.
// Each worker owns a connection, bounded packet buffers, and typed destinations.
// This isolates server/protocol costs from shared database/sql pool bookkeeping.
type leanMySQLReader struct {
	conn      net.Conn
	input     *bufio.Reader
	header    [4]byte
	packet    []byte
	output    []byte
	sequence  byte
	statement uint32
	binary    bool
	id, value int64
	data      []byte
	rows      int
}

func (r *leanMySQLReader) readPacket() ([]byte, error) {
	if _, err := io.ReadFull(r.input, r.header[:]); err != nil {
		return nil, err
	}
	n := int(r.header[0]) | int(r.header[1])<<8 | int(r.header[2])<<16
	if r.header[3] != r.sequence {
		return nil, fmt.Errorf("MySQL packet sequence %d, expected %d", r.header[3], r.sequence)
	}
	r.sequence++
	if n > leanPacketLimit {
		return nil, fmt.Errorf("benchmark MySQL packet exceeds %d-byte limit", leanPacketLimit)
	}
	if cap(r.packet) < n {
		r.packet = make([]byte, n)
	} else {
		r.packet = r.packet[:n]
	}
	if _, err := io.ReadFull(r.input, r.packet); err != nil {
		return nil, err
	}
	if len(r.packet) == 0 {
		return nil, errors.New("empty MySQL response packet")
	}
	if r.packet[0] == 0xff {
		return nil, leanServerError(r.packet)
	}
	return r.packet, nil
}

func leanServerError(p []byte) error {
	if len(p) < 3 {
		return errors.New("malformed MySQL error packet")
	}
	code := binary.LittleEndian.Uint16(p[1:3])
	message := p[3:]
	if len(message) >= 6 && message[0] == '#' {
		message = message[6:]
	}
	return fmt.Errorf("MySQL error %d: %s", code, message)
}

func (r *leanMySQLReader) writePacket(p []byte) error {
	if len(p) > leanPacketLimit {
		return fmt.Errorf("benchmark MySQL request exceeds %d-byte limit", leanPacketLimit)
	}
	r.output = append(r.output[:0], byte(len(p)), byte(len(p)>>8), byte(len(p)>>16), r.sequence)
	r.sequence++
	r.output = append(r.output, p...)
	remaining := r.output
	for len(remaining) > 0 {
		n, err := r.conn.Write(remaining)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(remaining) {
			return io.ErrShortWrite
		}
		remaining = remaining[n:]
	}
	return nil
}

func (r *leanMySQLReader) command(p []byte) error {
	r.sequence = 0
	return r.writePacket(p)
}

func (r *leanMySQLReader) handshake() error {
	p, err := r.readPacket()
	if err != nil {
		return err
	}
	if p[0] != 10 {
		return errors.New("benchmark requires MySQL protocol 10")
	}
	end := bytes.IndexByte(p[1:], 0)
	if end < 0 {
		return errors.New("malformed MySQL greeting")
	}
	position := end + 2 + 4 + 8 + 1
	if len(p) < position+7 {
		return errors.New("short MySQL greeting")
	}
	capabilities := uint32(binary.LittleEndian.Uint16(p[position:])) | uint32(binary.LittleEndian.Uint16(p[position+5:]))<<16
	if capabilities&leanCapabilityFlags != leanCapabilityFlags {
		return errors.New("server does not support required MySQL optional metadata capabilities")
	}
	response := make([]byte, 32, 96)
	binary.LittleEndian.PutUint32(response, leanCapabilityFlags)
	binary.LittleEndian.PutUint32(response[4:], leanPacketLimit)
	response[8] = 45 // utf8mb4
	response = append(response, "slopdb"...)
	response = append(response, 0, 0) // username terminator and empty auth response
	response = append(response, "slopdb"...)
	response = append(response, 0)
	response = append(response, "mysql_native_password"...)
	response = append(response, 0)
	if err = r.writePacket(response); err != nil {
		return err
	}
	p, err = r.readPacket()
	if err != nil {
		return err
	}
	return leanOK(p, 0)
}

func leanLength(p []byte) (uint64, []byte, error) {
	if len(p) == 0 {
		return 0, nil, errors.New("missing MySQL length")
	}
	switch p[0] {
	case 0xfc:
		if len(p) >= 3 {
			return uint64(binary.LittleEndian.Uint16(p[1:3])), p[3:], nil
		}
	case 0xfd:
		if len(p) >= 4 {
			return uint64(p[1]) | uint64(p[2])<<8 | uint64(p[3])<<16, p[4:], nil
		}
	case 0xfe:
		if len(p) >= 9 {
			return binary.LittleEndian.Uint64(p[1:9]), p[9:], nil
		}
	case 0xfb, 0xff:
		return 0, nil, errors.New("unexpected NULL or error in MySQL length")
	default:
		return uint64(p[0]), p[1:], nil
	}
	return 0, nil, errors.New("truncated MySQL length")
}

func leanString(p []byte) ([]byte, []byte, error) {
	n, rest, err := leanLength(p)
	if err != nil {
		return nil, nil, err
	}
	if n > uint64(len(rest)) {
		return nil, nil, errors.New("truncated MySQL value")
	}
	return rest[:int(n)], rest[int(n):], nil
}

func leanOK(p []byte, marker byte) error {
	if len(p) == 0 || p[0] != marker {
		return errors.New("expected MySQL OK packet")
	}
	_, p, err := leanLength(p[1:])
	if err != nil {
		return err
	}
	_, p, err = leanLength(p)
	if err != nil || len(p) < 4 {
		return errors.New("malformed MySQL OK packet")
	}
	if binary.LittleEndian.Uint16(p)&8 != 0 {
		return errors.New("unexpected additional MySQL result set")
	}
	return nil
}

func (r *leanMySQLReader) configure() error {
	if err := r.command([]byte("\x03SET SESSION resultset_metadata=NONE")); err != nil {
		return err
	}
	p, err := r.readPacket()
	if err != nil {
		return err
	}
	if err = leanOK(p, 0); err != nil {
		return err
	}
	if !r.binary {
		return nil
	}
	if err = r.command(append([]byte{22}, query...)); err != nil {
		return err
	}
	p, err = r.readPacket()
	if err != nil {
		return err
	}
	if len(p) != 13 || p[0] != 0 || binary.LittleEndian.Uint16(p[5:7]) != 3 || binary.LittleEndian.Uint16(p[7:9]) != 1 || p[12] != 0 {
		return errors.New("unexpected lean MySQL prepare metadata")
	}
	r.statement = binary.LittleEndian.Uint32(p[1:5])
	return nil
}

func (r *leanMySQLReader) query(id int64) error {
	r.rows = 0
	// output contains the frame for the previous request. Build the next command
	// in stack scratch so writePacket can reuse output without aliasing it.
	var request [128]byte
	p := request[:0]
	if r.binary {
		p = append(p, 23)
		p = binary.LittleEndian.AppendUint32(p, r.statement)
		p = append(p, 0, 1, 0, 0, 0, 0, 1, 8, 0) // cursor, iterations, null bitmap, fresh BIGINT type
		p = binary.LittleEndian.AppendUint64(p, uint64(id))
	} else {
		p = append(p, 3)
		p = append(p, wireQueryPrefix...)
		p = strconv.AppendInt(p, id, 10)
	}
	if err := r.command(p); err != nil {
		return err
	}
	p, err := r.readPacket()
	if err != nil {
		return err
	}
	columns, rest, err := leanLength(p)
	// MySQL's implementation emits column count THEN metadata_follows, despite
	// the opposite order shown in parts of the generated protocol documentation.
	if err != nil || columns != 3 || len(rest) != 1 || rest[0] != 0 {
		return errors.New("expected three-column MySQL result with omitted metadata")
	}
	var decodeErr error
	for {
		p, err = r.readPacket()
		if err != nil {
			return err
		}
		if p[0] == 0xfe && len(p) < 9 {
			if err = leanOK(p, 0xfe); err != nil {
				return err
			}
			break
		}
		r.rows++
		if r.rows == 1 {
			if r.binary {
				decodeErr = r.decodeBinaryRow(p)
			} else {
				decodeErr = r.decodeTextRow(p)
			}
		}
	}
	if decodeErr != nil {
		return decodeErr
	}
	if r.rows != 1 {
		return fmt.Errorf("id %d: got %d lean MySQL rows", id, r.rows)
	}
	return nil
}

func (r *leanMySQLReader) decodeBinaryRow(p []byte) error {
	if len(p) < 18 || p[0] != 0 || p[1]&0x1c != 0 {
		return errors.New("invalid or NULL typed MySQL binary row")
	}
	r.id = int64(binary.LittleEndian.Uint64(p[2:10]))
	r.value = int64(binary.LittleEndian.Uint64(p[10:18]))
	value, rest, err := leanString(p[18:])
	if err != nil || len(rest) != 0 || len(value) != payloadBytes {
		return errors.New("invalid MySQL binary BLOB value")
	}
	r.data = append(r.data[:0], value...)
	return nil
}

func (r *leanMySQLReader) decodeTextRow(p []byte) error {
	id, rest, err := leanString(p)
	if err != nil {
		return err
	}
	r.id, err = strconv.ParseInt(string(id), 10, 64)
	if err != nil {
		return err
	}
	value, rest, err := leanString(rest)
	if err != nil {
		return err
	}
	r.value, err = strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		return err
	}
	blob, rest, err := leanString(rest)
	if err != nil || len(rest) != 0 || len(blob) != payloadBytes {
		return errors.New("invalid MySQL text BLOB value")
	}
	r.data = append(r.data[:0], blob...)
	return nil
}

func (r *leanMySQLReader) validate(id int64, expected []byte) error {
	if len(expected) != payloadBytes {
		return errors.New("invalid lean MySQL validation buffer")
	}
	payload(id, expected)
	if r.rows != 1 || r.id != id || r.value != id*17 || !bytes.Equal(r.data, expected) {
		return fmt.Errorf("id %d: incorrect lean MySQL typed result", id)
	}
	return nil
}

func newLeanMySQLBackend(c config, database *db.Database) (_ *benchmarkBackend, err error) {
	if c.Workers < 1 || c.QueryMode != "text" && c.QueryMode != "prepared" {
		return nil, errors.New("invalid lean MySQL benchmark configuration")
	}
	var server *mysqlserver.Server
	var served chan error
	backend := &benchmarkBackend{}
	var once sync.Once
	var closeErr error
	backend.close = func() error {
		once.Do(func() {
			for _, reader := range backend.readers {
				closeErr = errors.Join(closeErr, reader.(*leanMySQLReader).conn.Close())
			}
			if server != nil {
				closeErr = errors.Join(closeErr, server.Close())
				if served != nil {
					if e := <-served; e != nil && !errors.Is(e, net.ErrClosed) {
						closeErr = errors.Join(closeErr, e)
					}
				}
			}
		})
		return closeErr
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, backend.close())
		}
	}()
	address := c.Endpoint
	if address == "" {
		server, err = mysqlserver.New(database, mysqlserver.Config{})
		if err != nil {
			return nil, err
		}
		listener, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			return nil, e
		}
		address = listener.Addr().String()
		served = make(chan error, 1)
		go func() { served <- server.Serve(listener) }()
	}
	for i := 0; i < c.Workers; i++ {
		conn, e := net.DialTimeout("tcp", address, 5*time.Second)
		if e != nil {
			return nil, e
		}
		reader := &leanMySQLReader{conn: conn, input: bufio.NewReaderSize(conn, 4096), binary: c.QueryMode == "prepared", data: make([]byte, 0, payloadBytes), output: make([]byte, 0, 256), packet: make([]byte, 0, 1024)}
		backend.readers = append(backend.readers, reader)
		if e = conn.SetDeadline(time.Now().Add(30 * time.Second)); e == nil {
			e = reader.handshake()
		}
		if e == nil {
			e = reader.configure()
		}
		if e == nil {
			e = reader.query(1)
		}
		if e == nil {
			e = reader.validate(1, make([]byte, payloadBytes))
		}
		if e == nil {
			e = conn.SetDeadline(time.Time{})
		}
		if e != nil {
			return nil, e
		}
	}
	return backend, nil
}
