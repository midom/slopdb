package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func leanTestFrame(sequence byte, p []byte) []byte {
	return append([]byte{byte(len(p)), byte(len(p) >> 8), byte(len(p) >> 16), sequence}, p...)
}

// This mock uses explicit ordinal schema and an independently assembled wire
// transcript: no result names or table identifiers are sent to the reader.
func TestLeanMySQLReaderProtocolTranscript(t *testing.T) {
	for _, binaryRows := range []bool{false, true} {
		t.Run(map[bool]string{false: "text", true: "prepared"}[binaryRows], func(t *testing.T) {
			conn, server := net.Pipe()
			t.Cleanup(func() { conn.Close(); server.Close() })
			if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				defer server.Close()
				read := func(sequence byte) ([]byte, error) {
					var h [4]byte
					if _, err := io.ReadFull(server, h[:]); err != nil {
						return nil, err
					}
					if h[3] != sequence {
						return nil, errors.New("wrong request sequence")
					}
					p := make([]byte, int(h[0])|int(h[1])<<8|int(h[2])<<16)
					_, err := io.ReadFull(server, p)
					return p, err
				}
				write := func(sequence byte, p []byte) error { _, err := server.Write(leanTestFrame(sequence, p)); return err }
				greeting := append([]byte{10}, "8.4.0-test"...)
				greeting = append(greeting, 0)
				greeting = append(greeting, make([]byte, 4+8+1)...)
				greeting = binary.LittleEndian.AppendUint16(greeting, uint16(leanCapabilityFlags&0xffff))
				greeting = append(greeting, 45, 2, 0)
				greeting = binary.LittleEndian.AppendUint16(greeting, uint16(leanCapabilityFlags>>16))
				if err := write(0, greeting); err != nil {
					done <- err
					return
				}
				response, err := read(1)
				if err != nil {
					done <- err
					return
				}
				if len(response) < 32 || binary.LittleEndian.Uint32(response) != leanCapabilityFlags {
					done <- errors.New("capabilities not negotiated")
					return
				}
				ok := []byte{0, 0, 0, 2, 0, 0, 0}
				if err = write(2, ok); err != nil {
					done <- err
					return
				}
				response, err = read(0)
				if err != nil {
					done <- err
					return
				}
				if !bytes.Equal(response, []byte("\x03SET SESSION resultset_metadata=NONE")) {
					done <- errors.New("metadata not explicitly disabled")
					return
				}
				if err = write(1, ok); err != nil {
					done <- err
					return
				}
				if binaryRows {
					response, err = read(0)
					if err != nil {
						done <- err
						return
					}
					if !bytes.Equal(response, append([]byte{22}, query...)) {
						done <- errors.New("wrong prepared query")
						return
					}
					if err = write(1, []byte{0, 7, 0, 0, 0, 3, 0, 1, 0, 0, 0, 0, 0}); err != nil {
						done <- err
						return
					}
				}
				for _, id := range []int64{2, 3} {
					response, err = read(0)
					if err != nil {
						done <- err
						return
					}
					if binaryRows {
						if len(response) != 22 || response[0] != 23 || binary.LittleEndian.Uint32(response[1:5]) != 7 || int64(binary.LittleEndian.Uint64(response[14:])) != id {
							done <- errors.New("wrong binary binding")
							return
						}
					} else if !bytes.Equal(response, []byte("\x03"+wireQueryPrefix+string(rune('0'+id)))) {
						done <- errors.New("wrong literal query")
						return
					}
					if err = write(1, []byte{3, 0}); err != nil {
						done <- err
						return
					}
					blob := make([]byte, payloadBytes)
					payload(id, blob)
					var row []byte
					if binaryRows {
						row = []byte{0, 0}
						row = binary.LittleEndian.AppendUint64(row, uint64(id))
						row = binary.LittleEndian.AppendUint64(row, uint64(id*17))
					} else {
						row = []byte{1, byte('0' + id), 2, byte('0' + id*17/10), byte('0' + id*17%10)}
					}
					row = append(row, payloadBytes)
					row = append(row, blob...)
					if err = write(2, row); err != nil {
						done <- err
						return
					}
					if err = write(3, []byte{0xfe, 0, 0, 2, 0, 0, 0}); err != nil {
						done <- err
						return
					}
				}
				done <- nil
			}()
			reader := &leanMySQLReader{conn: conn, input: bufio.NewReader(conn), binary: binaryRows}
			if err := reader.handshake(); err != nil {
				t.Fatal(err)
			}
			if err := reader.configure(); err != nil {
				t.Fatal(err)
			}
			for _, id := range []int64{2, 3} {
				if err := reader.query(id); err != nil {
					t.Fatal(err)
				}
				if err := reader.validate(id, make([]byte, payloadBytes)); err != nil {
					t.Fatal(err)
				}
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLeanMySQLDecoderRejectsMalformedRowsAndFrames(t *testing.T) {
	for _, p := range [][]byte{nil, {0xfc}, {0xfd, 0}, {0xfe, 0, 0}, {0xfb}, {0xff}} {
		if _, _, err := leanLength(p); err == nil {
			t.Fatalf("accepted malformed length %x", p)
		}
	}
	for _, p := range [][]byte{nil, {0}, {0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 250}} {
		if err := new(leanMySQLReader).decodeBinaryRow(p); err == nil {
			t.Fatalf("accepted malformed row %x", p)
		}
	}
	for name, data := range map[string][]byte{
		"sequence":     leanTestFrame(7, []byte{0}),
		"empty":        leanTestFrame(0, nil),
		"oversized":    {0xff, 0xff, 0x1f, 0},
		"truncated":    {2, 0, 0, 0, 1},
		"server-error": leanTestFrame(0, []byte{0xff, 0x15, 4, '#', '4', '2', 'S', '0', '2', 'n', 'o'}),
	} {
		t.Run(name, func(t *testing.T) {
			r := &leanMySQLReader{input: bufio.NewReader(bytes.NewReader(data))}
			if _, err := r.readPacket(); err == nil {
				t.Fatal("malformed packet accepted")
			}
		})
	}
}

func TestLeanMySQLAccessLabelAndConfiguration(t *testing.T) {
	c, err := accessConfig(config{API: "mysql-lean", Endpoint: "127.0.0.1:3306"})
	if err != nil {
		t.Fatal(err)
	}
	if apiMode(c) != "typed-wire:mysql-lean:prepared" {
		t.Fatal(apiMode(c))
	}
	if _, err = accessConfig(config{API: "mysql-lean", ReuseResults: true}); err == nil {
		t.Fatal("native result reuse accepted")
	}
}

// The reader must consume the terminator before rejecting a malformed answer,
// so its next operation starts at a command boundary rather than a stale row.
func TestLeanMySQLReaderDrainsInvalidAnswers(t *testing.T) {
	for _, invalid := range []string{"null", "empty", "extra"} {
		t.Run(invalid, func(t *testing.T) {
			makeRow := func(id int64) []byte {
				row := []byte{0, 0}
				row = binary.LittleEndian.AppendUint64(row, uint64(id))
				row = binary.LittleEndian.AppendUint64(row, uint64(id*17))
				row = append(row, payloadBytes)
				blob := make([]byte, payloadBytes)
				payload(id, blob)
				return append(row, blob...)
			}
			var transcript []byte
			appendResult := func(rows ...[]byte) {
				transcript = append(transcript, leanTestFrame(1, []byte{3, 0})...)
				sequence := byte(2)
				for _, row := range rows {
					transcript = append(transcript, leanTestFrame(sequence, row)...)
					sequence++
				}
				transcript = append(transcript, leanTestFrame(sequence, []byte{0xfe, 0, 0, 2, 0, 0, 0})...)
			}
			switch invalid {
			case "null":
				row := makeRow(1)
				row[1] = 4
				appendResult(row)
			case "empty":
				appendResult()
			case "extra":
				appendResult(makeRow(1), makeRow(1))
			}
			appendResult(makeRow(2))
			reader := &leanMySQLReader{conn: leanDiscardConn{}, input: bufio.NewReader(bytes.NewReader(transcript)), binary: true}
			if err := reader.query(1); err == nil {
				t.Fatal("invalid answer accepted")
			}
			if err := reader.query(2); err != nil {
				t.Fatal("query after invalid answer", err)
			}
			if err := reader.validate(2, make([]byte, payloadBytes)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type leanDiscardConn struct{ net.Conn }

func (leanDiscardConn) Write(p []byte) (int, error) { return len(p), nil }
