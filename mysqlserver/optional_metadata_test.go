package mysqlserver

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"testing"

	"slopdb/db"
	"slopdb/sql"
)

type metadataCaptureConn struct {
	net.Conn
	output bytes.Buffer
}

func (c *metadataCaptureConn) Write(p []byte) (int, error) { return c.output.Write(p) }

func capturedMetadataPackets(t *testing.T, conn *metadataCaptureConn) [][]byte {
	t.Helper()
	var packets [][]byte
	var sequence byte
	for conn.output.Len() > 0 {
		packet, next, err := readPacket(&conn.output, sequence, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		sequence = next
		packets = append(packets, packet)
	}
	return packets
}

func TestOptionalMetadataHeaderWireFormat(t *testing.T) {
	for _, flags := range []uint32{0, clientDeprecateEOF, clientOptionalMetadata, clientOptionalMetadata | clientDeprecateEOF} {
		for _, mode := range []byte{resultsetMetadataFull, resultsetMetadataNone} {
			for _, count := range []int{1, 251} {
				t.Run(fmt.Sprintf("flags%x/mode%d/columns%d", flags, mode, count), func(t *testing.T) {
					conn := &metadataCaptureConn{}
					c := &client{capabilities: flags, variables: defaultSessionVariables(), writer: packetWriter{conn: conn}, warningTotal: 2}
					c.variables.resultsetMetadata = mode
					columns := make([]sql.ColumnDef, count)
					for i := range columns {
						columns[i] = sql.ColumnDef{Name: fmt.Sprintf("named_column_%d", i), ColType: sql.ColInteger}
					}
					if err := c.writeResultHeaderStatus(columns, 10); err != nil {
						t.Fatal(err)
					}
					packets := capturedMetadataPackets(t, conn)
					// Independent expected bytes prove count precedes flag, including
					// a length-encoded count too large for a one-byte header.
					header := []byte{1}
					if count == 251 {
						header = []byte{0xfc, 0xfb, 0}
					}
					full := flags&clientOptionalMetadata == 0 || mode == resultsetMetadataFull
					if flags&clientOptionalMetadata != 0 {
						header = append(header, mode)
					}
					if !bytes.Equal(packets[0], header) {
						t.Fatalf("header %x, want %x", packets[0], header)
					}
					want := 1
					if full {
						want += count
					}
					if flags&clientDeprecateEOF == 0 {
						want++
						if !bytes.Equal(packets[len(packets)-1], []byte{0xfe, 2, 0, 10, 0}) {
							t.Fatalf("metadata EOF %x", packets[len(packets)-1])
						}
					}
					if len(packets) != want {
						t.Fatalf("%d packets, want %d", len(packets), want)
					}
					if !full {
						for _, packet := range packets {
							if bytes.Contains(packet, []byte("named_column_")) {
								t.Fatal("column names sent with metadata NONE")
							}
						}
					}
				})
			}
		}
	}
}

func TestDeprecateEOFTerminatorStatusWarningsAndSessionTrack(t *testing.T) {
	for _, flags := range []uint32{0, clientSessionTrack, clientDeprecateEOF, clientDeprecateEOF | clientSessionTrack} {
		for _, position := range []string{"", "test-position"} {
			t.Run(fmt.Sprintf("flags%x/position%s", flags, position), func(t *testing.T) {
				conn := &metadataCaptureConn{}
				c := &client{capabilities: flags, writer: packetWriter{conn: conn}, warningTotal: 3}
				result := &db.ExecResult{Changes: 99, LastID: 88, CommitPosition: position}
				if err := c.resultTerminatorStatus(result, 11); err != nil {
					t.Fatal(err)
				}
				packets := capturedMetadataPackets(t, conn)
				if len(packets) != 1 {
					t.Fatalf("%d packets", len(packets))
				}
				packet := packets[0]
				if flags&clientDeprecateEOF == 0 {
					if !bytes.Equal(packet, []byte{0xfe, 3, 0, 11, 0}) {
						t.Fatalf("legacy EOF %x", packet)
					}
					return
				}
				if len(packet) < 7 || packet[0] != 0xfe || packet[1] != 0 || packet[2] != 0 {
					t.Fatalf("OK replacement %x", packet)
				}
				status := uint16(11)
				if flags&clientSessionTrack != 0 && position != "" {
					status |= 1 << 14
				}
				if binary.LittleEndian.Uint16(packet[3:]) != status || binary.LittleEndian.Uint16(packet[5:]) != 3 {
					t.Fatalf("status or warnings %x", packet)
				}
				info := ""
				if position != "" {
					info = "slopdb_commit_position=" + position
				}
				if flags&clientSessionTrack == 0 {
					if string(packet[7:]) != info {
						t.Fatalf("info %q", packet[7:])
					}
					return
				}
				d := decoder{data: packet[7:]}
				if string(d.str()) != info {
					t.Fatal("bad session-track info")
				}
				if position != "" {
					state := decoder{data: d.str()}
					if state.u8() != 0 {
						t.Fatal("bad session-track type")
					}
					variables := decoder{data: state.str()}
					if string(variables.str()) != "slopdb_commit_position" || string(variables.str()) != position || variables.err != nil || len(variables.data) != 0 || state.err != nil || len(state.data) != 0 {
						t.Fatal("bad session-track variable payload")
					}
				}
				if d.err != nil || len(d.data) != 0 {
					t.Fatalf("trailing terminator payload %x, %v", d.data, d.err)
				}
			})
		}
	}
}

func optionalMetadataPrepare(t *testing.T, c *rawClient, flags uint32, full bool, query string) uint32 {
	t.Helper()
	packet := c.command(t, append([]byte{22}, query...))
	wantSize := 12
	if flags&clientOptionalMetadata != 0 {
		wantSize++
	}
	if len(packet) != wantSize || packet[0] != 0 {
		t.Fatalf("prepare header %x", packet)
	}
	if wantSize == 13 && packet[12] != map[bool]byte{false: 0, true: 1}[full] {
		t.Fatalf("prepare flag %x", packet)
	}
	params, columns := int(binary.LittleEndian.Uint16(packet[7:])), int(binary.LittleEndian.Uint16(packet[5:]))
	if params != 1 || columns != 1 {
		t.Fatalf("prepare counts params=%d columns=%d", params, columns)
	}
	sequence := byte(2)
	read := func() []byte {
		t.Helper()
		p, next, err := readPacket(c.conn, sequence, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		sequence = next
		return p
	}
	for _, count := range []int{params, columns} {
		if full {
			for i := 0; i < count; i++ {
				p := read()
				if len(p) == 0 || p[0] != 3 || !bytes.HasPrefix(p, []byte{3, 'd', 'e', 'f'}) {
					t.Fatalf("column definition %x", p)
				}
			}
		}
		if count > 0 && flags&clientDeprecateEOF == 0 {
			p := read()
			if !bytes.Equal(p, []byte{0xfe, 0, 0, 2, 0}) {
				t.Fatalf("prepare metadata EOF %x", p)
			}
		}
	}
	id := binary.LittleEndian.Uint32(packet[1:])
	// A fresh command also proves no extra metadata or EOF packet remains.
	expectOK(t, c.command(t, []byte{14}), 2)
	return id
}

func TestOptionalMetadataPrepareCapabilityAndSessionReset(t *testing.T) {
	for _, flags := range []uint32{0, clientDeprecateEOF, clientOptionalMetadata, clientOptionalMetadata | clientDeprecateEOF} {
		t.Run(fmt.Sprintf("flags%x", flags), func(t *testing.T) {
			s := startTestServer(t, "tcp", Config{})
			c := connectRawWithFlags(t, s, flags)
			optionalMetadataPrepare(t, c, flags, true, "SELECT ? AS lean_column_name")
			for _, query := range []string{"SET resultset_metadata='NONE'", "SET SESSION resultset_metadata=NONE", "SET @@session.resultset_metadata=0", "SET @@resultset_metadata='none'"} {
				packet := c.command(t, append([]byte{3}, query...))
				if flags&clientOptionalMetadata == 0 {
					expectCode(t, packet, 3640)
					optionalMetadataPrepare(t, c, flags, true, "SELECT ? AS lean_column_name")
				} else {
					expectOK(t, packet, 2)
					optionalMetadataPrepare(t, c, flags, false, "SELECT ? AS lean_column_name")
				}
			}
			expectCode(t, c.command(t, append([]byte{3}, "SET resultset_metadata='invalid'"...)), 1231)
			expectOK(t, c.command(t, append([]byte{3}, "SET resultset_metadata='FULL'"...)), 2)
			optionalMetadataPrepare(t, c, flags, true, "SELECT ? AS lean_column_name")
			if flags&clientOptionalMetadata != 0 {
				expectOK(t, c.command(t, append([]byte{3}, "SET resultset_metadata='NONE'"...)), 2)
			}
			expectOK(t, c.command(t, []byte{31}), 2)
			optionalMetadataPrepare(t, c, flags, true, "SELECT ? AS lean_column_name")
		})
	}
}

// Read complete server results without relying on a client library's metadata
// defaults. The expected row payloads are fixed MySQL protocol bytes.
func readOptionalMetadataResult(t *testing.T, c *rawClient, flags uint32, full bool, columns []string, rows [][]byte, status uint16, sequence byte) byte {
	t.Helper()
	read := func() []byte {
		t.Helper()
		p, next, err := readPacket(c.conn, sequence, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		sequence = next
		return p
	}
	header := read()
	expected := []byte{byte(len(columns))}
	if flags&clientOptionalMetadata != 0 {
		flag := byte(0)
		if full {
			flag = 1
		}
		expected = append(expected, flag)
	}
	if !bytes.Equal(header, expected) {
		t.Fatalf("header %x, want %x", header, expected)
	}
	if full {
		for _, name := range columns {
			packet := read()
			d := decoder{data: packet}
			for i := 0; i < 4; i++ {
				d.str()
			}
			if got := string(d.str()); got != name || d.err != nil {
				t.Fatalf("column name %q, want %q: %x", got, name, packet)
			}
		}
	}
	if flags&clientDeprecateEOF == 0 {
		if packet := read(); !bytes.Equal(packet, []byte{0xfe, 0, 0, byte(status), byte(status >> 8)}) {
			t.Fatalf("metadata EOF %x", packet)
		}
	}
	for _, expected := range rows {
		if packet := read(); !bytes.Equal(packet, expected) {
			t.Fatalf("row %x, want %x", packet, expected)
		}
	}
	packet := read()
	terminator := []byte{0xfe, 0, 0, byte(status), byte(status >> 8)}
	if flags&clientDeprecateEOF != 0 {
		terminator = []byte{0xfe, 0, 0, byte(status), byte(status >> 8), 0, 0}
		if flags&clientSessionTrack != 0 {
			terminator = append(terminator, 0)
		}
	}
	if !bytes.Equal(packet, terminator) {
		t.Fatalf("result terminator %x, want %x", packet, terminator)
	}
	return sequence
}

func TestOptionalMetadataTextPreparedAndMultipleResultsets(t *testing.T) {
	for _, negotiated := range []uint32{0, clientDeprecateEOF, clientOptionalMetadata, clientOptionalMetadata | clientDeprecateEOF, clientOptionalMetadata | clientDeprecateEOF | clientSessionTrack} {
		t.Run(fmt.Sprintf("flags%x", negotiated), func(t *testing.T) {
			s := startTestServer(t, "tcp", Config{})
			flags := negotiated | clientMultiStatements | clientMultiResults
			c := connectRawWithFlags(t, s, flags)
			expectOK(t, c.command(t, append([]byte{3}, "CREATE TABLE lean_metadata_rows(id BIGINT PRIMARY KEY, v TEXT)"...)), 2)
			expectOK(t, c.command(t, append([]byte{3}, "INSERT INTO lean_metadata_rows VALUES(7,'value')"...)), 2)
			for _, full := range []bool{true, false, true} {
				if !full && flags&clientOptionalMetadata == 0 {
					continue
				}
				mode := "NONE"
				if full {
					mode = "FULL"
				}
				expectOK(t, c.command(t, append([]byte{3}, "SET resultset_metadata="+mode...)), 2)
				// A literal SELECT takes the materialized response path.
				c.send(t, append([]byte{3}, "SELECT 7 AS lean_column_name"...))
				readOptionalMetadataResult(t, c, flags, full, []string{"lean_column_name"}, [][]byte{{1, '7'}}, 2, 1)
				// A typed base-table projection takes the streaming response path.
				c.send(t, append([]byte{3}, "SELECT id,v FROM lean_metadata_rows WHERE id=7"...))
				readOptionalMetadataResult(t, c, flags, full, []string{"id", "v"}, [][]byte{{1, '7', 5, 'v', 'a', 'l', 'u', 'e'}}, 2, 1)
				c.send(t, append([]byte{3}, "SELECT id,v FROM lean_metadata_rows WHERE id=8"...))
				readOptionalMetadataResult(t, c, flags, full, []string{"id", "v"}, nil, 2, 1)
				id := optionalMetadataPrepare(t, c, flags, full, "SELECT id FROM lean_metadata_rows WHERE id=?")
				execute := append32([]byte{23}, id)
				execute = append(execute, 0, 1, 0, 0, 0, 0, 1, 8, 0, 7, 0, 0, 0, 0, 0, 0, 0)
				c.send(t, execute)
				readOptionalMetadataResult(t, c, flags, full, []string{"id"}, [][]byte{{0, 0, 7, 0, 0, 0, 0, 0, 0, 0}}, 2, 1)
				c.send(t, append([]byte{3}, "SELECT 7 AS first_result; SELECT 8 AS second_result"...))
				sequence := readOptionalMetadataResult(t, c, flags, full, []string{"first_result"}, [][]byte{{1, '7'}}, 10, 1)
				readOptionalMetadataResult(t, c, flags, full, []string{"second_result"}, [][]byte{{1, '8'}}, 2, sequence)
				expectOK(t, c.command(t, []byte{14}), 2)
			}
		})
	}
}
