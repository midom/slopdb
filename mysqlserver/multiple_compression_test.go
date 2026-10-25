package mysqlserver

import (
	"bytes"
	"compress/zlib"
	gosql "database/sql"
	"encoding/binary"
	driver "github.com/go-sql-driver/mysql"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestMySQLMultipleStatementsAndResults(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	config := driver.NewConfig()
	config.User = "slopdb"
	config.DBName = "slopdb"
	config.Net = s.network
	config.Addr = s.address
	config.MultiStatements = true
	pool, err := gosql.Open("mysql", config.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	rows, err := pool.Query("SELECT 'a;b' AS value; SELECT 42 AS value; SELECT 'last' AS value")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for i, want := range []string{"a;b", "42", "last"} {
		if i > 0 && !rows.NextResultSet() {
			t.Fatalf("missing result %d: %v", i, rows.Err())
		}
		if !rows.Next() {
			t.Fatal(rows.Err())
		}
		var got string
		if err = rows.Scan(&got); err != nil || got != want {
			t.Fatal(got, err)
		}
		if rows.Next() {
			t.Fatal("extra row")
		}
	}
	if rows.NextResultSet() || rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	mustExec(t, pool, "CREATE TABLE multi_items(id INT PRIMARY KEY,n INT); INSERT INTO multi_items VALUES(1,10); UPDATE multi_items SET n=n+1")
	if _, err = pool.Exec("INSERT INTO multi_items VALUES(2,20); INSERT INTO multi_items VALUES(1,30); INSERT INTO multi_items VALUES(3,30)"); err == nil {
		t.Fatal("middle failure accepted")
	}
	var n int
	if err = pool.QueryRow("SELECT COUNT(*) FROM multi_items").Scan(&n); err != nil || n != 2 {
		t.Fatal(n, err)
	}
	mustExec(t, pool, "CREATE TABLE multi_audit(v INT); CREATE TRIGGER multi_trigger AFTER INSERT ON multi_items BEGIN INSERT INTO multi_audit VALUES(NEW.id); INSERT INTO multi_audit VALUES(NEW.n); END; INSERT INTO multi_items VALUES(4,40)")
	if err = pool.QueryRow("SELECT COUNT(*) FROM multi_audit").Scan(&n); err != nil || n != 2 {
		t.Fatal(n, err)
	}
}
func TestMySQLCompressedCommandsAndResults(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	raw := connectRawWithFlags(t, s, clientCompress|clientMultiStatements)
	compressed := newCompressedConn(raw.conn)
	compressed.writeSequence = 0
	writer := packetWriter{conn: compressed}
	query := "SELECT '" + strings.Repeat("compress me ", 200) + "' AS v; SELECT 7"
	if err := writer.write(append([]byte{3}, query...)); err != nil {
		t.Fatal(err)
	}
	compressed.readSequence.Store(uint32(compressed.writeSequence))
	sequence := byte(1)
	// Each SELECT consists of column count/definition/EOF/row/EOF.
	for result := 0; result < 2; result++ {
		for packet := 0; packet < 5; packet++ {
			p, next, err := readPacket(compressed, sequence, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			sequence = next
			if packet == 4 {
				status := binary.LittleEndian.Uint16(p[3:])
				if (status&8 != 0) != (result == 0) {
					t.Fatalf("bad MORE_RESULTS status %x", status)
				}
			}
		}
	}
	// Independently reset compression and ordinary sequence numbers next command.
	compressed.writeSequence = 0
	writer.sequence = 0
	if err := writer.write([]byte{14}); err != nil {
		t.Fatal(err)
	}
	compressed.readSequence.Store(uint32(compressed.writeSequence))
	p, _, err := readPacket(compressed, 1, 1<<20)
	if err != nil || p[0] != 0 {
		t.Fatal(p, err)
	}
}
func TestMySQLCompressedFramingRejectsInvalidData(t *testing.T) {
	for _, name := range []string{"length", "checksum", "sequence"} {
		t.Run(name, func(t *testing.T) {
			a, b := net.Pipe()
			defer a.Close()
			defer b.Close()
			_ = a.SetDeadline(time.Now().Add(time.Second))
			var encoded bytes.Buffer
			z := zlib.NewWriter(&encoded)
			_, _ = z.Write([]byte("abcdef"))
			_ = z.Close()
			payload := encoded.Bytes()
			size := len(payload)
			h := []byte{byte(size), byte(size >> 8), byte(size >> 16), 0, 6, 0, 0}
			switch name {
			case "length":
				h[4] = 5
			case "checksum":
				payload[len(payload)-1] ^= 1
			case "sequence":
				h[3] = 1
			}
			done := make(chan struct{})
			go func() { defer close(done); _, _ = b.Write(append(h, payload...)); _ = b.Close() }()
			_, err := io.ReadAll(newCompressedConn(a))
			if err == nil {
				t.Fatal("bad frame accepted")
			}
			_ = a.Close()
			<-done
		})
	}
}
