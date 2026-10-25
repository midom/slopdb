package mysqlserver

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"slopdb/sql"
)

func localReader(t *testing.T, name, input string) {
	t.Helper()
	driver.RegisterReaderHandler(name, func() io.Reader { return io.NopCloser(strings.NewReader(input)) })
	t.Cleanup(func() { driver.DeregisterReaderHandler(name) })
}
func TestLocalInfileSharedBulkAndDriverRecovery(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	ctx := context.Background()
	c, e := pool.Conn(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if _, e = c.ExecContext(ctx, "CREATE TABLE local_nodes(id BIGINT PRIMARY KEY,parent BIGINT REFERENCES local_nodes(id),v TEXT)"); e != nil {
		t.Fatal(e)
	}
	name := t.Name()
	localReader(t, name, "1\t3\tfirst\\nline\n2\t\\N\t🪐\n3\t\\N\tlast\n")
	result, e := c.ExecContext(ctx, "LOAD DATA LOCAL INFILE 'Reader::"+name+"' INTO TABLE local_nodes")
	if e != nil {
		t.Fatal(e)
	}
	if n, e := result.RowsAffected(); e != nil || n != 3 {
		t.Fatal(n, e)
	}
	var v string
	if e = c.QueryRowContext(ctx, "SELECT v FROM local_nodes WHERE id=1").Scan(&v); e != nil || v != "first\nline" {
		t.Fatal(v, e)
	}
	bad := name + "_bad"
	localReader(t, bad, "4\t3\tpartial\n1\t3\tduplicate\n5\t3\tlate\n")
	if _, e = c.ExecContext(ctx, "LOAD DATA LOCAL INFILE 'Reader::"+bad+"' INTO TABLE local_nodes"); e == nil {
		t.Fatal("duplicate accepted")
	}
	var n int64
	if e = c.QueryRowContext(ctx, "SELECT count(*) FROM local_nodes").Scan(&n); e != nil || n != 3 {
		t.Fatalf("rollback or packet resync failed: %d %v", n, e)
	}
	if _, e = c.ExecContext(ctx, "LOAD DATA INFILE '/server/filesystem' INTO TABLE local_nodes"); e == nil {
		t.Fatal("server file access accepted")
	}
	if e = c.QueryRowContext(ctx, "SELECT 1").Scan(&n); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	if _, e = c.ExecContext(ctx, "CREATE TABLE auto_local(id BIGINT AUTO_INCREMENT PRIMARY KEY,v TEXT)"); e != nil {
		t.Fatal(e)
	}
	auto := name + "_auto"
	localReader(t, auto, "a\nb\n")
	result, e = c.ExecContext(ctx, "LOAD DATA LOCAL INFILE 'Reader::"+auto+"' INTO TABLE auto_local(v)")
	if e != nil {
		t.Fatal(e)
	}
	if id, e := result.LastInsertId(); e != nil || id != 1 {
		t.Fatalf("automatic id %d %v", id, e)
	}
}
func TestLocalInfileCSVNullBytesAndColumnLists(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	mustExec(t, pool, "CREATE TABLE local_csv(id INTEGER PRIMARY KEY,label TEXT,b BLOB,note TEXT)")
	name := t.Name()
	localReader(t, name, "id,label,b,note\r\n1,\"a \"\"quote\"\",\nnext\",\\0\\Z,NULL\r\n2,\"NULL\",,\"NULL\"\r\n")
	q := "LOAD DATA LOCAL INFILE 'Reader::" + name + `' INTO TABLE local_csv FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES (id,label,b,note)`
	if _, e := pool.Exec(q); e != nil {
		t.Fatal(e)
	}
	var label string
	var blob []byte
	var null bool
	if e := pool.QueryRow("SELECT label,b,note IS NULL FROM local_csv WHERE id=1").Scan(&label, &blob, &null); e != nil || label != "a \"quote\",\nnext" || !bytes.Equal(blob, []byte{0, 26}) || !null {
		t.Fatalf("%q %x %v %v", label, blob, null, e)
	}
	var note string
	if e := pool.QueryRow("SELECT label,note,b IS NULL FROM local_csv WHERE id=2").Scan(&label, &note, &null); e != nil || label != "NULL" || note != "NULL" || null {
		t.Fatal(label, note, null, e)
	}
}
func TestLocalInfileLockRetryKeepsSingleClientStream(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	mustExec(t, pool, "CREATE TABLE local_retry(id INTEGER PRIMARY KEY)")
	tx, e := s.database.Begin()
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	if _, e = tx.Exec("INSERT INTO local_retry VALUES(2)"); e != nil {
		t.Fatal(e)
	}
	name := t.Name()
	var calls atomic.Int64
	driver.RegisterReaderHandler(name, func() io.Reader { calls.Add(1); return io.NopCloser(strings.NewReader("1\n2\n3\n")) })
	t.Cleanup(func() { driver.DeregisterReaderHandler(name) })
	done := make(chan error, 1)
	go func() {
		_, e := pool.Exec("LOAD DATA LOCAL INFILE 'Reader::" + name + "' INTO TABLE local_retry")
		done <- e
	}()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for len(s.database.LockWaits()) == 0 {
		select {
		case e := <-done:
			t.Fatalf("did not wait: %v", e)
		case <-timer.C:
			t.Fatal("no import waiter")
		case <-tick.C:
		}
	}
	if e = tx.Rollback(); e != nil {
		t.Fatal(e)
	}
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	if calls.Load() != 1 {
		t.Fatalf("file requested %d times", calls.Load())
	}
	var n int
	if e = pool.QueryRow("SELECT count(*) FROM local_retry").Scan(&n); e != nil || n != 3 {
		t.Fatal(n, e)
	}
}
func TestLocalInfileDisconnectRollsBack(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	mustExec(t, pool, "CREATE TABLE local_disconnect(id INTEGER PRIMARY KEY)")
	c := connectRawWithFlags(t, s, clientLocalFiles)
	response := c.command(t, append([]byte{3}, []byte("LOAD DATA LOCAL INFILE 'client-stream' INTO TABLE local_disconnect")...))
	if len(response) == 0 || response[0] != 0xfb {
		t.Fatalf("file request %x", response)
	}
	writer := packetWriter{conn: c.conn, sequence: 2}
	if e := writer.write([]byte("1\n")); e != nil {
		t.Fatal(e)
	}
	_ = c.conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, e := pool.ExecContext(ctx, "INSERT INTO local_disconnect VALUES(1)"); e != nil {
		t.Fatalf("disconnect retained import/locks: %v", e)
	}
}
func TestLocalInfileRecordsAndSyntaxBounds(t *testing.T) {
	command := &localLoad{delimiter: ',', quote: '"', escape: '\\', line: "\n"}
	r := loadRecords{reader: bufio.NewReader(strings.NewReader("\"NULL\",NULL,\\N,\"a\"\"b\"\n")), command: command, limit: 100, width: 4}
	fields, e := r.next()
	if e != nil {
		t.Fatal(e)
	}
	if len(fields) != 4 || fields[0].null || !fields[1].null || !fields[2].null || string(fields[3].data) != "a\"b" {
		t.Fatalf("%+v", fields)
	}
	for _, input := range []string{"a,b,c,d,e\n", strings.Repeat("x", 101), "\"unterminated"} {
		r = loadRecords{reader: bufio.NewReader(strings.NewReader(input)), command: command, limit: 100, width: 4}
		if _, e = r.next(); e == nil {
			t.Fatalf("accepted invalid input %q", input)
		}
	}
	for _, q := range []string{"LOAD DATA INFILE 'server' INTO TABLE t", "LOAD DATA LOCAL INFILE 'x' REPLACE INTO TABLE t", "LOAD DATA LOCAL INFILE 'x' INTO TABLE t SET a=1", "LOAD DATA LOCAL INFILE 'x' INTO TABLE t FIELDS TERMINATED BY ''", "LOAD DATA LOCAL INFILE 'x' INTO TABLE t LINES STARTING BY '#'", "LOAD DATA LOCAL INFILE 'x' INTO TABLE t;DROP TABLE t"} {
		if _, e := parseLocalLoad(q, sqlParseOptions()); e == nil {
			t.Fatal(q)
		}
	}
}
func sqlParseOptions() sql.ParseOptions { return sql.ParseOptions{MySQLQuoting: true} }

func TestLocalInfileIndependentMySQLCLI(t *testing.T) {
	binary, e := exec.LookPath("mysql")
	if e != nil {
		t.Skip("mysql client unavailable")
	}
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	mustExec(t, pool, "CREATE TABLE local_cli(id INTEGER PRIMARY KEY,v TEXT)")
	path := filepath.Join(t.TempDir(), "input.tsv")
	if e = os.WriteFile(path, []byte("1\tone\n2\t🪐\n"), 0600); e != nil {
		t.Fatal(e)
	}
	host, port, e := net.SplitHostPort(s.address)
	if e != nil {
		t.Fatal(e)
	}

	for _, compressed := range []bool{false, true} {
		name := "plain"
		if compressed {
			name = "compressed"
		}
		t.Run(name, func(t *testing.T) {
			mustExec(t, pool, "DELETE FROM local_cli")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			args := []string{"--no-defaults", "--local-infile=1", "--batch", "--skip-column-names", "--protocol=TCP", "--host=" + host, "--port=" + port, "--user=slopdb", "--skip-password", "--database=slopdb"}
			if compressed {
				args = append(args, "--compress")
			}
			query := "LOAD DATA LOCAL INFILE '" + path + "' INTO TABLE local_cli; SELECT count(*) FROM local_cli"
			args = append(args, "--execute="+query)
			output, e := exec.CommandContext(ctx, binary, args...).CombinedOutput()
			if e != nil {
				t.Fatalf("mysql CLI: %v %s", e, output)
			}
			if !strings.Contains(string(output), "2\n") {
				t.Fatalf("output %s", output)
			}
		})
	}

}

func TestLocalInfileRejectsUnnegotiatedCapabilityAndResumes(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	c := connectRaw(t, s)
	packet := c.command(t, append([]byte{3}, []byte("LOAD DATA LOCAL INFILE 'client-stream' INTO TABLE absent")...))
	if len(packet) < 3 || packet[0] != 0xff {
		t.Fatalf("capability error %x", packet)
	}
	packet = c.command(t, append([]byte{3}, []byte("SELECT 1")...))
	if len(packet) != 1 || packet[0] != 1 {
		t.Fatalf("session did not resume %x", packet)
	}
}

func TestLocalInfileCompressedRejectionKeepsSequence(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	raw := connectRawWithFlags(t, s, clientCompress|clientLocalFiles)
	compressed := newCompressedConn(raw.conn)
	send := func(q string) {
		t.Helper()
		compressed.writeSequence = 0
		w := packetWriter{conn: compressed}
		if e := w.write(append([]byte{3}, []byte(q)...)); e != nil {
			t.Fatal(e)
		}
		compressed.readSequence.Store(uint32(compressed.writeSequence))
	}
	send("LOAD DATA LOCAL INFILE 'client-stream' INTO TABLE absent_local")
	packet, _, e := readPacket(compressed, 1, 1<<20)
	if e != nil || len(packet) == 0 || packet[0] != 0xff {
		t.Fatalf("rejection %x %v", packet, e)
	}
	send("SELECT 7")
	sequence := byte(1)
	for i := 0; i < 5; i++ {
		var next byte
		packet, next, e = readPacket(compressed, sequence, 1<<20)
		if e != nil {
			t.Fatal(e)
		}
		sequence = next
		if i == 3 && !bytes.Equal(packet, []byte{1, '7'}) {
			t.Fatalf("row %x", packet)
		}
	}
}
