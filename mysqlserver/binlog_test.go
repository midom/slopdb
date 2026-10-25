package mysqlserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"hash/crc32"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"slopdb/db"
	"slopdb/sql"
	"slopdb/storage"
)

func dumpRequest(file string, position uint32, nonblock bool) []byte {
	p := binary.LittleEndian.AppendUint32([]byte{18}, position)
	flags := uint16(0)
	if nonblock {
		flags = 1
	}
	p = binary.LittleEndian.AppendUint16(p, flags)
	p = binary.LittleEndian.AppendUint32(p, 1234)
	return append(p, file...)
}
func readBinlogDump(t *testing.T, c *rawClient, request []byte) [][]byte {
	t.Helper()
	c.send(t, request)
	sequence := byte(1)
	var events [][]byte
	for {
		p, next, err := readPacket(c.conn, sequence, 64<<20)
		if err != nil {
			t.Fatal(err)
		}
		sequence = next
		if len(p) == 0 {
			t.Fatal("empty binlog response")
		}
		if p[0] == 0xfe && len(p) < 9 {
			return events
		}
		if p[0] != 0 || len(p) < 24 {
			t.Fatalf("binlog error %x", p)
		}
		event := p[1:]
		if int(binary.LittleEndian.Uint32(event[9:])) != len(event) {
			t.Fatal("invalid event length")
		}
		if got, want := binary.LittleEndian.Uint32(event[len(event)-4:]), crc32.ChecksumIEEE(event[:len(event)-4]); got != want {
			t.Fatalf("checksum%08x !=%08x", got, want)
		}
		events = append(events, event)
	}
}
func TestBinlogDurableProtocolResumeAndExactRows(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	var file, doDB, ignoreDB, gtid string
	var position uint64
	if err := pool.QueryRow("SHOW MASTER STATUS").Scan(&file, &position, &doDB, &ignoreDB, &gtid); err != nil {
		t.Fatal(err)
	}
	mustExec(t, pool, "CREATE DATABASE stream")
	mustExec(t, pool, "CREATE TABLE stream.events(id BIGINT UNSIGNED PRIMARY KEY,d DECIMAL(30,5),j JSON,b BLOB)")
	mustExec(t, pool, `INSERT INTO stream.events VALUES(18446744073709551615,-12345678901234567890.01234,'{"n":18446744073709551615,"f":1.2500,"a":[true,null,"🪐"]}',X'00FF')`)
	mustExec(t, pool, "UPDATE stream.events SET d=42.00100 WHERE id=18446744073709551615")
	mustExec(t, pool, "BEGIN")
	mustExec(t, pool, "DELETE FROM stream.events")
	mustExec(t, pool, "ROLLBACK")
	mustExec(t, pool, "DELETE FROM stream.events")
	c := connectRaw(t, s)
	events := readBinlogDump(t, c, dumpRequest(file, 4, true))
	counts := map[byte]int{}
	last := uint32(4)
	var resume uint32
	var writes [][]byte
	for _, event := range events {
		kind := event[4]
		counts[kind]++
		end := binary.LittleEndian.Uint32(event[13:])
		if end == 0 {
			continue
		}
		if end != last+uint32(len(event)) {
			t.Fatalf("byte offset %d after %d (%d bytes)", end, last, len(event))
		}
		last = end
		if kind == binlogWriteRows {
			resume = end
			writes = append(writes, event)
		}
	}
	for _, kind := range []byte{binlogFormat, binlogGTID, binlogQuery, binlogXID, binlogTableMap, binlogWriteRows, binlogUpdateRows, binlogDeleteRows} {
		if counts[kind] == 0 {
			t.Fatalf("missing event %d counts%v", kind, counts)
		}
	}
	if counts[binlogDeleteRows] != 1 {
		t.Fatalf("rolled back delete leaked: %v", counts)
	}
	if len(writes) != 1 || !bytes.Contains(writes[0], bytes.Repeat([]byte{0xff}, 8)) {
		t.Fatal("unsigned64 row missing")
	}
	resumed := readBinlogDump(t, connectRaw(t, s), dumpRequest(file, resume, true))
	for _, event := range resumed {
		if event[4] == binlogWriteRows {
			t.Fatal("resume repeated committed write")
		}
	}
	bad := connectRaw(t, s).command(t, dumpRequest(file, resume+1, true))
	if len(bad) == 0 || bad[0] != 0 { // artificial rotate precedes validation, then error.
		if len(bad) == 0 || bad[0] != 0xff {
			t.Fatal(bad)
		}
	}
	// Rebuilding the offset index from the persisted journal is deterministic.
	s.server.binlogMu.Lock()
	s.server.binlog = nil
	s.server.binlogMu.Unlock()
	again := readBinlogDump(t, connectRaw(t, s), dumpRequest(file, 4, true))
	if len(events) != len(again) {
		t.Fatal("rebuild event count changed")
	}
	for i := range events {
		if !bytes.Equal(events[i], again[i]) {
			t.Fatalf("rebuild changed event%d", i)
		}
	}
	if binlog, err := exec.LookPath("mysqlbinlog"); err == nil {
		var data = []byte{0xfe, 'b', 'i', 'n'}
		for _, event := range events {
			if binary.LittleEndian.Uint32(event[13:]) != 0 {
				data = append(data, event...)
			}
		}
		path := filepath.Join(t.TempDir(), file)
		if err = os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		output, err := exec.Command(binlog, "--no-defaults", "--verify-binlog-checksum", "--base64-output=DECODE-ROWS", "-vv", path).CombinedOutput()
		if err != nil {
			t.Fatalf("independent MySQL parser: %v\n%s", err, output)
		}
		for _, want := range []string{"18446744073709551615", "-12345678901234567890.01234", "42.00100", "DELETE FROM `stream`.`events`", "INSERT INTO `stream`.`events`"} {
			if !strings.Contains(string(output), want) {
				t.Fatalf("mysqlbinlog output missing %q:\n%s", want, output)
			}
		}
	}
}
func TestBinlogGTIDFilteringAndLiveWake(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	snapshot, err := s.server.binlogSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, pool, "CREATE TABLE changes(id INTEGER PRIMARY KEY)")
	mustExec(t, pool, "INSERT INTO changes VALUES(1)")
	snapshot, err = s.server.binlogSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	source, _ := hex.DecodeString(snapshot.source)
	set := binary.LittleEndian.AppendUint64(nil, 1)
	set = append(set, source...)
	set = binary.LittleEndian.AppendUint64(set, 1)
	set = binary.LittleEndian.AppendUint64(set, 1)
	set = binary.LittleEndian.AppendUint64(set, 3)
	request := binary.LittleEndian.AppendUint16([]byte{30}, 1)
	request = binary.LittleEndian.AppendUint32(request, 123)
	request = binary.LittleEndian.AppendUint32(request, 0)
	request = binary.LittleEndian.AppendUint64(request, 4)
	request = binary.LittleEndian.AppendUint32(request, uint32(len(set)))
	request = append(request, set...)
	for _, event := range readBinlogDump(t, connectRaw(t, s), request) {
		if event[4] == binlogGTID {
			t.Fatal("GTID already executed")
		}
	}
	live := connectRaw(t, s)
	file := snapshot.files[len(snapshot.files)-1]
	live.send(t, dumpRequest(binlogFilename(file.number), file.size, false))
	for sequence := byte(1); sequence < 3; sequence++ {
		if _, _, err := readPacket(live.conn, sequence, 1<<20); err != nil {
			t.Fatal(err)
		}
	}
	mustExec(t, pool, "INSERT INTO changes VALUES(2)")
	event, _, err := readPacket(live.conn, 3, 1<<20)
	if err != nil || len(event) < 6 || event[5] != binlogGTID {
		t.Fatal(event, err)
	}
	if err = live.conn.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestBinlogPositionSurvivesDatabaseReopen(t *testing.T) {
	path := t.TempDir()
	options := storage.DefaultOptions()
	options.MySQLQuoting = true
	database, err := db.Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(database, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = server.binlogSnapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = database.Exec("CREATE TABLE durable(id BIGINT PRIMARY KEY,d DECIMAL(10,2),j JSON); INSERT INTO durable VALUES(1,123.45,'{\"x\":true}')"); err != nil {
		t.Fatal(err)
	}
	before, err := server.binlogSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = server.Close(); err != nil {
		t.Fatal(err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = db.Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	server, err = New(database, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	after, err := server.binlogSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if before.source != after.source || before.serverID != after.serverID || len(before.entries) != len(after.entries) || before.files[0] != after.files[0] {
		t.Fatal(before, after)
	}
	for i := range before.entries {
		if before.entries[i] != after.entries[i] {
			t.Fatal(before.entries, after.entries)
		}
	}
}
func TestBinlogValuesIndependentFixtures(t *testing.T) {
	for _, test := range []struct {
		text             string
		precision, scale int
		hex              string
	}{{"1234567890.1234", 14, 4, "810dfb38d204d2"}, {"-1234567890.1234", 14, 4, "7ef204c72dfb2d"}, {"0.00", 5, 2, "800000"}} {
		encoded, err := binlogDecimal(test.text, test.precision, test.scale)
		if err != nil {
			t.Fatal(err)
		}
		if hex.EncodeToString(encoded) != test.hex {
			t.Fatalf("decimal %s: %x want%s", test.text, encoded, test.hex)
		}
	}
	exact, err := binlogJSON(`{"u":18446744073709551615,"d":1.2500}`)
	if err != nil || !bytes.Contains(exact, bytes.Repeat([]byte{0xff}, 8)) {
		t.Fatal(exact, err)
	}
	if _, err = binlogRow(db.CDCTable{Columns: []sql.ColumnDef{{ColType: sql.ColJSON}}}, []sql.Value{{Type: sql.ColJSON, Text: `{"x":1}`}}); err != nil {
		t.Fatal(err)
	}
}

func TestBinlogIndependentMySQLRemoteClient(t *testing.T) {
	binary, err := exec.LookPath("mysqlbinlog")
	if err != nil {
		t.Skip("mysqlbinlog unavailable")
	}
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	if _, err = s.server.binlogSnapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	mustExec(t, pool, "CREATE TABLE remote_stream(id BIGINT PRIMARY KEY,v DECIMAL(20,3))")
	mustExec(t, pool, "INSERT INTO remote_stream VALUES(42,123456789.125)")
	host, port, err := net.SplitHostPort(s.address)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, binary, "--no-defaults", "--read-from-remote-server", "--protocol=TCP", "--host="+host, "--port="+port, "--user=slopdb", "--base64-output=DECODE-ROWS", "--verify-binlog-checksum", "-vv", binlogFilename(1)).CombinedOutput()
	if err != nil {
		t.Fatalf("independent remote MySQL client: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "123456789.125") {
		t.Fatalf("remote row missing:\n%s", output)
	}
}

func TestBinlogIndependentColumnFamilies(t *testing.T) {
	binary, err := exec.LookPath("mysqlbinlog")
	if err != nil {
		t.Skip("mysqlbinlog unavailable")
	}
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	if _, err = s.server.binlogSnapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	mustExec(t, pool, "CREATE TABLE wire_families(id INT PRIMARY KEY,t TINYINT UNSIGNED,s SMALLINT,m MEDIUMINT,f FLOAT,b BINARY(5),v VARBINARY(12),vc VARCHAR(12),e ENUM('alpha','beta'),d DATE,dt DATETIME(6),ts TIMESTAMP(3))")
	mustExec(t, pool, `INSERT INTO wire_families VALUES(1,255,-1234,-8000000,1.5,X'00FF',X'FE00','hello 🪐','beta','2026-10-25','2026-10-25 12:34:56.123456','2026-10-25 12:34:56.123')`)
	events := readBinlogDump(t, connectRaw(t, s), dumpRequest(binlogFilename(1), 4, true))
	data := []byte{0xfe, 'b', 'i', 'n'}
	for _, event := range events {
		if binaryLittlePosition(event) != 0 {
			data = append(data, event...)
		}
	}
	path := filepath.Join(t.TempDir(), "column.bin")
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command(binary, "--no-defaults", "--verify-binlog-checksum", "--base64-output=DECODE-ROWS", "-vv", path).CombinedOutput()
	if err != nil {
		t.Fatalf("column parser %v\n%s", err, output)
	}
	for _, want := range []string{"255", "-1234", "-8000000", "1.5", "hello", "2026-10-25 12:34:56.123456", "2026-10-25 12:34:56.123"} {
		if !strings.Contains(string(output), want) {
			t.Fatalf("missing %q\n%s", want, output)
		}
	}
}
func binaryLittlePosition(event []byte) uint32 { return binary.LittleEndian.Uint32(event[13:]) }

func TestBinlogRotationAndNamespaceDrop(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	if _, err := s.server.binlogSnapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.server.binlog.mu.Lock()
	s.server.binlog.maxFileBytes = 750
	s.server.binlog.mu.Unlock()
	mustExec(t, pool, "CREATE DATABASE rotated")
	mustExec(t, pool, "CREATE TABLE rotated.t(id BIGINT PRIMARY KEY)")
	for i := 0; i < 4; i++ {
		mustExec(t, pool, "INSERT INTO rotated.t VALUES(?)", i)
	}
	mustExec(t, pool, "DROP DATABASE rotated CASCADE")
	events := readBinlogDump(t, connectRaw(t, s), dumpRequest(binlogFilename(1), 4, true))
	rotations := 0
	formats := 0
	var dropped bool
	for _, event := range events {
		if event[4] == binlogRotate {
			rotations++
		}
		if event[4] == binlogFormat {
			formats++
		}
		if event[4] == binlogQuery && bytes.Contains(event, []byte("DROP DATABASE `rotated`")) {
			dropped = true
		}
	}
	if rotations < 2 || formats < 2 || !dropped {
		t.Fatal(rotations, formats, dropped)
	}
	snapshot, err := s.server.binlogSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range snapshot.files {
		if file.size > 750 {
			t.Fatal("rotation exceeded limit", file)
		}
	}
}

func TestBinlogRejectsAtomicMultipleDDL(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	if _, err := s.server.binlogSnapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.database.Exec("BEGIN; CREATE DATABASE first_atomic; CREATE DATABASE second_atomic; COMMIT"); err != nil {
		t.Fatal(err)
	}
	packet := connectRaw(t, s).command(t, dumpRequest(binlogFilename(1), 4, true))
	if len(packet) < 3 || packet[0] != 0xff || binary.LittleEndian.Uint16(packet[1:]) != 1236 || !bytes.Contains(packet, []byte("atomic DDL")) {
		t.Fatalf("expected explicit replication shape error: %x", packet)
	}
	reader, err := s.database.NewCDCReader(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	transactions, err := reader.Read(context.Background(), 10)
	if err != nil || len(transactions) != 1 || len(transactions[0].CreatedNamespaces) != 2 {
		t.Fatal(transactions, err)
	}
}
