package mysqlserver

import (
	"encoding/binary"
	"testing"
)

func TestMySQLPrepareResultMetadataWithoutExecution(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	mustExec(t, pool, "CREATE TABLE metadata_items(id BIGINT UNSIGNED PRIMARY KEY,label VARCHAR(20),b BINARY(4),n FLOAT)")
	c := connectRaw(t, s)
	p := c.command(t, append([]byte{22}, "SELECT id,label,b,n,CAST(? AS CHAR) AS value FROM metadata_items WHERE id=? FOR UPDATE"...))
	if len(p) != 12 || p[0] != 0 || binary.LittleEndian.Uint16(p[5:]) != 5 || binary.LittleEndian.Uint16(p[7:]) != 2 {
		t.Fatalf("prepare header %x", p)
	}
	seq := byte(2)
	for i := 0; i < 3; i++ {
		_, next, err := readPacket(c.conn, seq, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		seq = next
	}
	for i, want := range []byte{typeLongLong, typeString, typeFixedString, typeFloat, typeString} {
		packet, next, err := readPacket(c.conn, seq, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		seq = next
		d := decoder{data: packet}
		for j := 0; j < 6; j++ {
			d.str()
		}
		d.u8()
		d.u16()
		d.u32()
		got := d.u8()
		if d.err != nil || got != want {
			t.Fatalf("column %d: type%d want%d %v", i, got, want, d.err)
		}
	}
	eof, _, err := readPacket(c.conn, seq, 1<<20)
	if err != nil || eof[0] != 0xfe {
		t.Fatal(err)
	}
	if len(s.database.LockWaits()) != 0 {
		t.Fatal("prepare acquired row lock")
	}
	_ = rawPrepare(t, c, "SELECT 1/0 AS division") // An execution error, never evaluated at PREPARE.
	_ = rawPrepare(t, c, "SELECT GET_LOCK('prepare-does-not-run',0)")
	var free int
	if err = pool.QueryRow("SELECT IS_FREE_LOCK('prepare-does-not-run')").Scan(&free); err != nil || free != 1 {
		t.Fatal(free, err)
	}
	expectCode(t, c.command(t, append([]byte{22}, "SELECT nonexistent FROM metadata_items"...)), 1105)
	expectCode(t, c.command(t, append([]byte{22}, "SELECT id FROM nonexistent_table"...)), 1146)
}
func TestMySQLPrepareDerivedMetadataAndSchemaChanges(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	mustExec(t, pool, "CREATE TABLE metadata_source(id INTEGER PRIMARY KEY,label TEXT)")
	mustExec(t, pool, "CREATE VIEW metadata_view AS SELECT id,label FROM metadata_source")
	// Core API allows multiple statements; network clients negotiate that separately.
	c := connectRaw(t, s)
	for _, query := range []string{
		"SELECT * FROM metadata_view",
		"WITH chosen AS (SELECT id,label FROM metadata_source) SELECT * FROM chosen",
		"SELECT id,label FROM metadata_source UNION ALL SELECT id,label FROM metadata_source",
		"SELECT ROW_NUMBER() OVER (ORDER BY id),label FROM metadata_source",
		"SELECT a.id,b.label FROM metadata_source a LEFT JOIN metadata_source b ON a.id=b.id",
		"SELECT a.id,(SELECT b.label FROM metadata_source b WHERE b.id=a.id) AS value FROM metadata_source a",
	} {
		_ = rawPrepare(t, c, query)
	}
	statement, err := pool.Prepare("SELECT * FROM metadata_source")
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Close()
	mustExec(t, pool, "ALTER TABLE metadata_source ADD COLUMN more BLOB")
	rows, err := statement.Query()
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil || len(cols) != 3 {
		t.Fatal(cols, err)
	}
}
