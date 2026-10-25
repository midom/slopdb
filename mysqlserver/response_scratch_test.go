package mysqlserver

import (
	"bytes"
	"testing"

	"slopdb/db"
	"slopdb/sql"
)

func TestMySQLResponseScratchPreservesRowsAndUnsignedWidths(t *testing.T) {
	conn := &metadataCaptureConn{}
	c := &client{writer: packetWriter{conn: conn}, autocommit: true, variables: defaultSessionVariables()}
	columns := []sql.ColumnDef{
		{Name: "tiny", ColType: sql.ColUnsigned, IntegerBits: 8, Unsigned: true, SemanticType: sql.TypeUint8},
		{Name: "short", ColType: sql.ColUnsigned, IntegerBits: 16, Unsigned: true, SemanticType: sql.TypeUint16},
		{Name: "long", ColType: sql.ColUnsigned, IntegerBits: 32, Unsigned: true, SemanticType: sql.TypeUint32},
		{Name: "blob", ColType: sql.ColBlob, SemanticType: sql.TypeBinary},
	}
	result := &db.ExecResult{Columns: columns, Rows: []map[string]sql.Value{
		{"tiny": sql.NewUnsignedValue(255), "short": sql.NewUnsignedValue(65535), "long": sql.NewUnsignedValue(4294967295), "blob": sql.NewBlobValue([]byte{1, 2})},
		{"tiny": sql.NewNullValue(), "short": sql.NewUnsignedValue(1), "long": sql.NewUnsignedValue(2), "blob": sql.NewBlobValue([]byte{})},
	}}
	for i := 0; i < 2; i++ {
		conn.output.Reset()
		c.writer.sequence = 0
		c.writer.beginBatch()
		if err := c.result(result, true); err != nil {
			t.Fatal(err)
		}
		if err := c.writer.endBatch(); err != nil {
			t.Fatal(err)
		}
		packets := capturedMetadataPackets(t, conn)
		if len(packets) != 9 || !bytes.Equal(packets[6], []byte{0, 0, 255, 255, 255, 255, 255, 255, 255, 2, 1, 2}) || !bytes.Equal(packets[7], []byte{0, 4, 1, 0, 2, 0, 0, 0, 0}) {
			t.Fatalf("binary row widths, null bits or scratch aliasing changed: %x", packets)
		}
		if len(c.resultPacket) != 0 || cap(c.resultPacket) == 0 || cap(c.resultPacket) > maxRetainedResponseBytes {
			t.Fatal("row scratch was not retained within its bound")
		}
	}
}

func TestMySQLResultInferenceDoesNotMutateCallerMetadata(t *testing.T) {
	conn := &metadataCaptureConn{}
	c := &client{writer: packetWriter{conn: conn}, variables: defaultSessionVariables()}
	result := &db.ExecResult{Columns: []sql.ColumnDef{{Name: "mixed", ColType: sql.ColInteger, NotNull: true}}, Rows: []map[string]sql.Value{
		{"mixed": sql.NewIntValue(7)}, {"mixed": sql.NewTextValue("text")},
	}}
	if err := c.result(result, false); err != nil {
		t.Fatal(err)
	}
	if result.Columns[0].ColType != sql.ColInteger || !result.Columns[0].NotNull {
		t.Fatal("response inference modified caller-owned column metadata")
	}
	if len(c.resultColumns) != 1 || c.resultColumns[0].Name != "" {
		t.Fatal("response inference scratch retained caller metadata")
	}
}

func TestMySQLPreparedResponseRetentionIsBounded(t *testing.T) {
	small := &db.ExecResult{Rows: []map[string]sql.Value{{"b": sql.NewBlobValue([]byte{1})}}}
	if !retainPreparedResult(small) {
		t.Fatal("small prepared result was not reusable")
	}
	large := &db.ExecResult{Rows: []map[string]sql.Value{{"b": sql.NewBlobValue(make([]byte, maxRetainedResponseBytes+1))}}}
	if retainPreparedResult(large) || retainPreparedResult(&db.ExecResult{Rows: make([]map[string]sql.Value, 65)}) {
		t.Fatal("oversized response remained cached on a prepared statement")
	}
	large.Rows = large.Rows[:0]
	if retainPreparedResult(large) {
		t.Fatal("empty point answer retained an oversized hidden row buffer")
	}
}
