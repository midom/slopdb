package mysqlserver

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"math"
	"strconv"
	"strings"
	"time"

	"slopdb/db"
	"slopdb/sql"
)

const (
	binlogQuery         byte = 2
	binlogRotate        byte = 4
	binlogFormat        byte = 15
	binlogXID           byte = 16
	binlogTableMap      byte = 19
	binlogWriteRows     byte = 30
	binlogUpdateRows    byte = 31
	binlogDeleteRows    byte = 32
	binlogGTID          byte = 33
	binlogPreviousGTIDs byte = 35
	binlogHeaderSize         = 19
)

type binlogEvent struct {
	kind byte
	time uint32
	data []byte
}

func (e binlogEvent) size() uint32 { return uint32(binlogHeaderSize + len(e.data) + 4) }
func (e binlogEvent) encode(serverID, position uint32) []byte {
	p := make([]byte, binlogHeaderSize, binlogHeaderSize+len(e.data)+4)
	binary.LittleEndian.PutUint32(p, e.time)
	p[4] = e.kind
	binary.LittleEndian.PutUint32(p[5:], serverID)
	binary.LittleEndian.PutUint32(p[9:], e.size())
	binary.LittleEndian.PutUint32(p[13:], position)
	p = append(p, e.data...)
	return binary.LittleEndian.AppendUint32(p, crc32.ChecksumIEEE(p))
}
func binlogFormatEvent() binlogEvent {
	p := binary.LittleEndian.AppendUint16(nil, 4)
	version := make([]byte, 50)
	copy(version, "8.0.36-slopdb")
	p = append(p, version...)
	p = binary.LittleEndian.AppendUint32(p, 0)
	p = append(p, binlogHeaderSize)
	headers := make([]byte, 40)
	headers[binlogQuery-1] = 13
	headers[binlogRotate-1] = 8
	headers[binlogTableMap-1] = 8
	headers[binlogWriteRows-1] = 10
	headers[binlogUpdateRows-1] = 10
	headers[binlogDeleteRows-1] = 10
	headers[binlogGTID-1] = 42
	p = append(p, headers...)
	p = append(p, 1) // BINLOG_CHECKSUM_ALG_CRC32
	return binlogEvent{kind: binlogFormat, data: p}
}
func binlogRotateEvent(file uint32, timestamp uint32) binlogEvent {
	p := binary.LittleEndian.AppendUint64(nil, 4)
	p = append(p, binlogFilename(file)...)
	return binlogEvent{kind: binlogRotate, time: timestamp, data: p}
}
func binlogPreviousEvent(source string, previous uint64) binlogEvent {
	data := binary.LittleEndian.AppendUint64(nil, 0)
	if previous > 0 {
		data = binary.LittleEndian.AppendUint64(nil, 1)
		sid, _ := hex.DecodeString(source)
		data = append(data, sid...)
		data = binary.LittleEndian.AppendUint64(data, 1)
		data = binary.LittleEndian.AppendUint64(data, 1)
		data = binary.LittleEndian.AppendUint64(data, previous+1)
	}
	return binlogEvent{kind: binlogPreviousGTIDs, data: data}
}
func binlogQueryEvent(schema, query string, timestamp uint32) binlogEvent {
	p := make([]byte, 13)
	p[8] = byte(len(schema))                                 // thread, execution time, schema length, error, status length
	status := binary.LittleEndian.AppendUint64([]byte{1}, 0) // Q_SQL_MODE_CODE, standard MySQL quoting.
	status = append(status, 4)                               // Q_CHARSET_CODE
	status = binary.LittleEndian.AppendUint16(status, 45)
	status = binary.LittleEndian.AppendUint16(status, 46)
	status = binary.LittleEndian.AppendUint16(status, 46)
	binary.LittleEndian.PutUint16(p[11:], uint16(len(status)))
	p = append(p, status...)
	p = append(p, schema...)
	p = append(p, 0)
	p = append(p, query...)
	return binlogEvent{kind: binlogQuery, time: timestamp, data: p}
}
func binlogIdentifier(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }
func binlogTableName(table db.ChangeTable) string {
	return binlogIdentifier(table.Schema) + "." + binlogIdentifier(table.Name)
}
func binlogColumnType(column sql.ColumnDef) byte {
	switch column.ColType {
	case sql.ColInteger, sql.ColUnsigned:
		switch binlogIntegerWidth(column) {
		case 1:
			return typeTiny
		case 2:
			return typeShort
		case 3:
			return binlogInt24
		case 4:
			return typeLong
		default:
			return typeLongLong
		}
	case sql.ColReal:
		if column.Declared == "FLOAT" {
			return typeFloat
		}
		return typeDouble
	case sql.ColDecimal:
		return typeNewDecimal
	case sql.ColJSON:
		return typeJSON
	default:
		switch column.Declared {
		case "DATE":
			return typeDate
		case "DATETIME":
			return binlogDatetime2
		case "TIMESTAMP":
			return binlogTimestamp2
		case "ENUM":
			return typeFixedString
		}
		if column.FixedBinary {
			return typeFixedString
		}
		if column.Length > 0 && (column.Declared == "" || column.Declared == "VARCHAR" || column.Declared == "VARBINARY") && binlogStringMax(column) <= 65535 {
			return binlogVarchar
		}
		return typeBlob
	}
}
func binlogTableEvent(table db.ChangeTable, timestamp uint32) (binlogEvent, error) {
	if table.ID >= 1<<48 || len(table.Schema) > 255 || len(table.Name) > 255 {
		return binlogEvent{}, fmt.Errorf("table identity exceeds MySQL binlog limits")
	}
	p := make([]byte, 8)
	for i := 0; i < 6; i++ {
		p[i] = byte(table.ID >> uint(8*i))
	}
	p = append(p, byte(len(table.Schema)))
	p = append(p, table.Schema...)
	p = append(p, 0, byte(len(table.Name)))
	p = append(p, table.Name...)
	p = append(p, 0)
	p = appendLen(p, uint64(len(table.Columns)))
	var metadata []byte
	nulls := make([]byte, (len(table.Columns)+7)/8)
	var signedness []bool
	for i, column := range table.Columns {
		kind := binlogColumnType(column)
		p = append(p, kind)
		metadata = append(metadata, binlogColumnMetadata(column)...)
		if !column.NotNull {
			nulls[i/8] |= 1 << uint(i%8)
		}
		if column.ColType == sql.ColInteger || column.ColType == sql.ColUnsigned || column.ColType == sql.ColReal || column.ColType == sql.ColDecimal {
			signedness = append(signedness, column.Unsigned || column.ColType == sql.ColUnsigned)
		}
	}
	p = appendLen(p, uint64(len(metadata)))
	p = append(p, metadata...)
	p = append(p, nulls...)
	// SIGNEDNESS optional metadata uses most significant bit first and includes
	// one bit per numeric column, not per table column.
	if len(signedness) > 0 {
		bits := make([]byte, (len(signedness)+7)/8)
		for i, unsigned := range signedness {
			if unsigned {
				bits[i/8] |= 1 << uint(7-i%8)
			}
		}
		p = append(p, 1)
		p = appendLen(p, uint64(len(bits)))
		p = append(p, bits...)
	}
	// COLUMN_NAME makes the stream self-describing for CDC consumers.
	var names []byte
	for _, column := range table.Columns {
		names = appendLen(names, uint64(len(column.Name)))
		names = append(names, column.Name...)
	}
	p = append(p, 4)
	p = appendLen(p, uint64(len(names)))
	p = append(p, names...)
	// COLUMN_CHARSET distinguishes binary blobs from Unicode text even though
	// both use MYSQL_TYPE_BLOB in row events.
	var charsets []byte
	for _, column := range table.Columns {
		kind := binlogColumnType(column)
		if kind == typeBlob || kind == binlogVarchar || (kind == typeFixedString && column.Declared != "ENUM") {
			collation := uint64(46) // utf8mb4_bin
			if column.ColType == sql.ColBlob || column.Binary {
				collation = 63
			}
			charsets = appendLen(charsets, collation)
		}
	}
	if len(charsets) > 0 {
		p = append(p, 3)
		p = appendLen(p, uint64(len(charsets)))
		p = append(p, charsets...)
	}
	return binlogEvent{kind: binlogTableMap, time: timestamp, data: p}, nil
}
func binlogRow(table db.ChangeTable, row []sql.Value) ([]byte, error) {
	if len(row) != len(table.Columns) {
		return nil, fmt.Errorf("CDC row width mismatch for %s", binlogTableName(table))
	}
	p := make([]byte, (len(row)+7)/8)
	for i, value := range row {
		if value.Null {
			p[i/8] |= 1 << uint(i%8)
			continue
		}
		var data []byte
		var err error
		column := table.Columns[i]
		switch kind := binlogColumnType(column); kind {
		case typeTiny, typeShort, binlogInt24, typeLong, typeLongLong:
			n := uint64(value.Int)
			if value.Type == sql.ColUnsigned {
				n = value.Uint
			}
			p = appendBinlogLittleEndian(p, n, binlogIntegerWidth(column))
		case typeFloat:
			p = binary.LittleEndian.AppendUint32(p, math.Float32bits(float32(value.Real)))
		case typeDouble:
			n := value.Real
			if value.Type == sql.ColInteger {
				n = float64(value.Int)
			}
			p = binary.LittleEndian.AppendUint64(p, math.Float64bits(n))
		case typeNewDecimal:
			data, err = binlogDecimal(value.Text, column.Precision, column.Scale)
			p = append(p, data...)
		case typeJSON:
			data, err = binlogJSON(value.Text)
			p = binary.LittleEndian.AppendUint32(p, uint32(len(data)))
			p = append(p, data...)
		case typeDate, binlogTimestamp2, binlogDatetime2:
			data, err = binlogTemporal(value.Text, column)
			p = append(p, data...)
		case typeFixedString:
			if column.Declared == "ENUM" {
				ordinal := 0
				for i, item := range column.EnumValues {
					if item == value.Text {
						ordinal = i + 1
						break
					}
				}
				if ordinal == 0 {
					err = fmt.Errorf("ENUM row does not match metadata")
				}
				width := 1
				if len(column.EnumValues) > 255 {
					width = 2
				}
				p = appendBinlogLittleEndian(p, uint64(ordinal), width)
			} else {
				data = valueBytes(value)
				width := 1
				if column.Length > 255 {
					width = 2
				}
				p = appendBinlogLittleEndian(p, uint64(len(data)), width)
				p = append(p, data...)
			}
		case binlogVarchar:
			data = valueBytes(value)
			width := 1
			if binlogStringMax(column) > 255 {
				width = 2
			}
			p = appendBinlogLittleEndian(p, uint64(len(data)), width)
			p = append(p, data...)
		default:
			data = valueBytes(value)
			width := binlogBlobLengthSize(column)
			if uint64(len(data)) > uint64(1)<<(uint(width)*8)-1 {
				err = fmt.Errorf("string exceeds MySQL binary row field capacity")
			}
			p = appendBinlogLittleEndian(p, uint64(len(data)), width)
			p = append(p, data...)
		}

		if err != nil {
			return nil, fmt.Errorf("CDC %s.%s: %w", binlogTableName(table), table.Columns[i].Name, err)
		}
	}
	return p, nil
}
func binlogRowsEvent(table db.ChangeTable, change db.RowChange, timestamp uint32) (binlogEvent, error) {
	kind := binlogWriteRows
	if change.Kind == "update" {
		kind = binlogUpdateRows
	} else if change.Kind == "delete" {
		kind = binlogDeleteRows
	} else if change.Kind != "insert" {
		return binlogEvent{}, fmt.Errorf("unknown CDC row operation %q", change.Kind)
	}
	p := make([]byte, 10)
	for i := 0; i < 6; i++ {
		p[i] = byte(table.ID >> uint(8*i))
	}
	p[6] = 1 | 2 // STMT_END_F | NO_FOREIGN_KEY_CHECKS_F; source cascades are explicit rows.
	p[8] = 2     // STMT_END_F and extra-data length includes itself.
	p = appendLen(p, uint64(len(table.Columns)))
	present := make([]byte, (len(table.Columns)+7)/8)
	for i := range table.Columns {
		present[i/8] |= 1 << uint(i%8)
	}
	p = append(p, present...)
	if kind == binlogUpdateRows {
		p = append(p, present...)
	}
	if kind != binlogWriteRows {
		before, err := binlogRow(table, change.Before)
		if err != nil {
			return binlogEvent{}, err
		}
		p = append(p, before...)
	}
	if kind != binlogDeleteRows {
		after, err := binlogRow(table, change.After)
		if err != nil {
			return binlogEvent{}, err
		}
		p = append(p, after...)
	}
	return binlogEvent{kind: kind, time: timestamp, data: p}, nil
}
func binlogTransaction(source string, transaction db.ChangeTransaction) ([]binlogEvent, error) {
	timestamp := uint32(time.Unix(0, transaction.Time).Unix())
	sid, err := hex.DecodeString(source)
	if err != nil || len(sid) != 16 {
		return nil, fmt.Errorf("invalid CDC source UUID")
	}
	if transaction.Sequence == 0 || transaction.Sequence >= math.MaxInt64 {
		return nil, fmt.Errorf("CDC sequence exceeds MySQL GTID range")
	}
	gtid := append([]byte{1}, sid...)
	gtid = binary.LittleEndian.AppendUint64(gtid, transaction.Sequence)
	gtid = append(gtid, 2)
	gtid = binary.LittleEndian.AppendUint64(gtid, transaction.Sequence-1)
	gtid = binary.LittleEndian.AppendUint64(gtid, transaction.Sequence)
	gtid = appendBinlogLittleEndian(gtid, uint64(transaction.Time/1000), 7)
	events := []binlogEvent{{kind: binlogGTID, time: timestamp, data: gtid}}
	var ddl []binlogEvent
	droppedTables := make(map[uint64]bool)
	for _, change := range transaction.SchemaChanges {
		if change.Before != nil && change.After == nil {
			droppedTables[change.Before.ID] = true
		}
	}
	changes := make([]db.RowChange, 0, len(transaction.Changes))
	for _, change := range transaction.Changes {
		if !droppedTables[change.TableID] {
			changes = append(changes, change)
		}
	}
	dropped := make(map[string]bool, len(transaction.DroppedNamespaces))
	for _, schema := range transaction.DroppedNamespaces {
		dropped[schema] = true
	}
	for _, schema := range transaction.CreatedNamespaces {
		if schema != "main" {
			ddl = append(ddl, binlogQueryEvent("", "CREATE DATABASE "+binlogIdentifier(schema), timestamp))
		}
	}
	for _, change := range transaction.SchemaChanges {
		if change.Before != nil && change.After == nil {
			if !dropped[change.Before.Schema] {
				ddl = append(ddl, binlogQueryEvent(change.Before.Schema, "DROP TABLE "+binlogTableName(*change.Before), timestamp))
			}
			continue
		}
		if change.Before == nil && change.After != nil {
			// The complete declaration is supplied by the SQL catalog; see the helper
			// in binlog_schema.go for supported MySQL projection of native metadata.
			query, err := binlogCreateTable(*change.After)
			if err != nil {
				return nil, err
			}
			ddl = append(ddl, binlogQueryEvent(change.After.Schema, query, timestamp))
			continue
		}
		if change.Before != nil && change.After != nil {
			queries, err := binlogAlterTable(*change.Before, *change.After)
			if err != nil {
				return nil, err
			}
			for _, query := range queries {
				ddl = append(ddl, binlogQueryEvent(change.After.Schema, query, timestamp))
			}
		}
	}
	for _, schema := range transaction.DroppedNamespaces {
		if schema != "main" {
			ddl = append(ddl, binlogQueryEvent("", "DROP DATABASE "+binlogIdentifier(schema), timestamp))
		}
	}
	if len(ddl) > 1 || len(ddl) > 0 && len(changes) > 0 {
		return nil, fmt.Errorf("CDC transaction %d combines atomic DDL with other changes; MySQL implicit-DDL transactions cannot represent it", transaction.Sequence)
	}
	singleDDL := len(ddl) == 1 && len(changes) == 0
	if !singleDDL {
		events = append(events, binlogQueryEvent("", "BEGIN", timestamp))
	}
	events = append(events, ddl...)
	if len(ddl) == 0 {
		events[0].data[0] = 0
	} // FLAG_MAY_HAVE_SBR is clear for pure row transactions.
	tables := make(map[uint64]db.ChangeTable, len(transaction.Tables))
	for _, table := range transaction.Tables {
		tables[table.ID] = table
	}
	for _, change := range changes {
		table, ok := tables[change.TableID]
		if !ok {
			return nil, fmt.Errorf("CDC table %d metadata missing", change.TableID)
		}
		mapping, err := binlogTableEvent(table, timestamp)
		if err != nil {
			return nil, err
		}
		row, err := binlogRowsEvent(table, change, timestamp)
		if err != nil {
			return nil, err
		}
		events = append(events, mapping, row)
	}
	if !singleDDL {
		events = append(events, binlogEvent{kind: binlogXID, time: timestamp, data: binary.LittleEndian.AppendUint64(nil, transaction.Sequence)})
	}
	// The length field includes its own variable width. Converge before
	// appending the identical originating/immediate server version (8.0.36).
	var rest uint64
	for _, event := range events[1:] {
		rest += uint64(event.size())
	}
	base := events[0].data
	length := rest + uint64(events[0].size()) + 5
	for {
		encoded := appendLen(nil, length)
		next := rest + uint64(binlogHeaderSize+len(base)+len(encoded)+4+4)
		if next == length {
			events[0].data = append(base, encoded...)
			events[0].data = binary.LittleEndian.AppendUint32(events[0].data, 80036)
			break
		}
		length = next
	}
	return events, nil
}
func binlogFilename(number uint32) string { return "slopdb-bin." + fmt.Sprintf("%06d", number) }
func parseBinlogFilename(name string) (uint32, error) {
	suffix, ok := strings.CutPrefix(name, "slopdb-bin.")
	if !ok {
		return 0, fmt.Errorf("unknown binary log %q", name)
	}
	n, err := strconv.ParseUint(suffix, 10, 32)
	if err != nil || n == 0 || binlogFilename(uint32(n)) != name {
		return 0, fmt.Errorf("unknown binary log %q", name)
	}
	return uint32(n), nil
}
