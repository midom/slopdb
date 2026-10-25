package mysqlserver

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"

	"slopdb/sql"
)

const (
	binlogInt24      byte = 9
	binlogVarchar    byte = 15
	binlogTimestamp2 byte = 17
	binlogDatetime2  byte = 18
	binlogEnum       byte = 247
)

func binlogIntegerWidth(column sql.ColumnDef) int {
	if !column.MySQL && !column.Unsigned && (column.Declared == "" || column.Declared == "INTEGER") {
		return 8
	}
	switch column.IntegerBits {
	case 8:
		return 1
	case 16:
		return 2
	case 24:
		return 3
	case 32:
		return 4
	default:
		return 8
	}
}
func binlogStringMax(column sql.ColumnDef) uint64 {
	n := uint64(column.Length)
	if column.ColType == sql.ColText {
		if n > math.MaxUint64/4 {
			return math.MaxUint64
		}
		n *= 4
	}
	return n
}
func binlogColumnMetadata(column sql.ColumnDef) []byte {
	switch kind := binlogColumnType(column); kind {
	case typeNewDecimal:
		return []byte{byte(column.Precision), byte(column.Scale)}
	case typeFloat:
		return []byte{4}
	case typeDouble:
		return []byte{8}
	case typeBlob:
		return []byte{byte(binlogBlobLengthSize(column))}
	case typeJSON:
		return []byte{4}
	case binlogVarchar:
		return binary.LittleEndian.AppendUint16(nil, uint16(binlogStringMax(column)))
	case typeFixedString:
		if column.Declared == "ENUM" {
			width := byte(1)
			if len(column.EnumValues) > 255 {
				width = 2
			}
			return []byte{binlogEnum, width}
		}
		n := uint16(column.Length)
		return []byte{typeFixedString | byte(((n>>4)&0x30)^0x30), byte(n)}
	case binlogTimestamp2, binlogDatetime2:
		return []byte{byte(column.TemporalPrecision)}
	}
	return nil
}
func binlogBlobLengthSize(column sql.ColumnDef) int {
	switch column.Declared {
	case "TINYBLOB", "TINYTEXT":
		return 1
	case "BLOB", "TEXT":
		return 2
	case "MEDIUMBLOB", "MEDIUMTEXT":
		return 3
	default:
		return 4
	}
}
func appendBinlogBigEndian(p []byte, n uint64, width int) []byte {
	for i := width - 1; i >= 0; i-- {
		p = append(p, byte(n>>uint(i*8)))
	}
	return p
}
func appendBinlogLittleEndian(p []byte, n uint64, width int) []byte {
	for i := 0; i < width; i++ {
		p = append(p, byte(n>>uint(i*8)))
	}
	return p
}
func binlogTemporal(value string, column sql.ColumnDef) ([]byte, error) {
	date := time.Time{}
	var err error
	zero := strings.HasPrefix(value, "0000-00-00")
	if !zero {
		layout := "2006-01-02 15:04:05"
		if column.Declared == "DATE" {
			layout = "2006-01-02"
		}
		date, err = time.Parse(layout, value)
		if err != nil {
			return nil, err
		}
	}
	if column.Declared == "DATE" {
		n := uint64(0)
		if !zero {
			n = uint64(date.Year()<<9 | int(date.Month())<<5 | date.Day())
		}
		return appendBinlogLittleEndian(nil, n, 3), nil
	}
	var p []byte
	if column.Declared == "TIMESTAMP" {
		seconds := int64(0)
		if !zero {
			seconds = date.Unix()
		}
		if seconds < 0 || seconds > math.MaxUint32 {
			return nil, fmt.Errorf("TIMESTAMP outside MySQL binary log range")
		}
		p = appendBinlogBigEndian(nil, uint64(seconds), 4)
	} else {
		n := uint64(0)
		if !zero {
			ymd := (date.Year()*13+int(date.Month()))<<5 | date.Day()
			hms := date.Hour()<<12 | date.Minute()<<6 | date.Second()
			n = uint64(ymd)<<17 | uint64(hms)
		}
		p = appendBinlogBigEndian(nil, n+0x8000000000, 5)
	}
	width := (column.TemporalPrecision + 1) / 2
	micro := uint64(date.Nanosecond() / 1000)
	switch width {
	case 0:
		return p, nil
	case 1:
		micro /= 10000
	case 2:
		micro /= 100
	case 3:
	default:
		return nil, fmt.Errorf("invalid temporal precision")
	}
	return appendBinlogBigEndian(p, micro, width), nil
}
