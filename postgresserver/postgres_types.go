package postgresserver

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"slopdb/sql"
)

func postgresCodec(oid uint32) (pgtype.Codec, any) {
	switch oid {
	case oidUUID:
		return pgtype.UUIDCodec{}, &pgtype.UUID{}
	case oidDate:
		return pgtype.DateCodec{}, &pgtype.Date{}
	case oidTime:
		return pgtype.TimeCodec{}, &pgtype.Time{}
	case oidTimestamp:
		return &pgtype.TimestampCodec{}, &pgtype.Timestamp{}
	case oidTimestamptz:
		return &pgtype.TimestamptzCodec{}, &pgtype.Timestamptz{}
	case oidInterval:
		return pgtype.IntervalCodec{}, &pgtype.Interval{}
	}
	return nil, nil
}
func postgresOIDName(oid uint32) string {
	switch oid {
	case oidUUID:
		return "UUID"
	case oidDate:
		return "DATE"
	case oidTime:
		return "TIME"
	case oidTimestamp:
		return "TIMESTAMP"
	case oidTimestamptz:
		return "TIMESTAMPTZ"
	case oidInterval:
		return "INTERVAL"
	case oidTimetz:
		return "TIMETZ"
	}
	return ""
}
func encodePostgresTypedValue(v sql.Value, oid uint32, binaryFormat bool) ([]byte, error) {
	if oid == oidTimetz {
		return encodeTimetz(v.Text, binaryFormat)
	}
	codec, target := postgresCodec(oid)
	if codec == nil {
		return nil, fmt.Errorf("unsupported PostgreSQL type %d", oid)
	}
	switch val := target.(type) {
	case *pgtype.UUID:
		if len(v.Blob) != 16 {
			return nil, fmt.Errorf("invalid UUID result")
		}
		copy(val.Bytes[:], v.Blob)
		val.Valid = true
	case *pgtype.Interval:
		m, d, u, e := sql.ParsePostgresInterval(v.Text)
		if e != nil {
			return nil, e
		}
		*val = pgtype.Interval{Months: m, Days: d, Microseconds: u, Valid: true}
	default:
		if err := codec.PlanScan(nil, oid, pgtype.TextFormatCode, target).Scan([]byte(v.Text), target); err != nil {
			return nil, err
		}
	}
	format := int16(pgtype.TextFormatCode)
	if binaryFormat {
		format = pgtype.BinaryFormatCode
	}
	plan := codec.PlanEncode(nil, oid, format, target)
	if plan == nil {
		return nil, fmt.Errorf("unsupported PostgreSQL result type %d", oid)
	}
	return plan.Encode(target, nil)
}
func decodePostgresTypedValue(data []byte, oid uint32, binaryFormat bool) (any, error) {
	if !binaryFormat {
		return sql.CastPostgresValue(sql.NewTextValue(string(data)), postgresOIDName(oid))
	}
	if oid == oidTimetz {
		if len(data) != 12 {
			return nil, &protocolError{"22P03", "invalid binary TIMETZ length"}
		}
		us := int64(binary.BigEndian.Uint64(data))
		west := int32(binary.BigEndian.Uint32(data[8:]))
		if us < 0 || us > 86400e6 || west < -15*3600-59*60-59 || west > 15*3600+59*60+59 {
			return nil, &protocolError{"22008", "TIMETZ out of range"}
		}
		sec := us / 1e6
		fraction := us % 1e6
		text := fmt.Sprintf("%02d:%02d:%02d", sec/3600, sec/60%60, sec%60)
		if fraction > 0 {
			text += strings.TrimRight(fmt.Sprintf(".%06d", fraction), "0")
		}
		east := -west
		sign := "+"
		if east < 0 {
			sign = "-"
			east = -east
		}
		text += fmt.Sprintf("%s%02d:%02d", sign, east/3600, east/60%60)
		if east%60 != 0 {
			text += fmt.Sprintf(":%02d", east%60)
		}
		return sql.Value{Type: sql.ColText, Text: text, SemanticType: sql.TypeTimeTZ}, nil
	}
	codec, target := postgresCodec(oid)
	if err := codec.PlanScan(nil, oid, pgtype.BinaryFormatCode, target).Scan(data, target); err != nil {
		return nil, &protocolError{"22P03", err.Error()}
	}
	switch val := target.(type) {
	case *pgtype.UUID:
		v := sql.NewBlobValue(append([]byte(nil), val.Bytes[:]...))
		v = v.WithSemanticType(sql.TypeUUID)
		return v, nil
	case *pgtype.Interval:
		return sql.Value{Type: sql.ColText, Text: sql.FormatPostgresInterval(val.Months, val.Days, val.Microseconds), SemanticType: sql.TypeInterval}, nil
	}
	text, err := codec.PlanEncode(nil, oid, pgtype.TextFormatCode, target).Encode(target, nil)
	if err != nil {
		return nil, err
	}
	return sql.CastPostgresValue(sql.NewTextValue(string(text)), postgresOIDName(oid))
}
func encodeTimetz(text string, binaryFormat bool) ([]byte, error) {
	if !binaryFormat {
		return []byte(text), nil
	}
	is24 := strings.HasPrefix(text, "24:00:00")
	parsedText := text
	if is24 {
		parsedText = "00" + text[2:]
	}
	t, e := time.Parse("15:04:05.999999-07:00:00", parsedText)
	if e != nil {
		t, e = time.Parse("15:04:05.999999-07:00", parsedText)
	}
	if e != nil {
		return nil, e
	}
	_, offset := t.Zone()
	us := int64(t.Hour()*3600+t.Minute()*60+t.Second())*1e6 + int64(t.Nanosecond()/1000)
	if is24 {
		us += 86400e6
	}
	result := binary.BigEndian.AppendUint64(nil, uint64(us))
	return binary.BigEndian.AppendUint32(result, uint32(-offset)), nil
}
