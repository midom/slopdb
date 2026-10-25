package postgresserver

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"slopdb/db"
	"slopdb/sql"
)

const (
	oidBool        uint32 = 16
	oidBytea       uint32 = 17
	oidInt8        uint32 = 20
	oidInt2        uint32 = 21
	oidInt4        uint32 = 23
	oidText        uint32 = 25
	oidFloat4      uint32 = 700
	oidFloat8      uint32 = 701
	oidVarchar     uint32 = 1043
	oidNumeric     uint32 = 1700
	oidJSON        uint32 = 114
	oidJSONB       uint32 = 3802
	oidDate        uint32 = 1082
	oidTime        uint32 = 1083
	oidTimestamp   uint32 = 1114
	oidTimestamptz uint32 = 1184
	oidInterval    uint32 = 1186
	oidTimetz      uint32 = 1266
	oidUUID        uint32 = 2950
)

type field struct {
	name string
	oid  uint32
}
type prepared struct {
	copy                   *copyCommand
	stmt                   *db.Stmt
	native, original, kind string
	positions              []int
	params                 []uint32
	fields                 []field
	empty, helper          bool
	references             int
}

func (p *prepared) close() {
	if p.stmt != nil {
		_ = p.stmt.Close()
	}
}
func typeName(name string) (string, uint32) {
	upper := strings.ToUpper(name)
	if oid := sql.PostgresColumnOID(sql.ColumnDef{Declared: upper}); oid != 0 {
		return upper, oid
	}
	switch upper {
	case "BLOB":
		return "BLOB", oidBytea
	case "FLOAT":
		return "FLOAT", oidFloat8
	}
	return "", 0
}

// Preserve quoted text and comments. Dollar markers are recognized only as
// tokens outside literals/identifiers. Repeated and reordered markers retain
// their original PostgreSQL parameter positions.
func translate(query string) (string, []int, map[int]uint32, error) {
	tokenizer := sql.NewTokenizer(query)
	var out strings.Builder
	last := 0
	var positions []int
	hints := make(map[int]uint32)
	for tok := tokenizer.Next(); tok.Type != sql.TokEOF; tok = tokenizer.Next() {
		if tok.Type == sql.TokParameter {
			return "", nil, nil, &protocolError{"42601", "use PostgreSQL $1 parameters"}
		}
		if tok.Type != sql.TokInvalid || tok.Value != "$" {
			continue
		}
		start := tok.Start
		number := tokenizer.Next()
		end := tokenizer.Pos()
		if number.Type != sql.TokInt || number.Start != start+1 {
			return "", nil, nil, &protocolError{"0A000", "dollar-quoted SQL and nonnumeric dollar markers are unsupported"}
		}
		n, err := strconv.ParseInt(number.Value, 10, 32)
		if err != nil || n < 1 || n > 65535 || len(positions) >= 65535 {
			return "", nil, nil, &protocolError{"54000", "invalid or excessive PostgreSQL parameters"}
		}
		replacement := "?"
		out.WriteString(query[last:start])
		out.WriteString(replacement)
		last = end
		positions = append(positions, int(n)-1)
	}
	out.WriteString(query[last:])
	return out.String(), positions, hints, nil
}
func (c *client) prepare(ctx context.Context, query string, oids []uint32) (*prepared, error) {
	copyCommand, copyErr := parseCopy(query)
	if copyErr != nil {
		return nil, copyErr
	}
	if copyCommand != nil {
		if len(oids) > 0 {
			return nil, &protocolError{"0A000", "COPY parameters are unsupported"}
		}
		if _, _, err := c.copyFields(ctx, copyCommand); err != nil {
			return nil, err
		}
		return &prepared{copy: copyCommand, kind: "COPY", original: query, native: query}, nil
	}
	native, positions, hints, err := translate(query)
	if err != nil {
		return nil, err
	}
	p := &prepared{native: native, original: query, positions: positions}
	for _, index := range positions {
		if index+1 > len(p.params) {
			p.params = append(p.params, make([]uint32, index+1-len(p.params))...)
		}
	}
	if len(oids) > len(p.params) {
		return nil, &protocolError{"08P01", "too many declared parameter types"}
	}
	copy(p.params, oids)
	for index, oid := range hints {
		if p.params[index] == 0 {
			p.params[index] = oid
		}
	}
	if helper, ok := c.helperMetadata(native); ok {
		if len(positions) != 0 {
			return nil, &protocolError{"0A000", "parameters are unsupported in this session command"}
		}
		p.helper = true
		p.kind = helper.kind
		p.fields = helper.fields
		return p, nil
	}
	tokenizer := sql.NewTokenizer(native)
	first := tokenizer.Next()
	p.kind = strings.ToUpper(first.Value)
	if first.Type == sql.TokEOF {
		p.empty = true
		return p, nil
	}
	p.stmt, err = c.session.PrepareContext(ctx, native)
	if err != nil {
		return nil, err
	}
	successful := false
	defer func() {
		if !successful {
			p.stmt.Close()
		}
	}()
	if p.stmt.NumInput() != len(positions) {
		return nil, errMalformed
	}
	metadata := &metadata{client: c, ctx: ctx, prepared: p, tables: make(map[string][]field)}
	fields, err := metadata.statement(nil, nil, 0)
	if err != nil {
		return nil, err
	}
	p.fields = fields
	for i := range p.params {
		if p.params[i] == 0 {
			p.params[i] = oidText
		}
		if !supportedOID(p.params[i]) {
			return nil, &protocolError{"0A000", fmt.Sprintf("unsupported parameter type OID %d", p.params[i])}
		}
	}
	successful = true
	return p, nil
}
func supportedOID(oid uint32) bool {
	switch oid {
	case oidBool, oidBytea, 19, 26, 2205, 2206, 4089, oidInt8, oidInt2, oidInt4, oidText, oidFloat4, oidFloat8, oidVarchar, oidNumeric, oidJSON, oidJSONB, oidDate, oidTime, oidTimestamp, oidTimestamptz, oidInterval, oidTimetz, oidUUID:
		return true
	}
	return false
}
func nativeOID(typ sql.ColumnType) uint32 {
	switch typ {
	case sql.ColInteger:
		return oidInt8
	case sql.ColUnsigned, sql.ColDecimal:
		return oidNumeric
	case sql.ColJSON:
		return oidJSON
	case sql.ColReal:
		return oidFloat8
	case sql.ColBlob:
		return oidBytea
	default:
		return oidText
	}
}
