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
	oidBool    uint32 = 16
	oidBytea   uint32 = 17
	oidInt8    uint32 = 20
	oidInt2    uint32 = 21
	oidInt4    uint32 = 23
	oidText    uint32 = 25
	oidFloat4  uint32 = 700
	oidFloat8  uint32 = 701
	oidVarchar uint32 = 1043
	oidNumeric uint32 = 1700
	oidJSON    uint32 = 114
	oidJSONB   uint32 = 3802
)

type field struct {
	name string
	oid  uint32
}
type prepared struct {
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
	if strings.HasPrefix(upper, "DECIMAL(") || strings.HasPrefix(upper, "NUMERIC(") {
		return upper, oidNumeric
	}
	switch upper {
	case "DECIMAL", "NUMERIC":
		return "DECIMAL", oidNumeric
	case "JSON":
		return "JSON", oidJSON
	case "JSONB":
		return "JSON", oidJSONB
	case "INT", "INTEGER", "INT4":
		return "INTEGER", oidInt4
	case "BIGINT", "INT8":
		return "BIGINT", oidInt8
	case "SMALLINT", "INT2":
		return "INTEGER", oidInt2
	case "FLOAT8", "DOUBLE":
		return "REAL", oidFloat8
	case "REAL", "FLOAT4":
		return "REAL", oidFloat4
	case "TEXT", "VARCHAR":
		return "TEXT", oidText
	case "BYTEA", "BLOB":
		return "BLOB", oidBytea
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
		if strings.HasPrefix(query[end:], "::") {
			// PostgreSQL parameter casts are translated without changing identifiers.
			first := tokenizer.Next()
			second := tokenizer.Next()
			typ := tokenizer.Next()
			if first.Start != end || second.Start != end+1 || first.Value != ":" || second.Value != ":" || (typ.Type != sql.TokIdent && typ.Type != sql.TokKeyword) {
				return "", nil, nil, &protocolError{"42601", "invalid parameter cast"}
			}
			native, oid := typeName(typ.Value)
			if oid == 0 {
				return "", nil, nil, &protocolError{"0A000", "unsupported parameter cast: " + typ.Value}
			}
			replacement = "CAST(? AS " + native + ")"
			end = tokenizer.Pos()
			if hints[int(n)-1] == 0 {
				hints[int(n)-1] = oid
			}
		}
		out.WriteString(query[last:start])
		out.WriteString(replacement)
		last = end
		positions = append(positions, int(n)-1)
	}
	out.WriteString(query[last:])
	return out.String(), positions, hints, nil
}
func (c *client) prepare(ctx context.Context, query string, oids []uint32) (*prepared, error) {
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
	parsed, err := sql.Prepare(native)
	if err != nil {
		return nil, err
	}
	markers := make([]sql.Value, len(positions))
	for i := range markers {
		markers[i] = sql.NewIntValue(int64(i + 1))
	}
	bound, err := parsed.Bind(markers)
	if err != nil {
		return nil, err
	}
	defer bound.Release()
	if parsed.NumInput() != len(positions) {
		return nil, errMalformed
	}
	p.kind = kindOf(bound.Statements()[0], p.kind)
	metadata := &metadata{client: c, ctx: ctx, prepared: p, tables: make(map[string][]field)}
	fields, err := metadata.statement(bound.Statements()[0], nil, 0)
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
	p.stmt, err = c.session.PrepareContext(ctx, native)
	if err != nil {
		return nil, err
	}
	return p, nil
}
func supportedOID(oid uint32) bool {
	switch oid {
	case oidBool, oidBytea, oidInt8, oidInt2, oidInt4, oidText, oidFloat4, oidFloat8, oidVarchar, oidNumeric, oidJSON, oidJSONB:
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
