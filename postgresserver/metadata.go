package postgresserver

import (
	"context"
	"fmt"
	"slopdb/sql"
	"strings"
)

// The SQL engine owns all AST traversal and parameter/result inference. This
// adapter only maps its neutral types to PostgreSQL wire identifiers.
type metadata struct {
	client   *client
	ctx      context.Context
	prepared *prepared
	tables   map[string][]field
}

func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
func metadataFields(columns []sql.ColumnDef) []field {
	out := make([]field, len(columns))
	for i, c := range columns {
		oid := sql.PostgresOID(c.LogicalType())
		if oid == 0 {
			oid = oidText
		}
		out[i] = field{c.ResultName(), oid}
	}
	return out
}
func (m *metadata) table(name string, scope map[string][]field, depth int) ([]field, error) {
	if name == "" {
		return nil, nil
	}
	name = strings.ToLower(name)
	if f, ok := scope[name]; ok {
		return f, nil
	}
	if f, ok := m.tables[name]; ok {
		return f, nil
	}
	if depth > 64 {
		return nil, &protocolError{"54001", "metadata nesting limit exceeded"}
	}
	parts := strings.Split(name, ".")
	for i := range parts {
		parts[i] = "\"" + strings.ReplaceAll(parts[i], "\"", "\"\"") + "\""
	}
	p, err := m.client.session.PrepareContext(m.ctx, "SELECT * FROM "+strings.Join(parts, "."))
	if err != nil {
		return nil, err
	}
	defer p.Close()
	d, err := m.client.session.DescribePreparedTypes(m.client.postgresContext(m.ctx), p, sql.DescribeOptions{})
	if err != nil {
		return nil, err
	}
	fields := metadataFields(d.Results)
	if m.tables != nil {
		m.tables[name] = fields
	}
	return fields, nil
}
func (m *metadata) statement(_ sql.Statement, _ map[string][]field, depth int) ([]field, error) {
	if depth > 64 {
		return nil, &protocolError{"54001", "metadata nesting limit exceeded"}
	}
	if m.prepared == nil {
		return nil, fmt.Errorf("metadata requires a prepared source")
	}
	p := m.prepared
	stmt := p.stmt
	if stmt == nil {
		var err error
		stmt, err = m.client.session.PrepareContext(m.ctx, p.native)
		if err != nil {
			return nil, err
		}
		defer stmt.Close()
	}
	opts := sql.DescribeOptions{ParameterPositions: p.positions, ParameterTypes: make([]sql.LogicalType, len(p.params))}
	for i, oid := range p.params {
		if oid != 0 {
			opts.ParameterTypes[i] = sql.PostgresTypeDefinition(oid).LogicalType()
		}
	}
	d, err := m.client.session.DescribePreparedTypes(m.client.postgresContext(m.ctx), stmt, opts)
	if err != nil {
		return nil, err
	}
	for i, t := range d.Parameters {
		if i < len(p.params) && p.params[i] == 0 {
			p.params[i] = sql.PostgresOID(t)
		}
	}
	if d.Kind != "" {
		p.kind = d.Kind
	}
	return metadataFields(d.Results), nil
}
