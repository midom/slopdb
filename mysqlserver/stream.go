package mysqlserver

import (
	"slopdb/db"
	"slopdb/sql"
	"time"
)

type streamWriteError struct{ error }

// Only catalog-typed base-column projections can advertise fixed types before
// reading every row. Expression/view/mixed shapes retain materialized inference.
func (c *client) streamQuery(query string) (bool, error) {
	statements, e := sql.ParseWithOptions(query, c.currentDatabase().ParseOptions())
	if e != nil || len(statements) != 1 {
		return false, nil
	}
	stmt, ok := statements[0].(*sql.SelectStmt)
	if !ok || stmt.From == "" || stmt.FromExpr != nil || stmt.Compound != nil || stmt.With != nil || len(stmt.Joins) != 0 || stmt.Virtual != nil || stmt.Lock != nil || stmt.Distinct || len(stmt.GroupBy) != 0 || stmt.Having != nil {
		return false, nil
	}
	for _, column := range stmt.Columns {
		if column.Star {
			continue
		}
		if _, ok := column.Expr.(*sql.ColumnRefExpr); !ok {
			return false, nil
		}
	}
	c.beginDiagnostics(query)
	if e = c.authorize(stmt); e != nil {
		return true, e
	}
	if !c.autocommit && !c.session.InTransaction() {
		if _, e = c.session.ExecContext(c.ctx, "BEGIN"); e != nil {
			return true, e
		}
	}
	status := c.status()
	eof := func() error { return c.writer.write(append16(append16([]byte{0xfe}, c.warningCount()), status)) }
	rows, e := c.session.QueryContext(c.mysqlContext(), query)
	if e != nil {
		return true, e
	}
	defer rows.Close()
	stop := rows.OnCancel(func() { _ = c.conn.SetWriteDeadline(time.Now()) })
	defer func() { stop(); _ = c.conn.SetWriteDeadline(time.Time{}) }()
	columns := rows.Columns()
	if rows.Materialized() {
		result := &db.ExecResult{Columns: columns}
		for rows.Next() {
			values, e := rows.Values()
			if e != nil {
				return true, e
			}
			row := make(map[string]sql.Value, len(columns))
			for i, col := range columns {
				row[col.Name] = values[i]
			}
			result.Rows = append(result.Rows, row)
		}
		if e = rows.Err(); e != nil {
			return true, e
		}
		if e = c.result(result, false); e != nil {
			return true, &streamWriteError{e}
		}
		return true, nil
	}
	if e = c.writer.write(appendLen(nil, uint64(len(columns)))); e != nil {
		return true, &streamWriteError{e}
	}
	for _, col := range columns {
		if e = c.writer.write(columnPacket(col, c.selected)); e != nil {
			return true, &streamWriteError{e}
		}
	}
	if e = eof(); e != nil {
		return true, &streamWriteError{e}
	}
	for rows.Next() {
		values, e := rows.Values()
		if e != nil {
			return true, e
		}
		var packet []byte
		for _, v := range values {
			if v.Null {
				packet = append(packet, 0xfb)
			} else {
				packet = appendString(packet, valueBytes(v))
			}
		}
		if e = c.writer.write(packet); e != nil {
			return true, &streamWriteError{e}
		}
	}
	if e = rows.Err(); e != nil {
		return true, e
	}
	if e = eof(); e != nil {
		return true, &streamWriteError{e}
	}
	return true, nil
}
