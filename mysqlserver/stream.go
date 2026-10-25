package mysqlserver

import (
	"context"
	"slopdb/db"
	"slopdb/sql"
	"strings"
	"time"
)

type streamWriteError struct{ error }

// Only catalog-typed base-column projections can advertise fixed types before
// reading every row. Expression/view/mixed shapes retain materialized inference.
func (c *client) streamQuery(query string) (bool, error) {
	if !strings.EqualFold(sql.NewTokenizerWithOptions(query, c.parseOptions()).Next().Value, "SELECT") {
		return false, nil
	}
	statements, e := sql.ParseWithOptions(query, c.parseOptions())
	if e != nil || len(statements) != 1 {
		return false, nil
	}
	stmt, ok := statements[0].(*sql.SelectStmt)
	if !ok || stmt.CalcFoundRows || stmt.From == "" || stmt.FromExpr != nil || stmt.Compound != nil || stmt.With != nil || len(stmt.Joins) != 0 || stmt.Virtual != nil || stmt.Lock != nil || stmt.Distinct || len(stmt.GroupBy) != 0 || stmt.Having != nil {
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
	if !c.autocommit && !c.session.InTransaction() && !sql.IsLocalInspectionStatement(stmt, sql.DialectMySQL) {
		if _, e = c.session.ExecContext(sql.WithActivityQuery(c.mysqlContext(), query), "BEGIN"); e != nil {
			return true, e
		}
	}
	status := c.status()
	eof := func() error { return c.resultTerminatorStatus(nil, status) }
	ctx := sql.WithActivityQuery(c.mysqlContext(), query)
	result, rows, e := c.session.QueryParsedContextInto(ctx, &c.textResult, query, stmt)
	if e != nil {
		return true, e
	}
	if rows != nil {
		defer rows.Close()
	}
	// Keep shutdown cancellation alive through the final response write, even
	// after a point read or normal row exhaustion has released its snapshot.
	closed := make(chan struct{})
	stopClosing := context.AfterFunc(c.session.ClosingContext(), func() {
		defer close(closed)
		_ = c.conn.Close()
	})
	defer func() {
		if !stopClosing() {
			<-closed
		}
	}()
	if result != nil {
		defer func() {
			if !retainPreparedResult(&c.textResult) {
				c.textResult = db.ExecResult{}
			}
		}()
		if e = c.result(result, false); e != nil {
			return true, &streamWriteError{e}
		}
		if e = c.writer.flush(); e != nil {
			return true, &streamWriteError{e}
		}
		return true, nil
	}
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
		if e = c.writer.flush(); e != nil {
			return true, &streamWriteError{e}
		}
		return true, nil
	}
	if e = c.writeResultHeaderStatus(columns, status); e != nil {
		return true, &streamWriteError{e}
	}
	packet := c.resultPacket[:0]
	values := c.streamValues
	defer func() {
		if cap(packet) <= maxRetainedResponseBytes {
			c.resultPacket = packet[:0]
		} else {
			c.resultPacket = nil
		}
		bytes := 0
		for _, value := range values {
			bytes += len(value.Text) + cap(value.Blob)
		}
		if cap(values) <= 64 && bytes <= maxRetainedResponseBytes {
			c.streamValues = values
		} else {
			c.streamValues = nil
		}
	}()
	for rows.Next() {
		values, e = rows.ValuesInto(values)
		if e != nil {
			return true, e
		}
		packet = packet[:0]
		for _, v := range values {
			if v.Null {
				packet = append(packet, 0xfb)
			} else {
				packet = appendTextValue(packet, v)
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
	if e = c.writer.flush(); e != nil {
		return true, &streamWriteError{e}
	}
	return true, nil
}
