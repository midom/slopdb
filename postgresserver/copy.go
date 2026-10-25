package postgresserver

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"slopdb/sql"
)

type frontendPacket struct {
	kind byte
	body []byte
}
type copyInput struct {
	client  *client
	ctx     context.Context
	pending []byte
	done    bool
}

func (r *copyInput) Read(out []byte) (int, error) {
	if len(out) == 0 {
		return 0, nil
	}
	for len(r.pending) == 0 {
		if r.done {
			return 0, io.EOF
		}
		select {
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		case packet, ok := <-r.client.incoming:
			if !ok {
				return 0, io.ErrUnexpectedEOF
			}
			switch packet.kind {
			case 'd':
				r.pending = packet.body
			case 'c':
				if len(packet.body) != 0 {
					return 0, errMalformed
				}
				r.done = true
			case 'f':
				m, err := decodeMessage(packet.kind, packet.body)
				if err != nil {
					return 0, err
				}
				return 0, &protocolError{"57014", "COPY from stdin failed: " + m.(*pgproto3.CopyFail).Message}
			case 'H', 'S':
				if len(packet.body) != 0 {
					return 0, errMalformed
				}
			case 'X':
				r.client.cancel()
				return 0, io.ErrUnexpectedEOF
			default:
				return 0, &protocolError{"08P01", "unexpected message during COPY from stdin"}
			}
		}
	}
	n := copy(out, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}
func (c *client) copyFields(ctx context.Context, command *copyCommand) ([]field, string, error) {
	if command.query != "" {
		p, err := c.prepare(ctx, command.query, nil)
		if err != nil {
			return nil, "", err
		}
		defer p.close()
		if len(p.params) > 0 || p.kind != "SELECT" {
			return nil, "", &protocolError{"0A000", "COPY query must be an unparameterized SELECT"}
		}
		return append([]field(nil), p.fields...), p.native, nil
	}
	m := metadata{client: c, ctx: ctx, tables: map[string][]field{}}
	fields, err := m.table(command.table, nil, 0)
	if err != nil {
		return nil, "", err
	}
	if len(fields) > 32767 {
		return nil, "", &protocolError{"54000", "too many COPY columns"}
	}
	if len(fields) == 0 {
		return nil, "", &protocolError{"42P01", "COPY table does not exist"}
	}
	if len(command.columns) > 0 {
		selected := make([]field, 0, len(command.columns))
		seen := map[string]bool{}
		for _, name := range command.columns {
			name = strings.ToLower(name)
			if seen[name] {
				return nil, "", &protocolError{"42701", "COPY column specified more than once"}
			}
			seen[name] = true
			found := false
			for _, f := range fields {
				if strings.EqualFold(f.name, name) {
					selected = append(selected, f)
					found = true
					break
				}
			}
			if !found {
				return nil, "", &protocolError{"42703", "unknown COPY column " + name}
			}
		}
		fields = selected
	}
	for _, options := range []map[string]bool{command.forceQuote, command.forceNull, command.forceNotNull} {
		for name := range options {
			if name == "*" {
				continue
			}
			found := false
			for _, f := range fields {
				if f.name == name {
					found = true
				}
			}
			if !found {
				return nil, "", &protocolError{"42703", "unknown COPY option column " + name}
			}
		}
	}
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = copyIdent(f.name)
	}
	// Quoting each qualification component preserves virtual namespace resolution.
	table := command.tableSQL
	if command.from {
		params := make([]string, len(fields))
		for i := range params {
			params[i] = "?"
		}
		return fields, "INSERT INTO " + table + " (" + strings.Join(names, ",") + ") VALUES (" + strings.Join(params, ",") + ")", nil
	}
	return fields, "SELECT " + strings.Join(names, ",") + " FROM " + table, nil
}
func (c *client) runCopy(ctx context.Context, command *copyCommand) (count int64, err error) {
	if c.failed {
		return 0, &protocolError{"25P02", "current transaction is aborted; roll back before continuing"}
	}
	ctx = sql.WithActivityQuery(sql.WithStatementTime(c.postgresContext(ctx), sql.StatementTime(ctx)), c.currentSQL)
	defer func() {
		if err != nil && ctx.Err() != nil {
			err = ctx.Err()
		}
	}()

	// Cancellation must also interrupt a COPY TO blocked behind a slow reader.
	writeCancelled := make(chan struct{})
	stopWriteCancel := context.AfterFunc(ctx, func() { _ = c.conn.SetWriteDeadline(time.Now()); close(writeCancelled) })
	defer func() {
		if !stopWriteCancel() {
			<-writeCancelled
		}
		_ = c.conn.SetWriteDeadline(time.Time{})
	}()
	fields, query, err := c.copyFields(ctx, command)
	if err != nil {
		return 0, err
	}
	own := !c.session.InTransaction()
	savepoint := fmt.Sprintf("slopdb_copy_%d", c.id)
	begin := "SAVEPOINT " + savepoint
	if own {
		begin = "BEGIN"
	}
	if _, err = c.session.ExecContext(ctx, begin); err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			cleanup, cancel := context.WithTimeout(sql.WithActivityQuery(context.Background(), c.currentSQL), 5*time.Second)
			defer cancel()
			if own {
				_, _ = c.session.ExecContext(cleanup, "ROLLBACK")
			} else {
				_, _ = c.session.ExecContext(cleanup, "ROLLBACK TO "+savepoint)
				_, _ = c.session.ExecContext(cleanup, "RELEASE "+savepoint)
			}
		}
	}()
	formats := make([]uint16, len(fields))
	overall := byte(0)
	if command.format == "binary" {
		overall = 1
		for i := range formats {
			formats[i] = 1
		}
	}
	if command.from {

		if err = c.send(&pgproto3.CopyInResponse{OverallFormat: overall, ColumnFormatCodes: formats}); err != nil {
			return 0, err
		}
		input := &copyInput{client: c}
		records := copyRecords{reader: bufio.NewReaderSize(input, 32*1024), command: command, limit: c.server.config.MaxPacketBytes}
		columns := make([]string, len(fields))
		for i, f := range fields {
			columns[i] = f.name
		}
		first := true
		var rowNumber int64
		count, err = c.session.BulkInsert(ctx, command.table, columns, func(call context.Context) ([]any, error) {
			input.ctx = call
			for {
				record, e := records.next(len(fields))
				if e == io.EOF {
					return nil, io.EOF
				}
				if e != nil {
					return nil, copyDataError(e, rowNumber+1)
				}
				if len(record) != len(fields) {
					return nil, &protocolError{"22P04", fmt.Sprintf("COPY row %d has %d fields, expected %d", rowNumber+1, len(record), len(fields))}
				}
				if first && command.header {
					first = false
					if command.headerMatch {
						for i, f := range fields {
							if string(record[i].data) != f.name {
								return nil, &protocolError{"22P04", "COPY header does not match table columns"}
							}
						}
					}
					continue
				}
				first = false
				args := make([]any, len(fields))
				for i, f := range fields {
					args[i], e = command.decode(record[i], f)
					if e != nil {
						return nil, copyDataError(e, rowNumber+1)
					}
				}
				rowNumber++
				return args, nil
			}
		})
		if err != nil {
			return 0, err
		}
	} else {
		rows, e := c.session.QueryContext(ctx, query)
		if e != nil {
			return 0, e
		}
		// Query's snapshot and session gate are released before transaction control.
		defer rows.Close()
		stopIOCancel := rows.OnCancel(func() { _ = c.conn.SetWriteDeadline(time.Now()) })
		defer stopIOCancel()
		if len(rows.Columns()) != len(fields) {
			return 0, &protocolError{"0A000", "COPY query result descriptor changed"}
		}
		if err = c.send(&pgproto3.CopyOutResponse{OverallFormat: overall, ColumnFormatCodes: formats}); err != nil {
			return 0, err
		}
		sendData := func(data []byte) error {
			for len(data) > 0 {
				n := len(data)
				if n > 32*1024 {
					n = 32 * 1024
				}
				if e := c.send(&pgproto3.CopyData{Data: data[:n]}); e != nil {
					return e
				}
				data = data[n:]
			}
			return nil
		}
		if command.format == "binary" {
			header := append([]byte(nil), copySignature...)
			header = binary.BigEndian.AppendUint32(header, 0)
			header = binary.BigEndian.AppendUint32(header, 0)
			if err = sendData(header); err != nil {
				return 0, err
			}
		}
		if command.header {
			values := make([]sql.Value, len(fields))
			headerFields := make([]field, len(fields))
			for i, f := range fields {
				values[i] = sql.NewTextValue(f.name)
				headerFields[i] = field{f.name, oidText}
			}
			data, e := command.encode(values, headerFields)
			if e != nil {
				return 0, e
			}
			if err = sendData(data); err != nil {
				return 0, err
			}
		}
		for rows.Next() {
			values, e := rows.Values()
			if e != nil {
				return 0, e
			}
			data, e := command.encode(values, fields)
			if e != nil {
				return 0, e
			}
			if err = sendData(data); err != nil {
				return 0, err
			}
			count++
		}
		if err = rows.Err(); err != nil {
			return 0, err
		}
		if err = rows.Close(); err != nil {
			return 0, err
		}
		if command.format == "binary" {
			if err = sendData([]byte{255, 255}); err != nil {
				return 0, err
			}
		}
		if err = c.send(&pgproto3.CopyDone{}); err != nil {
			return 0, err
		}
	}
	end := "RELEASE " + savepoint
	if own {
		end = "COMMIT"
	}
	result, e := c.session.ExecContext(ctx, end)
	if e != nil {
		return 0, e
	}
	if err = c.reportCommitPosition(result); err != nil {
		return 0, err
	}
	return count, nil
}
func copyDataError(err error, row int64) error {
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		return &protocolError{"22P04", fmt.Sprintf("unexpected end of COPY data at row %d", row)}
	}
	return fmt.Errorf("COPY row %d: %w", row, err)
}
