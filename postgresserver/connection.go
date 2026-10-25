package postgresserver

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgproto3"
	"slopdb/db"
	"slopdb/sql"
)

type client struct {
	incoming                    <-chan frontendPacket
	server                      *Server
	conn                        net.Conn
	ctx                         context.Context
	cancel                      context.CancelFunc
	session                     *db.Session
	id, secret                  uint32
	activeMu                    sync.Mutex
	activeCancel                context.CancelFunc
	extendedImplicit            bool
	explicitTransaction         bool
	reportPositions             bool
	failed, skipUntilSync       bool
	currentSQL, applicationName string
	statements                  map[string]*prepared
	portals                     map[string]*portal
}
type portal struct {
	prepared *prepared
	args     []any
	formats  []int16
	result   *db.ExecResult
	position int
	started  bool
	tag      string
}

func (c *client) run() {
	if err := c.startup(); err != nil {
		return
	}
	session, err := c.server.database.NewInspectionSession(c.ctx)
	if err != nil {
		_ = c.fatal("57P01", err.Error())
		return
	}
	c.session = session
	if err := session.SetPostgresMode(true); err != nil {
		_ = c.fatal("57P01", err.Error())
		return
	}
	defer session.Close()
	defer c.clearStatements()
	defer c.clearPortals()
	c.server.mu.Lock()
	c.server.backends[c.id] = c
	c.server.mu.Unlock()
	defer func() { c.server.mu.Lock(); delete(c.server.backends, c.id); c.server.mu.Unlock() }()
	if err := c.welcome(); err != nil {
		return
	}
	incoming := make(chan frontendPacket)
	c.incoming = incoming
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		defer close(incoming)
		defer c.cancel()
		defer c.conn.Close()
		for {
			var kind [1]byte
			if _, err := io.ReadFull(c.conn, kind[:]); err != nil {
				return
			}
			body, err := readBody(c.conn, c.server.config.MaxPacketBytes)
			if err != nil {
				return
			}
			select {
			case incoming <- frontendPacket{kind[0], body}:
			case <-c.ctx.Done():
				return
			}
		}
	}()
	defer func() { c.cancel(); _ = c.conn.Close(); <-readerDone }()
	for packet := range incoming {
		if c.ctx.Err() != nil {
			return
		}
		// COPY consumes its input synchronously. PostgreSQL ignores late COPY data after errors.
		if packet.kind == 'd' || packet.kind == 'c' || packet.kind == 'f' {
			continue
		}
		if c.skipUntilSync && packet.kind != 'S' && packet.kind != 'X' {
			continue
		}
		message, err := decodeMessage(packet.kind, packet.body)
		if err != nil {
			if c.writeError(err) != nil {
				return
			}
			c.skipUntilSync = packet.kind != 'Q'
			if packet.kind == 'Q' {
				if c.ready() != nil {
					return
				}
			}
			continue
		}
		if _, ok := message.(*pgproto3.Terminate); ok {
			return
		}
		ctx, cancel := context.WithCancel(c.ctx)
		c.activeMu.Lock()
		c.activeCancel = cancel
		c.activeMu.Unlock()
		err = c.dispatch(ctx, message)
		c.activeMu.Lock()
		c.activeCancel = nil
		c.activeMu.Unlock()
		cancel()
		if err != nil {
			if c.writeError(err) != nil {
				return
			}
			if _, simple := message.(*pgproto3.Query); simple {
				if c.ready() != nil {
					return
				}
			} else {
				c.skipUntilSync = true
			}
		}
	}
}
func (c *client) clearStatements() {
	for name, p := range c.statements {
		p.release()
		delete(c.statements, name)
	}
}
func (c *client) clearPortals() {
	for name, p := range c.portals {
		p.prepared.release()
		delete(c.portals, name)
	}
}
func (p *prepared) retain() { p.references++ }
func (p *prepared) release() {
	p.references--
	if p.references == 0 {
		p.close()
	}
}
func (c *client) dispatch(ctx context.Context, message pgproto3.FrontendMessage) error {
	switch m := message.(type) {
	case *pgproto3.Query:
		return c.simple(ctx, m.String)
	case *pgproto3.Parse:
		if old := c.statements[m.Name]; old != nil {
			if m.Name != "" {
				return &protocolError{"42P05", "prepared statement already exists"}
			}
			old.release()
			delete(c.statements, m.Name)
		}
		if len(c.statements) >= c.server.config.MaxPreparedStatements {
			return &protocolError{"54000", "too many prepared statements"}
		}
		c.currentSQL = m.Query
		c.reportPositions = !strings.Contains(m.Query, "$")
		p, err := c.prepare(ctx, m.Query, m.ParameterOIDs)
		if err != nil {
			return err
		}
		p.references = 1
		c.statements[m.Name] = p
		return c.send(&pgproto3.ParseComplete{})
	case *pgproto3.Bind:
		prepared := c.statements[m.PreparedStatement]
		if prepared == nil {
			return &protocolError{"26000", "unknown prepared statement"}
		}
		if len(m.Parameters) != len(prepared.params) {
			return &protocolError{"08P01", "incorrect parameter count"}
		}
		if err := validateFormats(m.ParameterFormatCodes, len(m.Parameters)); err != nil {
			return err
		}
		if err := validateFormats(m.ResultFormatCodes, len(prepared.fields)); err != nil {
			return err
		}
		if old := c.portals[m.DestinationPortal]; old != nil {
			if m.DestinationPortal != "" {
				return &protocolError{"42P03", "portal already exists"}
			}
			old.prepared.release()
			delete(c.portals, m.DestinationPortal)
		}
		if len(c.portals) >= c.server.config.MaxPortals {
			return &protocolError{"54000", "too many portals"}
		}
		values := make([]any, len(m.Parameters))
		for i, value := range m.Parameters {
			decoded, err := decodeValue(value, prepared.params[i], formatAt(m.ParameterFormatCodes, i) == 1)
			if err != nil {
				return err
			}
			values[i] = decoded
		}
		args := make([]any, len(prepared.positions))
		for i, index := range prepared.positions {
			args[i] = values[index]
		}
		prepared.retain()
		c.portals[m.DestinationPortal] = &portal{prepared: prepared, args: args, formats: append([]int16(nil), m.ResultFormatCodes...)}
		return c.send(&pgproto3.BindComplete{})
	case *pgproto3.Describe:
		var fields []field
		var formats []int16
		switch m.ObjectType {
		case 'S':
			p := c.statements[m.Name]
			if p == nil {
				return &protocolError{"26000", "unknown prepared statement"}
			}
			if err := c.send(&pgproto3.ParameterDescription{ParameterOIDs: p.params}); err != nil {
				return err
			}
			fields = p.fields
		case 'P':
			p := c.portals[m.Name]
			if p == nil {
				return &protocolError{"34000", "unknown portal"}
			}
			fields, formats = p.prepared.fields, p.formats
		default:
			return errMalformed
		}
		if len(fields) == 0 {
			return c.send(&pgproto3.NoData{})
		}
		return c.send(fieldDescription(fields, formats))
	case *pgproto3.Execute:
		portal := c.portals[m.Portal]
		if portal == nil {
			return &protocolError{"34000", "unknown portal"}
		}
		return c.executePortal(ctx, portal, m.MaxRows)
	case *pgproto3.Close:
		switch m.ObjectType {
		case 'S':
			if p := c.statements[m.Name]; p != nil {
				p.release()
				delete(c.statements, m.Name)
			}
		case 'P':
			if p := c.portals[m.Name]; p != nil {
				p.prepared.release()
				delete(c.portals, m.Name)
			}
		default:
			return errMalformed
		}
		return c.send(&pgproto3.CloseComplete{})
	case *pgproto3.Sync:
		c.skipUntilSync = false
		if err := c.finishImplicit(ctx); err != nil {
			if writeErr := c.writeError(err); writeErr != nil {
				return writeErr
			}
			return c.ready()
		}
		if !c.session.InTransaction() {
			c.clearPortals()
		}
		return c.ready()
	case *pgproto3.Flush:
		return nil // Responses are written immediately.
	default:
		return &protocolError{"0A000", "unsupported PostgreSQL message"}
	}
}
func (c *client) executeSQL(ctx context.Context, p *prepared, args []any) (*db.ExecResult, string, error) {
	ctx = c.postgresContext(ctx)
	c.currentSQL = p.original
	c.reportPositions = false
	wasInTx := c.session.InTransaction()
	if c.failed {
		if p.kind == "COMMIT" || p.kind == "END" || p.kind == "ROLLBACK" {
			rollbackTo := false
			if p.kind == "ROLLBACK" {
				t := sql.NewTokenizer(p.native)
				t.Next()
				rollbackTo = strings.EqualFold(t.Next().Value, "TO")
			}
			if !c.session.InTransaction() {
				if rollbackTo {
					return nil, "", &protocolError{"3B001", "the transaction was rolled back; no savepoint remains"}
				}
				c.failed = false
				c.explicitTransaction = false
				c.extendedImplicit = false
				c.clearPortals()
				return &db.ExecResult{}, "ROLLBACK", nil
			}
			if p.kind == "COMMIT" || p.kind == "END" {
				result, err := c.session.ExecContext(ctx, "ROLLBACK")
				if err == nil {
					c.failed = false
					c.explicitTransaction = false
					c.extendedImplicit = false
					c.clearPortals()
				}
				return result, "ROLLBACK", err
			}
		} else {
			return nil, "", &protocolError{"25P02", "current transaction is aborted; roll back before continuing"}
		}
	}

	var result *db.ExecResult
	var err error
	if p.helper {
		h, ok := c.helperMetadata(p.native)
		if !ok {
			return nil, "", errMalformed
		}
		result, err = c.runHelper(ctx, h)
	} else if p.stmt != nil {
		result, err = p.stmt.ExecContext(ctx, args...)
	} else {
		result, err = c.session.ExecContext(ctx, p.native)
	}
	if err != nil {
		return nil, "", err
	}
	if c.session.InTransaction() && !c.extendedImplicit && (p.kind == "BEGIN" || p.kind == "START" || p.kind == "SAVEPOINT") {
		c.explicitTransaction = true
	}
	if err := c.reportCommitPosition(result); err != nil {
		return nil, "", err
	}
	if !c.session.InTransaction() {
		c.extendedImplicit = false
		c.explicitTransaction = false
		if wasInTx {
			c.clearPortals()
		}
	}
	if p.kind == "ROLLBACK" {
		c.clearPortals()
	}
	if p.kind == "ROLLBACK" || !c.session.InTransaction() {
		c.failed = false
	}
	kind := p.kind
	if kind == "POSITION" {
		kind = "SELECT"
	}
	if p.helper {
		if kind == "PID" {
			kind = "SELECT"
		}
	}
	if kind == "CREATE" || kind == "DROP" || kind == "ALTER" {
		t := sql.NewTokenizer(p.native)
		t.Next()
		object := strings.ToUpper(t.Next().Value)
		if object == "UNIQUE" {
			object = strings.ToUpper(t.Next().Value)
		}
		kind += " " + object
	}
	return result, completion(kind, result), nil
}
func (c *client) executePortal(ctx context.Context, p *portal, maxRows uint32) error {
	if c.failed && p.prepared.kind != "ROLLBACK" && p.prepared.kind != "COMMIT" {
		return &protocolError{"25P02", "current transaction is aborted; roll back before continuing"}
	}
	if p.prepared.empty {
		return c.send(&pgproto3.EmptyQueryResponse{})
	}
	if !p.started {
		if !c.session.InTransaction() && !transactionCommand(p.prepared.kind) && p.prepared.kind != "VACUUM" && p.prepared.kind != "DISCARD" {
			if _, err := c.session.ExecContext(ctx, "BEGIN"); err != nil {
				return err
			}
			c.extendedImplicit = true
		}
		if p.prepared.copy != nil {
			count, err := c.runCopy(ctx, p.prepared.copy)
			if err != nil {
				return err
			}
			p.tag = fmt.Sprintf("COPY %d", count)
			p.started = true
			return c.send(&pgproto3.CommandComplete{CommandTag: []byte(p.tag)})
		}
		result, tag, err := c.executeSQL(ctx, p.prepared, p.args)
		if err != nil {
			return err
		}
		if len(result.Columns) != len(p.prepared.fields) {
			return &protocolError{"0A000", "cached result column count changed; prepare again"}
		}
		for i, f := range p.prepared.fields {
			if result.Columns[i].ResultName() != f.name {
				return &protocolError{"0A000", "cached result column names changed; prepare again"}
			}
		}
		for _, row := range result.Rows {
			for i, f := range p.prepared.fields {
				value, ok := row[result.Columns[i].Name]
				if !ok {
					value = sql.NewNullValue()
				}
				if err := validateValue(value, f.oid); err != nil {
					return err
				}
			}
		}
		p.result, p.tag, p.started = result, tag, true
	}
	if p.prepared.copy != nil {
		return c.send(&pgproto3.CommandComplete{CommandTag: []byte(p.tag)})
	}
	end := len(p.result.Rows)
	if maxRows > 0 && uint64(maxRows) < uint64(end-p.position) {
		end = p.position + int(maxRows)
	}
	for p.position < end {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.row(p.result.Rows[p.position], p.result.Columns, p.prepared.fields, p.formats); err != nil {
			return err
		}
		p.position++
	}
	if p.position < len(p.result.Rows) {
		return c.send(&pgproto3.PortalSuspended{})
	}
	return c.send(&pgproto3.CommandComplete{CommandTag: []byte(p.tag)})
}
func (c *client) simple(ctx context.Context, query string) error {
	if err := c.finishImplicit(ctx); err != nil {
		return err
	}
	if p := c.statements[""]; p != nil {
		p.release()
		delete(c.statements, "")
	}
	if p := c.portals[""]; p != nil {
		p.prepared.release()
		delete(c.portals, "")
	}
	parts := splitQueries(query)
	if len(parts) == 0 {
		if err := c.send(&pgproto3.EmptyQueryResponse{}); err != nil {
			return err
		}
		return c.ready()
	}
	firstWord := strings.ToUpper(sql.NewTokenizer(parts[0]).Next().Value)
	singleDML := firstWord == "INSERT" || firstWord == "UPDATE" || firstWord == "DELETE" || firstWord == "REPLACE" || firstWord == "WITH"
	implicit := (len(parts) > 1 || singleDML) && !c.explicitTransaction && !c.session.InTransaction()
	if implicit {
		for _, part := range parts {
			word := strings.ToUpper(sql.NewTokenizer(part).Next().Value)
			switch word {
			case "BEGIN", "START", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE":
				implicit = false
			}
		}
	}
	if implicit {
		if _, err := c.session.ExecContext(ctx, "BEGIN"); err != nil {
			return err
		}
	}
	successful := false
	defer func() {
		if implicit && !successful {
			_, _ = c.session.Exec("ROLLBACK")
			c.failed = false
		}
	}()
	for _, part := range parts {
		c.currentSQL = part
		c.reportPositions = len(parts) == 1 && part == query && !strings.Contains(part, "$")
		copyCommand, err := parseCopy(part)
		if err != nil {
			return err
		}
		if copyCommand != nil {
			count, e := c.runCopy(ctx, copyCommand)
			if e != nil {
				return e
			}
			if e = c.send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("COPY %d", count))}); e != nil {
				return e
			}
			continue
		}
		native, positions, _, err := translate(part)
		if err != nil {
			return err
		}
		if len(positions) != 0 {
			return &protocolError{"42P02", "simple queries cannot bind parameters"}
		}
		kind := strings.ToUpper(sql.NewTokenizer(part).Next().Value)
		p := &prepared{native: native, original: part, kind: kind}
		var parsed sql.Statement
		if h, ok := c.helperMetadata(native); ok {
			p.helper = true
			p.kind = h.kind
		} else {
			statements, err := sql.Parse(native)
			if err != nil {
				return err
			}
			if len(statements) != 1 {
				return &protocolError{"42601", "invalid query boundary"}
			}
			p.kind = kindOf(statements[0], p.kind)
			parsed = statements[0]
		}
		result, tag, err := c.executeSQL(ctx, p, nil)
		if err != nil {
			return err
		}
		fields := resultFields(result)
		if len(result.Rows) == 0 && len(fields) > 0 && parsed != nil {
			m := &metadata{client: c, ctx: ctx, prepared: p, tables: make(map[string][]field)}
			if described, e := m.statement(parsed, nil, 0); e == nil && len(described) == len(fields) {
				fields = described
			}
		}
		if len(fields) > 0 {
			if err := c.send(fieldDescription(fields, nil)); err != nil {
				return err
			}
			for _, row := range result.Rows {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := c.row(row, result.Columns, fields, nil); err != nil {
					return err
				}
			}
		}
		if err := c.send(&pgproto3.CommandComplete{CommandTag: []byte(tag)}); err != nil {
			return err
		}
	}
	if implicit {
		result, err := c.session.ExecContext(ctx, "COMMIT")
		if err != nil {
			return err
		}
		if err := c.reportCommitPosition(result); err != nil {
			return err
		}
	}
	successful = true
	return c.ready()
}

func kindOf(stmt sql.Statement, fallback string) string {
	if _, ok := stmt.(*sql.ReadBarrierStmt); ok {
		return "SET"
	}
	if position, ok := stmt.(*sql.ReplicationPositionStmt); ok {
		if position.Operation == "wait" {
			return "WAIT"
		}
		return "POSITION"
	}
	switch stmt.(type) {
	case *sql.SelectStmt, *sql.UnionStmt:
		return "SELECT"
	case *sql.InsertStmt:
		return "INSERT"
	case *sql.UpdateStmt:
		return "UPDATE"
	case *sql.DeleteStmt:
		return "DELETE"
	}
	return fallback
}

func transactionCommand(kind string) bool {
	switch kind {
	case "BEGIN", "START", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE", "WAIT", "POSITION":
		return true
	}
	return false
}
func (c *client) finishImplicit(ctx context.Context) error {
	if !c.extendedImplicit {
		return nil
	}
	c.extendedImplicit = false
	if !c.session.InTransaction() {
		c.failed = false
		c.clearPortals()
		return nil
	}
	command := "COMMIT"
	if c.failed {
		command = "ROLLBACK"
	}
	result, err := c.session.ExecContext(ctx, command)
	if err == nil {
		err = c.reportCommitPosition(result)
	}
	if err != nil {
		_, _ = c.session.Exec("ROLLBACK")
	}
	c.failed = false
	c.clearPortals()
	return err
}

func (c *client) reportCommitPosition(result *db.ExecResult) error {
	if result != nil && result.CommitPosition != "" {
		return c.send(&pgproto3.ParameterStatus{Name: "slopdb_commit_position", Value: result.CommitPosition})
	}
	return nil
}
