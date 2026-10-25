package mysqlserver

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"slopdb/db"
	"slopdb/sql"
)

type client struct {
	capabilities            uint32
	moreResults             bool
	preparedPath            []string
	binlogSkip              []gtidInterval
	replicaID               uint32
	replicaUUID             string
	replicationHeartbeat    time.Duration
	replicationChecksumNone bool
	transport               *sessionIOConn
	server                  *Server
	conn                    net.Conn
	writer                  packetWriter
	ctx                     context.Context
	cancel                  context.CancelFunc
	session                 *db.Session
	id                      uint32
	selected                string
	username                string
	account                 string
	database                *db.Database
	variables               sessionVariables
	warningMu               sync.Mutex
	warnings                []mysqlWarning
	warningTotal            uint64
	autocommit              bool
	statements              map[uint32]*prepared
	nextStmt                uint32
}

func (c *client) run() {
	c.variables = defaultSessionVariables()
	defer c.server.namedLocks.releaseOwner(c.id)
	defer func() {
		if c.session != nil {
			_ = c.session.Close()
		}
	}()
	if err := c.handshake(); err != nil {
		return
	}
	c.installNetworkTimeouts()
	defer c.closeStatements()
	// A separate reader detects EOF even while SQL waits on another session's
	// locks. It cancels execution immediately so disconnects release transactions.
	type command struct {
		data               []byte
		sequence           byte
		compressedSequence byte
	}
	commands := make(chan command)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		defer close(commands)
		defer c.cancel()
		defer c.conn.Close()
		for {
			c.transport.beginCommand()
			compressedSequence := byte(0)
			if compressed, ok := c.conn.(*compressedConn); ok {
				compressed.readSequence = 0
			}
			data, next, err := readPacket(c.conn, 0, c.server.config.MaxPacketBytes)
			if compressed, ok := c.conn.(*compressedConn); ok {
				compressedSequence = compressed.readSequence
			}
			if err != nil {
				return
			}
			select {
			case commands <- command{data, next, compressedSequence}:
			case <-c.ctx.Done():
				return
			}
		}
	}()
	defer func() { c.cancel(); _ = c.conn.Close(); <-readerDone }()
	for cmd := range commands {
		if c.ctx.Err() != nil {
			return
		}
		c.writer.sequence = cmd.sequence
		if compressed, ok := c.conn.(*compressedConn); ok {
			compressed.writeSequence = cmd.compressedSequence
		}
		if len(cmd.data) == 0 {
			if c.writeError(errMalformed) != nil {
				return
			}
			continue
		}
		if cmd.data[0] == 1 {
			return
		} // COM_QUIT has no response.
		if err := c.dispatch(cmd.data); err != nil {
			return
		}
	}
}
func (c *client) closeStatements() {
	for _, s := range c.statements {
		_ = s.stmt.Close()
	}
	clear(c.statements)
}
func (c *client) dispatch(p []byte) error {
	switch p[0] {
	case 18: // COM_BINLOG_DUMP streams the persisted logical commit journal.
		return c.dumpBinlog(p[1:])
	case 21: // COM_REGISTER_SLAVE / COM_REGISTER_REPLICA
		return c.registerReplica(p[1:])
	case 30: // COM_BINLOG_DUMP_GTID
		return c.dumpBinlogGTID(p[1:])
	case 2: // COM_INIT_DB
		return c.use(string(p[1:]))
	case 3: // COM_QUERY
		return c.queryResults(string(p[1:]))
	case 27: // COM_SET_OPTION: enable/disable multi-statements on this connection.
		if len(p) != 3 {
			return c.writeError(errMalformed)
		}
		switch binary.LittleEndian.Uint16(p[1:]) {
		case 0:
			c.capabilities |= clientMultiStatements
		case 1:
			c.capabilities &^= clientMultiStatements
		default:
			return c.writeError(errMalformed)
		}
		return c.eof()

	case 14: // COM_PING
		if len(p) != 1 {
			return c.writeError(errMalformed)
		}
		return c.ok(nil)
	case 22:
		return c.prepare(string(p[1:]))
	case 23:
		return c.executePrepared(p[1:])
	case 24:
		return c.longData(p[1:])
	case 25: // COM_STMT_CLOSE has no response, even for unknown IDs.
		if len(p) == 5 {
			id := binary.LittleEndian.Uint32(p[1:])
			if s := c.statements[id]; s != nil {
				_ = s.stmt.Close()
				delete(c.statements, id)
			}
		}
		return nil
	case 26: // COM_STMT_RESET
		if len(p) != 5 {
			return c.writeError(errMalformed)
		}
		s := c.statements[binary.LittleEndian.Uint32(p[1:])]
		if s == nil {
			return c.writeError(unknownStatement())
		}
		s.long = nil
		s.longErr = nil
		return c.ok(nil)
	case 31: // COM_RESET_CONNECTION rolls back pooled connection state.
		c.server.namedLocks.releaseOwner(c.id)
		c.variables = defaultSessionVariables()
		if c.transport != nil {
			c.transport.configure(c.variables)
		}
		if len(p) != 1 {
			return c.writeError(errMalformed)
		}
		if err := c.session.Close(); err != nil {
			return c.writeError(err)
		}
		c.closeStatements()
		session, err := c.server.database.NewSession()
		if err != nil {
			return c.writeError(err)
		}
		c.session = session
		c.database = c.server.database
		if err := enableMySQLSession(session); err != nil {
			return c.writeError(err)
		}
		if err := session.RegisterRootSchemaAlias(c.ctx, c.server.config.Database); err != nil {
			return c.writeError(err)
		}
		c.autocommit = true
		c.selected = ""
		return c.ok(nil)
	default:
		return c.writeError(&protocolError{1047, "08S01", fmt.Sprintf("unsupported MySQL command %d", p[0])})
	}
}
func (c *client) use(name string) error {
	if err := c.switchDatabase(name); err != nil {
		return c.writeError(err)
	}
	return c.ok(nil)
}

func scalar(name string, value sql.Value) *db.ExecResult {
	return &db.ExecResult{Columns: []sql.ColumnDef{{Name: name, ColType: value.Type}}, Rows: []map[string]sql.Value{{name: value}}}
}

func (c *client) query(query string, stmt *db.Stmt, args []any) (*db.ExecResult, error) {
	c.beginDiagnostics(query)
	if stmt == nil {
		if result, handled, err := c.control(query); handled {
			return result, err
		}
		statements, err := sql.ParseWithOptions(query, c.currentDatabase().ParseOptions())
		if err != nil {
			return nil, err
		}
		if len(statements) != 1 {
			return nil, &protocolError{1064, "42000", "exactly one SQL statement is required"}
		}
	}
	first := strings.ToUpper(sql.NewTokenizerWithOptions(query, c.currentDatabase().ParseOptions()).Next().Value)
	if !c.autocommit && !c.session.InTransaction() && first != "BEGIN" && first != "START" && first != "COMMIT" && first != "ROLLBACK" && first != "WAIT" {
		if _, err := c.session.ExecContext(c.ctx, "BEGIN"); err != nil {
			return nil, err
		}
	}
	if stmt != nil {
		return stmt.ExecContext(c.mysqlContext(), args...)
	}
	return c.session.ExecContext(c.mysqlContext(), query)
}

// control implements only settings whose effects the server can honor. Unknown
// SETs go to the SQL engine and fail; they must never be silently acknowledged.
func (c *client) control(query string) (*db.ExecResult, bool, error) {
	result, handled, err := c.controlRaw(query)
	if handled && err == nil {
		if result == nil {
			c.session.RecordMySQLProtocolResult(false, 0, 0)
		} else {
			c.session.RecordMySQLProtocolResult(len(result.Columns) > 0, result.Changes, uint64(len(result.Rows)))
		}
	}
	return result, handled, err
}

func (c *client) controlRaw(query string) (*db.ExecResult, bool, error) {
	if result, handled, err := c.binlogControl(query); handled {
		return result, true, err
	}
	if result, handled, err := c.administration(query); handled {
		return result, true, err
	}
	if result, handled, err := c.compatibilityControl(query); handled {
		return result, true, err
	}
	tokenizer := sql.NewTokenizerWithOptions(query, c.currentDatabase().ParseOptions())
	var tokens []sql.Token
	for tok := tokenizer.Next(); tok.Type != sql.TokEOF; tok = tokenizer.Next() {
		tokens = append(tokens, tok)
		if len(tokens) > 16 {
			return nil, false, nil
		}
	}
	if len(tokens) > 0 && tokens[len(tokens)-1].Type == sql.TokSemicolon {
		tokens = tokens[:len(tokens)-1]
	}
	// Let the SQL parser distinguish multiple statements from semicolons
	// inside a single CREATE TRIGGER body before any SQL is executed.
	for _, t := range tokens {
		if t.Type == sql.TokSemicolon {
			return nil, false, nil
		}
	}
	if len(tokens) == 0 {
		return nil, true, nil
	}
	word := func(i int, w string) bool {
		return i < len(tokens) && strings.EqualFold(tokens[i].Value, w) && (tokens[i].Type == sql.TokIdent || tokens[i].Type == sql.TokKeyword)
	}
	if word(0, "USE") && len(tokens) == 2 && (tokens[1].Type == sql.TokIdent || tokens[1].Type == sql.TokKeyword) {
		name := tokens[1].Value
		return nil, true, c.switchDatabase(name)
	}
	if len(tokens) == 2 && word(0, "SHOW") && word(1, "DATABASES") {
		return scalar("Database", sql.NewTextValue(c.server.config.Database)), true, nil
	}
	if word(0, "SET") {
		t := tokens[1:]
		sessionDefault := false
		if len(t) > 0 && strings.EqualFold(t[0].Value, "SESSION") {
			sessionDefault = true
			t = t[1:]
		}
		if len(t) == 5 && strings.EqualFold(t[0].Value, "TRANSACTION") && strings.EqualFold(t[1].Value, "ISOLATION") && strings.EqualFold(t[2].Value, "LEVEL") {
			level, err := sql.ParseIsolationLevel(t[3].Value + " " + t[4].Value)
			if err != nil {
				return nil, true, err
			}
			if sessionDefault {
				return nil, true, c.session.SetIsolationLevel(level)
			}
			return nil, true, c.session.SetNextTransactionIsolation(level)
		}
		if len(t) == 3 && strings.EqualFold(t[0].Value, "autocommit") && t[1].Type == sql.TokEq {
			var enabled bool
			switch strings.ToUpper(t[2].Value) {
			case "0", "OFF":
				enabled = false
			case "1", "ON":
				enabled = true
			default:
				return nil, true, errors.New("autocommit must be 0, 1, ON or OFF")
			}
			if enabled && !c.autocommit && c.session.InTransaction() {
				result, err := c.session.ExecContext(c.ctx, "COMMIT")
				if err != nil {
					return nil, true, err
				}
				c.autocommit = enabled
				return result, true, nil
			}
			c.autocommit = enabled
			return nil, true, nil
		}
		if len(t) == 2 && strings.EqualFold(t[0].Value, "NAMES") && (t[1].Type == sql.TokIdent || t[1].Type == sql.TokString) {
			switch strings.ToLower(t[1].Value) {
			case "utf8", "utf8mb3", "utf8mb4":
				return nil, true, nil
			}
			return nil, true, &protocolError{1115, "42000", "only UTF-8 client character sets are supported"}
		}
	}
	if word(0, "SELECT") && len(tokens) == 4 && tokens[2].Type == sql.TokLParen && tokens[3].Type == sql.TokRParen {
		name := strings.ToUpper(tokens[1].Value)
		switch name {
		case "DATABASE":
			v := sql.NewNullValue()
			if c.selected != "" {
				v = sql.NewTextValue(c.selected)
			}
			return scalar("DATABASE()", v), true, nil
		case "VERSION":
			return scalar("VERSION()", sql.NewTextValue(serverVersion)), true, nil
		case "CONNECTION_ID":
			return scalar("CONNECTION_ID()", sql.NewIntValue(int64(c.id))), true, nil
		}
	}
	// @@ variables are not part of SlopDB's expression grammar.
	normalized := strings.ToUpper(strings.Join(strings.Fields(strings.TrimSuffix(strings.TrimSpace(query), ";")), " "))
	if normalized == "SELECT @@AUTOCOMMIT" || normalized == "SELECT @@SESSION.AUTOCOMMIT" {
		value := int64(0)
		if c.autocommit {
			value = 1
		}
		return scalar("@@autocommit", sql.NewIntValue(value)), true, nil
	}
	return nil, false, nil
}

func enableMySQLSession(session *db.Session) error {
	return session.SetMySQLMode(true)
}
