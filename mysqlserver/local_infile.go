package mysqlserver

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"slopdb/db"
	"slopdb/sql"
)

type infileRequest struct {
	start    bool
	sequence byte
}
type infilePacket struct {
	data       []byte
	next       byte
	compressed byte
}
type infileInput struct {
	client  *client
	ctx     context.Context
	pending []byte
	done    bool
}

func (r *infileInput) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for len(r.pending) == 0 {
		if r.done {
			return 0, io.EOF
		}
		select {
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		case packet, ok := <-r.client.infilePackets:
			if !ok {
				return 0, io.ErrUnexpectedEOF
			}
			r.client.writer.sequence = packet.next
			if compressed, ok := r.client.conn.(*compressedConn); ok {
				compressed.writeSequence = packet.compressed
			}
			if len(packet.data) == 0 {
				r.done = true
				return 0, io.EOF
			}
			r.pending = packet.data
		}
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}
func loadPrefix(query string, options sql.ParseOptions) bool {
	t := sql.NewTokenizerWithOptions(query, options)
	first := t.Next()
	return (first.Type == sql.TokIdent || first.Type == sql.TokKeyword) && strings.EqualFold(first.Value, "LOAD")
}
func (c *client) isLocalCommand(data []byte) bool {
	if len(data) == 0 || data[0] != 3 {
		return false
	}
	return loadPrefix(string(data[1:]), c.parseOptions())
}

// Prefetch the first post-command packet before awaiting the decision. EOF can
// therefore cancel a LOAD waiting on catalog locks as for every other command.
func readAnyPacket(r io.Reader, limit int) ([]byte, byte, byte, error) {
	var h [4]byte
	if _, e := io.ReadFull(r, h[:]); e != nil {
		return nil, 0, 0, e
	}
	data, next, e := readPacket(io.MultiReader(bytes.NewReader(h[:]), r), h[3], limit)
	return data, h[3], next, e
}
func (c *client) receiveLocalUpload() (pending []byte, next byte, compressedNext byte, err error) {
	data, first, next, e := readAnyPacket(c.conn, c.server.config.MaxPacketBytes)
	if e != nil {
		return nil, 0, 0, e
	}
	var request infileRequest
	select {
	case request = <-c.infileDecision:
	case <-c.ctx.Done():
		return nil, 0, 0, c.ctx.Err()
	}
	if !request.start {
		if first != 0 {
			return nil, 0, 0, errMalformed
		}
		if compressed, ok := c.conn.(*compressedConn); ok {
			compressedNext = byte(compressed.readSequence.Load())
		}
		return data, next, compressedNext, nil
	}
	expected := request.sequence
	for {
		if first != expected {
			return nil, 0, 0, fmt.Errorf("invalid LOCAL INFILE packet sequence: got %d want %d", first, expected)
		}
		sequence := byte(0)
		if compressed, ok := c.conn.(*compressedConn); ok {
			sequence = byte(compressed.readSequence.Load())
		}
		select {
		case c.infilePackets <- infilePacket{data: data, next: next, compressed: sequence}:
		case <-c.ctx.Done():
			return nil, 0, 0, c.ctx.Err()
		}
		if len(data) == 0 {
			return nil, 0, 0, nil
		}
		expected = next
		data, next, e = readPacket(c.conn, expected, c.server.config.MaxPacketBytes)
		if e != nil {
			return nil, 0, 0, e
		}
		first = expected
	}
}
func (c *client) loadData(query string) (*db.ExecResult, bool, error) {
	command, e := parseLocalLoad(query, c.parseOptions())
	if command == nil {
		return nil, false, e
	}
	if e != nil {
		return nil, true, e
	}
	if !c.infileAwaiting || c.capabilities&clientLocalFiles == 0 {
		return nil, true, &protocolError{1148, "42000", "LOAD DATA LOCAL requires a standalone query and LOCAL INFILE client capability"}
	}
	ctx := sql.WithStatementTime(c.mysqlContext(), sql.StatementTime(c.ctx))
	snapshot, e := c.session.CatalogSnapshot(ctx)
	if e != nil {
		return nil, true, e
	}
	ns, path, e := c.session.NamespaceSettings(ctx)
	if e != nil {
		return nil, true, e
	}
	_ = ns
	name := strings.ToLower(command.table)
	resolve := func(n string) (sql.CatalogRelation, bool) {
		if strings.HasPrefix(n, strings.ToLower(c.server.config.Database)+".") {
			n = strings.TrimPrefix(n, strings.ToLower(c.server.config.Database)+".")
		}
		return snapshot.Relation(n)
	}
	relation, found := resolve(name)
	if !strings.Contains(name, ".") {
		found = false
		for _, schema := range path {
			key := name
			if schema != "main" {
				key = schema + "." + name
			}
			if candidate, ok := resolve(key); ok {
				relation = candidate
				found = true
				break
			}
		}
	}
	if !found || relation.Kind != "table" {
		return nil, true, &protocolError{1146, "42S02", "LOAD DATA target table does not exist"}
	}
	columns := command.columns
	if len(columns) == 0 {
		for _, col := range relation.Columns {
			columns = append(columns, col.Name)
		}
	}
	defs := make([]sql.ColumnDef, len(columns))
	seen := map[string]bool{}
	for i, name := range columns {
		if seen[name] {
			return nil, true, &protocolError{1110, "42000", "LOAD DATA column specified twice"}
		}
		seen[name] = true
		ok := false
		for _, col := range relation.Columns {
			if col.Name == name {
				defs[i] = col
				ok = true
				break
			}
		}
		if !ok {
			return nil, true, &protocolError{1054, "42S22", "unknown LOAD DATA column " + name}
		}
	}
	c.beginDiagnostics(query)
	if !c.autocommit && !c.session.InTransaction() {
		if _, e = c.session.ExecContext(ctx, "BEGIN"); e != nil {
			return nil, true, e
		}
	}
	// Advertise only the exact client-provided stream name; the server never opens
	// a path or executes a program. The client retains its own file/reader policy.
	c.infileNegotiated = true
	inputSequence := c.writer.sequence + 1
	if compressed, ok := c.conn.(*compressedConn); ok {
		// Native clients align LOCAL upload numbering with compression packets.
		// The small filename request uses one inner header and one body frame.
		inputSequence = compressed.writeSequence + 2
		compressed.readSequence.Store(uint32(inputSequence))
	}
	c.infileDecision <- infileRequest{start: true, sequence: inputSequence}
	_ = c.conn.SetReadDeadline(time.Now().Add(time.Duration(c.variables.netReadTimeout) * time.Second))
	if e = c.writer.write(append([]byte{0xfb}, []byte(command.file)...)); e != nil {
		return nil, true, e
	}
	input := &infileInput{client: c, ctx: c.ctx}
	records := &loadRecords{reader: bufio.NewReaderSize(input, 32*1024), command: command, limit: c.server.config.MaxPacketBytes, width: len(columns)}
	var row int64
	result, e := c.session.BulkInsertResult(ctx, name, columns, func(call context.Context) ([]any, error) {
		input.ctx = call
		for {
			fields, er := records.next()
			if er != nil {
				return nil, er
			}
			row++
			if row <= command.ignore {
				continue
			}
			if len(fields) != len(columns) {
				return nil, &protocolError{1261, "HY000", fmt.Sprintf("LOAD DATA row %d has %d fields, expected %d", row, len(fields), len(columns))}
			}
			values := make([]any, len(fields))
			for i, f := range fields {
				if f.null {
					values[i] = nil
				} else if defs[i].ColType == sql.ColBlob && defs[i].LogicalType().Kind != sql.TypeUUID {
					values[i] = append([]byte{}, f.data...)
				} else {
					values[i] = string(f.data)
				}
			}
			return values, nil
		}
	})
	if e != nil && (errors.Is(e, context.Canceled) || errors.Is(e, context.DeadlineExceeded) || errors.Is(e, db.ErrSessionClosed)) {
		_ = c.conn.Close()
		return nil, true, e
	}
	input.ctx = c.ctx
	if _, drainErr := io.Copy(io.Discard, input); drainErr != nil {
		_ = c.conn.Close()
		if e == nil {
			e = drainErr
		}
	}
	return result, true, e
}

type localLoad struct {
	file, table              string
	columns                  []string
	delimiter, quote, escape byte
	line                     string
	ignore                   int64
}

func parseLocalLoad(query string, options sql.ParseOptions) (*localLoad, error) {
	if !loadPrefix(query, options) {
		return nil, nil
	}
	ts := sqlTokens(query, options)
	if len(ts) == 0 || !strings.EqualFold(ts[0].Value, "LOAD") {
		return nil, nil
	}
	c := &localLoad{delimiter: '\t', escape: '\\', line: "\n"}
	i := 1
	bad := func() (*localLoad, error) {
		return c, &protocolError{1235, "42000", "unsupported LOAD DATA syntax; use LOCAL INFILE, delimiters, optional enclosure/escape, line terminators, IGNORE lines and a column list"}
	}
	word := func(w string) bool {
		if i < len(ts) && ts[i].Type != sql.TokString && strings.EqualFold(ts[i].Value, w) {
			i++
			return true
		}
		return false
	}
	if !word("DATA") || !word("LOCAL") || !word("INFILE") {
		return bad()
	}
	if i >= len(ts) || ts[i].Type != sql.TokString {
		return bad()
	}
	c.file = ts[i].Value
	i++
	if len(c.file) == 0 || len(c.file) > 4096 || strings.ContainsRune(c.file, 0) {
		return bad()
	}
	if !word("INTO") || !word("TABLE") {
		return bad()
	}
	identifier := func() (string, bool) {
		if i >= len(ts) || (ts[i].Type != sql.TokIdent && ts[i].Type != sql.TokKeyword) {
			return "", false
		}
		n := strings.ToLower(ts[i].Value)
		i++
		return n, true
	}
	var ok bool
	c.table, ok = identifier()
	if !ok {
		return bad()
	}
	if i < len(ts) && ts[i].Type == sql.TokDot {
		i++
		n, ok := identifier()
		if !ok {
			return bad()
		}
		c.table += "." + n
	}
	if word("CHARACTER") {
		if !word("SET") || i >= len(ts) {
			return bad()
		}
		enc := strings.ToLower(ts[i].Value)
		i++
		if enc != "utf8" && enc != "utf8mb3" && enc != "utf8mb4" && enc != "binary" {
			return bad()
		}
	}
	if word("FIELDS") || word("COLUMNS") {
		seen := map[string]bool{}
		for i < len(ts) {
			key := ""
			optional := word("OPTIONALLY")
			switch {
			case word("TERMINATED"):
				key = "delimiter"
			case word("ENCLOSED"):
				key = "quote"
			case word("ESCAPED"):
				key = "escape"
			default:
				if optional {
					return bad()
				}
				goto fieldsDone
			}
			if seen[key] || optional && key != "quote" {
				return bad()
			}
			seen[key] = true
			if !word("BY") || i >= len(ts) || ts[i].Type != sql.TokString {
				return bad()
			}
			s := ts[i].Value
			i++
			if len(s) > 1 || key == "delimiter" && len(s) != 1 {
				return bad()
			}
			b := byte(0)
			if len(s) == 1 {
				b = s[0]
			}
			switch key {
			case "delimiter":
				c.delimiter = b
			case "quote":
				c.quote = b
			case "escape":
				c.escape = b
			}
		}
	}
fieldsDone:
	if word("LINES") {
		if !word("TERMINATED") || !word("BY") || i >= len(ts) || ts[i].Type != sql.TokString {
			return bad()
		}
		c.line = ts[i].Value
		i++
		if c.line != "\n" && c.line != "\r\n" {
			return bad()
		}
	}
	if word("IGNORE") {
		if i >= len(ts) || ts[i].Type != sql.TokInt {
			return bad()
		}
		count, e := strconv.ParseInt(ts[i].Value, 10, 64)
		if e != nil || count < 0 {
			return bad()
		}
		c.ignore = count
		i++
		if !word("LINES") && !word("ROWS") {
			return bad()
		}
	}
	if i < len(ts) && ts[i].Type == sql.TokLParen {
		i++
		for {
			n, ok := identifier()
			if !ok {
				return bad()
			}
			c.columns = append(c.columns, n)
			if i < len(ts) && ts[i].Type == sql.TokComma {
				i++
				continue
			}
			if i >= len(ts) || ts[i].Type != sql.TokRParen {
				return bad()
			}
			i++
			break
		}
	}
	if i != len(ts) || c.delimiter == c.quote && c.quote != 0 || c.delimiter == c.line[0] {
		return bad()
	}
	return c, nil
}

type loadField struct {
	data []byte
	null bool
}
type loadRecords struct {
	reader  *bufio.Reader
	command *localLoad
	limit   int
	width   int
}

func (r *loadRecords) next() ([]loadField, error) {
	var fields []loadField
	total := 0
	started := false
	for {
		var data, raw []byte
		quoted := false
		closed := false
		if peek, e := r.reader.Peek(1); e == io.EOF {
			if !started {
				return nil, io.EOF
			}
			return append(fields, loadField{data: []byte{}}), nil
		} else if e != nil {
			return nil, e
		} else if r.command.quote != 0 && peek[0] == r.command.quote {
			quoted = true
			_, _ = r.reader.ReadByte()
			total++
			started = true
		}
		finish := func() {
			null := r.command.escape != 0 && len(raw) == 2 && raw[0] == r.command.escape && raw[1] == 'N' || !quoted && string(raw) == "NULL" && (r.command.quote != 0 || r.command.escape == 0)
			fields = append(fields, loadField{data: data, null: null})
		}
		for {
			b, e := r.reader.ReadByte()
			if e == io.EOF {
				if quoted && !closed {
					return nil, io.ErrUnexpectedEOF
				}
				finish()
				if r.width > 0 && len(fields) > r.width {
					return nil, &protocolError{1262, "HY000", "too many LOAD DATA fields"}
				}
				return fields, nil
			}
			if e != nil {
				return nil, e
			}
			total++
			started = true
			if total > r.limit {
				return nil, &protocolError{1153, "08S01", "LOAD DATA row exceeds configured packet limit"}
			}
			if r.command.escape != 0 && b == r.command.escape && !(quoted && b == r.command.quote) {
				next, e := r.reader.ReadByte()
				if e != nil {
					return nil, io.ErrUnexpectedEOF
				}
				total++
				if total > r.limit {
					return nil, &protocolError{1153, "08S01", "LOAD DATA row exceeds configured packet limit"}
				}
				raw = append(raw, b, next)
				switch next {
				case '0':
					next = 0
				case 'b':
					next = '\b'
				case 'n':
					next = '\n'
				case 'r':
					next = '\r'
				case 't':
					next = '\t'
				case 'Z':
					next = 26
				}
				data = append(data, next)
				continue
			}
			if quoted && !closed {
				if b == r.command.quote {
					peek, e := r.reader.Peek(1)
					if e == nil && peek[0] == b {
						_, _ = r.reader.ReadByte()
						total++
						raw = append(raw, b, b)
						data = append(data, b)
						continue
					}
					closed = true
					continue
				}
				raw = append(raw, b)
				data = append(data, b)
				continue
			}
			if b == r.command.delimiter {
				finish()
				if r.width > 0 && len(fields) >= r.width {
					return nil, &protocolError{1262, "HY000", "too many LOAD DATA fields"}
				}
				break
			}
			line := false
			if b == r.command.line[0] {
				if len(r.command.line) == 1 {
					line = true
				} else if peek, e := r.reader.Peek(1); e == nil && peek[0] == '\n' {
					_, _ = r.reader.ReadByte()
					total++
					line = true
				}
			}
			if line {
				finish()
				if r.width > 0 && len(fields) > r.width {
					return nil, &protocolError{1262, "HY000", "too many LOAD DATA fields"}
				}
				return fields, nil
			}
			if closed {
				return nil, &protocolError{1261, "HY000", "characters follow enclosed LOAD DATA field"}
			}
			raw = append(raw, b)
			data = append(data, b)
		}
	}
}
