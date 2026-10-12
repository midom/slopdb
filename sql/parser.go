package sql

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Parser struct {
	tok            *Tokenizer
	cur            Token
	parameters     bool
	parameterCount int
}

func Parse(sql string) ([]Statement, error) {
	p := &Parser{tok: NewTokenizer(sql)}
	return p.parse()
}

// ParseError locates a syntax error in the original SQL text. ByteOffset is
// zero based; Line and Column are one based. Column counts Unicode code points.
// Unwrap preserves the parser's specific error for callers inspecting it.
type ParseError struct {
	ByteOffset int
	Line       int
	Column     int
	Err        error
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("line %d, column %d: %v", e.Line, e.Column, e.Err)
}

func (e *ParseError) Unwrap() error { return e.Err }

func (p *Parser) parseError(err error) error {
	if err == nil {
		return nil
	}
	input := p.tok.Input()
	offset := p.cur.Start
	if offset > len(input) {
		offset = len(input)
	}
	line, column := 1, 1
	for _, r := range input[:offset] {
		if r == '\n' {
			line++
			column = 1
		} else {
			column++
		}
	}
	return &ParseError{ByteOffset: offset, Line: line, Column: column, Err: err}
}

func (p *Parser) parse() ([]Statement, error) {
	p.advance()

	var stmts []Statement
	for p.cur.Type != TokEOF {
		if p.cur.Type == TokSemicolon {
			p.advance()
			continue
		}
		stmt, err := p.parseStatement()
		if err != nil {
			return nil, p.parseError(err)
		}
		stmts = append(stmts, stmt)
		if p.cur.Type == TokSemicolon {
			p.advance()
		} else if p.cur.Type != TokEOF {
			return nil, p.parseError(fmt.Errorf("expected semicolon, got %q", p.cur.Value))
		}
	}
	return stmts, nil
}

func (p *Parser) advance() {
	p.cur = p.tok.Next()
}

func (p *Parser) expect(typ TokenType) error {
	if p.cur.Type != typ {
		return fmt.Errorf("expected %s, got %s", tokenDescription(Token{Type: typ}), tokenDescription(p.cur))
	}
	p.advance()
	return nil
}

func tokenDescription(token Token) string {
	if token.Type == TokEOF {
		return "end of SQL"
	}
	if token.Value != "" {
		return strconv.Quote(token.Value)
	}
	switch token.Type {
	case TokIdent:
		return "an identifier"
	case TokString:
		return "a string literal"
	case TokInt:
		return "an integer"
	case TokFloat:
		return "a number"
	case TokLParen:
		return strconv.Quote("(")
	case TokRParen:
		return strconv.Quote(")")
	case TokComma:
		return strconv.Quote(",")
	case TokSemicolon:
		return strconv.Quote(";")
	case TokEq:
		return strconv.Quote("=")
	default:
		return fmt.Sprintf("token %d", token.Type)
	}
}

func (p *Parser) expectKeyword(kw string) error {
	if p.cur.Type != TokKeyword || !strings.EqualFold(p.cur.Value, kw) {
		return fmt.Errorf("expected keyword %q, got %s", kw, tokenDescription(p.cur))
	}
	p.advance()
	return nil
}

func (p *Parser) parseLimit(limit, offset *int64, limitParam, offsetParam *int) error {
	if p.cur.Value != "LIMIT" {
		return nil
	}
	p.advance()
	number := func(param *int) (int64, error) {
		if p.cur.Type == TokParameter {
			if !p.parameters {
				return 0, fmt.Errorf("parameters require Prepare")
			}
			p.parameterCount++
			*param = p.parameterCount
			p.advance()
			return 0, nil
		}
		sign := ""
		if p.cur.Type == TokMinus || p.cur.Type == TokPlus {
			sign = p.cur.Value
			p.advance()
		}
		if p.cur.Type != TokInt {
			return 0, fmt.Errorf("expected integer LIMIT/OFFSET")
		}
		n, err := strconv.ParseInt(sign+p.cur.Value, 10, 64)
		if err != nil {
			return 0, err
		}
		p.advance()
		return n, nil
	}
	n, err := number(limitParam)
	if err != nil {
		return err
	}
	*limit = n
	if p.cur.Type == TokComma {
		p.advance()
		*offset = *limit
		*offsetParam, *limitParam = *limitParam, 0
		*limit, err = number(limitParam)
	} else if p.cur.Value == "OFFSET" {
		p.advance()
		*offset, err = number(offsetParam)
	}
	if *offset < 0 {
		*offset = 0
	}
	return err
}

func (p *Parser) parseStatement() (Statement, error) {
	if p.showWord("RENAME") {
		return p.parseRenameTables()
	}
	if p.cur.Type == TokKeyword {
		switch strings.ToUpper(p.cur.Value) {
		case "SHOW":
			return p.parseShow()
		case "DESCRIBE", "DESC":
			return p.parseDescribe()
		case "EXPLAIN":
			return p.parseExplain()
		case "PRAGMA":
			return p.parsePragma()
		case "ANALYZE":
			return p.parseAnalyze()
		case "CREATE":
			return p.parseCreate()
		case "INSERT", "REPLACE":
			return p.parseInsert()
		case "SELECT":
			return p.parseSelect()
		case "WITH":
			return p.parseWith()
		case "DELETE":
			return p.parseDelete()
		case "UPDATE":
			return p.parseUpdate()
		case "DROP":
			return p.parseDrop()
		case "BEGIN", "START":
			return p.parseBegin()
		case "COMMIT":
			return p.parseCommit()
		case "ROLLBACK":
			return p.parseRollback()
		case "SAVEPOINT":
			return p.parseSavepoint()
		case "RELEASE":
			return p.parseRelease()
		case "RENAME":
			return p.parseRenameTables()
		case "ALTER":
			return p.parseAlterTable()
		case "VACUUM":
			p.advance()
			return &VacuumStmt{}, nil
		}
	}
	return nil, fmt.Errorf("unexpected token: %q", p.cur.Value)
}

func (p *Parser) parseCreate() (Statement, error) {
	p.advance()
	kw := strings.ToUpper(p.cur.Value)
	switch kw {
	case "TABLE":
		return p.parseCreateTable()
	case "INDEX", "UNIQUE":
		return p.parseCreateIndex()
	case "VIEW":
		return p.parseCreateView()
	case "TRIGGER":
		return p.parseCreateTrigger()
	}
	return nil, fmt.Errorf("expected TABLE, INDEX, VIEW, or TRIGGER after CREATE, got %q", p.cur.Value)
}

func (p *Parser) parseCreateTable() (Statement, error) {
	p.advance()

	ifNotExists := false
	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "IF" {
		p.advance()
		if err := p.expectKeyword("NOT"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		ifNotExists = true
	}

	tableName := p.cur.Value
	if err := p.expect(TokIdent); err != nil {
		return nil, err
	}
	if err := p.expect(TokLParen); err != nil {
		return nil, err
	}

	stmt := &CreateTableStmt{Table: strings.ToLower(tableName), IfNotExists: ifNotExists}

	expectComma := false
	for {
		if p.cur.Type == TokRParen {
			p.advance()
			break
		}

		if expectComma {
			if p.cur.Type != TokComma {
				return nil, fmt.Errorf("expected comma in CREATE TABLE")
			}
			p.advance()
		}
		expectComma = true

		if p.cur.Type == TokRParen {
			p.advance()
			break
		}

		kw := strings.ToUpper(p.cur.Value)
		if p.cur.Type == TokKeyword && (kw == "INDEX" || kw == "KEY") {
			p.advance()
			def, err := p.parseInlineIndex(false, "")
			if err != nil {
				return nil, err
			}
			stmt.Indexes = append(stmt.Indexes, def)
			continue
		}
		if p.cur.Type == TokKeyword && kw == "CONSTRAINT" {
			c, err := p.parseAlterConstraint()
			if err != nil {
				return nil, err
			}
			stmt.Constraints = append(stmt.Constraints, *c)
			switch c.Kind {
			case "PRIMARY":
				if len(stmt.PKColumns) > 0 {
					return nil, fmt.Errorf("multiple PRIMARY KEY declarations")
				}
				stmt.PKColumns = c.Columns
			case "UNIQUE":
				stmt.TableUnique = append(stmt.TableUnique, c.Columns)
			case "CHECK":
				stmt.TableCheck = append(stmt.TableCheck, c.Check)
			case "FOREIGN":
				stmt.ForeignKeys = append(stmt.ForeignKeys, ForeignKeyDef{Columns: c.Columns, RefTable: c.RefTable, RefColumns: c.RefColumns})
			}
			continue
		}
		if p.cur.Type == TokKeyword && kw == "PRIMARY" {
			if err := p.parseTableConstraint(stmt); err != nil {
				return nil, err
			}
			continue
		}
		if p.cur.Type == TokKeyword && kw == "UNIQUE" {
			if err := p.parseTableConstraint(stmt); err != nil {
				return nil, err
			}
			continue
		}
		if p.cur.Type == TokKeyword && kw == "CHECK" {
			if err := p.parseTableConstraint(stmt); err != nil {
				return nil, err
			}
			continue
		}
		if p.cur.Type == TokKeyword && kw == "FOREIGN" {
			if err := p.parseTableConstraint(stmt); err != nil {
				return nil, err
			}
			continue
		}

		col := ColumnDef{}
		col.Name = strings.ToLower(p.cur.Value)
		if err := p.expect(TokIdent); err != nil {
			return nil, err
		}
		if err := p.parseColumnDef(&col); err != nil {
			return nil, err
		}
		stmt.Columns = append(stmt.Columns, col)
	}

	inline := 0
	for _, col := range stmt.Columns {
		if col.PrimaryKey {
			inline++
		}
	}
	if inline > 1 || (inline > 0 && len(stmt.PKColumns) > 0) {
		return nil, fmt.Errorf("multiple PRIMARY KEY declarations")
	}
	return stmt, nil
}

func (p *Parser) parseColumnDef(col *ColumnDef, alteration ...bool) error {
	if p.cur.Type == TokKeyword || (p.cur.Type == TokIdent && (strings.EqualFold(p.cur.Value, "BIGINT") || strings.EqualFold(p.cur.Value, "DOUBLE"))) {
		switch strings.ToUpper(p.cur.Value) {
		case "INTEGER", "INT", "BIGINT":
			col.ColType = ColInteger
			p.advance()
		case "TEXT":
			col.ColType = ColText
			p.advance()
		case "VARCHAR":
			col.ColType = ColText
			p.advance()
			length, err := p.parseStringLength()
			if err != nil {
				return err
			}
			col.Length = length
		case "REAL", "DOUBLE":
			double := strings.EqualFold(p.cur.Value, "DOUBLE")
			col.ColType = ColReal
			p.advance()
			if double && strings.EqualFold(p.cur.Value, "PRECISION") {
				p.advance()
			}
		case "BLOB":
			col.ColType = ColBlob
			p.advance()
		case "VARBINARY":
			col.ColType = ColBlob
			p.advance()
			length, err := p.parseStringLength()
			if err != nil {
				return err
			}
			col.Length = length
		default:
			col.ColType = ColText
		}
	} else {
		col.ColType = ColText
	}

	pending := ""
	nameConstraint := func(kind string) {
		if pending != "" {
			if col.ConstraintNames == nil {
				col.ConstraintNames = make(map[string]string)
			}
			col.ConstraintNames[kind] = pending
			pending = ""
		}
	}
	for p.cur.Type != TokComma && p.cur.Type != TokRParen && p.cur.Type != TokSemicolon && p.cur.Type != TokEOF {
		kw := strings.ToUpper(p.cur.Value)
		if kw == "CONSTRAINT" {
			if pending != "" {
				return fmt.Errorf("constraint name without a definition")
			}
			p.advance()
			var err error
			pending, err = p.alterName()
			if err != nil {
				return err
			}
			continue
		}
		if len(alteration) > 0 && alteration[0] && (kw == "FIRST" || kw == "AFTER") {
			return nil
		}

		if p.cur.Type == TokKeyword && kw == "PRIMARY" {
			p.advance()
			if err := p.expectKeyword("KEY"); err != nil {
				return err
			}
			if col.PrimaryKey {
				return fmt.Errorf("multiple PRIMARY KEY declarations")
			}
			col.PrimaryKey = true
			nameConstraint("PRIMARY")
			if p.cur.Type == TokKeyword {
				if strings.ToUpper(p.cur.Value) == "ASC" || strings.ToUpper(p.cur.Value) == "DESC" {
					p.advance()
				}
			}
			if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "AUTOINCREMENT" {
				col.AutoIncrement = true
				p.advance()
			}
			continue
		}

		if p.cur.Type == TokKeyword && kw == "NOT" {
			p.advance()
			if err := p.expectKeyword("NULL"); err != nil {
				return err
			}
			col.NotNull = true
			nameConstraint("NOT_NULL")
			if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "ON" {
				if err := p.parseConflictClause(); err != nil {
					return err
				}
			}
			continue
		}

		if p.cur.Type == TokKeyword && kw == "DEFAULT" {
			p.advance()
			if p.cur.Type == TokLParen {
				p.advance()
				var err error
				col.Default, err = p.parseExpression()
				if err != nil {
					return err
				}
				if err := p.expect(TokRParen); err != nil {
					return err
				}
			} else {
				var err error
				col.Default, err = p.parseExpression()
				if err != nil {
					return err
				}
			}
			continue
		}

		if p.cur.Type == TokKeyword && kw == "CHECK" {
			p.advance()
			if err := p.expect(TokLParen); err != nil {
				return err
			}
			var err error
			col.Check, err = p.parseExpression()
			nameConstraint("CHECK")
			if err != nil {
				return err
			}
			if err := p.expect(TokRParen); err != nil {
				return err
			}
			continue
		}

		if p.cur.Type == TokKeyword && kw == "UNIQUE" {
			p.advance()
			col.Unique = true
			nameConstraint("UNIQUE")
			if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "ON" {
				if err := p.parseConflictClause(); err != nil {
					return err
				}
			}
			continue
		}

		if p.cur.Type == TokKeyword && kw == "REFERENCES" {
			p.advance()
			nameConstraint("FOREIGN")
			col.RefTable = strings.ToLower(p.cur.Value)
			if err := p.expect(TokIdent); err != nil {
				return err
			}
			if p.cur.Type == TokLParen {
				p.advance()
				for {
					col.RefColumns = append(col.RefColumns, strings.ToLower(p.cur.Value))
					if err := p.expect(TokIdent); err != nil {
						return err
					}
					if p.cur.Type != TokComma {
						break
					}
					p.advance()
				}
				if err := p.expect(TokRParen); err != nil {
					return err
				}
			}
			if err := p.parseReferenceActions(); err != nil {
				return err
			}
			continue
		}

		if p.cur.Type == TokKeyword && kw == "COLLATE" {
			p.advance()
			if !strings.EqualFold(p.cur.Value, "BINARY") {
				return fmt.Errorf("unsupported collation: %s", p.cur.Value)
			}
			if col.ColType != ColText {
				return fmt.Errorf("BINARY collation requires a TEXT column")
			}
			col.Binary = true
			p.advance()
			continue
		}
		if p.cur.Type == TokKeyword && kw == "BINARY" {
			if col.ColType != ColText {
				return fmt.Errorf("BINARY modifier requires a TEXT column")
			}
			col.Binary = true
			p.advance()
			continue
		}

		if p.cur.Type == TokKeyword && kw == "AUTOINCREMENT" {
			p.advance()
			col.AutoIncrement = true
			continue
		}

		if p.cur.Type == TokLParen {
			p.advance()
			p.parseExpression()
			p.expect(TokRParen)
			continue
		}

		return fmt.Errorf("unsupported column constraint: %s", p.cur.Value)
	}

	if pending != "" {
		return fmt.Errorf("constraint name without a supported definition")
	}
	return nil
}

func (p *Parser) parseStringLength() (int64, error) {
	if err := p.expect(TokLParen); err != nil {
		return 0, err
	}
	if p.cur.Type != TokInt {
		return 0, fmt.Errorf("expected positive VARCHAR/VARBINARY length")
	}
	length, err := strconv.ParseInt(p.cur.Value, 10, 64)
	if err != nil || length <= 0 {
		return 0, fmt.Errorf("invalid VARCHAR/VARBINARY length %q", p.cur.Value)
	}
	p.advance()
	if err := p.expect(TokRParen); err != nil {
		return 0, err
	}
	return length, nil
}

func (p *Parser) parseTableConstraint(stmt *CreateTableStmt) error {
	kw := strings.ToUpper(p.cur.Value)

	if kw == "PRIMARY" {
		p.advance()
		if err := p.expectKeyword("KEY"); err != nil {
			return err
		}
		if err := p.expect(TokLParen); err != nil {
			return err
		}
		var cols []string
		for {
			cols = append(cols, strings.ToLower(p.cur.Value))
			p.expect(TokIdent)
			if p.cur.Type == TokComma {
				p.advance()
			} else {
				break
			}
		}
		if err := p.expect(TokRParen); err != nil {
			return err
		}
		if len(stmt.PKColumns) > 0 {
			return fmt.Errorf("multiple PRIMARY KEY declarations")
		}
		stmt.PKColumns = cols
		return nil
	}

	if kw == "UNIQUE" {
		p.advance()
		if p.cur.Type != TokLParen {
			def, err := p.parseInlineIndex(true, "")
			if err != nil {
				return err
			}
			stmt.Indexes = append(stmt.Indexes, def)
			return nil
		}
		if err := p.expect(TokLParen); err != nil {
			return err
		}
		var cols []string
		for {
			cols = append(cols, strings.ToLower(p.cur.Value))
			if p.cur.Type == TokIdent || p.cur.Type == TokKeyword {
				p.advance()
			}
			if p.cur.Type == TokComma {
				p.advance()
			} else {
				break
			}
		}
		if err := p.expect(TokRParen); err != nil {
			return err
		}
		stmt.TableUnique = append(stmt.TableUnique, cols)
		return nil
	}

	if kw == "CHECK" {
		p.advance()
		if err := p.expect(TokLParen); err != nil {
			return err
		}
		expr, err := p.parseExpression()
		if err != nil {
			return err
		}
		if err := p.expect(TokRParen); err != nil {
			return err
		}
		stmt.TableCheck = append(stmt.TableCheck, expr)
		return nil
	}

	if kw == "FOREIGN" {
		p.advance()
		if err := p.expectKeyword("KEY"); err != nil {
			return err
		}
		if err := p.expect(TokLParen); err != nil {
			return err
		}
		var cols []string
		for {
			cols = append(cols, strings.ToLower(p.cur.Value))
			if p.cur.Type == TokIdent || p.cur.Type == TokKeyword {
				p.advance()
			}
			if p.cur.Type == TokComma {
				p.advance()
			} else {
				break
			}
		}
		if err := p.expect(TokRParen); err != nil {
			return err
		}
		if err := p.expectKeyword("REFERENCES"); err != nil {
			return err
		}
		refTable := strings.ToLower(p.cur.Value)
		if p.cur.Type == TokIdent || p.cur.Type == TokKeyword {
			p.advance()
		}
		var refCols []string
		if p.cur.Type == TokLParen {
			p.advance()
			for {
				refCols = append(refCols, strings.ToLower(p.cur.Value))
				if err := p.expect(TokIdent); err != nil {
					return err
				}
				if p.cur.Type != TokComma {
					break
				}
				p.advance()
			}
			if err := p.expect(TokRParen); err != nil {
				return err
			}
		}
		if err := p.parseReferenceActions(); err != nil {
			return err
		}

		stmt.ForeignKeys = append(stmt.ForeignKeys, ForeignKeyDef{
			Columns:    cols,
			RefTable:   refTable,
			RefColumns: refCols,
		})
		return nil
	}

	return fmt.Errorf("unknown table constraint: %s", kw)
}

func (p *Parser) parseConflictClause() error {
	p.advance()
	if err := p.expectKeyword("CONFLICT"); err != nil {
		return err
	}
	if !strings.EqualFold(p.cur.Value, "ABORT") {
		return fmt.Errorf("unsupported constraint conflict policy: %s", p.cur.Value)
	}
	p.advance()
	return nil
}

func (p *Parser) parseCreateIndex() (Statement, error) {
	unique := false
	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "UNIQUE" {
		unique = true
		p.advance()
	}

	if err := p.expectKeyword("INDEX"); err != nil {
		return nil, err
	}

	ifNotExists := false
	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "IF" {
		p.advance()
		if err := p.expectKeyword("NOT"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		ifNotExists = true
	}

	indexName := p.cur.Value
	if err := p.expect(TokIdent); err != nil {
		return nil, err
	}

	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}

	tableName := p.cur.Value
	if err := p.expect(TokIdent); err != nil {
		return nil, err
	}

	columns, err := p.parseIndexColumns()
	if err != nil {
		return nil, err
	}

	abortAfter, err := p.parsePublicationPolicy()
	if err != nil {
		return nil, err
	}
	return &CreateIndexStmt{Index: strings.ToLower(indexName), Table: strings.ToLower(tableName), Column: columns[0], Columns: columns, Unique: unique, IfNotExists: ifNotExists, PublicationAbortAfter: abortAfter}, nil
}

// parsePublicationPolicy accepts only quoted Go duration syntax, including an
// explicit zero for immediate cancellation. Absence preserves ordinary waits.
func (p *Parser) parsePublicationPolicy() (*time.Duration, error) {
	if !p.optionalAlterWord("KILL") {
		return nil, nil
	}
	if err := p.alterWords("TRANSACTIONS", "AFTER"); err != nil {
		return nil, err
	}
	if p.cur.Type != TokString {
		return nil, fmt.Errorf("KILL TRANSACTIONS AFTER requires a quoted duration")
	}
	duration, err := time.ParseDuration(p.cur.Value)
	if err != nil || duration < 0 {
		return nil, fmt.Errorf("KILL TRANSACTIONS AFTER requires a nonnegative duration, got %q", p.cur.Value)
	}
	p.advance()
	return &duration, nil
}

func (p *Parser) parseCreateView() (Statement, error) {
	p.advance()

	viewName := strings.ToLower(p.cur.Value)
	if p.cur.Type == TokIdent || p.cur.Type == TokKeyword {
		p.advance()
	} else {
		return nil, fmt.Errorf("expected view name, got %q", p.cur.Value)
	}

	stmt := &CreateViewStmt{View: viewName}

	if p.cur.Type == TokLParen {
		p.advance()
		for {
			colName := strings.ToLower(p.cur.Value)
			if p.cur.Type == TokIdent || p.cur.Type == TokKeyword {
				p.advance()
			}
			stmt.Columns = append(stmt.Columns, colName)
			if p.cur.Type == TokComma {
				p.advance()
			} else {
				break
			}
		}
		if err := p.expect(TokRParen); err != nil {
			return nil, err
		}
	}

	if err := p.expectKeyword("AS"); err != nil {
		return nil, err
	}

	sel, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	selected := selectQuery(sel)
	stmt.Select = selected
	stmt.SQL = formatSelectSQL(stmt.Select)

	return stmt, nil
}

func (p *Parser) parseCreateTrigger() (Statement, error) {
	p.advance()
	if p.cur.Type != TokIdent && p.cur.Type != TokKeyword {
		return nil, fmt.Errorf("expected trigger name")
	}
	stmt := &CreateTriggerStmt{Name: strings.ToLower(p.cur.Value), Timing: "BEFORE"}
	p.advance()
	if p.cur.Value == "INSTEAD" {
		p.advance()
		if err := p.expectKeyword("OF"); err != nil {
			return nil, err
		}
		stmt.InsteadOf = true
		stmt.Timing = "INSTEAD OF"
	} else if p.cur.Value == "BEFORE" || p.cur.Value == "AFTER" {
		stmt.Timing = p.cur.Value
		p.advance()
	}
	if p.cur.Value != "INSERT" && p.cur.Value != "UPDATE" && p.cur.Value != "DELETE" {
		return nil, fmt.Errorf("expected trigger event")
	}
	stmt.Event = p.cur.Value
	p.advance()
	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}
	if p.cur.Type != TokIdent && p.cur.Type != TokKeyword {
		return nil, fmt.Errorf("expected trigger target")
	}
	stmt.View = strings.ToLower(p.cur.Value)
	p.advance()
	if p.cur.Value == "FOR" {
		p.advance()
		if err := p.expectKeyword("EACH"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("ROW"); err != nil {
			return nil, err
		}
		stmt.ForEachRow = true
	}
	if p.cur.Value == "WHEN" {
		p.advance()
		var err error
		stmt.When, err = p.parseExpression()
		if err != nil {
			return nil, err
		}
	}
	if err := p.expectKeyword("BEGIN"); err != nil {
		return nil, err
	}
	for p.cur.Value != "END" {
		if p.cur.Type == TokEOF {
			return nil, fmt.Errorf("unterminated trigger body")
		}
		start := p.cur.Start
		body, err := p.parseStatement()
		if err != nil {
			return nil, err
		}
		if body != nil {
			stmt.Body = append(stmt.Body, body)
			stmt.BodySQL = append(stmt.BodySQL, strings.TrimSpace(p.tok.Input()[start:p.cur.Start]))
		}
		if err := p.expect(TokSemicolon); err != nil {
			return nil, err
		}
	}
	p.advance()
	return stmt, nil
}

func (p *Parser) parseInsert() (Statement, error) {
	replace := p.cur.Value == "REPLACE"
	p.advance()
	if err := p.expectKeyword("INTO"); err != nil {
		return nil, err
	}
	name := p.cur.Value
	if err := p.expect(TokIdent); err != nil {
		return nil, err
	}
	stmt := &InsertStmt{Table: strings.ToLower(name), Replace: replace}
	if p.cur.Type == TokLParen {
		p.advance()
		for {
			if p.cur.Type != TokIdent && p.cur.Type != TokKeyword {
				return nil, fmt.Errorf("expected INSERT column")
			}
			stmt.ColumnNames = append(stmt.ColumnNames, strings.ToLower(p.cur.Value))
			p.advance()
			if p.cur.Type != TokComma {
				break
			}
			p.advance()
		}
		if err := p.expect(TokRParen); err != nil {
			return nil, err
		}
	}
	if p.cur.Value == "VALUES" {
		p.advance()
		first := true
		for {
			if err := p.expect(TokLParen); err != nil {
				return nil, err
			}
			var group []Expression
			for {
				expr, err := p.parseExpression()
				if err != nil {
					return nil, err
				}
				group = append(group, expr)
				if p.cur.Type != TokComma {
					break
				}
				p.advance()
			}
			if err := p.expect(TokRParen); err != nil {
				return nil, err
			}
			if first {
				stmt.Values = group
				first = false
			} else {
				stmt.ValueGroups = append(stmt.ValueGroups, group)
			}
			if p.cur.Type != TokComma {
				break
			}
			p.advance()
		}
	} else if p.cur.Value == "SELECT" || p.cur.Value == "WITH" {
		selected, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		sel := selectQuery(selected)
		stmt.Select = sel
	} else if p.cur.Value == "DEFAULT" {
		p.advance()
		if err := p.expectKeyword("VALUES"); err != nil {
			return nil, err
		}
		stmt.DefaultValues = true
	} else {
		return nil, fmt.Errorf("expected VALUES, DEFAULT VALUES or SELECT")
	}
	if p.cur.Value == "ON" {
		if stmt.Replace {
			return nil, fmt.Errorf("REPLACE cannot have ON CONFLICT")
		}
		var conflict *OnConflictClause
		var err error
		p.advance() // ON
		if p.cur.Value == "DUPLICATE" {
			conflict, err = p.parseOnDuplicateKeyUpdate()
		} else {
			conflict, err = p.parseOnConflictAfterOn()
		}
		if err != nil {
			return nil, err
		}
		stmt.OnConflict = conflict
	}
	if p.cur.Value == "RETURNING" {
		p.advance()
		for {
			col, err := p.parseSelectColumn()
			if err != nil {
				return nil, err
			}
			stmt.Returning = append(stmt.Returning, col)
			if p.cur.Type != TokComma {
				break
			}
			p.advance()
		}
	}
	return stmt, nil
}

// parseOnDuplicateKeyUpdate accepts MySQL's all-unique-keys UPSERT form.
// VALUES(column) references the value from the attempted insert row.
func (p *Parser) parseOnDuplicateKeyUpdate() (*OnConflictClause, error) {
	if !strings.EqualFold(p.cur.Value, "DUPLICATE") {
		return nil, fmt.Errorf("expected DUPLICATE, got %q", p.cur.Value)
	}
	p.advance()
	if err := p.expectKeyword("KEY"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("UPDATE"); err != nil {
		return nil, err
	}
	c := &OnConflictClause{}
	for {
		if p.cur.Type != TokIdent && p.cur.Type != TokKeyword {
			return nil, fmt.Errorf("expected duplicate-key update column")
		}
		name := strings.ToLower(p.cur.Value)
		p.advance()
		if err := p.expect(TokEq); err != nil {
			return nil, err
		}
		e, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		e, err = rewriteInsertValues(e)
		if err != nil {
			return nil, err
		}
		c.Set = append(c.Set, UpdateSet{Column: name, Value: e})
		if p.cur.Type != TokComma {
			break
		}
		p.advance()
	}
	return c, nil
}

func (p *Parser) parseSelect() (Statement, error) {
	if p.cur.Value == "WITH" {
		query, err := p.parseWith()
		if err != nil {
			return nil, err
		}
		switch query.(type) {
		case *SelectStmt, *UnionStmt:
			return query, nil
		}
		return nil, fmt.Errorf("expected SELECT after WITH in query context")
	}
	if err := p.expectKeyword("SELECT"); err != nil {
		return nil, err
	}

	stmt := &SelectStmt{Limit: -1, Offset: 0}

	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "DISTINCT" {
		stmt.Distinct = true
		p.advance()
	}

	for {
		col, err := p.parseSelectColumn()
		if err != nil {
			return nil, err
		}
		stmt.Columns = append(stmt.Columns, col)
		if p.cur.Type != TokComma {
			break
		}
		p.advance()
	}
	stmt.SelectAll = len(stmt.Columns) == 1 && stmt.Columns[0].Star

	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "FROM" {
		p.advance()
		stmt.From, stmt.FromAlias, stmt.FromExpr = p.parseTableRef()
		if stmt.FromAlias == "" {
			stmt.FromAlias = stmt.From
		}
		for p.cur.Type == TokComma {
			p.advance()
			t, a, e := p.parseTableRef()
			if a == "" {
				a = t
			}
			jc := JoinClause{Type: "CROSS", Table: t, Alias: a}
			if sub, ok := e.(*SubqueryExpr); ok && sub != nil {
				jc.Subquery = sub.Stmt
			}
			stmt.Joins = append(stmt.Joins, jc)
		}
		for p.cur.Type == TokKeyword {
			kw := strings.ToUpper(p.cur.Value)
			if kw == "INNER" || kw == "LEFT" || kw == "RIGHT" || kw == "CROSS" || kw == "NATURAL" {
				jc, err := p.parseJoinClause(kw)
				if err != nil {
					return nil, err
				}
				stmt.Joins = append(stmt.Joins, jc)
			} else if kw == "JOIN" {
				jc, err := p.parseJoinClause("INNER")
				if err != nil {
					return nil, err
				}
				stmt.Joins = append(stmt.Joins, jc)
			} else {
				break
			}
		}
	}

	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "WHERE" {
		p.advance()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "GROUP" {
		p.advance()
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		for {
			expr, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			stmt.GroupBy = append(stmt.GroupBy, expr)
			if p.cur.Type == TokComma {
				p.advance()
			} else {
				break
			}
		}
	}

	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "HAVING" {
		p.advance()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Having = expr
	}

	if err := p.parseWindowDefinitions(stmt); err != nil {
		return nil, err
	}

	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "ORDER" {
		p.advance()
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		for {
			expr, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			desc := false
			if p.cur.Type == TokKeyword {
				if strings.ToUpper(p.cur.Value) == "DESC" {
					desc = true
					p.advance()
				} else if strings.ToUpper(p.cur.Value) == "ASC" {
					p.advance()
				}
			}
			nullsFirst := false
			nullsLast := false
			if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "NULLS" {
				p.advance()
				if p.cur.Type == TokKeyword {
					if strings.ToUpper(p.cur.Value) == "FIRST" {
						nullsFirst = true
						p.advance()
					} else if strings.ToUpper(p.cur.Value) == "LAST" {
						nullsLast = true
						p.advance()
					}
				}
			}
			stmt.OrderBy = append(stmt.OrderBy, OrderByClause{Expr: expr, Desc: desc, NullsFirst: nullsFirst, NullsLast: nullsLast})
			if p.cur.Type == TokComma {
				p.advance()
			} else {
				break
			}
		}
	}

	if err := p.parseLimit(&stmt.Limit, &stmt.Offset, &stmt.LimitParam, &stmt.OffsetParam); err != nil {
		return nil, err
	}
	if p.cur.Type == TokKeyword && (p.cur.Value == "FOR" || p.cur.Value == "LOCK") {
		lock := &SelectLock{}
		if p.cur.Value == "FOR" {
			p.advance()
			switch p.cur.Value {
			case "UPDATE":
				lock.Exclusive = true
			case "SHARE":
			default:
				return nil, fmt.Errorf("expected UPDATE or SHARE after FOR")
			}
			p.advance()
		} else {
			p.advance()
			for _, keyword := range []string{"IN", "SHARE", "MODE"} {
				if err := p.expectKeyword(keyword); err != nil {
					return nil, err
				}
			}
			lock.ShareMode = true
		}
		if p.cur.Type == TokKeyword && p.cur.Value == "NOWAIT" {
			lock.Wait = LockNoWait
			p.advance()
		} else if p.cur.Type == TokKeyword && p.cur.Value == "SKIP" {
			p.advance()
			if err := p.expectKeyword("LOCKED"); err != nil {
				return nil, err
			}
			lock.Wait = LockSkipLocked
		}
		stmt.Lock = lock
	}

	if p.cur.Type == TokKeyword {
		kw := strings.ToUpper(p.cur.Value)
		if kw == "UNION" || kw == "INTERSECT" || kw == "EXCEPT" {
			if stmt.Lock != nil {
				return nil, fmt.Errorf("locking clauses are not supported on compound SELECTs")
			}
			all := false
			p.advance()
			if kw == "UNION" && p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "ALL" {
				all = true
				p.advance()
			}
			right, err := p.parseSelect()
			if err != nil {
				return nil, err
			}
			union := &UnionStmt{Left: stmt, Right: right, All: all, Op: kw, Limit: -1}
			switch tail := right.(type) {
			case *SelectStmt:
				if tail.Lock != nil {
					return nil, fmt.Errorf("locking clauses are not supported on compound SELECTs")
				}
				copy := *tail
				union.OrderBy = copy.OrderBy
				union.Limit = copy.Limit
				union.Offset = copy.Offset
				union.LimitParam, union.OffsetParam = copy.LimitParam, copy.OffsetParam
				copy.OrderBy = nil
				copy.Limit = -1
				copy.Offset = 0
				copy.LimitParam, copy.OffsetParam = 0, 0
				union.Right = &copy
			case *UnionStmt:
				copy := *tail
				union.OrderBy = copy.OrderBy
				union.Limit = copy.Limit
				union.Offset = copy.Offset
				union.LimitParam, union.OffsetParam = copy.LimitParam, copy.OffsetParam
				copy.OrderBy = nil
				copy.Limit = -1
				copy.Offset = 0
				copy.LimitParam, copy.OffsetParam = 0, 0
				union.Right = &copy
			}
			return union, nil
		}
	}

	return stmt, nil
}

func (p *Parser) parseSelectColumn() (SelectColumn, error) {
	if p.cur.Type == TokStar {
		p.advance()
		return SelectColumn{Star: true}, nil
	}

	expr, err := p.parseExpression()
	if err != nil {
		return SelectColumn{}, err
	}

	sc := SelectColumn{Expr: expr}
	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "AS" {
		p.advance()
		sc.Alias = p.cur.Value
		p.advance()
	} else if p.cur.Type == TokIdent {
		sc.Alias = p.cur.Value
		p.advance()
	}
	return sc, nil
}

func (p *Parser) parseDelete() (Statement, error) {
	p.advance()

	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}

	stmt := &DeleteStmt{Limit: -1}
	stmt.From = strings.ToLower(p.cur.Value)
	if err := p.expect(TokIdent); err != nil {
		return nil, err
	}

	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "WHERE" {
		p.advance()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	if err := p.parseLimit(&stmt.Limit, &stmt.Offset, &stmt.LimitParam, &stmt.OffsetParam); err != nil {
		return nil, err
	}

	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "RETURNING" {
		p.advance()
		for {
			col, err := p.parseSelectColumn()
			if err != nil {
				return nil, err
			}
			stmt.Returning = append(stmt.Returning, col)
			if p.cur.Type == TokComma {
				p.advance()
			} else {
				break
			}
		}
	}

	return stmt, nil
}

func (p *Parser) parseUpdate() (Statement, error) {
	p.advance()

	stmt := &UpdateStmt{Limit: -1}
	stmt.Table = strings.ToLower(p.cur.Value)
	if err := p.expect(TokIdent); err != nil {
		return nil, err
	}

	if err := p.expectKeyword("SET"); err != nil {
		return nil, err
	}

	for {
		set := UpdateSet{}
		set.Column = strings.ToLower(p.cur.Value)
		if err := p.expect(TokIdent); err != nil {
			return nil, err
		}
		if err := p.expect(TokEq); err != nil {
			return nil, err
		}
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		set.Value = expr
		stmt.Set = append(stmt.Set, set)

		if p.cur.Type == TokComma {
			p.advance()
		} else {
			break
		}
	}

	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "WHERE" {
		p.advance()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	if err := p.parseLimit(&stmt.Limit, &stmt.Offset, &stmt.LimitParam, &stmt.OffsetParam); err != nil {
		return nil, err
	}

	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "RETURNING" {
		p.advance()
		for {
			col, err := p.parseSelectColumn()
			if err != nil {
				return nil, err
			}
			stmt.Returning = append(stmt.Returning, col)
			if p.cur.Type == TokComma {
				p.advance()
			} else {
				break
			}
		}
	}

	return stmt, nil
}

func (p *Parser) parseDrop() (Statement, error) {
	p.advance()
	if p.cur.Value == "TRIGGER" {
		p.advance()
		if p.cur.Type != TokIdent && p.cur.Type != TokKeyword {
			return nil, fmt.Errorf("expected trigger name")
		}
		name := strings.ToLower(p.cur.Value)
		p.advance()
		return &DropTriggerStmt{Name: name}, nil
	}

	kw := strings.ToUpper(p.cur.Value)
	if kw == "TABLE" {
		p.advance()
		tableName := p.cur.Value
		if err := p.expect(TokIdent); err != nil {
			return nil, err
		}
		return &DropTableStmt{Table: strings.ToLower(tableName)}, nil
	}
	if kw == "VIEW" {
		p.advance()
		viewName := p.cur.Value
		if p.cur.Type == TokIdent || p.cur.Type == TokKeyword {
			p.advance()
		}
		return &DropViewStmt{View: strings.ToLower(viewName)}, nil
	}
	if kw == "INDEX" {
		p.advance()
		idxName := p.cur.Value
		if p.cur.Type == TokIdent || p.cur.Type == TokKeyword {
			p.advance()
		}
		abortAfter, err := p.parsePublicationPolicy()
		if err != nil {
			return nil, err
		}
		return &DropIndexStmt{Index: strings.ToLower(idxName), PublicationAbortAfter: abortAfter}, nil
	}
	return nil, fmt.Errorf("expected TABLE, VIEW, or INDEX after DROP, got %q", p.cur.Value)
}

func (p *Parser) parseExpression() (Expression, error) {
	return p.parseOr()
}

func (p *Parser) parseOr() (Expression, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "OR" {
		p.advance()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: "OR", Right: right}
	}

	return left, nil
}

func (p *Parser) parseAnd() (Expression, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "AND" {
		p.advance()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: "AND", Right: right}
	}

	return left, nil
}

func (p *Parser) parseNot() (Expression, error) {
	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "NOT" {
		peek := p.tok.Peek()
		if peek.Type == TokKeyword && strings.ToUpper(peek.Value) == "EXISTS" {
			p.advance()
			p.advance()
			sub, err := p.parseSubqueryParens()
			if err != nil {
				return nil, err
			}
			return &ExistsExpr{Stmt: sub, Not: true}, nil
		}
		p.advance()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: "NOT", Right: right}, nil
	}
	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "EXISTS" {
		p.advance()
		sub, err := p.parseSubqueryParens()
		if err != nil {
			return nil, err
		}
		return &ExistsExpr{Stmt: sub, Not: false}, nil
	}
	return p.parseComparison()
}

func (p *Parser) parseComparison() (Expression, error) {
	left, err := p.parseBitwise()
	if err != nil {
		return nil, err
	}

	ops := map[TokenType]string{TokEq: "=", TokLt: "<", TokGt: ">", TokLe: "<=", TokGe: ">=", TokNe: "<>"}
	if op, ok := ops[p.cur.Type]; ok {
		p.advance()
		if p.cur.Type == TokLParen && p.tok.Peek().Type == TokKeyword &&
			(strings.ToUpper(p.tok.Peek().Value) == "SELECT" || strings.ToUpper(p.tok.Peek().Value) == "WITH") {
			sub, err := p.parseSubqueryParens()
			if err != nil {
				return nil, err
			}
			return &BinaryExpr{Left: left, Op: op, Right: &SubqueryExpr{Stmt: sub}}, nil
		}
		right, err := p.parseBitwise()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: op, Right: right}, nil
	}

	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "IS" {
		p.advance()
		not := false
		if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "NOT" {
			not = true
			p.advance()
		}
		right, err := p.parseBitwise()
		if err != nil {
			return nil, err
		}
		op := "IS"
		if not {
			op = "IS NOT"
		}
		return &BinaryExpr{Left: left, Op: op, Right: right}, nil

	}

	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "LIKE" {
		p.advance()
		right, err := p.parseBitwise()
		if err != nil {
			return nil, err
		}
		be := &BinaryExpr{Left: left, Op: "LIKE", Right: right}
		if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "ESCAPE" {
			p.advance()
			if p.cur.Type == TokString {
				be.Escape = p.cur.Value[0]
				p.advance()
			}
		}
		return be, nil
	}

	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "GLOB" {
		p.advance()
		right, err := p.parseBitwise()
		if err != nil {
			return nil, err
		}
		be := &BinaryExpr{Left: left, Op: "GLOB", Right: right}
		if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "ESCAPE" {
			p.advance()
			if p.cur.Type == TokString {
				be.Escape = p.cur.Value[0]
				p.advance()
			}
		}
		return be, nil
	}

	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "REGEXP" {
		p.advance()
		right, err := p.parseBitwise()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: "REGEXP", Right: right}, nil
	}

	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "NOT" {
		peek := p.tok.Peek()
		if peek.Type == TokKeyword && strings.ToUpper(peek.Value) == "LIKE" {
			p.advance()
			p.advance()
			right, err := p.parseBitwise()
			if err != nil {
				return nil, err
			}
			be := &BinaryExpr{Left: left, Op: "NOT LIKE", Right: right}
			if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "ESCAPE" {
				p.advance()
				if p.cur.Type == TokString {
					be.Escape = p.cur.Value[0]
					p.advance()
				}
			}
			return be, nil
		}
		if peek.Type == TokKeyword && strings.ToUpper(peek.Value) == "GLOB" {
			p.advance()
			p.advance()
			right, err := p.parseBitwise()
			if err != nil {
				return nil, err
			}
			be := &BinaryExpr{Left: left, Op: "NOT GLOB", Right: right}
			if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "ESCAPE" {
				p.advance()
				if p.cur.Type == TokString {
					be.Escape = p.cur.Value[0]
					p.advance()
				}
			}
			return be, nil
		}
		if peek.Type == TokKeyword && strings.ToUpper(peek.Value) == "REGEXP" {
			p.advance()
			p.advance()
			right, err := p.parseBitwise()
			if err != nil {
				return nil, err
			}
			return &BinaryExpr{Left: left, Op: "NOT REGEXP", Right: right}, nil
		}
		if peek.Type == TokKeyword && strings.ToUpper(peek.Value) == "IN" {
			p.advance()
			p.advance()
			if err := p.expect(TokLParen); err != nil {
				return nil, err
			}
			if p.cur.Type == TokKeyword && (strings.ToUpper(p.cur.Value) == "SELECT" || strings.ToUpper(p.cur.Value) == "WITH") {
				sub, err := p.parseSelect()
				if err != nil {
					return nil, err
				}
				if err := p.expect(TokRParen); err != nil {
					return nil, err
				}
				selected := selectQuery(sub)
				return &BinaryExpr{Left: left, Op: "NOT IN", Right: &SubqueryExpr{Stmt: selected}}, nil
			}
			var list []Expression
			for p.cur.Type != TokRParen && p.cur.Type != TokEOF {
				expr, err := p.parseExpression()
				if err != nil {
					return nil, err
				}
				list = append(list, expr)
				if p.cur.Type == TokComma {
					p.advance()
				}
			}
			if err := p.expect(TokRParen); err != nil {
				return nil, err
			}
			return &InListExpr{Expr: left, List: list, Not: true}, nil
		}
		if peek.Type == TokKeyword && strings.ToUpper(peek.Value) == "BETWEEN" {
			p.advance()
			p.advance()
			low, err := p.parseBitwise()
			if err != nil {
				return nil, err
			}
			if err := p.expectKeyword("AND"); err != nil {
				return nil, err
			}
			high, err := p.parseBitwise()
			if err != nil {
				return nil, err
			}
			return &BetweenExpr{Expr: left, Low: low, High: high, Not: true}, nil
		}
	}

	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "IN" {
		p.advance()
		if err := p.expect(TokLParen); err != nil {
			return nil, err
		}
		if p.cur.Type == TokKeyword && (strings.ToUpper(p.cur.Value) == "SELECT" || strings.ToUpper(p.cur.Value) == "WITH") {
			sub, err := p.parseSelect()
			if err != nil {
				return nil, err
			}
			if err := p.expect(TokRParen); err != nil {
				return nil, err
			}
			selected := selectQuery(sub)
			return &BinaryExpr{Left: left, Op: "IN", Right: &SubqueryExpr{Stmt: selected}}, nil
		}
		var list []Expression
		for p.cur.Type != TokRParen && p.cur.Type != TokEOF {
			expr, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			list = append(list, expr)
			if p.cur.Type == TokComma {
				p.advance()
			}
		}
		if err := p.expect(TokRParen); err != nil {
			return nil, err
		}
		return &InListExpr{Expr: left, List: list, Not: false}, nil
	}

	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "BETWEEN" {
		p.advance()
		low, err := p.parseBitwise()
		if err != nil {
			return nil, err
		}
		if err := p.expectKeyword("AND"); err != nil {
			return nil, err
		}
		high, err := p.parseBitwise()
		if err != nil {
			return nil, err
		}
		return &BetweenExpr{Expr: left, Low: low, High: high, Not: false}, nil
	}

	return left, nil
}

func (p *Parser) parseBitwise() (Expression, error) {
	left, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}

	for p.cur.Type == TokBitOr || p.cur.Type == TokAmp || p.cur.Type == TokLShift || p.cur.Type == TokRShift {
		op := p.cur.Value
		p.advance()
		right, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}

	return left, nil
}

func (p *Parser) parseAdditive() (Expression, error) {
	left, err := p.parseConcat()
	if err != nil {
		return nil, err
	}

	for p.cur.Type == TokPlus || p.cur.Type == TokMinus {
		op := p.cur.Value
		p.advance()
		right, err := p.parseConcat()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}

	return left, nil
}

func (p *Parser) parseConcat() (Expression, error) {
	left, err := p.parseMultiplicative()
	if err != nil {
		return nil, err
	}

	for p.cur.Type == TokConcat {
		p.advance()
		right, err := p.parseMultiplicative()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: "||", Right: right}
	}

	return left, nil
}

func (p *Parser) parseMultiplicative() (Expression, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for p.cur.Type == TokStar || p.cur.Type == TokSlash || p.cur.Type == TokPercent {
		op := p.cur.Value
		p.advance()
		if op == "-" && p.cur.Type == TokInt && p.cur.Value == "9223372036854775808" {
			p.advance()
			return &LiteralExpr{Value: NewIntValue(-9223372036854775808)}, nil
		}
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}

	return left, nil
}

func (p *Parser) parseUnary() (Expression, error) {
	if p.cur.Type == TokKeyword && strings.EqualFold(p.cur.Value, "BINARY") {
		p.advance()
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: "BINARY", Right: right}, nil
	}
	if p.cur.Type == TokMinus || p.cur.Type == TokPlus || p.cur.Type == TokTilde {
		op := p.cur.Value
		p.advance()
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: op, Right: right}, nil
	}

	expr, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	for p.cur.Type == TokKeyword && strings.EqualFold(p.cur.Value, "COLLATE") {
		p.advance()
		if !strings.EqualFold(p.cur.Value, "BINARY") || p.cur.Type != TokIdent && p.cur.Type != TokKeyword {
			return nil, fmt.Errorf("unsupported collation: %s", p.cur.Value)
		}
		p.advance()
		expr = &UnaryExpr{Op: "BINARY", Right: expr}
	}
	return expr, nil
}

func (p *Parser) parsePrimary() (Expression, error) {
	if p.cur.Type == TokParameter {
		if !p.parameters {
			return nil, fmt.Errorf("parameters require Prepare")
		}
		e := &ParameterExpr{Index: p.parameterCount}
		p.parameterCount++
		p.advance()
		return e, nil
	}
	if p.cur.Type == TokLParen {
		if p.tok.Peek().Type == TokKeyword && (strings.ToUpper(p.tok.Peek().Value) == "SELECT" || strings.ToUpper(p.tok.Peek().Value) == "WITH") {
			sub, err := p.parseSubqueryParens()
			if err != nil {
				return nil, err
			}
			return &SubqueryExpr{Stmt: sub}, nil
		}
		p.advance()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if err := p.expect(TokRParen); err != nil {
			return nil, err
		}
		return expr, nil
	}

	if p.cur.Type == TokString {
		val := p.cur.Value
		p.advance()
		return &LiteralExpr{Value: NewTextValue(val)}, nil
	}

	if p.cur.Type == TokBlob {
		hexStr := p.cur.Value
		blob, err := hex.DecodeString(hexStr)
		if err != nil {
			return nil, fmt.Errorf("invalid blob literal: %w", err)
		}
		p.advance()
		return &LiteralExpr{Value: NewBlobValue(blob)}, nil
	}

	if p.cur.Type == TokInt {
		text := p.cur.Value
		n, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			f, err := strconv.ParseFloat(text, 64)
			if err != nil {
				return nil, err
			}
			p.advance()
			return &LiteralExpr{Value: NewRealValue(f)}, nil
		}
		p.advance()
		return &LiteralExpr{Value: NewIntValue(n)}, nil
	}

	if p.cur.Type == TokFloat {
		f, err := strconv.ParseFloat(p.cur.Value, 64)
		if err != nil {
			return nil, err
		}
		p.advance()
		return &LiteralExpr{Value: NewRealValue(f)}, nil
	}

	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "CASE" {
		return p.parseCase()
	}

	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "CAST" {
		p.advance()
		if err := p.expect(TokLParen); err != nil {
			return nil, err
		}
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if err := p.expectKeyword("AS"); err != nil {
			return nil, err
		}
		typeName := strings.ToUpper(p.cur.Value)
		p.advance()
		if typeName == "DOUBLE" && strings.EqualFold(p.cur.Value, "PRECISION") {
			typeName = "DOUBLE PRECISION"
			p.advance()
		} else if typeName == "VARCHAR" || typeName == "VARBINARY" {
			length, err := p.parseStringLength()
			if err != nil {
				return nil, err
			}
			typeName = fmt.Sprintf("%s(%d)", typeName, length)
		}
		if err := p.expect(TokRParen); err != nil {
			return nil, err
		}
		return &FuncCallExpr{Name: "CAST", Args: []Expression{expr, &LiteralExpr{Value: NewTextValue(typeName)}}}, nil
	}

	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "NULL" {
		p.advance()
		return &LiteralExpr{Value: NewNullValue()}, nil
	}

	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "TRUE" {
		p.advance()
		return &LiteralExpr{Value: NewIntValue(1)}, nil
	}

	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "FALSE" {
		p.advance()
		return &LiteralExpr{Value: NewIntValue(0)}, nil
	}

	if p.cur.Type == TokIdent {
		name := strings.ToLower(p.cur.Value)
		p.advance()

		if p.cur.Type == TokLParen {
			p.advance()
			fc := &FuncCallExpr{Name: strings.ToUpper(name)}
			if strings.EqualFold(p.cur.Value, "DISTINCT") {
				fc.Distinct = true
				p.advance()
			} else if strings.EqualFold(p.cur.Value, "ALL") {
				p.advance()
			}
			for p.cur.Type != TokRParen && p.cur.Type != TokEOF {
				arg, err := p.parseExpression()
				if err != nil {
					return nil, err
				}
				fc.Args = append(fc.Args, arg)
				if p.cur.Type == TokComma {
					p.advance()
				} else {
					break
				}
			}
			if err := p.expect(TokRParen); err != nil {
				return nil, err
			}
			return p.parseWindowCall(fc)
		}

		if p.cur.Type == TokDot {
			p.advance()
			colName := strings.ToLower(p.cur.Value)
			p.expect(TokIdent)
			return &ColumnRefExpr{Name: name + "." + colName}, nil
		}

		return &ColumnRefExpr{Name: name}, nil
	}

	if p.cur.Type == TokKeyword {
		name := p.cur.Value
		if p.tok.Peek().Type == TokLParen {
			p.advance()
			p.advance()
			fc := &FuncCallExpr{Name: strings.ToUpper(name)}
			if strings.EqualFold(p.cur.Value, "DISTINCT") {
				fc.Distinct = true
				p.advance()
			} else if strings.EqualFold(p.cur.Value, "ALL") {
				p.advance()
			}
			for p.cur.Type != TokRParen && p.cur.Type != TokEOF {
				arg, err := p.parseExpression()
				if err != nil {
					return nil, err
				}
				fc.Args = append(fc.Args, arg)
				if p.cur.Type == TokComma {
					p.advance()
				} else {
					break
				}
			}
			if err := p.expect(TokRParen); err != nil {
				return nil, err
			}
			return p.parseWindowCall(fc)
		}
	}

	if p.cur.Type == TokStar {
		p.advance()
		return &ColumnRefExpr{Name: "*"}, nil
	}

	return nil, fmt.Errorf("unexpected token in expression: %q", p.cur.Value)
}

func (p *Parser) parseCase() (Expression, error) {
	p.advance()

	caseExpr := &CaseExpr{}

	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "WHEN" {
	} else {
		var err error
		caseExpr.Expr, err = p.parseExpression()
		if err != nil {
			return nil, err
		}
	}

	for p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "WHEN" {
		p.advance()
		when := CaseWhen{}
		var err error
		if caseExpr.Expr != nil {
			when.Cond, err = p.parseExpression()
		} else {
			when.Cond, err = p.parseExpression()
		}
		if err != nil {
			return nil, err
		}
		if err := p.expectKeyword("THEN"); err != nil {
			return nil, err
		}
		when.Then, err = p.parseExpression()
		if err != nil {
			return nil, err
		}
		caseExpr.Whens = append(caseExpr.Whens, when)
	}

	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "ELSE" {
		p.advance()
		var err error
		caseExpr.ElseExpr, err = p.parseExpression()
		if err != nil {
			return nil, err
		}
	}

	if err := p.expectKeyword("END"); err != nil {
		return nil, err
	}

	return caseExpr, nil
}

func (p *Parser) parseTableRef() (string, string, Expression) {
	if p.cur.Type == TokLParen {
		subquery, err := p.parseSubqueryParens()
		if err != nil {
			return "", "", nil
		}
		alias := ""
		if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "AS" {
			p.advance()
			if p.cur.Type == TokIdent || p.cur.Type == TokKeyword {
				alias = p.cur.Value
				p.advance()
			}
		} else if p.cur.Type == TokIdent || p.cur.Type == TokKeyword {
			if !isReservedWord(p.cur.Value) {
				alias = p.cur.Value
				p.advance()
			}
		}
		return "", alias, &SubqueryExpr{Stmt: subquery}
	}
	name := strings.ToLower(p.cur.Value)
	alias := ""
	if p.cur.Type == TokIdent || p.cur.Type == TokKeyword {
		p.advance()
		if (p.cur.Type == TokIdent || p.cur.Type == TokKeyword) && !isReservedWord(p.cur.Value) {
			alias = strings.ToLower(p.cur.Value)
			p.advance()
		}
		if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "AS" {
			p.advance()
			if p.cur.Type == TokIdent || p.cur.Type == TokKeyword {
				alias = strings.ToLower(p.cur.Value)
				p.advance()
			}
		}
	}
	if alias == "" {
		alias = name
	}
	return name, alias, nil
}

var reservedWords = map[string]bool{
	"ON": true, "WHERE": true, "JOIN": true, "GROUP": true, "HAVING": true, "WINDOW": true,
	"ORDER": true, "LIMIT": true, "OFFSET": true, "INNER": true, "LEFT": true,
	"RIGHT": true, "CROSS": true, "NATURAL": true, "FULL": true, "OUTER": true,
	"UNION": true, "EXCEPT": true, "INTERSECT": true, "SET": true, "VALUES": true,
	"INTO": true, "FROM": true, "END": true, "ELSE": true, "THEN": true, "WHEN": true,
	"RETURNING": true, "USING": true, "AS": true,
	"FOR": true, "LOCK": true, "NOWAIT": true, "SKIP": true, "LOCKED": true,
}

func isReservedWord(s string) bool {
	return reservedWords[strings.ToUpper(s)]
}

func (p *Parser) parseJoinClause(kw string) (JoinClause, error) {
	jc := JoinClause{Type: kw}
	natural := false
	if p.cur.Value == "NATURAL" {
		natural = true
		p.advance()
		kw = "INNER"
		if p.cur.Value == "LEFT" || p.cur.Value == "RIGHT" || p.cur.Value == "INNER" || p.cur.Value == "CROSS" {
			kw = p.cur.Value
			p.advance()
		}
	}
	if p.cur.Value != "JOIN" {
		p.advance()
		if p.cur.Value == "OUTER" {
			p.advance()
		}
	}
	if err := p.expectKeyword("JOIN"); err != nil {
		return jc, err
	}
	jc.Type = kw
	if natural {
		jc.Type = "NATURAL"
	}
	var expr Expression
	jc.Table, jc.Alias, expr = p.parseTableRef()
	if jc.Table == "" && expr == nil {
		return jc, fmt.Errorf("expected join table")
	}
	if sub, ok := expr.(*SubqueryExpr); ok {
		jc.Subquery = sub.Stmt
	}
	if p.cur.Value == "ON" {
		p.advance()
		var err error
		jc.On, err = p.parseExpression()
		if err != nil {
			return jc, err
		}
	} else if p.cur.Value == "USING" {
		p.advance()
		if err := p.expect(TokLParen); err != nil {
			return jc, err
		}
		for {
			if p.cur.Type != TokIdent && p.cur.Type != TokKeyword {
				return jc, fmt.Errorf("expected USING column")
			}
			jc.Using = append(jc.Using, strings.ToLower(p.cur.Value))
			p.advance()
			if p.cur.Type != TokComma {
				break
			}
			p.advance()
		}
		if err := p.expect(TokRParen); err != nil {
			return jc, err
		}
	}
	return jc, nil
}

func (p *Parser) parseSubquery() (*SelectStmt, error) {
	if err := p.expect(TokLParen); err != nil {
		return nil, err
	}
	stmt, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	return selectQuery(stmt), nil
}

func (p *Parser) parseSubqueryParens() (*SelectStmt, error) {
	sub, err := p.parseSubquery()
	if err != nil {
		return nil, err
	}
	if err := p.expect(TokRParen); err != nil {
		return nil, err
	}
	return sub, nil
}

func (p *Parser) parseAlterTable() (Statement, error) { return p.parseAlter() }

func (p *Parser) parseExplain() (Statement, error) {
	p.advance()
	analyze := false
	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "ANALYZE" {
		analyze = true
		p.advance()
	}
	inner, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	return &ExplainStmt{Stmt: inner, Analyze: analyze}, nil
}

func (p *Parser) parsePragma() (Statement, error) {
	p.advance()
	var name string
	if p.cur.Type == TokIdent {
		name = strings.ToLower(p.cur.Value)
		p.advance()
	} else if p.cur.Type == TokKeyword {
		name = strings.ToLower(p.cur.Value)
		p.advance()
	} else {
		return nil, fmt.Errorf("expected pragma name, got %q", p.cur.Value)
	}

	stmt := &PragmaStmt{Name: name}

	if p.cur.Type == TokLParen {
		p.advance()
		for p.cur.Type != TokRParen && p.cur.Type != TokEOF {
			arg, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			stmt.Args = append(stmt.Args, arg)
			if p.cur.Type == TokComma {
				p.advance()
			}
		}
		if err := p.expect(TokRParen); err != nil {
			return nil, err
		}
	} else if p.cur.Type == TokEq {
		p.advance()
		val, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Value = val
	}

	return stmt, nil
}

func (p *Parser) parseAnalyze() (Statement, error) {
	p.advance()
	stmt := &AnalyzeStmt{}
	if p.cur.Type == TokIdent || p.cur.Type == TokKeyword {
		stmt.Table = strings.ToLower(p.cur.Value)
		p.advance()
	}

	return stmt, nil
}
func (p *Parser) parseBegin() (Statement, error) {
	start := p.cur.Value == "START"
	p.advance()
	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "TRANSACTION" {
		p.advance()
	} else if start {
		return nil, fmt.Errorf("START requires TRANSACTION")
	}
	if strings.EqualFold(p.cur.Value, "ISOLATION") {
		for _, word := range []string{"ISOLATION", "LEVEL", "READ", "COMMITTED"} {
			if !strings.EqualFold(p.cur.Value, word) {
				return nil, fmt.Errorf("only READ COMMITTED isolation is supported")
			}
			p.advance()
		}
	}
	return &BeginStmt{}, nil
}

func (p *Parser) parseCommit() (Statement, error) {
	p.advance()
	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "TRANSACTION" {
		p.advance()
	}
	return &CommitStmt{}, nil
}

func (p *Parser) parseRollback() (Statement, error) {
	p.advance()
	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "TRANSACTION" {
		p.advance()
		return &RollbackStmt{}, nil
	}
	if p.cur.Type == TokKeyword && strings.ToUpper(p.cur.Value) == "TO" {
		p.advance()
		if p.cur.Type == TokKeyword && p.cur.Value == "SAVEPOINT" {
			p.advance()
		}
		name := strings.ToLower(p.cur.Value)
		if p.cur.Type == TokIdent || p.cur.Type == TokKeyword {
			p.advance()
		}
		return &ReleaseStmt{Name: name, ToSave: true}, nil
	}
	return &RollbackStmt{}, nil
}

func (p *Parser) parseSavepoint() (Statement, error) {
	p.advance()
	name := strings.ToLower(p.cur.Value)
	if p.cur.Type == TokIdent || p.cur.Type == TokKeyword {
		p.advance()
	}
	return &SavepointStmt{Name: name}, nil
}

func (p *Parser) parseRelease() (Statement, error) {
	p.advance()
	if p.cur.Type == TokKeyword && p.cur.Value == "SAVEPOINT" {
		p.advance()
	}
	if p.cur.Type != TokIdent && p.cur.Type != TokKeyword {
		return nil, fmt.Errorf("expected savepoint name")
	}
	name := strings.ToLower(p.cur.Value)
	p.advance()
	return &ReleaseStmt{Name: name}, nil
}

func quoteIdent(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }
func formatRef(name string) string {
	parts := strings.Split(name, ".")
	for i, part := range parts {
		if part != "*" {
			parts[i] = quoteIdent(part)
		}
	}
	return strings.Join(parts, ".")
}
func formatSelectSQL(s *SelectStmt) string {
	if s.With != nil {
		cp := *s
		cp.With = nil
		return formatWithSQL(s.With) + formatSelectSQL(&cp)
	}
	if s.Compound != nil {
		return formatCTEQuery(s.Compound)
	}
	var b strings.Builder
	b.WriteString("SELECT ")
	if s.Distinct {
		b.WriteString("DISTINCT ")
	}
	if s.SelectAll {
		b.WriteString("*")
	} else {
		for i, col := range s.Columns {
			if i > 0 {
				b.WriteString(", ")
			}
			if col.Star {
				b.WriteString("*")
			} else {
				b.WriteString(formatExpr(col.Expr))
				if col.Alias != "" {
					b.WriteString(" AS " + quoteIdent(col.Alias))
				}
			}
		}
	}
	if s.From != "" {
		b.WriteString(" FROM " + quoteIdent(s.From))
	} else if s.FromExpr != nil {
		b.WriteString(" FROM " + formatExpr(s.FromExpr))
	}
	if s.FromAlias != "" {
		b.WriteString(" AS " + quoteIdent(s.FromAlias))
	}
	for _, jc := range s.Joins {
		b.WriteString(" " + jc.Type + " JOIN ")
		if jc.Subquery != nil {
			b.WriteString("(" + formatSelectSQL(jc.Subquery) + ")")
		} else {
			b.WriteString(quoteIdent(jc.Table))
		}
		if jc.Alias != "" {
			b.WriteString(" AS " + quoteIdent(jc.Alias))
		}
		if jc.On != nil {
			b.WriteString(" ON " + formatExpr(jc.On))
		}
		if len(jc.Using) > 0 {
			b.WriteString(" USING (")
			for i, name := range jc.Using {
				if i > 0 {
					b.WriteString(",")
				}
				b.WriteString(quoteIdent(name))
			}
			b.WriteString(")")
		}
	}
	if s.Where != nil {
		b.WriteString(" WHERE " + formatExpr(s.Where))
	}
	if len(s.GroupBy) > 0 {
		b.WriteString(" GROUP BY ")
		for i, e := range s.GroupBy {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(formatExpr(e))
		}
	}
	if s.Having != nil {
		b.WriteString(" HAVING " + formatExpr(s.Having))
	}
	if len(s.Windows) > 0 {
		b.WriteString(formatWindowDefinitions(s.Windows))
	}

	if len(s.OrderBy) > 0 {
		b.WriteString(" ORDER BY ")
		for i, o := range s.OrderBy {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(formatExpr(o.Expr))
			if o.Desc {
				b.WriteString(" DESC")
			}
			if o.NullsFirst {
				b.WriteString(" NULLS FIRST")
			}
			if o.NullsLast {
				b.WriteString(" NULLS LAST")
			}
		}
	}
	if s.Limit >= 0 {
		fmt.Fprintf(&b, " LIMIT %d", s.Limit)
	}
	if s.Offset > 0 {
		fmt.Fprintf(&b, " OFFSET %d", s.Offset)
	}
	if s.Lock != nil {
		switch {
		case s.Lock.Exclusive:
			b.WriteString(" FOR UPDATE")
		case s.Lock.ShareMode:
			b.WriteString(" LOCK IN SHARE MODE")
		default:
			b.WriteString(" FOR SHARE")
		}
		switch s.Lock.Wait {
		case LockNoWait:
			b.WriteString(" NOWAIT")
		case LockSkipLocked:
			b.WriteString(" SKIP LOCKED")
		}
	}
	return b.String()
}

func formatExpr(e Expression) string {
	if e == nil {
		return "NULL"
	}
	switch x := e.(type) {
	case *LiteralExpr:
		v := x.Value
		if v.Null {
			return "NULL"
		}
		switch v.Type {
		case ColText:
			return "'" + strings.ReplaceAll(v.Text, "'", "''") + "'"
		case ColBlob:
			return "X'" + hex.EncodeToString(v.Blob) + "'"
		default:
			return v.String()
		}
	case *ColumnRefExpr:
		return formatRef(x.Name)
	case *BinaryExpr:
		escape := ""
		if x.Escape != 0 {
			escape = " ESCAPE " + formatExpr(&LiteralExpr{Value: NewTextValue(string([]byte{x.Escape}))})
		}
		return "(" + formatExpr(x.Left) + " " + x.Op + " " + formatExpr(x.Right) + escape + ")"
	case *UnaryExpr:
		return "(" + x.Op + " " + formatExpr(x.Right) + ")"
	case *FuncCallExpr:
		if strings.EqualFold(x.Name, "CAST") && len(x.Args) == 2 {
			if typ, ok := x.Args[1].(*LiteralExpr); ok {
				return "CAST(" + formatExpr(x.Args[0]) + " AS " + typ.Value.Text + ")"
			}
		}
		parts := make([]string, len(x.Args))
		for i, a := range x.Args {
			parts[i] = formatExpr(a)
		}
		modifier := ""
		if x.Distinct {
			modifier = "DISTINCT "
		}
		return x.Name + "(" + modifier + strings.Join(parts, ", ") + ")" + formatWindowCall(x.Window)
	case *SubqueryExpr:
		return "(" + formatSelectSQL(x.Stmt) + ")"
	case *ExistsExpr:
		prefix := "EXISTS "
		if x.Not {
			prefix = "NOT EXISTS "
		}
		return prefix + "(" + formatSelectSQL(x.Stmt) + ")"
	case *InListExpr:
		parts := make([]string, len(x.List))
		for i, v := range x.List {
			parts[i] = formatExpr(v)
		}
		not := ""
		if x.Not {
			not = "NOT "
		}
		return formatExpr(x.Expr) + " " + not + "IN (" + strings.Join(parts, ", ") + ")"
	case *BetweenExpr:
		not := ""
		if x.Not {
			not = "NOT "
		}
		return formatExpr(x.Expr) + " " + not + "BETWEEN " + formatExpr(x.Low) + " AND " + formatExpr(x.High)
	case *CaseExpr:
		b := strings.Builder{}
		b.WriteString("CASE ")
		if x.Expr != nil {
			b.WriteString(formatExpr(x.Expr) + " ")
		}
		for _, w := range x.Whens {
			b.WriteString("WHEN " + formatExpr(w.Cond) + " THEN " + formatExpr(w.Then) + " ")
		}
		if x.ElseExpr != nil {
			b.WriteString("ELSE " + formatExpr(x.ElseExpr) + " ")
		}
		b.WriteString("END")
		return b.String()
	}
	return fmt.Sprintf("%T", e)
}

// NO ACTION and RESTRICT use immediate statement validation. Unsupported
// referential actions must never be accepted as if they were enforced.
func (p *Parser) parseReferenceActions() error {
	seen := make(map[string]bool)
	for p.showWord("ON") {
		p.advance()
		event := strings.ToUpper(p.cur.Value)
		if event != "DELETE" && event != "UPDATE" {
			return fmt.Errorf("expected DELETE or UPDATE in foreign key action")
		}
		if seen[event] {
			return fmt.Errorf("duplicate foreign key ON %s", event)
		}
		seen[event] = true
		p.advance()
		if p.showWord("RESTRICT") {
			p.advance()
		} else if p.showWord("NO") {
			p.advance()
			if !p.showWord("ACTION") {
				return fmt.Errorf("expected NO ACTION")
			}
			p.advance()
		} else {
			return fmt.Errorf("unsupported foreign key action: %s", p.cur.Value)
		}
	}
	return nil
}
