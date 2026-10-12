package sql

import (
	"fmt"
	"strings"
)

func (p *Parser) alterName() (string, error) {
	if p.cur.Type != TokIdent && p.cur.Type != TokKeyword {
		return "", fmt.Errorf("expected identifier, got %q", p.cur.Value)
	}
	name := strings.ToLower(p.cur.Value)
	p.advance()
	return name, nil
}
func (p *Parser) alterWords(words ...string) error {
	for _, w := range words {
		if !p.showWord(w) {
			return fmt.Errorf("expected %s, got %q", w, p.cur.Value)
		}
		p.advance()
	}
	return nil
}
func (p *Parser) optionalAlterWord(word string) bool {
	if p.showWord(word) {
		p.advance()
		return true
	}
	return false
}
func (p *Parser) alterIfExists() (bool, error) {
	if !p.optionalAlterWord("IF") {
		return false, nil
	}
	return true, p.alterWords("EXISTS")
}
func (p *Parser) alterIfNotExists() (bool, error) {
	if !p.optionalAlterWord("IF") {
		return false, nil
	}
	return true, p.alterWords("NOT", "EXISTS")
}

func (p *Parser) parseAlter() (Statement, error) {
	p.advance()
	kind := strings.ToUpper(p.cur.Value)
	if kind != "TABLE" && kind != "INDEX" && kind != "VIEW" {
		return nil, fmt.Errorf("expected TABLE, INDEX or VIEW after ALTER")
	}
	p.advance()
	stmt := &AlterTableStmt{ObjectKind: kind}
	var err error
	stmt.TableIfExists, err = p.alterIfExists()
	if err != nil {
		return nil, err
	}
	stmt.Table, err = p.alterName()
	if err != nil {
		return nil, err
	}
	if kind != "TABLE" {
		if p.optionalAlterWord("RENAME") {
			if err = p.alterWords("TO"); err != nil {
				return nil, err
			}
			stmt.NewName, err = p.alterName()
			stmt.Operation = "RENAME_TO"
			return stmt, err
		}
		if kind == "INDEX" && p.optionalAlterWord("REBUILD") {
			stmt.Operation = "REBUILD_INDEX"
			return stmt, nil
		}
		if kind == "VIEW" {
			if p.cur.Type == TokLParen {
				stmt.ViewColumns, err = p.parseIndexColumns()
				if err != nil {
					return nil, err
				}
			}
			if err = p.alterWords("AS"); err != nil {
				return nil, err
			}
			query, err := p.parseStatement()
			if err != nil {
				return nil, err
			}
			if selectQuery(query) == nil {
				return nil, fmt.Errorf("ALTER VIEW requires a query")
			}
			stmt.ViewSQL = formatCTEQuery(query)
			stmt.Operation = "SET_VIEW"
			return stmt, nil
		}
		return nil, fmt.Errorf("unsupported ALTER %s operation %q", kind, p.cur.Value)
	}
	for {
		a, err := p.parseAlterAction(stmt.Table)
		if err != nil {
			return nil, err
		}
		stmt.Actions = append(stmt.Actions, a)
		if p.cur.Type != TokComma {
			break
		}
		p.advance()
	}
	abortAfter, err := p.parsePublicationPolicy()
	if err != nil {
		return nil, err
	}
	if abortAfter != nil {
		if len(stmt.Actions) != 1 || (stmt.Actions[0].Operation != "ADD_INDEX" && stmt.Actions[0].Operation != "DROP_INDEX") {
			return nil, fmt.Errorf("KILL TRANSACTIONS AFTER requires a single ALTER TABLE ADD INDEX or DROP INDEX action")
		}
		stmt.Actions[0].PublicationAbortAfter = abortAfter
	}
	if len(stmt.Actions) == 1 {
		a := stmt.Actions[0]
		a.ObjectKind = kind
		a.TableIfExists = stmt.TableIfExists
		return a, nil
	}
	stmt.Operation = "MULTI"
	return stmt, nil
}

func (p *Parser) parseAlterAction(table string) (*AlterTableStmt, error) {
	a := &AlterTableStmt{Table: table}
	kw := strings.ToUpper(p.cur.Value)
	p.advance()
	var err error
	switch kw {
	case "ADD":
		if p.showWord("UNIQUE") && (strings.EqualFold(p.tok.Peek().Value, "INDEX") || strings.EqualFold(p.tok.Peek().Value, "KEY")) {
			p.advance()
			def, e := p.parseInlineIndex(true, "")
			if e != nil {
				return nil, e
			}
			a.Operation = "ADD_INDEX"
			a.Constraint = &ConstraintDef{Name: def.Name, Kind: "UNIQUE_INDEX", Columns: def.Columns}
			return a, nil
		}
		if p.showWord("CONSTRAINT") || p.showWord("PRIMARY") || p.showWord("UNIQUE") || p.showWord("CHECK") || p.showWord("FOREIGN") {
			a.Constraint, err = p.parseAlterConstraint()
			a.Operation = "ADD_CONSTRAINT"
			return a, err
		}
		if p.showWord("INDEX") || p.showWord("KEY") {
			p.advance()
			def, err := p.parseInlineIndex(false, "")
			if err != nil {
				return nil, err
			}
			a.Operation = "ADD_INDEX"
			a.Constraint = &ConstraintDef{Name: def.Name, Kind: "INDEX", Columns: def.Columns}
			return a, nil
		}
		p.optionalAlterWord("COLUMN")
		a.IfNotExists, err = p.alterIfNotExists()
		if err != nil {
			return nil, err
		}
		col := &ColumnDef{}
		col.Name, err = p.alterName()
		if err != nil {
			return nil, err
		}
		if err = p.parseColumnDef(col, true); err != nil {
			return nil, err
		}
		a.Column = col
		a.Operation = "ADD_COLUMN"
		err = p.parseAlterPosition(a)
	case "DROP":
		if p.optionalAlterWord("CONSTRAINT") {
			a.Operation = "DROP_CONSTRAINT"
			a.IfExists, err = p.alterIfExists()
			if err == nil {
				a.NewName, err = p.alterName()
			}
		} else if p.optionalAlterWord("PRIMARY") {
			a.Operation = "DROP_PRIMARY"
			err = p.alterWords("KEY")
		} else if p.optionalAlterWord("INDEX") || p.optionalAlterWord("KEY") {
			a.Operation = "DROP_INDEX"
			a.IfExists, err = p.alterIfExists()
			if err == nil {
				a.NewName, err = p.alterName()
			}
		} else {
			p.optionalAlterWord("COLUMN")
			a.Operation = "DROP_COLUMN"
			a.IfExists, err = p.alterIfExists()
			if err == nil {
				a.OldColName, err = p.alterName()
			}
		}
		if err == nil {
			a.Cascade = p.optionalAlterWord("CASCADE")
			p.optionalAlterWord("RESTRICT")
		}
	case "RENAME":
		if p.optionalAlterWord("TO") {
			a.Operation = "RENAME_TO"
			a.NewName, err = p.alterName()
			break
		}
		if p.optionalAlterWord("INDEX") || p.optionalAlterWord("KEY") {
			a.Operation = "RENAME_INDEX"
			a.OldColName, err = p.alterName()
		} else if p.optionalAlterWord("CONSTRAINT") {
			a.Operation = "RENAME_CONSTRAINT"
			a.OldColName, err = p.alterName()
		} else {
			p.optionalAlterWord("COLUMN")
			a.Operation = "RENAME_COLUMN"
			a.OldColName, err = p.alterName()
		}
		if err == nil {
			err = p.alterWords("TO")
		}
		if err == nil {
			a.NewColName, err = p.alterName()
		}
	case "ALTER":
		p.optionalAlterWord("COLUMN")
		a.OldColName, err = p.alterName()
		if err != nil {
			return nil, err
		}
		if p.optionalAlterWord("SET") {
			if p.optionalAlterWord("DEFAULT") {
				a.Operation = "SET_DEFAULT"
				a.Default, err = p.parseExpression()
			} else if p.optionalAlterWord("NOT") {
				a.Operation = "SET_NOT_NULL"
				err = p.alterWords("NULL")
			} else {
				p.optionalAlterWord("DATA")
				if err = p.alterWords("TYPE"); err == nil {
					a.Operation = "ALTER_TYPE"
					err = p.parseAlterType(a)
				}
			}
		} else if p.optionalAlterWord("DROP") {
			if p.optionalAlterWord("DEFAULT") {
				a.Operation = "DROP_DEFAULT"
			} else {
				a.Operation = "DROP_NOT_NULL"
				err = p.alterWords("NOT", "NULL")
			}
		} else if p.optionalAlterWord("TYPE") {
			a.Operation = "ALTER_TYPE"
			err = p.parseAlterType(a)
		} else if p.optionalAlterWord("COLLATE") {
			a.Operation = "SET_COLLATION"
			a.NewName, err = p.alterName()
		} else {
			return nil, fmt.Errorf("unsupported ALTER COLUMN action %q", p.cur.Value)
		}
	case "MODIFY", "CHANGE":
		p.optionalAlterWord("COLUMN")
		if kw == "CHANGE" {
			a.OldColName, err = p.alterName()
			if err != nil {
				return nil, err
			}
		}
		col := &ColumnDef{}
		col.Name, err = p.alterName()
		if err != nil {
			return nil, err
		}
		if kw == "MODIFY" {
			a.OldColName = col.Name
		}
		a.Column = col
		a.Operation = "MODIFY_COLUMN"
		if err = p.parseColumnDef(col, true); err == nil {
			err = p.parseAlterPosition(a)
		}
	default:
		return nil, fmt.Errorf("unsupported ALTER TABLE action %q", kw)
	}
	return a, err
}
func (p *Parser) parseAlterPosition(a *AlterTableStmt) error {
	if p.optionalAlterWord("FIRST") {
		a.Position = "FIRST"
	} else if p.optionalAlterWord("AFTER") {
		a.Position = "AFTER"
		var err error
		a.After, err = p.alterName()
		return err
	}
	return nil
}
func (p *Parser) parseAlterType(a *AlterTableStmt) error {
	name := strings.ToUpper(p.cur.Value)
	p.advance()
	c := &ColumnDef{Name: a.OldColName}
	switch name {
	case "INTEGER", "INT", "BIGINT":
		c.ColType = ColInteger
	case "REAL", "DOUBLE", "FLOAT":
		c.ColType = ColReal
		if name == "DOUBLE" {
			p.optionalAlterWord("PRECISION")
		}
	case "TEXT":
		c.ColType = ColText
	case "VARCHAR":
		c.ColType = ColText
		length, err := p.parseStringLength()
		if err != nil {
			return err
		}
		c.Length = length
	case "BLOB":
		c.ColType = ColBlob
	case "VARBINARY":
		c.ColType = ColBlob
		length, err := p.parseStringLength()
		if err != nil {
			return err
		}
		c.Length = length
	default:
		return fmt.Errorf("unsupported column type: %s", name)
	}
	a.Column = c
	if p.optionalAlterWord("COLLATE") {
		collation, err := p.alterName()
		if err != nil {
			return err
		}
		if collation != "binary" {
			return fmt.Errorf("unsupported collation: %s", collation)
		}
		if c.ColType != ColText {
			return fmt.Errorf("BINARY collation requires a TEXT column")
		}
		c.Binary = true
	}
	if p.optionalAlterWord("ALLOW") {
		if !p.optionalAlterWord("EXISTING") {
			return fmt.Errorf("expected EXISTING after ALLOW")
		}
		a.AllowExisting = true
	}
	if p.optionalAlterWord("USING") {
		var err error
		a.Using, err = p.parseExpression()
		return err
	}
	return nil
}
func (p *Parser) parseAlterConstraint() (*ConstraintDef, error) {
	c := &ConstraintDef{}
	var err error
	if p.optionalAlterWord("CONSTRAINT") {
		c.Name, err = p.alterName()
		if err != nil {
			return nil, err
		}
	}
	tmp := &CreateTableStmt{}
	kind := strings.ToUpper(p.cur.Value)
	if err = p.parseTableConstraint(tmp); err != nil {
		return nil, err
	}
	switch kind {
	case "PRIMARY":
		c.Kind = "PRIMARY"
		c.Columns = tmp.PKColumns
	case "UNIQUE":
		c.Kind = "UNIQUE"
		if len(tmp.TableUnique) > 0 {
			c.Columns = tmp.TableUnique[0]
		} else if len(tmp.Indexes) > 0 {
			c.Columns = tmp.Indexes[0].Columns
			if c.Name == "" {
				c.Name = tmp.Indexes[0].Name
			}
		}
	case "CHECK":
		c.Kind = "CHECK"
		c.Check = tmp.TableCheck[0]
		c.CheckSQL = formatExpr(c.Check)
	case "FOREIGN":
		c.Kind = "FOREIGN"
		fk := tmp.ForeignKeys[0]
		c.Columns = fk.Columns
		c.RefTable = fk.RefTable
		c.RefColumns = fk.RefColumns
	default:
		return nil, fmt.Errorf("unsupported constraint kind %s", kind)
	}
	return c, nil
}

func (p *Parser) parseRenameTables() (Statement, error) {
	p.advance()
	if err := p.alterWords("TABLE"); err != nil {
		return nil, err
	}
	s := &AlterTableStmt{ObjectKind: "TABLE", Operation: "RENAME_TABLES"}
	for {
		old, err := p.alterName()
		if err != nil {
			return nil, err
		}
		if err = p.alterWords("TO"); err != nil {
			return nil, err
		}
		name, err := p.alterName()
		if err != nil {
			return nil, err
		}
		s.Actions = append(s.Actions, &AlterTableStmt{Table: old, NewName: name, Operation: "RENAME_TO"})
		if p.cur.Type != TokComma {
			break
		}
		p.advance()
	}
	return s, nil
}
