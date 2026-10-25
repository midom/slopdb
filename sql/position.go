package sql

import "fmt"

// ReplicationPositionStmt is handled by the database's replication owner, with
// no data snapshot or SQL evaluation of its literal/bound arguments.
type ReplicationPositionStmt struct {
	Operation         string // current, last, wait
	Position, Timeout Expression
}

func (*ReplicationPositionStmt) stmt() {}

// ResultColumns is the fixed protocol-neutral frame shared by execution and
// prepared description. Waiting is a command and does not produce a rowset.
func (s *ReplicationPositionStmt) ResultColumns() []ColumnDef {
	if s.Operation == "wait" {
		return nil
	}
	column := LogicalType{Kind: TypeText}.Column()
	column.Name = "Position"
	return []ColumnDef{column}
}

func (s *Schema) LastCommitPosition() string            { return s.lastCommitPosition }
func (s *Schema) SetLastCommitPosition(position string) { s.lastCommitPosition = position }

func (p *Parser) parseWaitPosition() (Statement, error) {
	p.advance()
	if !p.showWord("FOR") {
		return nil, fmt.Errorf("expected FOR after WAIT")
	}
	p.advance()
	if !p.showWord("POSITION") {
		return nil, fmt.Errorf("expected POSITION after WAIT FOR")
	}
	p.advance()
	position, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	stmt := &ReplicationPositionStmt{Operation: "wait", Position: position}
	if p.showWord("TIMEOUT") {
		p.advance()
		stmt.Timeout, err = p.parsePrimary()
	}
	return stmt, err
}
