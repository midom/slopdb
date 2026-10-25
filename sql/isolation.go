package sql

import (
	"fmt"
	"strings"
)

// IsolationLevel controls consistent reads. Mutations and locking reads always
// use the statement's latest committed view, including under REPEATABLE READ.
type IsolationLevel string

const (
	ReadCommitted  IsolationLevel = "READ COMMITTED"
	RepeatableRead IsolationLevel = "REPEATABLE READ"
)

func ParseIsolationLevel(name string) (IsolationLevel, error) {
	name = strings.Join(strings.Fields(strings.ReplaceAll(strings.ToUpper(name), "-", " ")), " ")
	switch IsolationLevel(name) {
	case ReadCommitted, RepeatableRead:
		return IsolationLevel(name), nil
	default:
		return "", fmt.Errorf("unsupported transaction isolation level %q", name)
	}
}

func (s *Schema) IsolationLevel() IsolationLevel {
	if s.isolation == "" {
		if s.MySQLMode() {
			return RepeatableRead
		}
		return ReadCommitted
	}
	return s.isolation
}

// SetIsolationLevel changes the default for subsequent transactions; an active
// transaction retains the level captured by BeginWithOptions.
func (s *Schema) SetIsolationLevel(level IsolationLevel) error {
	parsed, err := ParseIsolationLevel(string(level))
	if err != nil {
		return err
	}
	s.isolation = parsed
	return nil
}

func (s *Schema) SetNextTransactionIsolation(level IsolationLevel) error {
	if s.inTx {
		return fmt.Errorf("transaction characteristics cannot be changed during a transaction")
	}
	parsed, err := ParseIsolationLevel(string(level))
	if err != nil {
		return err
	}
	s.nextIsolation = parsed
	return nil
}

func (s *Schema) ensureConsistentSnapshot() error {
	if s.consistentSnapshotActive {
		return nil
	}
	if s.statementActive {
		// Promote the already pinned statement view atomically. This preserves
		// certified replica/barrier views and the first read's exact watermark.
		if err := s.lsm.PromoteReaderSnapshot(s.snapshotReader, s.statementSnapshot); err != nil {
			return err
		}
		s.consistentSnapshot = s.statementSnapshot
	} else {
		s.consistentSnapshot = s.lsm.Snapshot()
	}
	s.consistentSnapshotActive = true
	return nil
}

func (s *Schema) releaseConsistentSnapshot() {
	if s.consistentSnapshotActive {
		s.lsm.ReleaseSnapshot(s.consistentSnapshot)
		s.consistentSnapshotActive = false
	}
}

// StatementReadView distinguishes SELECT-family statements from DML (whose
// subqueries also use current reads). Call before StartStatement, including on
// retries, so the retained transaction view never replaces a mutation view.
func (s *Schema) StatementReadView(statement Statement) {
	s.statementConsistent = false
	s.statementQuery = false
	switch stmt := statement.(type) {
	case *SelectStmt:
		s.statementQuery = true
		s.statementConsistent = stmt.Lock == nil && selectReadsTable(stmt)
	case *UnionStmt:
		s.statementQuery = true
		s.statementConsistent = true
	case *ExplainStmt:
		if stmt.Analyze {
			s.StatementReadView(stmt.Stmt)
		}
	}
}

// SELECT constants do not establish a table read view. Derived and CTE query
// blocks establish it when their first table SELECT executes.
func selectReadsTable(stmt *SelectStmt) bool {
	return stmt.From != "" || len(stmt.Joins) != 0
}

func (s *Schema) useConsistentStatementSnapshot() error {
	s.statementCurrentSnapshot = s.statementSnapshot
	if !s.inTx || s.txIsolation != RepeatableRead || !s.statementConsistent {
		return nil
	}
	if err := s.ensureConsistentSnapshot(); err != nil {
		return err
	}
	if s.statementSnapshot != s.consistentSnapshot {
		if err := s.lsm.RetainSnapshotForReader(s.snapshotReader, s.consistentSnapshot); err != nil {
			return err
		}
		s.lsm.ReleaseSnapshotForReader(s.snapshotReader, s.statementSnapshot)
		s.statementSnapshot = s.consistentSnapshot
	}
	return nil
}

// Nested locking query blocks use current data even inside a snapshot SELECT.
// Conversely, a nonlocking subquery in a SELECT FOR UPDATE is a consistent read;
// views expanded with physical row tracking inherit their parent's read view.
func (s *Schema) selectReadView(stmt *SelectStmt) (func(), error) {
	if !s.statementActive || !s.statementQuery || !s.inTx || s.txIsolation != RepeatableRead || !selectReadsTable(stmt) {
		return func() {}, nil
	}
	before := s.statementSnapshot
	want := s.statementCurrentSnapshot
	if stmt.Lock == nil && !(stmt.trackRows && before == s.statementCurrentSnapshot) {
		if err := s.ensureConsistentSnapshot(); err != nil {
			return nil, err
		}
		want = s.consistentSnapshot
	}
	validated := s.validatedRowKey
	s.statementSnapshot, s.validatedRowKey = want, ""
	return func() { s.statementSnapshot, s.validatedRowKey = before, validated }, nil
}

func (p *Parser) parseIsolationLevel() (IsolationLevel, error) {
	first := strings.ToUpper(p.cur.Value)
	p.advance()
	second := strings.ToUpper(p.cur.Value)
	p.advance()
	return ParseIsolationLevel(first + " " + second)
}

// Current-read updates remove current index keys, which can differ from the
// keys visible in an older transaction snapshot. Hide the historic payloads
// behind our row overlay too, without committing extra index deletions.
func (s *Schema) snapshotIndexDeletes() (map[string]bool, error) {
	if !s.inTx || !s.consistentSnapshotActive || s.statementSnapshot != s.consistentSnapshot || len(s.txLatest) == 0 {
		return nil, nil
	}
	if s.consistentIndexDeletes != nil {
		return s.consistentIndexDeletes, nil
	}
	deletes := make(map[string]bool)
	for key := range s.txLatest {
		data := []byte(key)
		if len(data) == 0 || data[0] != 1 || s.temporaryKey(data) {
			continue
		}
		table, info := s.rowTable(data)
		if info == nil {
			continue
		}
		old, found, err := s.lsm.GetAtForReader(data, s.consistentSnapshot, s.snapshotReader)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		row := decodeRow(old, info)
		for _, idx := range s.tableIndexes(table) {
			if idx.Fulltext {
				continue
			}
			deletes[string(encodeRowIndexKey(idx, row, data))] = true
		}
	}
	s.consistentIndexDeletes = deletes
	return deletes, nil
}

func (p *Parser) parseSetIsolation() (Statement, error) {
	p.advance() // SET
	stmt := &SetIsolationStmt{}
	if p.showWord("SESSION") || p.showWord("LOCAL") {
		stmt.Session = true
		p.advance()
	}
	if !p.optionalAlterWord("TRANSACTION") || !p.optionalAlterWord("ISOLATION") || !p.optionalAlterWord("LEVEL") {
		return nil, fmt.Errorf("expected [SESSION] TRANSACTION ISOLATION LEVEL")
	}
	level, err := p.parseIsolationLevel()
	stmt.Level = level
	return stmt, err
}
