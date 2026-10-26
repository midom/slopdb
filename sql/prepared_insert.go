package sql

import (
	"fmt"
	"strings"
)

// A bound AST exclusively owns its row and encodings. Constraint/index helpers
// consume them synchronously; transaction publication takes independent copies.
type preparedInsertPlan struct {
	literals     []*LiteralExpr
	row          map[string]Value
	key, encoded []byte
}

func (p *preparedInsertPlan) releaseValues() {
	clear(p.row)
	if cap(p.key) > 64<<10 {
		p.key = nil
	}
	if cap(p.encoded) > 64<<10 {
		p.encoded = nil
	}
}
func (s *Schema) compilePreparedInsert(stmt *InsertStmt, info *TableInfo) *preparedInsertPlan {
	if len(info.Columns) > 128 || len(info.PKColumns) == 0 || len(info.TableCheck) != 0 || len(info.ForeignKeys) != 0 || len(s.lookups().foreign[info.Name]) != 0 || len(s.lookups().references[info.Name]) != 0 {
		return nil
	}
	for _, tr := range s.triggers {
		if tr.Table == info.Name || tr.View == info.Name {
			return nil
		}
	}
	for _, col := range info.Columns {
		if col.AutoIncrement || col.Check != nil || col.RefTable != "" {
			return nil
		}
	}
	for _, name := range info.PKColumns {
		for _, col := range info.Columns {
			if col.Name == name && (col.ColType != ColInteger || col.Unsigned) {
				return nil
			}
		}
	}
	if len(stmt.Values) != len(info.Columns) || len(stmt.ColumnNames) != 0 && len(stmt.ColumnNames) != len(info.Columns) {
		return nil
	}
	p := &preparedInsertPlan{literals: make([]*LiteralExpr, len(info.Columns)), row: make(map[string]Value, len(info.Columns))}
	for i, col := range info.Columns {
		source := i
		if len(stmt.ColumnNames) != 0 {
			source = -1
			for j, name := range stmt.ColumnNames {
				if name == col.Name {
					if source != -1 {
						return nil
					}
					source = j
				}
			}
			if source < 0 {
				return nil
			}
		}
		literal, ok := stmt.Values[source].(*LiteralExpr)
		if !ok {
			return nil
		}
		p.literals[i] = literal
	}
	return p
}
func (s *Schema) insertPreparedValues(stmt *InsertStmt) (int64, bool, error) {
	if stmt.prepared == nil || !s.inTx || s.lockHandler == nil || stmt.With != nil || s.cteScope != nil || stmt.Source != nil || stmt.Select != nil || stmt.DefaultValues || stmt.Ignore || stmt.Replace || stmt.OnConflict != nil || len(stmt.Returning) != 0 || len(stmt.ValueGroups) != 0 {
		return 0, false, nil
	}
	info := s.tables[strings.ToLower(stmt.Table)]
	if info == nil {
		return 0, false, nil
	}
	defer s.restorePreparedPlan(s.usePreparedPlan(stmt.prepared, info))
	cache := stmt.prepared
	if !cache.insertReady {
		cache.insert = s.compilePreparedInsert(stmt, info)
		cache.insertReady = true
	}
	p := cache.insert
	if p == nil {
		return 0, false, nil
	}
	s.statementChanges = 0
	defer func() { s.lastInsertIDUnsigned = 0 }()
	for i, col := range info.Columns {
		literal := p.literals[i]
		value := literal.Value
		// Literal BLOBs belong to this execution's AST; the encoded write set owns
		// its bytes before the next bind. Preserve dialect numeric-literal rules.
		if value.Type != ColBlob || value.Null {
			var err error
			value, err = s.evalExpr(literal, nil)
			if err != nil {
				return 0, true, err
			}
		}
		p.row[col.Name] = assignmentAffinity(value, col)
	}
	if err := s.normalizeMySQLRow(info, p.row); err != nil {
		return 0, true, err
	}
	if err := validatePrimaryKey(info, p.row); err != nil {
		return 0, true, err
	}
	if len(info.PKColumns) == 1 && p.row[info.PKColumns[0]].Type != ColInteger {
		return 0, true, fmt.Errorf("datatype mismatch for INTEGER PRIMARY KEY")
	}
	p.key = append(p.key[:0], info.storagePrefix(info.Name)...)
	for _, name := range info.PKColumns {
		p.key = appendClusteredValue(p.key, p.row[name])
	}
	if err := s.lockRow(info.Name, info, p.row, p.key); err != nil {
		return 0, true, err
	}
	if err := s.checkConstraints(info.Name, info, p.row, nil); err != nil {
		return 0, true, err
	}
	p.encoded = p.encoded[:0]
	for _, col := range info.Columns {
		p.encoded = appendValue(p.encoded, p.row[col.Name])
	}
	if err := s.checkInterrupt(); err != nil {
		return 0, true, err
	}
	s.txPut(p.key, p.encoded)
	if err := s.updateIndexes(info.Name, info, p.row, p.key, true); err != nil {
		return 0, true, err
	}
	s.statementChanges = 1
	lastID := int64(0)
	if !s.MySQLMode() && len(info.PKColumns) == 1 {
		lastID = p.row[info.PKColumns[0]].Int
	}
	return lastID, true, nil
}
