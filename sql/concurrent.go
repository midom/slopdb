package sql

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"

	"slopdb/storage"
)

// PointUpdate executes a simple primary-key update over a stable catalog. An
// autocommit caller owns its row lock; transactions lock a matching row and
// append row/index mutations to their private write set until commit.
// Non-unique index changes share the row mutation batch. Unique-value changes,
// triggers and checks use the full transaction executor.
type PointUpdate struct {
	schema         *Schema
	info           *TableInfo
	stmt           *UpdateStmt
	key            []byte
	bindings       map[string]int
	targets        []int
	indexedTargets []int
	indexes        []*IndexInfo
}

func (s *Schema) PreparePointUpdate(stmt *UpdateStmt) *PointUpdate {
	return s.PreparePointUpdateInto(stmt, nil)
}

// PreparePointUpdateInto reuses a private execution destination. Its caller
// must not retain or share that destination across simultaneous executions.
func (s *Schema) PreparePointUpdateInto(stmt *UpdateStmt, dst *PointUpdate) *PointUpdate {
	if stmt.With != nil || s.cteScope != nil {
		return nil
	}
	info := s.tables[strings.ToLower(stmt.Table)]
	if stmt.prepared == nil || info == nil {
		plan := s.preparePointUpdateUncached(stmt)
		if dst != nil && plan != nil {
			*dst = *plan
			return dst
		}
		return plan
	}
	defer s.restorePreparedPlan(s.usePreparedPlan(stmt.prepared, info))
	cached := stmt.prepared
	if !cached.pointUpdateReady {
		cached.pointUpdate = s.preparePointUpdateUncached(stmt)
		cached.pointUpdateReady = true
		if cached.pointUpdate != nil {
			// Plans contain immutable bindings, targets and index pointers only.
			// The private execution gets its own schema, AST and key below.
			cached.pointUpdate.schema = nil
			cached.pointUpdate.stmt = nil
			cached.pointUpdate.key = nil
		}
	}
	if cached.pointUpdate == nil {
		return nil
	}
	key, ok := s.primaryKeyLookup(stmt.Where, info.Name, info)
	if !ok {
		return nil
	}
	if dst == nil {
		dst = new(PointUpdate)
	}
	*dst = *cached.pointUpdate
	dst.schema, dst.stmt, dst.key = s, stmt, key
	return dst
}
func (s *Schema) preparePointUpdateUncached(stmt *UpdateStmt) *PointUpdate {
	if stmt.With != nil || s.cteScope != nil || s.parent == nil || len(stmt.Returning) > 0 {
		return nil
	}
	table := strings.ToLower(stmt.Table)
	info := s.tables[table]
	if info == nil || len(info.PKColumns) == 0 || len(info.TableCheck) > 0 || len(info.ForeignKeys) > 0 {
		return nil
	}
	if len(s.lookups().foreign[table]) > 0 || len(s.lookups().references[table]) > 0 {
		return nil
	}
	for _, tr := range s.triggers {
		if tr.Table == table || tr.View == table {
			return nil
		}
	}
	key, ok := s.primaryKeyLookup(stmt.Where, table, info)
	if !ok && (stmt.prepared == nil || !stmt.prepared.pointComplete()) {
		return nil
	}
	blocked := map[string]bool{}
	for _, name := range info.PKColumns {
		blocked[name] = true
	}
	for _, col := range info.Columns {
		if col.Check != nil || col.RefTable != "" {
			return nil
		}
		if col.Unique {
			blocked[col.Name] = true
		}
	}
	for _, group := range info.TableUnique {
		for _, name := range group {
			blocked[name] = true
		}
	}
	for _, idx := range s.tableIndexes(table) {
		if !idx.IsUnique {
			continue
		}
		for _, name := range idx.columnNames() {
			blocked[name] = true
		}
	}
	p := &PointUpdate{schema: s, info: info, stmt: stmt, key: key, bindings: make(map[string]int)}
	var bind func(Expression) bool
	bind = func(expr Expression) bool {
		switch e := expr.(type) {
		case *LiteralExpr:
			return true
		case *ColumnRefExpr:
			// Preserve the full executor's column-name rules, including its errors.
			for i, col := range info.Columns {
				if strings.EqualFold(col.Name, e.Name) {
					p.bindings[e.Name] = i
					return true
				}
			}
			return false
		case *BinaryExpr:
			return bind(e.Left) && bind(e.Right)
		case *UnaryExpr:
			return bind(e.Right)
		default:
			return false
		}
	}
	if !bind(stmt.Where) {
		return nil
	}
	for _, set := range stmt.Set {
		name := strings.ToLower(set.Column)
		if blocked[name] || !bind(set.Value) {
			return nil
		}
		target := -1
		for i, col := range info.Columns {
			if col.Name == name {
				target = i
				break
			}
		}
		if target < 0 {
			return nil
		}
		p.targets = append(p.targets, target)
	}
	// Compile only indexes that can change. Their unchanged components are
	// decoded from the same locked row used for expression evaluation.
	for _, idx := range s.tableIndexes(table) {
		affected := false
		for _, name := range idx.columnNames() {
			for _, target := range p.targets {
				if info.Columns[target].Name == name {
					affected = true
				}
			}
		}
		if !affected {
			continue
		}
		p.indexes = append(p.indexes, idx)
		payloadColumns := idx.columnNames()
		if !s.isAutoIndex(idx) {
			payloadColumns = append(append([]string(nil), payloadColumns...), info.PKColumns...)
		}
		for _, name := range payloadColumns {
			for i, col := range info.Columns {
				if col.Name == name {
					p.bindings[name] = i
				}
			}
		}
	}
	// Validate each changed indexed column once, even when several indexes
	// share it. Unchanged components already satisfy the catalog invariant.
	indexed := make(map[string]bool)
	for _, idx := range p.indexes {
		for _, name := range idx.columnNames() {
			indexed[name] = true
		}
	}
	for _, target := range p.targets {
		name := info.Columns[target].Name
		if indexed[name] {
			p.indexedTargets = append(p.indexedTargets, target)
			delete(indexed, name)
		}
	}
	return p
}
func (p *PointUpdate) Key() []byte { return p.key }
func (p *PointUpdate) Run(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if p.stmt.Limit == 0 || p.stmt.Offset > 0 {
		return 0, nil
	}
	var data []byte
	var found bool
	var err error
	if p.schema.inTx {
		data, found, err = p.schema.schemaGet(p.key)
	} else if p.stmt.prepared != nil {
		cached := p.stmt.prepared
		data, found, err = p.schema.lsm.GetForReaderInto(p.key, p.schema.snapshotReader, cached.pointValueScratch)
		cached.pointValueScratch = data
	} else {
		data, found, err = p.schema.lsm.GetForReaderInto(p.key, p.schema.snapshotReader, nil)
	}
	if err != nil || !found {
		return 0, err
	}
	var localValues [32]Value
	values := localValues[:]
	var localOffsets [33]int
	offsets := localOffsets[:]
	if len(p.info.Columns) > 32 {
		values = make([]Value, len(p.info.Columns))
		offsets = make([]int, len(p.info.Columns)+1)
	}
	offset := 0
	for i := range p.info.Columns {
		offsets[i] = offset
		if offset >= len(data) {
			values[i] = NewNullValue()
			continue
		}
		v, n := decodePointUpdateValue(data[offset:])
		v.Binary = p.info.Columns[i].Binary
		values[i] = v
		offset += n
	}
	offsets[len(p.info.Columns)] = offset
	var row map[string]Value
	if p.stmt.prepared != nil {
		row = p.stmt.prepared.pointRowScratch
	}
	if row == nil {
		row = make(map[string]Value, len(p.bindings))
		if p.stmt.prepared != nil {
			p.stmt.prepared.pointRowScratch = row
		}
	}
	for name, i := range p.bindings {
		row[name] = values[i]
	}
	match, err := p.schema.evalCondition(p.stmt.Where, row)
	if err != nil || !match {
		return 0, err
	}
	if p.schema.inTx {
		if err := p.schema.RequestLock(LockRequest{Resource: "row:" + string(p.key), Exclusive: true}); err != nil {
			return 0, err
		}
		if err := p.schema.validateLockedRow(p.key); err != nil {
			return 0, err
		}
	}
	var localSet [8]Value
	setValues := localSet[:]
	if len(p.stmt.Set) > 8 {
		setValues = make([]Value, len(p.stmt.Set))
	}
	for i, set := range p.stmt.Set {
		v, e := p.schema.evalExpr(set.Value, row)
		if e != nil {
			return 0, e
		}
		col := p.info.Columns[p.targets[i]]
		v = applyAffinity(v, col.ColType)

		if err := validateColumnValue(p.info.Name, col, v); err != nil {
			return 0, err
		}
		setValues[i] = v
	}
	var localIndexKeys [8][]byte
	oldIndexKeys := localIndexKeys[:]
	if len(p.indexes) > len(localIndexKeys) {
		oldIndexKeys = make([][]byte, len(p.indexes))
	}
	for i, idx := range p.indexes {
		oldIndexKeys[i] = encodeRowIndexKey(idx, row, p.key)
	}
	var localChanged [32]bool
	changed := localChanged[:]
	if len(p.info.Columns) > 32 {
		changed = make([]bool, len(p.info.Columns))
	}
	for i, target := range p.targets {
		values[target] = setValues[i]
		changed[target] = true
	}
	for target, col := range p.info.Columns {
		if col.NotNull && values[target].Null {
			return 0, fmt.Errorf("NOT NULL constraint failed: %s.%s", p.info.Name, col.Name)
		}
	}
	for _, column := range p.indexedTargets {
		if err := checkIndexedColumnType(p.info.Name, p.info.Columns[column], values[column]); err != nil {
			return 0, err
		}
	}
	var encoded []byte
	if p.stmt.prepared != nil {
		encoded = p.stmt.prepared.pointWriteScratch[:0]
	}
	if cap(encoded) < len(data)+16 {
		encoded = make([]byte, 0, len(data)+16)
	}
	for i := range p.info.Columns {
		if changed[i] || offsets[i] == offsets[i+1] {
			encoded = appendValue(encoded, values[i])
		} else {
			encoded = append(encoded, data[offsets[i]:offsets[i+1]]...)
		}
	}
	if p.stmt.prepared != nil {
		p.stmt.prepared.pointWriteScratch = encoded
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	batch := make([]storage.Mutation, 0, 1+2*len(p.indexes))
	batch = append(batch, storage.Mutation{Key: p.key, Value: encoded})
	if len(p.indexes) > 0 {
		for name, col := range p.bindings {
			row[name] = values[col]
		}
	}
	for i, idx := range p.indexes {
		key := encodeRowIndexKey(idx, row, p.key)
		if bytes.Equal(key, oldIndexKeys[i]) {
			continue
		}
		batch = append(batch, storage.Mutation{Key: oldIndexKeys[i], Delete: true}, storage.Mutation{Key: key, Value: p.schema.encodeIndexValue(idx, row)})
	}
	if p.schema.inTx {
		for _, w := range batch {
			if w.Delete {
				p.schema.txDelete(w.Key)
			} else {
				p.schema.txPut(w.Key, w.Value)
			}
		}
		return 1, nil
	}
	if err := p.schema.writeBatchWithOnlineIndexes(batch); err != nil {
		return 0, err
	}
	p.schema.parent.commitVersion.Add(1)
	return 1, nil
}

// Point updates keep their input buffer alive through expression evaluation and
// encoding. Their restricted expressions cannot mutate a BLOB. Borrow it here;
// public results and transaction write sets continue to own independent bytes.
func decodePointUpdateValue(data []byte) (Value, int) {
	if len(data) > 0 && data[0] == binTypeBlob {
		length, n := binary.Uvarint(data[1:])
		if n > 0 && length <= uint64(len(data)-1-n) {
			return Value{Type: ColBlob, Blob: data[1+n : 1+n+int(length)]}, 1 + n + int(length)
		}
	}
	return decodeValue(data)
}
