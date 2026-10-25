package sql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"maps"
	"math"
	"sort"
	"strings"

	"slopdb/storage"
)

// Temporary catalogs and rows belong to one SQL session. The ordinary write
// overlay provides transactional rows; definitions deliberately survive ROLLBACK.
type temporaryCatalog struct {
	tables   map[string]*TableInfo
	indexes  map[string]*IndexInfo
	base     *schemaState
	rows     map[string][]byte
	ids      map[uint64]bool
	dropped  map[uint64]bool
	autoinc  map[string]int64
	nextRow  map[string]int64
	building bool
}

func (s *Schema) hasTemporaryTables() bool { return s.temporary != nil && len(s.temporary.tables) > 0 }
func (s *Schema) isTemporaryTable(name string) bool {
	return s.temporary != nil && s.temporary.tables[strings.ToLower(name)] != nil
}

// IsTemporaryTable reports whether this session resolves name to a private table.
func (s *Schema) IsTemporaryTable(name string) bool { return s.isTemporaryTable(name) }
func (s *Schema) temporaryKey(key []byte) bool {
	return s.temporary != nil && len(key) >= 10 && (key[0] == 1 || key[0] == 2) && key[1] == 0xff && s.temporary.ids[binary.BigEndian.Uint64(key[2:10])]
}
func (s *Schema) temporaryObjectID() uint64 {
	root := s
	if s.parent != nil {
		root = s.parent
	}
	id := math.MaxUint64 - root.nextTemporaryID.Add(1) + 1
	s.temporary.ids[id] = true
	return id
}
func (s *Schema) createTemporaryTable(stmt *CreateTableStmt) error {
	name := strings.ToLower(stmt.Table)
	if s.isTemporaryTable(name) {
		if stmt.IfNotExists {
			return nil
		}
		return fmt.Errorf("table %q already exists", name)
	}
	if len(stmt.ForeignKeys) > 0 {
		return fmt.Errorf("foreign keys on temporary tables are not supported")
	}
	for _, col := range stmt.Columns {
		if col.RefTable != "" {
			return fmt.Errorf("foreign keys on temporary tables are not supported")
		}
	}
	if s.temporary == nil {
		s.temporary = &temporaryCatalog{tables: map[string]*TableInfo{}, indexes: map[string]*IndexInfo{}, rows: map[string][]byte{}, ids: map[uint64]bool{}, dropped: map[uint64]bool{}, autoinc: map[string]int64{}, nextRow: map[string]int64{}}
	}
	persistent := s.captureState()
	// Copy before hiding a permanent table of the same name.
	s.ensureCatalogWritable()
	delete(s.tables, name)
	hiddenView := s.views[name]
	delete(s.views, name)
	for key, idx := range s.indexes {
		if idx.Table == name {
			delete(s.indexes, key)
		}
	}
	delete(s.autoincMax, name)
	delete(s.stats, name)
	delete(s.distributions, name)
	oldBuilding := s.temporary.building
	s.temporary.building = true
	copy := *stmt
	copy.Temporary = false
	err := s.CreateTable(&copy)
	if hiddenView != nil {
		s.views[name] = hiddenView
	}
	s.temporary.building = oldBuilding
	if err != nil {
		s.restoreState(persistent)
		return err
	}
	s.temporary.tables[name] = s.tables[name]
	for key, idx := range s.indexes {
		if idx.Table == name {
			s.temporary.indexes[key] = idx
		}
	}
	s.temporary.base = persistent
	s.catalogLookup = nil
	return nil
}

func (s *Schema) snapshotTemporaryDefinitions() *temporaryCatalog {
	if s.temporary == nil {
		return nil
	}
	copy := *s.temporary
	copy.tables = maps.Clone(copy.tables)
	copy.indexes = maps.Clone(copy.indexes)
	copy.autoinc = maps.Clone(copy.autoinc)
	copy.dropped = maps.Clone(copy.dropped)
	return &copy
}

// DiscardTemporaryTables releases all private definitions and rows at session end.
func (s *Schema) DiscardTemporaryTables() {
	if s.temporary == nil {
		return
	}
	state := s.captureState()
	s.temporary = nil
	s.restoreState(state)
}
func (s *Schema) dropTemporaryTable(stmt *DropTableStmt) error {
	name := strings.ToLower(stmt.Table)
	if !s.isTemporaryTable(name) {
		if stmt.IfExists {
			return nil
		}
		return fmt.Errorf("temporary table %q does not exist", name)
	}
	persistent := s.captureState()
	t := s.temporary
	tableID := t.tables[name].ID
	ids := map[uint64]bool{tableID: true}
	for key, idx := range t.indexes {
		if idx.Table == name {
			ids[idx.ID] = true
			delete(t.indexes, key)
		}
	}
	delete(t.tables, name)
	t.rows = maps.Clone(t.rows)
	t.dropped = maps.Clone(t.dropped)
	for id := range ids {
		t.dropped[id] = true
	}
	for key := range t.rows {
		data := []byte(key)
		if len(data) >= 10 && ids[binary.BigEndian.Uint64(data[2:10])] {
			delete(t.rows, key)
		}
	}
	s.restoreState(persistent)
	return nil
}

// Strip private definitions before catalog comparisons, persistence or sharing.
func (s *Schema) persistentTemporaryState(state *schemaState) *schemaState {
	if !s.hasTemporaryTables() {
		return state
	}
	t := s.temporary
	t.tables = maps.Clone(t.tables)
	t.indexes = map[string]*IndexInfo{}
	for name := range t.tables {
		if info := s.tables[name]; info != nil && t.ids[info.ID] {
			t.tables[name] = info
		}
	}
	for key, idx := range s.indexes {
		if t.ids[idx.ID] {
			t.indexes[key] = idx
		}
	}
	state.tables = maps.Clone(state.tables)
	state.indexes = maps.Clone(state.indexes)
	state.autoinc = maps.Clone(state.autoinc)
	state.stats = maps.Clone(state.stats)
	state.distributions = maps.Clone(state.distributions)
	for name := range t.tables {
		t.autoinc[name] = s.autoincMax[name]
		delete(state.tables, name)
		delete(state.autoinc, name)
		delete(state.stats, name)
		delete(state.distributions, name)
		if t.base != nil {
			if value, ok := t.base.tables[name]; ok {
				state.tables[name] = value
			}
			if value, ok := t.base.autoinc[name]; ok {
				state.autoinc[name] = value
			}
			if value, ok := t.base.stats[name]; ok {
				state.stats[name] = value
			}
			if value, ok := t.base.distributions[name]; ok {
				state.distributions[name] = value
			}
		}
	}
	for key, idx := range state.indexes {
		if t.ids[idx.ID] || t.tables[idx.Table] != nil {
			delete(state.indexes, key)
		}
	}
	if t.base != nil {
		for key, idx := range t.base.indexes {
			if t.tables[idx.Table] != nil {
				state.indexes[key] = idx
			}
		}
	}
	state.lookups = nil
	return state
}
func (s *Schema) overlayTemporaryState(state *schemaState) {
	if !s.hasTemporaryTables() {
		return
	}
	t := s.temporary
	t.base = state
	s.tables = maps.Clone(s.tables)
	s.indexes = maps.Clone(s.indexes)
	s.autoincMax = maps.Clone(s.autoincMax)
	s.stats = maps.Clone(s.stats)
	s.distributions = maps.Clone(s.distributions)
	for key, idx := range s.indexes {
		if t.tables[idx.Table] != nil {
			delete(s.indexes, key)
		}
	}
	for name, info := range t.tables {
		s.tables[name] = info
		s.autoincMax[name] = t.autoinc[name]
		delete(s.stats, name)
		delete(s.distributions, name)
	}
	for key, idx := range t.indexes {
		s.indexes[key] = idx
	}
	s.catalogLookup = nil
}
func (s *Schema) temporaryIterator(base storage.Iterator, lower, upper []byte) storage.Iterator {
	if s.temporary == nil || len(s.temporary.rows) == 0 {
		return base
	}
	rows := make([]storage.KeyValue, 0, len(s.temporary.rows))
	for key, value := range s.temporary.rows {
		data := []byte(key)
		if lower != nil && bytes.Compare(data, lower) < 0 || upper != nil && bytes.Compare(data, upper) >= 0 {
			continue
		}
		rows = append(rows, storage.KeyValue{Key: storage.InternalKey{UserKey: data, OpType: 1}, Value: value})
	}
	if len(rows) == 0 {
		return base
	}
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].Key.UserKey, rows[j].Key.UserKey) < 0 })
	it := &transactionIterator{base: base, rows: rows}
	it.Next()
	return it
}
func (s *Schema) splitTemporaryBatch(batch []storage.Mutation) (persistent, private []storage.Mutation) {
	if s.temporary == nil {
		return batch, nil
	}
	for _, mutation := range batch {
		if s.temporaryKey(mutation.Key) {
			private = append(private, mutation)
		} else {
			persistent = append(persistent, mutation)
		}
	}
	return
}
func (s *Schema) applyTemporaryBatch(batch []storage.Mutation) {
	for _, mutation := range batch {
		if s.temporary.dropped[binary.BigEndian.Uint64(mutation.Key[2:10])] {
			continue
		}
		if mutation.Delete {
			delete(s.temporary.rows, string(mutation.Key))
		} else {
			s.temporary.rows[string(mutation.Key)] = copyBytes(mutation.Value)
		}
	}
}
