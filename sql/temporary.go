package sql

import (
	"encoding/binary"
	"fmt"
	"maps"
	"math"
	"strconv"
	"strings"

	"slopdb/storage"
)

// Temporary catalogs and rows belong to one SQL session. The ordinary write
// overlay provides transactional rows; definitions deliberately survive ROLLBACK.
type temporaryCatalog struct {
	tables   map[string]*TableInfo
	indexes  map[string]*IndexInfo
	base     *schemaState
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
	return s.temporary != nil && len(key) >= 11 && key[0] == 3 && (key[1] == 1 || key[1] == 2) && key[2] == 0xff && s.temporary.ids[binary.BigEndian.Uint64(key[3:11])]
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
	if stmt.Like != "" {
		var err error
		stmt, err = s.expandCreateTableLike(stmt)
		if err != nil {
			return err
		}
	}
	for _, info := range s.tables {
		if s.isTemporaryTable(info.Name) {
			continue
		}
		for _, fk := range tableForeignKeys(info) {
			if fk.RefTable == name {
				return fmt.Errorf("temporary table cannot shadow foreign-key parent %s", name)
			}
		}
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
		s.temporary = &temporaryCatalog{tables: map[string]*TableInfo{}, indexes: map[string]*IndexInfo{}, ids: map[uint64]bool{}, dropped: map[uint64]bool{}, autoinc: map[string]int64{}, nextRow: map[string]int64{}}
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
	copy.Indexes = append([]IndexDef(nil), stmt.Indexes...)
	for i := range copy.Indexes {
		idx := &copy.Indexes[i]
		if idx.DisplayName == "" {
			idx.DisplayName = idx.Name
		}
		if s.indexes[idx.Name] != nil {
			idx.Name = fmt.Sprintf("__temporary_%d_%s", s.temporaryObjectID(), idx.Name)
		}
	}
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
	for key, idx := range maps.Clone(s.indexes) {
		if idx.Table == name {
			copy := *idx
			if copy.DisplayName == "" {
				copy.DisplayName = copy.Name
			}
			copy.Name = fmt.Sprintf("__temporary_%d", copy.ID)
			delete(s.indexes, key)
			s.indexes[copy.Name] = &copy
			s.temporary.indexes[copy.Name] = &copy
		}
	}
	s.temporary.base = persistent
	s.catalogLookup = nil
	return nil
}

func (s *Schema) renameTemporaryTable(table, name string) error {
	if s.isTemporaryTable(name) {
		return fmt.Errorf("temporary table %q already exists", name)
	}
	for _, info := range s.tables {
		if s.isTemporaryTable(info.Name) {
			continue
		}
		for _, fk := range tableForeignKeys(info) {
			if fk.RefTable == name {
				return fmt.Errorf("temporary table cannot shadow foreign-key parent %s", name)
			}
		}
	}
	persistent := s.captureState()
	t := s.temporary
	info := *s.tables[table]
	info.Name = name
	t.tables = maps.Clone(t.tables)
	delete(t.tables, table)
	t.tables[name] = &info
	for key, idx := range s.indexes {
		if idx.Table == table {
			copy := *idx
			copy.Table = name
			t.indexes[key] = &copy
		}
	}
	t.autoinc[name] = s.autoincMax[table]
	delete(t.autoinc, table)
	s.restoreState(persistent)
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
	_ = s.cleanupTemporaryIDs(s.temporary.ids)
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
	t.dropped = maps.Clone(t.dropped)
	for id := range ids {
		t.dropped[id] = true
	}
	if err := s.cleanupTemporaryIDs(ids); err != nil {
		return err
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
	for key := range state.rowIDs {
		if strings.HasPrefix(key, "#") {
			if id, err := strconv.ParseUint(key[1:], 10, 64); err == nil && t.ids[id] {
				delete(state.rowIDs, key)
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

// prepareTemporaryBatch marks local ranges without splitting native atomic
// publication. Dropped definitions must not resurrect earlier private writes.
func (s *Schema) prepareTemporaryBatch(batch []storage.Mutation) []storage.Mutation {
	if s.temporary == nil {
		return batch
	}
	out := batch[:0]
	for _, w := range batch {
		if s.temporaryKey(w.Key) {
			if s.temporary.dropped[binary.BigEndian.Uint64(w.Key[3:11])] {
				continue
			}
			w.NoWAL = true
		}
		out = append(out, w)
	}
	return out
}
func (s *Schema) cleanupTemporaryIDs(ids map[uint64]bool) error {
	batch := make([]storage.Mutation, 0, len(ids)*2)
	for id := range ids {
		for _, kind := range []byte{1, 2} {
			prefix := []byte(objectPrefix(kind, id))
			batch = append(batch, storage.Mutation{Key: prefix, Value: prefixSuccessor(prefix), RangeDelete: true, NoWAL: true})
		}
	}
	return s.lsm.WriteBatch(batch)
}

// Only a physical database's initial catalog open clears crash leftovers.
// Historical/repeated catalog views must not erase live sessions' private data.
func cleanupTemporaryRange(lsm *storage.LSMTree) error {
	it := lsm.NewIteratorSeek([]byte{3})
	exists := it.Valid() && len(it.Key().UserKey) > 0 && it.Key().UserKey[0] == 3
	err := storage.IteratorError(it)
	it.Close()
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	return lsm.WriteBatch([]storage.Mutation{{Key: []byte{3}, Value: []byte{4}, RangeDelete: true, NoWAL: true}})
}

// TemporaryCatalogInfo is a caller-owned snapshot of this session's private
// object names. It does not include persistent objects hidden by temporary ones.
type TemporaryCatalogInfo struct {
	Tables      map[string][]string
	IndexTables map[string]string
}

func (s *Schema) TemporaryCatalog() TemporaryCatalogInfo {
	result := TemporaryCatalogInfo{Tables: map[string][]string{}, IndexTables: map[string]string{}}
	if s.temporary == nil {
		return result
	}
	for name, info := range s.temporary.tables {
		columns := make([]string, len(info.Columns))
		for i, column := range info.Columns {
			columns[i] = column.Name
		}
		result.Tables[name] = columns
	}
	for name, index := range s.temporary.indexes {
		result.IndexTables[name] = index.Table
	}
	return result
}
