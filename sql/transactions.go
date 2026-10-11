package sql

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"

	"slopdb/storage"
)

// ErrConflict means an optimistic transaction was rolled back and may be retried.
var ErrConflict = errors.New("transaction conflict; retry transaction")

// NewSession creates private execution and transaction state over this catalog.
// Each session serializes its own calls; statements in different sessions may
// execute concurrently over immutable catalog and MVCC statement snapshots.
func (s *Schema) NewSession() *Schema {
	if s.parent != nil {
		s = s.parent
	}
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	if s.rowIDReservations == nil {
		s.rowIDReservations = maps.Clone(s.rowIDCount)
	}
	for table, next := range s.rowIDCount {
		if next > s.rowIDReservations[table] {
			s.rowIDReservations[table] = next
		}
	}
	s.nextSnapshotReader++
	child := &Schema{maxCTEIterations: s.maxCTEIterations, maxCTERows: s.maxCTERows, snapshotReader: s.nextSnapshotReader, lsm: s.lsm, pageSize: s.pageSize, parent: s, readVersion: s.commitVersion.Load(), readStateVersion: s.stateVersion, readCatalogVersion: s.catalogVersion}
	child.restoreState(s.captureState())
	child.rowIDCount = make(map[string]int64)
	return child
}

func (s *Schema) InTransaction() bool { return s.inTx }

// RefreshReadCommitted imports newly committed metadata while preserving this
// transaction's changes. Savepoints are rebased too, so rollback cannot remove
// schema objects committed by another session.
func (s *Schema) RefreshReadCommitted() error {
	if s.parent == nil {
		return nil
	}
	s.parent.commitMu.RLock()
	defer s.parent.commitMu.RUnlock()
	return s.refreshReadCommittedLocked()
}

// StartStatement atomically captures the catalog and its committed data view.
// A new snapshot for each statement gives READ COMMITTED, including in a tx.
func (s *Schema) StartStatement() error {
	s.EndStatement()
	root := s
	if s.parent != nil {
		root = s.parent
	}
	root.commitMu.RLock()
	defer root.commitMu.RUnlock()
	if err := s.refreshReadCommittedLocked(); err != nil {
		return err
	}
	s.statementSnapshot = s.lsm.SnapshotForReader(s.snapshotReader)
	s.statementActive = true
	return nil
}
func (s *Schema) EndStatement() {
	if s.statementActive {
		s.lsm.ReleaseSnapshotForReader(s.snapshotReader, s.statementSnapshot)
		s.statementActive = false
	}
	s.validatedRowKey = ""
}
func (s *Schema) committedGet(key []byte) ([]byte, bool, error) {
	if s.statementActive {
		return s.lsm.GetAt(key, s.statementSnapshot)
	}
	return s.lsm.Get(key)
}
func (s *Schema) committedIterator() storage.Iterator {
	if s.statementActive {
		return s.lsm.NewIteratorAt(s.statementSnapshot)
	}
	return s.lsm.NewIterator()
}
func (s *Schema) refreshReadCommittedLocked() error {
	if s.parent == nil {
		return nil
	}
	if s.inTx && s.txHasWrites && s.txCatalogVersion != s.parent.catalogVersion {
		return fmt.Errorf("%w: catalog changed", ErrConflict)
	}
	if s.inTx && s.txHasDDL && s.catalogChanged() && s.txDDLVersion != s.parent.commitVersion.Load() {
		return fmt.Errorf("%w: data changed during schema modification", ErrConflict)
	}
	// A statement retry or commit often sees the same committed catalog. Avoid
	// copying and rebasing all metadata again, while still importing row-ID
	// reservations made by other transactions before they commit.
	if s.readStateVersion != s.parent.stateVersion {
		latest := s.parent.captureState()
		if s.inTx {
			local := cloneState(mergeState(s.txState, s.captureState(), latest))
			for i := range s.savepoints {
				s.savepoints[i].state = cloneState(mergeState(s.txState, s.savepoints[i].state, latest))
			}
			s.txState = latest
			s.restoreState(local)
		} else {
			s.restoreState(latest)
		}
		s.readStateVersion = s.parent.stateVersion
	}
	s.readVersion = s.parent.commitVersion.Load()
	s.readCatalogVersion = s.parent.catalogVersion
	for table, next := range s.rowIDCount {
		if reserved := s.parent.rowIDReservations[table]; reserved > next {
			s.rowIDCount[table] = reserved
		}
	}
	return nil
}

func cloneState(state *schemaState) *schemaState {
	var copy Schema
	copy.restoreState(state)
	return copy.captureState()
}

func (s *Schema) persistentStateChanged() bool {
	return s.catalogChanged() || !reflect.DeepEqual(s.stats, s.txState.stats) || !reflect.DeepEqual(s.distributions, s.txState.distributions) ||
		!reflect.DeepEqual(s.autoincMax, s.txState.autoinc) || s.userVersion != s.txState.userVersion
}

func mergeMap[T any](base, local, latest map[string]T) map[string]T {
	merged := make(map[string]T, len(latest))
	for key, value := range latest {
		merged[key] = value
	}
	for key, old := range base {
		value, exists := local[key]
		if !exists {
			delete(merged, key)
		} else if !reflect.DeepEqual(old, value) {
			merged[key] = value
		}
	}
	for key, value := range local {
		if _, existed := base[key]; !existed {
			merged[key] = value
		}
	}
	return merged
}

func mergeCatalogMap[T any](base, local, latest map[string]T) map[string]T {
	if reflect.DeepEqual(base, local) {
		return latest
	}
	return mergeMap(base, local, latest)
}

func mergeCounter(base, local, latest map[string]int64) map[string]int64 {
	merged := mergeMap(base, local, latest)
	for key, value := range latest {
		if current, exists := merged[key]; exists && value > current {
			merged[key] = value
		}
	}
	return merged
}

func mergeState(base, local, latest *schemaState) *schemaState {
	version := latest.userVersion
	if local.userVersion != base.userVersion {
		version = local.userVersion
	}
	merged := &schemaState{
		tables:        mergeCatalogMap(base.tables, local.tables, latest.tables),
		indexes:       mergeCatalogMap(base.indexes, local.indexes, latest.indexes),
		views:         mergeCatalogMap(base.views, local.views, latest.views),
		triggers:      mergeCatalogMap(base.triggers, local.triggers, latest.triggers),
		stats:         mergeMap(base.stats, local.stats, latest.stats),
		distributions: mergeMap(base.distributions, local.distributions, latest.distributions),
		autoinc:       mergeCounter(base.autoinc, local.autoinc, latest.autoinc),
		rowIDs:        mergeCounter(base.rowIDs, local.rowIDs, latest.rowIDs),
		userVersion:   version,
	}
	if reflect.DeepEqual(merged.tables, latest.tables) && reflect.DeepEqual(merged.indexes, latest.indexes) {
		merged.lookups = latest.lookups
	}
	return merged
}

func (s *Schema) catalogChanged() bool {
	if s.txState == nil {
		return false
	}
	return !reflect.DeepEqual(s.tables, s.txState.tables) ||
		!reflect.DeepEqual(s.indexes, s.txState.indexes) ||
		!reflect.DeepEqual(s.views, s.txState.views) ||
		!reflect.DeepEqual(s.triggers, s.txState.triggers)
}

func (s *Schema) recordWrites(pos int) error {
	// The database's pessimistic row/predicate locks protect these mutations
	// until commit. Sessions without a lock handler retain version validation.
	if s.lockHandler == nil {
		previous := make(map[string]bool)
		for _, w := range s.txWrites[:pos] {
			previous[string(w.key)] = true
		}
		for _, w := range s.txWrites[pos:] {
			if !bytes.HasPrefix(w.key, []byte(dataPrefix)) {
				continue
			}
			if !previous[string(w.key)] {
				var version uint64
				var err error
				if s.statementActive {
					_, _, version, err = s.lsm.GetVersionAt(w.key, s.statementSnapshot)
				} else {
					_, _, version, err = s.lsm.GetVersion(w.key)
				}
				if err != nil {
					return err
				}
				s.txRowVersions[string(w.key)] = version
				previous[string(w.key)] = true
			}
		}
	}
	// Data-only writers already hold catalog/row/predicate locks. Recording
	// their statement does not need the catalog publication mutex.
	if s.parent != nil && s.lockHandler != nil && len(s.rowIDCount) == 0 && !s.catalogChanged() {
		if !s.txHasWrites && len(s.txWrites) > 0 {
			s.txCatalogVersion = s.readCatalogVersion
			s.txWriteVersion = s.readVersion
			s.txHasWrites = true
		}
		return nil
	}
	root := s
	if s.parent != nil {
		root = s.parent
	}
	root.commitMu.Lock()
	defer root.commitMu.Unlock()

	if s.parent != nil {
		if !s.txHasWrites && (len(s.txWrites) > 0 || s.catalogChanged()) {
			s.txCatalogVersion = s.parent.catalogVersion
			s.txWriteVersion = s.parent.commitVersion.Load()
			s.txHasWrites = true
		}
		if !s.txHasDDL && s.catalogChanged() {
			s.txDDLVersion = s.parent.commitVersion.Load()
			s.txHasDDL = true
		}
	}
	if root.rowIDReservations == nil {
		root.rowIDReservations = make(map[string]int64)
	}
	for table, next := range s.rowIDCount {
		if next > root.rowIDReservations[table] {
			root.rowIDReservations[table] = next
		}
	}
	return nil
}

func (s *Schema) prepareCommit() error {
	if err := s.checkInterrupt(); err != nil {
		return err
	}
	if err := s.refreshReadCommittedLocked(); err != nil {
		return err
	}
	if s.parent != nil && s.catalogChanged() && s.txDDLVersion != s.parent.commitVersion.Load() {
		return fmt.Errorf("%w: data changed during schema modification", ErrConflict)
	}
	// Pessimistic writers checked constraints under row and predicate locks.
	// Optimistic writers revalidate if another transaction committed meanwhile.
	if s.parent == nil || s.parent.commitVersion.Load() == s.txWriteVersion || s.lockHandler != nil {
		return nil
	}
	// Only validate writes that still exist after savepoint or statement rollback.
	final := make(map[string]txWrite)
	for _, w := range s.txWrites {
		if bytes.HasPrefix(w.key, []byte(dataPrefix)) {
			final[string(w.key)] = w
		}
	}
	for key := range final {
		if err := s.checkInterrupt(); err != nil {
			return err
		}
		_, _, version, err := s.lsm.GetVersion([]byte(key))
		if err != nil {
			return err
		}
		if version != s.txRowVersions[key] {
			return fmt.Errorf("%w: row changed", ErrConflict)
		}
	}
	// Unique values are predicates, not physical index keys (which include the
	// row key). Validate final rows against the latest committed rows plus our
	// overlay to catch concurrent inserts and changes to compound UNIQUE keys.
	for _, w := range final {
		if err := s.checkInterrupt(); err != nil {
			return err
		}
		if w.del {
			continue
		}
		if table, info := s.rowTable(w.key); info != nil {
			groups := s.uniqueGroups(table, info)
			if len(groups) == 0 {
				continue
			}
			row := decodeRow(w.value, info)
			for _, group := range groups {
				if err := s.checkUnique(table, group, row, w.key, info); err != nil {
					if !strings.HasPrefix(err.Error(), "UNIQUE constraint failed:") {
						return err
					}
					return fmt.Errorf("%w: %v", ErrConflict, err)
				}
			}
		}
	}
	return s.checkInterrupt()
}

func (s *Schema) endTransaction() {
	if s.parent != nil {
		s.rowIDCount = make(map[string]int64)
	}
	s.inTx = false
	s.txWrites = nil
	s.txLatest = nil
	s.txState = nil
	s.savepoints = nil
	s.txRowVersions = nil
	s.txHasWrites = false
	s.txHasDDL = false
}

func (s *Schema) resetRolledBackWrites() {
	if !s.catalogChanged() {
		s.txHasDDL = false
		if len(s.txWrites) == 0 {
			s.txHasWrites = false
		}
	}
}
