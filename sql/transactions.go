package sql

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"time"

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
	s.commitMu.RLock()
	if s.rowIDReservations == nil || !s.catalogShared {
		s.commitMu.RUnlock()
		s.commitMu.Lock()
		if s.rowIDReservations == nil {
			s.rowIDMu.Lock()
			s.rowIDReservations = maps.Clone(s.rowIDCount)
			s.rowIDMu.Unlock()
		}
		s.catalogShared = true
		s.commitMu.Unlock()
		s.commitMu.RLock()
	}
	defer s.commitMu.RUnlock()
	// Catalog snapshots are immutable; only reader-ID assignment mutates
	// shared state. Row allocation publishes its reservations separately.
	child := &Schema{maxCTEIterations: s.maxCTEIterations, maxCTERows: s.maxCTERows, snapshotReader: s.nextSnapshotReader.Add(1), lsm: s.lsm, parent: s, readVersion: s.commitVersion.Load(), readStateVersion: s.stateVersion, readCatalogVersion: s.catalogVersion}
	child.queryMemoryBytes, child.queryTempDir = s.queryMemoryBytes, s.queryTempDir
	child.disableAutoIncrementPersistence = s.disableAutoIncrementPersistence
	child.enableAutoIncrementGroupCommit = s.enableAutoIncrementGroupCommit
	child.mysqlMode = s.mysqlMode
	child.isolation = s.isolation
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
	s.parent.commitMu.RLockForReader(s.snapshotReader)
	defer s.parent.commitMu.RUnlockForReader(s.snapshotReader)
	return s.refreshReadCommittedLocked()
}

// StartStatement atomically captures the catalog and its committed data view.
// A new snapshot for each statement gives READ COMMITTED, including in a tx.
func (s *Schema) StartStatement() error {
	s.EndStatement()
	s.consistentIndexDeletes = nil
	s.statementTime = time.Now().UTC()
	if s.replicaReadOnly {
		s.replicaReadOnly = false
		s.readStateVersion = ^uint64(0)
	}
	if s.statementSnapshotHook != nil {
		if handled, err := s.statementSnapshotHook(s.ctx, s); handled || err != nil {
			if err != nil {
				return err
			}
			return s.useConsistentStatementSnapshot()
		}
	}
	root := s
	if s.parent != nil {
		root = s.parent
	}
	root.commitMu.RLockForReader(s.snapshotReader)
	defer root.commitMu.RUnlockForReader(s.snapshotReader)
	if err := s.refreshReadCommittedLocked(); err != nil {
		return err
	}
	s.statementSnapshot = s.lsm.SnapshotForReader(s.snapshotReader)
	s.statementActive = true
	return s.useConsistentStatementSnapshot()
}
func (s *Schema) EndStatement() {
	s.statementTime = time.Time{}
	if s.statementActive {
		s.lsm.ReleaseSnapshotForReader(s.snapshotReader, s.statementSnapshot)
		s.statementActive = false
	}
	s.validatedRowKey = ""
}
func (s *Schema) committedGet(key []byte) ([]byte, bool, error) {
	if s.temporaryKey(key) {
		return s.lsm.Get(key)
	}
	if s.statementActive {
		return s.lsm.GetAtForReader(key, s.statementSnapshot, s.snapshotReader)
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
	// A physical replica imports catalog and data together in the owner's
	// snapshot hook. Its ordinary SQL parent does not receive WAL catalogs.
	if s.replicaReadOnly {
		return nil
	}
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
	// copying and rebasing all metadata again. nextRowID imports shared ID
	// reservations under the allocator mutex when it actually allocates an ID.
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
	return nil
}

func cloneState(state *schemaState) *schemaState {
	var copy Schema
	copy.restoreState(state)
	return copy.captureState()
}

func (s *Schema) persistentStateChanged() bool {
	if s.hasTemporaryTables() {
		state := s.captureState()
		return !reflect.DeepEqual(state.autoincUnsigned, s.txState.autoincUnsigned) || s.catalogChanged() || !reflect.DeepEqual(state.stats, s.txState.stats) || !reflect.DeepEqual(state.distributions, s.txState.distributions) || !reflect.DeepEqual(state.autoinc, s.txState.autoinc) || s.userVersion != s.txState.userVersion || s.collation != s.txState.collation
	}
	return !reflect.DeepEqual(s.autoincUnsigned, s.txState.autoincUnsigned) || s.catalogChanged() || !reflect.DeepEqual(s.stats, s.txState.stats) || !reflect.DeepEqual(s.distributions, s.txState.distributions) ||
		!reflect.DeepEqual(s.autoincMax, s.txState.autoinc) || s.userVersion != s.txState.userVersion || s.collation != s.txState.collation
}

func (s *Schema) onlyAutoincMetadataChanged() bool {
	return !s.catalogChanged() && reflect.DeepEqual(s.stats, s.txState.stats) &&
		reflect.DeepEqual(s.distributions, s.txState.distributions) &&
		s.userVersion == s.txState.userVersion && s.collation == s.txState.collation
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
	collation := latest.collation
	if local.collation != base.collation {
		collation = local.collation
	}
	merged := &schemaState{
		namespaces:      mergeCatalogMap(base.namespaces, local.namespaces, latest.namespaces),
		tables:          mergeCatalogMap(base.tables, local.tables, latest.tables),
		indexes:         mergeCatalogMap(base.indexes, local.indexes, latest.indexes),
		views:           mergeCatalogMap(base.views, local.views, latest.views),
		triggers:        mergeCatalogMap(base.triggers, local.triggers, latest.triggers),
		stats:           mergeMap(base.stats, local.stats, latest.stats),
		distributions:   mergeMap(base.distributions, local.distributions, latest.distributions),
		autoinc:         mergeCounter(base.autoinc, local.autoinc, latest.autoinc),
		autoincUnsigned: mergeUnsignedCounter(base.autoincUnsigned, local.autoincUnsigned, latest.autoincUnsigned),
		rowIDs:          mergeCounter(base.rowIDs, local.rowIDs, latest.rowIDs),
		userVersion:     version,
		collation:       collation,
		nextObjectID:    max(local.nextObjectID, latest.nextObjectID),
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
	if !reflect.DeepEqual(s.namespaces, s.txState.namespaces) {
		return true
	}
	if s.hasTemporaryTables() {
		state := s.captureState()
		return s.nextObjectID != s.txState.nextObjectID || s.collation != s.txState.collation || !reflect.DeepEqual(state.tables, s.txState.tables) || !reflect.DeepEqual(state.indexes, s.txState.indexes) || !reflect.DeepEqual(s.views, s.txState.views) || !reflect.DeepEqual(s.triggers, s.txState.triggers)
	}
	return s.nextObjectID != s.txState.nextObjectID || s.collation != s.txState.collation || !reflect.DeepEqual(s.tables, s.txState.tables) ||
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
			if s.temporaryKey(w.key) {
				continue
			}
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
	if s.parent != nil && s.lockHandler != nil && !s.catalogChanged() {
		if !s.txHasWrites && len(s.txWrites) > 0 {
			s.txCatalogVersion = s.readCatalogVersion
			s.txWriteVersion = s.readVersion
			s.txHasWrites = true
		}
		if s.rowIDDirty {
			if s.hasTemporaryTables() {
				s.parent.reserveRowIDs(s.captureState().rowIDs)
			} else {
				s.parent.reserveRowIDs(s.rowIDCount)
			}
			s.rowIDDirty = false
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
	if s.hasTemporaryTables() {
		root.reserveRowIDs(s.captureState().rowIDs)
	} else {
		root.reserveRowIDs(s.rowIDCount)
	}
	s.rowIDDirty = false
	return nil
}

// The caller holds the catalog lock or the database's logical catalog lock.
// Reservations are monotonic and survive transaction rollback in this process.
func (s *Schema) reserveRowIDs(counters map[string]int64) {
	s.rowIDMu.Lock()
	defer s.rowIDMu.Unlock()
	if s.rowIDReservations == nil {
		s.rowIDReservations = make(map[string]int64)
	}
	for table, next := range counters {
		if next < 0 || s.rowIDReservations[table] >= 0 && next > s.rowIDReservations[table] {
			s.rowIDReservations[table] = next
		}
	}
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
		if bytes.HasPrefix(w.key, []byte(dataPrefix)) && !s.temporaryKey(w.key) {
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
			row := decodeRow(w.value, info)
			for _, idx := range s.tableIndexes(table) {
				if idx.IsUnique && idx.hasPrefixes() {
					if err := s.checkPrefixUnique(idx, row, w.key); err != nil {
						return fmt.Errorf("%w: %v", ErrConflict, err)
					}
				}
			}
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
	s.releaseConsistentSnapshot()
	s.consistentIndexDeletes = nil
	s.txIsolation = ""
	s.rowIDDirty = false
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
