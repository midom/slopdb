package sql

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"sync"

	"slopdb/storage"
)

const onlineIndexBatchRows = 256
const onlineIndexBatchBytes = 1 << 20

// ErrOnlineIndexCatchUp requests another shared-lock catch-up pass instead of
// doing an unbounded amount of reconciliation behind the publication lock.
var ErrOnlineIndexCatchUp = errors.New("online index needs another catch-up pass")

// OnlineIndexBuild keeps unpublished entries under a permanently reserved ID.
// Row mappings and UNIQUE counts spill through a private temporary LSM. The
// committed-change log is on disk, so build memory does not grow with the table
// or with the number of concurrent writes.
type OnlineIndexBuild struct {
	root           *Schema
	idx            *IndexInfo
	info           *TableInfo
	disk           *diskHash
	snapshot       uint64
	snapshotPinned bool
	duplicates     int64
	mu             sync.Mutex
	log            *os.File
	logCount       int64
	logErr         error
	closed         bool
}

// writeBatchWithOnlineIndexes orders change capture with snapshot checkpoints.
// Publishers hold a shared lock through storage publication and log append;
// registration/checkpoints take its exclusive side. A committed row mutation
// therefore cannot fall between a checkpoint and its corresponding log cut.
func (s *Schema) writeBatchWithOnlineIndexes(batch []storage.Mutation) error {
	return s.writeBatchWithOnlineIndexesQueued(batch, nil)
}

func (s *Schema) writeBatchWithOnlineIndexesQueued(batch []storage.Mutation, queued func()) error {
	batch = s.prepareTemporaryBatch(batch)
	root := s
	if root.parent != nil {
		root = root.parent
	}
	root.onlineIndexMu.RLockForReader(s.snapshotReader)
	defer root.onlineIndexMu.RUnlockForReader(s.snapshotReader)
	if err := s.lsm.WriteBatchWithEnqueue(batch, queued); err != nil {
		return err
	}
	for b := range root.onlineIndexBuilds {
		b.mu.Lock()
		for _, w := range batch {
			if !bytes.HasPrefix(w.Key, []byte(b.info.storagePrefix(b.info.Name))) {
				continue
			}
			b.logCount++
			if b.logErr != nil {
				continue
			}
			if uint64(len(w.Key)) > uint64(^uint32(0)) {
				b.logErr = fmt.Errorf("online index row key exceeds change-log limit")
				continue
			}
			var header [4]byte
			binary.LittleEndian.PutUint32(header[:], uint32(len(w.Key)))
			if _, err := b.log.Write(header[:]); err != nil {
				b.logErr = err
				continue
			}
			if _, err := b.log.Write(w.Key); err != nil {
				b.logErr = err
			}
		}
		b.mu.Unlock()
	}
	return nil
}

// OnlineCreateIndex recognizes standalone ALTER ADD INDEX too. Transactional
// and multi-action changes use the ordinary atomic DDL path.
func (s *Schema) OnlineCreateIndex(stmt Statement) (*CreateIndexStmt, bool, error) {
	switch a := stmt.(type) {
	case *CreateIndexStmt:
		if s.isTemporaryTable(a.Table) {
			return nil, false, nil
		}
	case *AlterTableStmt:
		if s.isTemporaryTable(a.Table) {
			return nil, false, nil
		}
	}
	if s.inTx {
		return nil, false, nil
	}
	switch a := stmt.(type) {
	case *CreateIndexStmt:
		return a, true, nil
	case *AlterTableStmt:
		if a.Operation != "ADD_INDEX" || (a.ObjectKind != "" && a.ObjectKind != "TABLE") {
			return nil, false, nil
		}
		info := s.tables[a.Table]
		if info == nil {
			if a.TableIfExists {
				return nil, true, nil
			}
			return nil, true, fmt.Errorf("table %q does not exist", a.Table)
		}
		c := a.Constraint
		name := c.Name
		if name == "" {
			name = info.Name + "_" + strings.Join(c.Columns, "_") + "_idx"
		}
		return &CreateIndexStmt{Table: info.Name, Index: name, Columns: c.Columns, PrefixLengths: c.PrefixLengths, Unique: c.Kind == "UNIQUE_INDEX", Fulltext: c.Fulltext, IfNotExists: a.IfNotExists, PublicationAbortAfter: a.PublicationAbortAfter}, true, nil
	}
	return nil, false, nil
}

func (s *Schema) StartOnlineIndex(stmt *CreateIndexStmt) (*OnlineIndexBuild, error) {
	name, table := strings.ToLower(stmt.Index), strings.ToLower(stmt.Table)
	name, displayName := s.indexIdentity(table, name, "")
	if _, exists := s.indexes[name]; exists || (s.MySQLMode() && s.findTableIndex(table, displayName) != nil) {
		if stmt.IfNotExists {
			return nil, nil
		}
		return nil, fmt.Errorf("index %q already exists", name)
	}
	info := s.tables[table]
	if info == nil {
		return nil, fmt.Errorf("table %q does not exist", table)
	}
	columns := normalizedIndexColumns(stmt.Columns, stmt.Column)
	if err := validateIndexColumns(info.Columns, columns); err != nil {
		return nil, err
	}
	disk, err := newDiskHash(s)
	if err != nil {
		return nil, err
	}
	log, err := os.CreateTemp(disk.dir, "changes-*")
	if err != nil {
		disk.close()
		return nil, err
	}
	root := s
	if root.parent != nil {
		root = root.parent
	}
	idx := newIndexInfo(name, table, columns, stmt.Unique)
	idx.DisplayName = displayName
	idx.Fulltext = stmt.Fulltext
	if idx.Fulltext {
		idx.FulltextVersion = fulltextIndexVersion
	}
	if err := validateFulltextIndex(info, idx); err != nil {
		log.Close()
		disk.close()
		return nil, err
	}
	idx.PrefixLengths = append([]int64(nil), stmt.PrefixLengths...)
	if idx.hasPrefixes() {
		idx.PrefixEncoding = 1
	}
	if err := validateIndexPrefixes(info.Columns, idx); err != nil {
		log.Close()
		disk.close()
		return nil, err
	}
	s.bindIndexCollation(idx)
	b := &OnlineIndexBuild{root: root, idx: idx, info: info, disk: disk, log: log}
	// Reserve the ID and a cleanup marker durably before any invisible entries
	// are written. Reservation survives cancellation/crash and is never reused.
	_, err = s.reserveOnlineIndexID(b)
	if err != nil {
		log.Close()
		disk.close()
		return nil, err
	}
	return b, nil
}

func (b *OnlineIndexBuild) Close() error {
	if b == nil || b.closed {
		return nil
	}
	b.closed = true
	b.root.onlineIndexMu.Lock()
	delete(b.root.onlineIndexBuilds, b)
	b.root.onlineIndexMu.Unlock()
	if b.snapshotPinned {
		b.root.lsm.ReleaseSnapshot(b.snapshot)
		b.snapshotPinned = false
	}
	_ = b.log.Close()
	b.disk.close()
	return b.root.CleanupIndexGarbage()
}

func (s *Schema) changeOnlineUniqueCount(b *OnlineIndexBuild, row map[string]Value, delta int64) error {
	if !b.idx.IsUnique {
		return nil
	}
	values, hasNull := indexValues(b.idx.columnNames(), row)
	if hasNull {
		return nil
	}
	signature := []byte{3}
	for i, value := range values {
		signature = appendValueIdentity(signature, b.idx.keyValue(b.idx.columnNames()[i], value))
	}
	raw, found, err := b.disk.index.Get(signature)
	if err != nil {
		return err
	}
	var before int64
	if found {
		if len(raw) != 8 {
			return fmt.Errorf("invalid temporary online index count")
		}
		before = int64(binary.LittleEndian.Uint64(raw))
	}
	after := before + delta
	if after < 0 {
		return fmt.Errorf("invalid temporary online index count")
	}
	if before > 1 {
		b.duplicates--
	}
	if after > 1 {
		b.duplicates++
	}
	if after == 0 {
		return b.disk.index.Delete(signature)
	}
	return b.disk.index.Put(signature, binary.LittleEndian.AppendUint64(nil, uint64(after)))
}

func (s *Schema) stageOnlineIndexRow(b *OnlineIndexBuild, key, data []byte, found bool) ([]storage.Mutation, error) {
	old, hadOld, err := b.disk.index.Get(key)
	if err != nil {
		return nil, err
	}
	if hadOld == found && (!found || bytes.Equal(old, data)) {
		return nil, nil
	}
	var batch []storage.Mutation
	if hadOld {
		row := decodeRow(old, b.info)
		if err = s.changeOnlineUniqueCount(b, row, -1); err != nil {
			return nil, err
		}
		mutations, e := s.indexRowMutations(b.idx, row, key, true)
		if e != nil {
			return nil, e
		}
		batch = append(batch, mutations...)
	}
	if found {
		row := decodeRow(data, b.info)
		if err = validateIndexRow(b.info, b.idx.columnNames(), row); err != nil {
			return nil, err
		}
		if err = s.changeOnlineUniqueCount(b, row, 1); err != nil {
			return nil, err
		}
		mutations, e := s.indexRowMutations(b.idx, row, key, false)
		if e != nil {
			return nil, e
		}
		batch = append(batch, mutations...)
		err = b.disk.index.Put(key, data)
	} else {
		err = b.disk.index.Delete(key)
	}
	return batch, err
}

func (s *Schema) ScanOnlineIndex(b *OnlineIndexBuild) error {
	prefix := []byte(b.info.storagePrefix(b.info.Name))
	cursor := prefix
	for {
		if err := s.checkInterrupt(); err != nil {
			return err
		}
		iter := s.lsm.NewIteratorAtSeek(b.snapshot, cursor)
		var rows []storage.Mutation
		size := 0
		for iter.Valid() && bytes.HasPrefix(iter.Key().UserKey, prefix) {
			key, value := copyBytes(iter.Key().UserKey), copyBytes(iter.Value())
			rows = append(rows, storage.Mutation{Key: key, Value: value})
			size += len(key) + len(value)
			iter.Next()
			if len(rows) >= onlineIndexBatchRows || size >= onlineIndexBatchBytes {
				break
			}
		}
		var next []byte
		if iter.Valid() && bytes.HasPrefix(iter.Key().UserKey, prefix) {
			next = copyBytes(iter.Key().UserKey)
		}
		err := storage.IteratorError(iter)
		iter.Close() // Never publish storage writes while retaining an iterator.
		if err != nil {
			return err
		}
		batch := make([]storage.Mutation, 0, len(rows))
		for _, row := range rows {
			if err := s.checkInterrupt(); err != nil {
				return err
			}
			mutations, err := s.stageOnlineIndexRow(b, row.Key, row.Value, true)
			if err != nil {
				return err
			}
			batch = append(batch, mutations...)
		}
		if err := s.lsm.WriteBatch(batch); err != nil {
			return err
		}
		if next == nil {
			break
		}
		cursor = next
	}
	s.lsm.ReleaseSnapshot(b.snapshot)
	b.snapshotPinned = false
	return nil
}

// CatchUpOnlineIndex replays one disk-log segment against a coherent snapshot.
// It may run under a shared catalog lock. Repeated row keys are idempotent
// because the temporary row mapping records each entry's actual staged value.
func (s *Schema) CatchUpOnlineIndex(b *OnlineIndexBuild) error {
	next, err := os.CreateTemp(b.disk.dir, "changes-*")
	if err != nil {
		return err
	}
	b.root.onlineIndexMu.Lock()
	b.mu.Lock()
	old, count, captureErr := b.log, b.logCount, b.logErr
	b.log, b.logCount = next, 0
	snapshot := s.lsm.Snapshot()
	b.mu.Unlock()
	b.root.onlineIndexMu.Unlock()
	defer s.lsm.ReleaseSnapshot(snapshot)
	defer func() { old.Close(); os.Remove(old.Name()) }()
	if captureErr != nil {
		return fmt.Errorf("online index change capture: %w", captureErr)
	}
	if _, err = old.Seek(0, io.SeekStart); err != nil {
		return err
	}
	reader := bufio.NewReader(old)
	var batch []storage.Mutation
	size := 0
	for i := int64(0); i < count; i++ {
		if err := s.checkInterrupt(); err != nil {
			return err
		}
		var header [4]byte
		if _, err := io.ReadFull(reader, header[:]); err != nil {
			return err
		}
		key := make([]byte, int(binary.LittleEndian.Uint32(header[:])))
		if _, err := io.ReadFull(reader, key); err != nil {
			return err
		}
		data, found, err := s.lsm.GetAt(key, snapshot)
		if err != nil {
			return err
		}
		mutations, err := s.stageOnlineIndexRow(b, key, data, found)
		if err != nil {
			return err
		}
		for _, mutation := range mutations {
			size += len(mutation.Key) + len(mutation.Value)
		}
		batch = append(batch, mutations...)
		if len(batch) >= onlineIndexBatchRows || size >= onlineIndexBatchBytes {
			if err = s.lsm.WriteBatch(batch); err != nil {
				return err
			}
			batch, size = nil, 0
		}
	}
	return s.lsm.WriteBatch(batch)
}

// FinishOnlineIndex must run under the exclusive catalog lock, after importing
// current metadata. Only a bounded final change segment and catalog mutation
// occur there; the table scan and entry installation have already completed.
func (s *Schema) FinishOnlineIndex(stmt *CreateIndexStmt, b *OnlineIndexBuild) error {
	if current := s.tables[b.idx.Table]; current == nil || !reflect.DeepEqual(current, b.info) {
		return fmt.Errorf("%w: table changed during online index build", ErrConflict)
	}
	test := *b.idx
	s.bindIndexCollation(&test)
	if !reflect.DeepEqual(test.collation, b.idx.collation) || !reflect.DeepEqual(test.binaryColumns, b.idx.binaryColumns) {
		return fmt.Errorf("%w: collation changed during online index build", ErrConflict)
	}
	if _, exists := s.indexes[b.idx.Name]; exists {
		if stmt.IfNotExists {
			return nil
		}
		return fmt.Errorf("index %q already exists", b.idx.Name)
	}
	b.mu.Lock()
	count := b.logCount
	b.mu.Unlock()
	if count > onlineIndexBatchRows {
		return ErrOnlineIndexCatchUp
	}
	if err := s.CatchUpOnlineIndex(b); err != nil {
		return err
	}
	if b.duplicates > 0 {
		return fmt.Errorf("UNIQUE constraint failed: %s.%s", b.idx.Table, strings.Join(b.idx.columnNames(), ", "))
	}
	s.ensureCatalogWritable()
	s.indexes[b.idx.Name] = b.idx
	if err := s.remove(indexCleanupKey(b.idx.ID)); err != nil {
		return err
	}
	return s.saveIndexes()
}
