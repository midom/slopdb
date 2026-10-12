package sql

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"

	"slopdb/storage"
)

const indexCleanupPrefix = "\x00__index_cleanup__:"

type indexCleanupRecord struct {
	Prefix []byte `json:"prefix"`
}

func indexCleanupKey(id uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte(indexCleanupPrefix), id)
}

func (s *Schema) markIndexCleanup(idx *IndexInfo) error {
	if idx.ID == 0 {
		return fmt.Errorf("index cleanup requires a storage object ID")
	}
	data, err := json.Marshal(indexCleanupRecord{Prefix: []byte(idx.storagePrefix())})
	if err != nil {
		return err
	}
	return s.write(indexCleanupKey(idx.ID), data)
}

func (s *Schema) reserveOnlineIndexID(b *OnlineIndexBuild) (uint64, error) {
	root := b.root
	root.commitMu.Lock()
	defer root.commitMu.Unlock()
	root.onlineIndexMu.Lock()
	defer root.onlineIndexMu.Unlock()
	if root.nextObjectID == math.MaxUint64 {
		return 0, fmt.Errorf("storage object IDs exhausted")
	}
	id := root.nextObjectID + 1
	data, err := json.Marshal(indexCleanupRecord{Prefix: []byte(objectPrefix(2, id))})
	if err != nil {
		return 0, err
	}
	if err = root.lsm.WriteBatch([]storage.Mutation{
		{Key: []byte(objectIDPrefix), Value: binary.BigEndian.AppendUint64(nil, id)},
		{Key: indexCleanupKey(id), Value: data},
	}); err != nil {
		return 0, err
	}
	root.nextObjectID = id
	root.stateVersion++
	root.commitVersion.Add(1)
	b.idx.ID = id
	if root.onlineIndexBuilds == nil {
		root.onlineIndexBuilds = make(map[*OnlineIndexBuild]struct{})
	}
	root.onlineIndexBuilds[b] = struct{}{}
	b.snapshot = s.lsm.Snapshot()
	b.snapshotPinned = true
	return id, nil
}

// CleanupIndexGarbage reclaims unpublished or dropped index prefixes using
// range tombstones. It visits only cleanup metadata; index entries and table
// rows are never scanned. MVCC keeps older statement snapshots readable, and
// compaction eventually removes the hidden physical entries. Cleanup markers
// remain durable until the range deletion succeeds, so startup safely retries
// builds interrupted before publication and drops interrupted after commit.
func (s *Schema) CleanupIndexGarbage() error {
	root := s
	if root.parent != nil {
		root = root.parent
	}
	root.commitMu.Lock()
	defer root.commitMu.Unlock()
	root.onlineIndexMu.RLock()
	defer root.onlineIndexMu.RUnlock()
	active := make(map[uint64]bool)
	for b := range root.onlineIndexBuilds {
		active[b.idx.ID] = true
	}
	live := make(map[uint64]bool)
	for _, idx := range root.indexes {
		live[idx.ID] = true
	}
	prefix := []byte(indexCleanupPrefix)
	cursor := prefix
	for {
		iter := root.lsm.NewIteratorSeek(cursor)
		var records []storage.Mutation
		visited := 0
		for iter.Valid() && bytes.HasPrefix(iter.Key().UserKey, prefix) {
			key := iter.Key().UserKey
			if len(key) != len(prefix)+8 {
				iter.Close()
				return fmt.Errorf("invalid index cleanup key")
			}
			id := binary.BigEndian.Uint64(key[len(prefix):])
			if !active[id] {
				records = append(records, storage.Mutation{Key: copyBytes(key), Value: copyBytes(iter.Value())})
			}
			visited++
			iter.Next()
			if visited >= onlineIndexBatchRows {
				break
			}
		}
		var next []byte
		if iter.Valid() && bytes.HasPrefix(iter.Key().UserKey, prefix) {
			next = copyBytes(iter.Key().UserKey)
		}
		err := storage.IteratorError(iter)
		iter.Close()
		if err != nil {
			return err
		}
		for _, record := range records {
			id := binary.BigEndian.Uint64(record.Key[len(prefix):])
			var cleanup indexCleanupRecord
			if err := json.Unmarshal(record.Value, &cleanup); err != nil {
				return fmt.Errorf("index cleanup metadata: %w", err)
			}
			expected := []byte(objectPrefix(2, id))
			if !bytes.Equal(cleanup.Prefix, expected) {
				return fmt.Errorf("invalid index cleanup prefix")
			}
			if !live[id] {
				if err = root.lsm.DeleteRange(cleanup.Prefix, prefixSuccessor(cleanup.Prefix)); err != nil {
					return err
				}
			}
			if err = root.lsm.Delete(record.Key); err != nil {
				return err
			}
		}
		if next == nil {
			return nil
		}
		cursor = next
	}
}
