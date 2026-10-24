package sql

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"

	"slopdb/storage"
)

const objectIDPrefix = "\x00__object_ids__"
const objectMigrationPrefix = "\x00__object_id_migration__"

// An invalid UTF-8 marker distinguishes IDs from every legacy catalog name.
func objectPrefix(namespace byte, id uint64) string {
	var b [11]byte
	b[0], b[1], b[10] = namespace, 0xff, ':'
	binary.BigEndian.PutUint64(b[2:10], id)
	return string(b[:])
}

func (t *TableInfo) storagePrefix(name string) string {
	if t.ID != 0 {
		return objectPrefix(1, t.ID)
	}
	return tableDataPrefix(name)
}
func (i *IndexInfo) storagePrefix() string {
	if i.ID != 0 {
		return objectPrefix(2, i.ID)
	}
	return indexDataPrefix(i.Name)
}
func (s *Schema) tableDataPrefix(name string) string {
	if t := s.tables[name]; t != nil {
		return t.storagePrefix(name)
	}
	return tableDataPrefix(name)
}
func (s *Schema) indexDataPrefix(name string) string {
	if i := s.indexes[name]; i != nil {
		return i.storagePrefix()
	}
	return indexDataPrefix(name)
}

func (s *Schema) allocateObjectID() (uint64, error) {
	if s.nextObjectID == math.MaxUint64 {
		return 0, fmt.Errorf("storage object IDs exhausted")
	}
	s.nextObjectID++
	if !s.inTx {
		if err := s.write([]byte(objectIDPrefix), binary.BigEndian.AppendUint64(nil, s.nextObjectID)); err != nil {
			return 0, err
		}
	}
	return s.nextObjectID, nil
}

type objectMigration struct {
	Source    [32]byte          `json:"source"`
	Tables    map[string]uint64 `json:"tables"`
	Indexes   map[string]uint64 `json:"indexes"`
	Published bool              `json:"published"`
}

func (s *Schema) objectCatalogHash() ([32]byte, error) {
	b, err := json.Marshal([]any{s.tables, s.indexes})
	return sha256.Sum256(b), err
}

// Legacy names are copied in bounded batches before publishing the ID catalog.
// Originals remain intact throughout copying, so a crash simply resumes it.
func (s *Schema) initializeObjectIDs() error {
	b, found, err := s.lsm.Get([]byte(objectIDPrefix))
	if err != nil {
		return err
	}
	if found {
		if len(b) != 8 {
			return fmt.Errorf("invalid storage object ID counter")
		}
		s.nextObjectID = binary.BigEndian.Uint64(b)
	}
	seen := make(map[uint64]bool)
	legacy := false
	for _, t := range s.tables {
		legacy = legacy || t.ID == 0
		if t.ID != 0 {
			if seen[t.ID] || t.ID > s.nextObjectID {
				return fmt.Errorf("invalid table storage ID %d", t.ID)
			}
			seen[t.ID] = true
		}
	}
	for _, i := range s.indexes {
		legacy = legacy || i.ID == 0
		if i.ID != 0 {
			if seen[i.ID] || i.ID > s.nextObjectID {
				return fmt.Errorf("invalid index storage ID %d", i.ID)
			}
			seen[i.ID] = true
		}
	}
	var plan objectMigration
	b, pending, err := s.lsm.Get([]byte(objectMigrationPrefix))
	if err != nil {
		return err
	}
	if s.lsm.ReplicaMode() {
		if legacy || pending {
			return fmt.Errorf("replica requires an upgraded object-ID catalog")
		}
		return nil
	}
	if pending {
		if err = json.Unmarshal(b, &plan); err != nil {
			return err
		}
	} else if legacy {
		if len(seen) > 0 {
			return fmt.Errorf("mixed legacy and ID-based storage catalog")
		}
		plan.Source, err = s.objectCatalogHash()
		if err != nil {
			return err
		}
		plan.Tables, plan.Indexes = make(map[string]uint64), make(map[string]uint64)
		names := make([]string, 0, len(s.tables))
		for name := range s.tables {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if s.tables[name].ID == 0 {
				id, e := s.allocateObjectID()
				if e != nil {
					return e
				}
				plan.Tables[name] = id
			}
		}
		names = nil
		for name := range s.indexes {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if s.indexes[name].ID == 0 {
				id, e := s.allocateObjectID()
				if e != nil {
					return e
				}
				plan.Indexes[name] = id
			}
		}
		b, err = json.Marshal(plan)
		if err != nil {
			return err
		}
		if err = s.lsm.Put([]byte(objectMigrationPrefix), b); err != nil {
			return err
		}
	} else {
		return nil
	}
	if !plan.Published {
		hash, e := s.objectCatalogHash()
		if e != nil {
			return e
		}
		if hash != plan.Source {
			return fmt.Errorf("storage ID migration source catalog changed")
		}
		reserved := make(map[uint64]bool)
		for name, t := range s.tables {
			if t.ID == 0 && plan.Tables[name] == 0 {
				return fmt.Errorf("migration missing table %s", name)
			}
		}
		for name, i := range s.indexes {
			if i.ID == 0 && plan.Indexes[name] == 0 {
				return fmt.Errorf("migration missing index %s", name)
			}
		}
		for name, id := range plan.Tables {
			if s.tables[name] == nil || s.tables[name].ID != 0 || id == 0 || id > s.nextObjectID || reserved[id] || seen[id] {
				return fmt.Errorf("invalid migration table %s", name)
			}
			reserved[id] = true
		}
		for name, id := range plan.Indexes {
			if s.indexes[name] == nil || s.indexes[name].ID != 0 || id == 0 || id > s.nextObjectID || reserved[id] || seen[id] {
				return fmt.Errorf("invalid migration index %s", name)
			}
			reserved[id] = true
		}
		// A resumed copy may have stale index entries if legacy rows were repaired
		// between attempts. Only reserved, unpublished destinations are cleared.
		for _, id := range plan.Tables {
			p := []byte(objectPrefix(1, id))
			if err = s.lsm.DeleteRange(p, prefixSuccessor(p)); err != nil {
				return err
			}
		}
		for _, id := range plan.Indexes {
			p := []byte(objectPrefix(2, id))
			if err = s.lsm.DeleteRange(p, prefixSuccessor(p)); err != nil {
				return err
			}
		}
		for name, info := range s.tables {
			original := info.storagePrefix(name)
			if id := plan.Tables[name]; id != 0 {
				info.ID = id
			}
			indexes := s.tableIndexes(name)
			for _, idx := range indexes {
				if id := plan.Indexes[idx.Name]; id != 0 {
					idx.ID = id
				}
			}
			if _, moved := plan.Tables[name]; !moved {
				needed := false
				for _, idx := range indexes {
					if _, ok := plan.Indexes[idx.Name]; ok {
						needed = true
					}
				}
				if !needed {
					continue
				}
			}
			var resume []byte
			for {
				it := s.lsm.NewIteratorSeek([]byte(original))
				if resume != nil {
					it.Seek(resume)
					if it.Valid() && bytes.Equal(it.Key().UserKey, resume) {
						it.Next()
					}
				}
				batch := make([]storage.Mutation, 0, 256)
				size := 0
				for it.Valid() && bytes.HasPrefix(it.Key().UserKey, []byte(original)) && size < 1<<20 {
					key := append([]byte(info.storagePrefix(name)), it.Key().UserKey[len(original):]...)
					value := append([]byte(nil), it.Value()...)
					batch = append(batch, storage.Mutation{Key: key, Value: value})
					size += len(key) + len(value)
					row := decodeRow(value, info)
					for _, idx := range indexes {
						if s.isClusteredIndex(idx) {
							continue
						}
						k := encodeRowIndexKey(idx, row, key)
						v := s.encodeIndexValue(idx, row)
						batch = append(batch, storage.Mutation{Key: k, Value: v})
						size += len(k) + len(v)
					}
					resume = append(resume[:0], it.Key().UserKey...)
					it.Next()
				}
				e = storage.IteratorError(it)
				it.Close()
				if e != nil {
					return e
				}
				if len(batch) == 0 {
					break
				}
				if e = s.lsm.WriteBatch(batch); e != nil {
					return e
				}
			}
		}
		for name, id := range plan.Indexes {
			s.indexes[name].ID = id
		}
		s.catalogLookup = nil
		// Durable copies must precede the catalog switch even with WAL disabled.
		if err = s.lsm.Flush(); err != nil {
			return err
		}
		plan.Published = true
		tables, e := json.Marshal(s.tables)
		if e != nil {
			return e
		}
		indexes, e := json.Marshal(s.indexes)
		if e != nil {
			return e
		}
		b, e = json.Marshal(plan)
		if e != nil {
			return e
		}
		if err = s.lsm.WriteBatch([]storage.Mutation{{Key: []byte(schemaPrefix), Value: tables}, {Key: []byte(schemaIdxPrefix), Value: indexes}, {Key: []byte(objectMigrationPrefix), Value: b}}); err != nil {
			return err
		}
		if err = s.lsm.Flush(); err != nil {
			return err
		}
	}
	for name, id := range plan.Tables {
		if s.tables[name] == nil || s.tables[name].ID != id {
			return fmt.Errorf("migration table ID mismatch")
		}
		prefix := []byte(tableDataPrefix(name))
		if err = s.lsm.DeleteRange(prefix, prefixSuccessor(prefix)); err != nil {
			return err
		}
	}
	for name, id := range plan.Indexes {
		if s.indexes[name] == nil || s.indexes[name].ID != id {
			return fmt.Errorf("migration index ID mismatch")
		}
		prefix := []byte(indexDataPrefix(name))
		if err = s.lsm.DeleteRange(prefix, prefixSuccessor(prefix)); err != nil {
			return err
		}
	}
	if err = s.lsm.Flush(); err != nil {
		return err
	}
	return s.lsm.Delete([]byte(objectMigrationPrefix))
}

func (s *Schema) rowCounterKey(table string) string {
	if info := s.tables[table]; info != nil && info.ID != 0 {
		return "#" + strconv.FormatUint(info.ID, 10)
	}
	return table
}
