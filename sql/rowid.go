package sql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"slopdb/storage"
)

func (s *Schema) nextRowID(table string) (id int64, err error) {
	counter := s.rowCounterKey(table)
	root := s
	if s.parent != nil {
		root = s.parent
	}
	root.commitMu.Lock()
	defer root.commitMu.Unlock()
	defer func() {
		if err != nil {
			return
		}
		if root.rowIDReservations == nil {
			root.rowIDReservations = make(map[string]int64)
		}
		if id+1 > root.rowIDReservations[counter] {
			root.rowIDReservations[counter] = id + 1
		}
	}()
	if next := root.rowIDReservations[counter]; next > s.rowIDCount[counter] {
		s.rowIDCount[counter] = next
	}
	if next, ok := s.rowIDCount[counter]; ok {
		s.rowIDCount[counter] = next + 1
		return next, nil
	}
	maxID, err := s.lastRowID(table, s.tables[table])
	if err != nil {
		return 0, err
	}
	id = maxID + 1
	s.rowIDCount[counter] = id + 1
	return id, nil
}

// Clustered keys and hidden row IDs are ordered. Initialize a table's
// counter lazily with a reverse bounded seek; opening never seeds all tables.
func (s *Schema) lastRowID(table string, info *TableInfo) (int64, error) {
	if err := s.checkInterrupt(); err != nil {
		return 0, err
	}
	prefix := []byte(s.tableDataPrefix(table))
	var maxID int64
	if info == nil || len(info.PKColumns) == 0 {
		iter := s.newRangeIterator(prefix, prefixSuccessor(prefix), true)
		defer iter.Close()
		if iter.Valid() {
			id, err := strconv.ParseInt(string(iter.Key().UserKey[len(prefix):]), 10, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid row ID for table %s: %w", table, err)
			}
			maxID = max(maxID, id)
		}
		return maxID, storage.IteratorError(iter)
	}
	if col := s.getPKColumn(info); len(info.PKColumns) == 1 && col != nil && col.ColType == ColInteger {
		low := append(append([]byte(nil), prefix...), 1)
		high := append(append([]byte(nil), prefix...), 2)
		iter := s.newRangeIterator(low, high, true)
		if iter.Valid() {
			key := iter.Key().UserKey[len(prefix):]
			if len(key) != 9 || key[0] != 1 {
				iter.Close()
				return 0, fmt.Errorf("invalid clustered integer key for table %s", table)
			}
			maxID = max(maxID, int64(binary.BigEndian.Uint64(key[1:])^(uint64(1)<<63)))
		}
		err := storage.IteratorError(iter)
		iter.Close()
		if err != nil {
			return 0, err
		}
	}
	// Nullable noninteger/composite PKs use a unique, zero-padded fallback ID.
	// Their ordinary primary values do not participate in row-ID allocation.
	nullPrefix := append(append([]byte(nil), prefix...), []byte("\xffnull:")...)
	iter := s.newRangeIterator(nullPrefix, prefixSuccessor(nullPrefix), true)
	defer iter.Close()
	if iter.Valid() {
		key := iter.Key().UserKey
		if !bytes.HasPrefix(key, nullPrefix) {
			return 0, fmt.Errorf("invalid nullable primary key for table %s", table)
		}
		id, err := strconv.ParseInt(strings.TrimPrefix(string(key[len(prefix):]), "\xffnull:"), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid nullable row ID for table %s: %w", table, err)
		}
		maxID = max(maxID, id)
	}
	return maxID, storage.IteratorError(iter)
}
