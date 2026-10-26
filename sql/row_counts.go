package sql

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"

	"slopdb/storage"
)

const rowCountPrefix = "\x00__row_counts_v1__"
const rowCountShards = 64

type rowCountBuild struct {
	delta     int64
	cancelled bool
	err       error
}
type rowCountWriteScratch struct {
	final map[string]int
	delta map[uint64]int64
}
type rowCountScratch struct {
	id     uint64
	keys   [rowCountShards + 1][]byte
	values []storage.PointValue
}

func rowCountKey(id uint64, shard byte) []byte {
	k := binary.BigEndian.AppendUint64([]byte(rowCountPrefix), id)
	return append(k, shard)
}
func rowTableID(key []byte) (uint64, bool) {
	if len(key) < 11 || key[0] != 1 || key[1] != 0xff || key[10] != ':' {
		return 0, false
	}
	return binary.BigEndian.Uint64(key[2:10]), true
}
func (s *Schema) rowCountRoot() *Schema {
	if s.parent != nil {
		return s.parent
	}
	return s
}
func (s *Schema) rowCountSnapshot() uint64 {
	if s.statementActive {
		return s.statementSnapshot
	}
	return math.MaxUint64
}
func readRowCount(v []byte) (int64, error) {
	n, err := strconv.ParseInt(string(v), 10, 64)
	if err != nil || len(v) == 0 || v[0] == '+' || (len(v) > 1 && v[0] == '0') || (len(v) > 1 && v[0] == '-' && v[1] == '0') {
		return 0, fmt.Errorf("invalid persisted row counter")
	}
	return n, nil
}
func counterMutation(id uint64, shard byte, n int64) storage.Mutation {
	return storage.Mutation{Key: rowCountKey(id, shard), Value: strconv.AppendInt(nil, n, 10)}
}

// prepareRowCounts runs under the online-publication gate. A writer's shard
// mutex stays held until its data and absolute shard value publish together.
// Only the final mutation of a key contributes, including savepoint rewinds.
func (s *Schema) prepareRowCounts(batch []storage.Mutation) ([]storage.Mutation, func(bool), error) {
	root := s.rowCountRoot()
	shard := byte(s.snapshotReader % rowCountShards)
	unlock := func() {}
	if s.rowCountWrite == nil {
		s.rowCountWrite = &rowCountWriteScratch{final: make(map[string]int), delta: make(map[uint64]int64)}
	}
	scratch := s.rowCountWrite
	if scratch.final == nil {
		scratch.final = make(map[string]int)
	}
	if scratch.delta == nil {
		scratch.delta = make(map[uint64]int64)
	}
	clear(scratch.final)
	clear(scratch.delta)
	delta, final := scratch.delta, scratch.final
	cleanup := func() {
		if len(delta) > 64 {
			scratch.delta = nil
		} else {
			clear(delta)
		}
	}
	prepared := false
	defer func() {
		if !prepared {
			cleanup()
		}
	}()
	defer func() {
		if len(final) > 4096 {
			scratch.final = nil
		}
		clear(final)
	}()

	for i, w := range batch {
		if _, ok := rowTableID(w.Key); ok {
			final[string(w.Key)] = i
		}
	}
	for _, i := range final {
		w := batch[i]
		id, _ := rowTableID(w.Key)
		if w.RangeDelete {
			unlock()
			return nil, nil, fmt.Errorf("row range deletion requires cardinality accounting")
		}
		exists := false
		var err error
		if pos, ok := s.txLatest[string(w.Key)]; s.inTx && ok && s.txWrites[pos].cardinalityKnown {
			exists = s.txWrites[pos].baseExists
		} else {
			exists, err = s.lsm.ExistsAtForReader(w.Key, math.MaxUint64, s.snapshotReader)
		}
		if err != nil {
			unlock()
			return nil, nil, err
		}
		if exists && w.Delete {
			delta[id]--
		} else if !exists && !w.Delete {
			delta[id]++
		}
	}
	newTables := make(map[uint64]bool)
	dropped := make(map[uint64]bool)
	if s.inTx && s.txState != nil && !reflect.DeepEqual(s.tables, s.txState.tables) {
		before := make(map[uint64]bool, len(s.txState.tables))
		after := make(map[uint64]bool, len(s.tables))
		for _, t := range s.txState.tables {
			before[t.ID] = true
		}
		for _, t := range s.tables {
			after[t.ID] = true
			if t.ID != 0 && !before[t.ID] && !s.isTemporaryTable(t.Name) {
				newTables[t.ID] = true
			}
		}
		for id := range before {
			if id != 0 && !after[id] {
				dropped[id] = true
			}
		}
	}
	changed := len(newTables) > 0 || len(dropped) > 0
	for _, d := range delta {
		changed = changed || d != 0
	}
	if !changed {
		cleanup()
		return batch, func(bool) {}, nil
	}
	root.rowCountLocks[shard].Lock()
	unlock = func() { root.rowCountLocks[shard].Unlock() }

	for id := range newTables {
		batch = append(batch, storage.Mutation{Key: rowCountKey(id, 255), Value: []byte("[1,64]")})
		for j := 0; j < rowCountShards; j++ {
			n := int64(0)
			if byte(j) == shard {
				n = delta[id]
			}
			batch = append(batch, counterMutation(id, byte(j), n))
		}
	}
	for id, d := range delta {
		if d == 0 || newTables[id] || dropped[id] {
			continue
		}
		marker, found, err := s.lsm.GetAtForReader(rowCountKey(id, 255), math.MaxUint64, s.snapshotReader)
		if err != nil {
			unlock()
			return nil, nil, err
		}
		if !found {
			continue
		}
		if !bytes.Equal(marker, []byte("[1,64]")) {
			unlock()
			return nil, nil, fmt.Errorf("invalid row counter format")
		}
		value, found, err := s.lsm.GetAtForReader(rowCountKey(id, shard), math.MaxUint64, s.snapshotReader)
		if err != nil || !found {
			unlock()
			if err == nil {
				err = fmt.Errorf("missing row counter shard")
			}
			return nil, nil, err
		}
		n, err := readRowCount(value)
		if err != nil || (d > 0 && n > math.MaxInt64-d) || (d < 0 && n < math.MinInt64-d) {
			unlock()
			if err == nil {
				err = fmt.Errorf("row counter overflow")
			}
			return nil, nil, err
		}
		batch = append(batch, counterMutation(id, shard, n+d))
	}
	for id := range dropped {
		for j := 0; j < rowCountShards; j++ {
			batch = append(batch, storage.Mutation{Key: rowCountKey(id, byte(j)), Delete: true})
		}
		batch = append(batch, storage.Mutation{Key: rowCountKey(id, 255), Delete: true})
	}
	// Build deltas are protected separately: publishers share the outer gate.
	prepared = true
	return batch, func(success bool) {
		if success && len(root.rowCountBuilds) > 0 {
			root.rowCountBuildMu.Lock()
			for id, d := range delta {
				if b := root.rowCountBuilds[id]; b != nil {
					if (d > 0 && b.delta > math.MaxInt64-d) || (d < 0 && b.delta < math.MinInt64-d) {
						b.err = fmt.Errorf("row count backfill delta overflow")
					} else {
						b.delta += d
					}
				}
			}
			for id := range dropped {
				if b := root.rowCountBuilds[id]; b != nil {
					b.cancelled = true
				}
			}
			root.rowCountBuildMu.Unlock()
		}
		unlock()
		cleanup()
	}, nil
}

// exactRowCount reads metadata at precisely the statement snapshot and then
// accounts for this transaction's final private writes at that same snapshot.
func (s *Schema) exactRowCount(info *TableInfo) (int64, bool, error) {
	if info.ID == 0 || s.isTemporaryTable(info.Name) {
		return 0, false, nil
	}
	if err := s.checkInterrupt(); err != nil {
		return 0, true, err
	}
	if s.rowCountRead == nil {
		s.rowCountRead = &rowCountScratch{}
	}
	scratch := s.rowCountRead
	if scratch.id != info.ID {
		for j := 0; j < rowCountShards+1; j++ {
			shard := byte(j)
			if j == rowCountShards {
				shard = 255
			}
			key := append(scratch.keys[j][:0], rowCountPrefix...)
			key = binary.BigEndian.AppendUint64(key, info.ID)
			scratch.keys[j] = append(key, shard)
		}
		scratch.id = info.ID
	}
	values, err := s.lsm.GetMultiAtForReaderInto(scratch.keys[:], s.rowCountSnapshot(), s.snapshotReader, scratch.values)
	scratch.values = values
	if err != nil {
		return 0, true, err
	}
	marker := values[rowCountShards]
	if !marker.Found {
		return 0, false, nil
	}
	if !bytes.Equal(marker.Value, []byte("[1,64]")) {
		return 0, true, fmt.Errorf("invalid row counter format")
	}
	var sum big.Int
	for j := 0; j < rowCountShards; j++ {
		if !values[j].Found {
			return 0, true, fmt.Errorf("missing row counter shard")
		}
		n, e := readRowCount(values[j].Value)
		if e != nil {
			return 0, true, e
		}
		sum.Add(&sum, big.NewInt(n))
	}
	if s.inTx {
		for _, i := range s.txLatest {
			w := s.txWrites[i]
			id, ok := rowTableID(w.key)
			if !ok || id != info.ID {
				continue
			}
			exists, e := s.lsm.ExistsAtForReader(w.key, s.rowCountSnapshot(), s.snapshotReader)
			if e != nil {
				return 0, true, e
			}
			if exists && w.del {
				sum.Sub(&sum, big.NewInt(1))
			} else if !exists && !w.del {
				sum.Add(&sum, big.NewInt(1))
			}
		}
	}
	if !sum.IsInt64() || sum.Sign() < 0 {
		return 0, true, fmt.Errorf("invalid row count total")
	}
	return sum.Int64(), true, nil
}

// ANALYZE upgrades legacy tables online. Registration, publication and the
// change-log cut use the same gate as online indexes; ordinary opens and
// SELECTs never write or scan to initialize counters.
func (s *Schema) ensureRowCount(info *TableInfo) error {
	if info.ID == 0 || s.isTemporaryTable(info.Name) || s.replicaReadOnly || s.lsm.ReplicaMode() {
		return nil
	}
	if s.inTx {
		existed := false
		for _, t := range s.txState.tables {
			if t.ID == info.ID {
				existed = true
				break
			}
		}
		if !existed {
			return nil
		}
	}
	root := s.rowCountRoot()
	root.onlineIndexMu.Lock()
	_, found, err := s.lsm.GetAtForReader(rowCountKey(info.ID, 255), math.MaxUint64, s.snapshotReader)
	if err != nil || found {
		root.onlineIndexMu.Unlock()
		return err
	}
	root.rowCountBuildMu.Lock()
	if root.rowCountBuilds == nil {
		root.rowCountBuilds = make(map[uint64]*rowCountBuild)
	}
	if root.rowCountBuilds[info.ID] != nil {
		root.rowCountBuildMu.Unlock()
		root.onlineIndexMu.Unlock()
		return nil
	}
	b := &rowCountBuild{}
	root.rowCountBuilds[info.ID] = b
	root.rowCountBuildMu.Unlock()
	reader := root.nextSnapshotReader.Add(1)
	snapshot := s.lsm.SnapshotForReader(reader)
	root.onlineIndexMu.Unlock()
	defer s.lsm.ReleaseSnapshotForReader(reader, snapshot)
	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	lower := []byte(info.storagePrefix(info.Name))
	upper := prefixSuccessor(lower)
	n, err := s.lsm.CountRangeAtContext(ctx, snapshot, lower, upper, reader)
	root.onlineIndexMu.Lock()
	defer root.onlineIndexMu.Unlock()
	root.rowCountBuildMu.Lock()
	delete(root.rowCountBuilds, info.ID)
	root.rowCountBuildMu.Unlock()
	if err != nil {
		return err
	}
	if b.cancelled {
		return nil
	}
	if b.err != nil {
		return b.err
	}
	if (b.delta > 0 && n > math.MaxInt64-b.delta) || n+b.delta < 0 {
		return fmt.Errorf("row count backfill overflow")
	}
	n += b.delta
	batch := make([]storage.Mutation, 0, rowCountShards+1)
	batch = append(batch, storage.Mutation{Key: rowCountKey(info.ID, 255), Value: []byte("[1,64]")})
	for j := 0; j < rowCountShards; j++ {
		v := int64(0)
		if j == 0 {
			v = n
		}
		batch = append(batch, counterMutation(info.ID, byte(j), v))
	}
	return s.lsm.WriteBatch(batch)
}

// The common final projection still applies LIMIT/OFFSET and ownership.
func (s *Schema) simpleCount(stmt *SelectStmt, info *TableInfo, cols []selectExprCol) ([]map[string]Value, bool, error) {
	if stmt.Where != nil || len(stmt.Joins) != 0 || len(stmt.GroupBy) != 0 || stmt.Having != nil || len(stmt.OrderBy) != 0 || len(cols) == 0 {
		return nil, false, nil
	}
	for _, c := range cols {
		f, ok := c.expr.(*FuncCallExpr)
		if !ok || !strings.EqualFold(f.Name, "COUNT") || f.Distinct || len(f.Args) != 1 {
			return nil, false, nil
		}
		switch a := f.Args[0].(type) {
		case *ColumnRefExpr:
			if a.Name != "*" {
				return nil, false, nil
			}
		case *LiteralExpr:
			if a.Value.Null {
				return nil, false, nil
			}
		default:
			return nil, false, nil
		}
	}
	n, ok, err := s.exactRowCount(info)
	if err != nil {
		return nil, true, err
	}
	if !ok || s.profile != nil {
		lower := []byte(info.storagePrefix(info.Name))
		upper := prefixSuccessor(lower)
		if s.profile == nil && (!s.inTx || len(s.txLatest) == 0) && !s.isTemporaryTable(info.Name) {
			ctx := s.ctx
			if ctx == nil {
				ctx = context.Background()
			}
			n, err = s.countRangeParallel(ctx, s.rowCountSnapshot(), lower, upper)
		} else {
			n = 0
			it := s.newKeyRangeReader(lower, upper, false, "Scan")
			defer it.Close()
			for it.Valid() {
				if n&255 == 0 {
					if e := s.checkInterrupt(); e != nil {
						return nil, true, e
					}
				}
				n++
				it.Next()
			}
			err = storage.IteratorError(it)
		}
		if err != nil {
			return nil, true, err
		}
	}
	row := make(map[string]Value, len(cols))
	for _, c := range cols {
		row[c.name] = NewIntValue(n)
	}
	return []map[string]Value{row}, true, nil
}
