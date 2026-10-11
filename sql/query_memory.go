package sql

import (
	"bufio"
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"slopdb/storage"
)

const defaultQueryMemory = int64(64 << 20)

type queryMemoryKey struct{}

// WithQueryMemory is used by API execution options; zero selects the default.
func WithQueryMemory(ctx context.Context, bytes int64) context.Context {
	return context.WithValue(ctx, queryMemoryKey{}, bytes)
}
func (s *Schema) memoryBudget() int64 {
	n := s.queryMemoryBytes
	if s.ctx != nil {
		if v, ok := s.ctx.Value(queryMemoryKey{}).(int64); ok {
			n = v
		}
	}
	if n <= 0 {
		n = defaultQueryMemory
	}
	return n
}
func rowMemory(row map[string]Value) int64 {
	n := int64(64)
	for k, v := range row {
		n += int64(128 + len(k) + len(v.Text) + len(v.Blob))
	}
	return n
}

// Spill records own their row values. Seq gives deterministic, stable ties.
type spillRecord struct {
	Row  map[string]Value
	Keys []string
	Key  string
	Seq  int64
}
type spillSorter struct {
	levels map[string]int
	s      *Schema
	less   func(spillRecord, spillRecord) bool
	rows   []spillRecord
	bytes  int64
	paths  []string
	dir    string
	seq    int64
}

func newSpillSorter(s *Schema, less func(spillRecord, spillRecord) bool) *spillSorter {
	return &spillSorter{s: s, less: less, levels: make(map[string]int)}
}
func (r *spillSorter) close() {
	if r.dir != "" {
		os.RemoveAll(r.dir)
	}
	r.rows = nil
	r.paths = nil
}
func (r *spillSorter) add(row map[string]Value, keys []string, key string) error {
	if err := r.s.checkInterrupt(); err != nil {
		return err
	}
	size := rowMemory(row) + int64(len(key)+len(keys)*32)
	if r.bytes+size > r.s.memoryBudget() && len(r.rows) > 0 {
		if err := r.flush(); err != nil {
			return err
		}
	}
	r.rows = append(r.rows, spillRecord{Row: copyRow(row), Keys: keys, Key: key, Seq: r.seq})
	r.seq++
	r.bytes += size
	return nil
}
func (r *spillSorter) create() (*os.File, error) {
	if r.dir == "" {
		d, e := os.MkdirTemp(r.s.queryTempDir, "slopdb-query-*")
		if e != nil {
			return nil, e
		}
		r.dir = d
	}
	return os.CreateTemp(r.dir, "run-*")
}
func (r *spillSorter) flush() error {
	sort.Slice(r.rows, func(i, j int) bool { return r.less(r.rows[i], r.rows[j]) })
	f, e := r.create()
	if e != nil {
		return e
	}
	w := bufio.NewWriterSize(f, 4096)
	enc := gob.NewEncoder(w)
	for _, row := range r.rows {
		if e = r.s.checkInterrupt(); e != nil {
			break
		}
		if e = enc.Encode(row); e != nil {
			break
		}
	}
	if e == nil {
		e = w.Flush()
	}
	if ce := f.Close(); e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	r.recordSpill(f.Name())
	r.paths = append(r.paths, f.Name())
	r.levels[f.Name()] = 0
	clear(r.rows)
	r.rows = nil
	r.bytes = 0
	if len(r.paths) >= 8 {
		return r.compactLevels()
	}
	return nil
}
func (r *spillSorter) recordSpill(path string) {
	if r.s.profile != nil {
		if st, e := os.Stat(path); e == nil {
			r.s.profile.SpillBytes += st.Size()
			r.s.profile.SpillRuns++
		}
	}
}

type mergeItem struct {
	row  spillRecord
	dec  *gob.Decoder
	file *os.File
}
type mergeHeap struct {
	items []*mergeItem
	less  func(spillRecord, spillRecord) bool
}

func (h mergeHeap) Len() int           { return len(h.items) }
func (h mergeHeap) Less(i, j int) bool { return h.less(h.items[i].row, h.items[j].row) }
func (h mergeHeap) Swap(i, j int)      { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *mergeHeap) Push(v any)        { h.items = append(h.items, v.(*mergeItem)) }
func (h *mergeHeap) Pop() any {
	n := len(h.items) - 1
	v := h.items[n]
	h.items = h.items[:n]
	return v
}
func (r *spillSorter) merge(paths []string, visit func(spillRecord) error) error {
	h := &mergeHeap{less: r.less}
	var opened []*os.File
	defer func() {
		for _, f := range opened {
			f.Close()
		}
	}()
	for _, p := range paths {
		f, e := os.Open(p)
		if e != nil {
			return e
		}
		opened = append(opened, f)
		item := &mergeItem{file: f, dec: gob.NewDecoder(bufio.NewReaderSize(f, 4096))}
		e = item.dec.Decode(&item.row)
		if e == nil {
			heap.Push(h, item)
		} else if !errors.Is(e, io.EOF) {
			return e
		}
	}
	for h.Len() > 0 {
		if e := r.s.checkInterrupt(); e != nil {
			return e
		}
		item := heap.Pop(h).(*mergeItem)
		if e := visit(item.row); e != nil {
			return e
		}
		item.row = spillRecord{}
		e := item.dec.Decode(&item.row)
		if e == nil {
			heap.Push(h, item)
		} else if !errors.Is(e, io.EOF) {
			return e
		}
	}
	return nil
}
func (r *spillSorter) mergeRuns(paths []string) (string, error) {
	f, e := r.create()
	if e != nil {
		return "", e
	}
	w := bufio.NewWriterSize(f, 4096)
	enc := gob.NewEncoder(w)
	e = r.merge(paths, func(row spillRecord) error { return enc.Encode(row) })
	if e == nil {
		e = w.Flush()
	}
	if ce := f.Close(); e == nil {
		e = ce
	}
	if e != nil {
		return "", e
	}
	removed := map[string]bool{}
	for _, p := range paths {
		if e = os.Remove(p); e != nil {
			return "", e
		}
		removed[p] = true
		delete(r.levels, p)
	}
	out := r.paths[:0]
	for _, p := range r.paths {
		if !removed[p] {
			out = append(out, p)
		}
	}
	r.paths = out
	r.recordSpill(f.Name())
	r.paths = append(r.paths, f.Name())
	return f.Name(), nil
}

// Merge equally sized generations, avoiding repeated rewrites of the largest
// run. Eight-way leveled merging bounds open files and total rewrite passes.
func (r *spillSorter) compactLevels() error {
	for {
		counts := map[int][]string{}
		var selected []string
		var level int
		for _, p := range r.paths {
			n := r.levels[p]
			counts[n] = append(counts[n], p)
			if len(counts[n]) == 8 {
				selected = counts[n]
				level = n
				break
			}
		}
		if selected == nil {
			return nil
		}
		p, e := r.mergeRuns(selected)
		if e != nil {
			return e
		}
		r.levels[p] = level + 1
	}
}

func (r *spillSorter) walk(visit func(spillRecord) error) error {
	if len(r.paths) == 0 {
		sort.Slice(r.rows, func(i, j int) bool { return r.less(r.rows[i], r.rows[j]) })
		for _, row := range r.rows {
			if e := r.s.checkInterrupt(); e != nil {
				return e
			}
			if e := visit(row); e != nil {
				return e
			}
		}
		return nil
	}
	if len(r.rows) > 0 {
		if e := r.flush(); e != nil {
			return e
		}
	}
	for len(r.paths) > 8 {
		if _, e := r.mergeRuns(append([]string(nil), r.paths[:8]...)); e != nil {
			return e
		}
	}
	return r.merge(r.paths, visit)
}
func stableRecordLess(s *Schema, orders []OrderByClause) func(spillRecord, spillRecord) bool {
	return func(a, b spillRecord) bool {
		if s.compareRows(a.Row, b.Row, orders) {
			return true
		}
		if s.compareRows(b.Row, a.Row, orders) {
			return false
		}
		return a.Seq < b.Seq
	}
}

// diskHash indexes bucket identities and insertion ordinals in a private,
// WAL-free LSM. A probe seeks directly to its bucket rather than rescanning
// unrelated rows in a hash partition. Memtables/caches have fixed byte bounds.
type diskHash struct {
	s         *Schema
	dir       string
	index     *storage.LSMTree
	ordinal   uint64
	rowsRead  int64
	closed    bool
	lastKey   string
	lastRows  []map[string]Value
	lastReady bool
}

func newDiskHash(s *Schema) (*diskHash, error) {
	dir, err := os.MkdirTemp(s.queryTempDir, "slopdb-query-hash-*")
	if err != nil {
		return nil, err
	}
	options := storage.DefaultOptions()
	// Reserve only a fraction of this operator's budget for two memtables,
	// decoded blocks, and the adjacent-bucket cache. Tiny budgets retain the
	// storage engine's minimum fixed overhead rather than creating one SST/row.
	size := s.memoryBudget() / 16
	if size < 64<<10 {
		size = 64 << 10
	}
	if size > 4<<20 {
		size = 4 << 20
	}
	options.WriteBufferSize = int(size)
	options.BlockCacheSize = int(size)
	options.MaxOpenFiles = 32
	options.DisableWAL = true
	options.SyncOnClose = false
	options.CompressionType = storage.NoCompression
	options.MaxBackgroundJobs = 1
	index, err := storage.NewLSMTree(dir, options)
	if err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	return &diskHash{s: s, dir: dir, index: index}, nil
}
func (h *diskHash) close() {
	if h == nil || h.closed {
		return
	}
	h.closed = true
	if h.index != nil {
		// Temporary data need not survive Close; Close releases background tasks
		// and file readers before removal. Failed shutdown durability is immaterial.
		_ = h.index.Close()
		if h.s.profile != nil {
			stats := h.index.Stats()
			h.s.profile.SpillBytes += stats.FlushOutputBytes + stats.CompactionOutputBytes
			h.s.profile.SpillRuns += stats.CompletedFlushes + stats.CompletedCompactions
		}
	}
	h.lastRows = nil
	os.RemoveAll(h.dir)
}
func spillBucketPrefix(key string) []byte {
	return appendClusteredValue(nil, NewBlobValue([]byte(key)))
}
func (h *diskHash) add(key string, row map[string]Value) error {
	if err := h.s.checkInterrupt(); err != nil {
		return err
	}
	if h.ordinal == ^uint64(0) {
		return fmt.Errorf("temporary spill row ordinal overflow")
	}
	prefix := spillBucketPrefix(key)
	encodedKey := binary.BigEndian.AppendUint64(prefix, h.ordinal)
	payload := binary.AppendUvarint(nil, uint64(len(row)))
	for name, value := range row {
		payload = binary.AppendUvarint(payload, uint64(len(name)))
		payload = append(payload, name...)
		payload = appendValue(payload, value)
	}
	if err := h.index.Put(encodedKey, payload); err != nil {
		return err
	}
	h.ordinal++
	if h.lastReady && h.lastKey == key {
		h.lastRows = nil
		h.lastReady = false
	}
	return nil
}
func decodeSpillMap(data []byte) (map[string]Value, error) {
	count, n := binary.Uvarint(data)
	if n <= 0 || count > uint64(len(data)) {
		return nil, fmt.Errorf("invalid temporary spill row")
	}
	data = data[n:]
	row := make(map[string]Value, int(count))
	for i := uint64(0); i < count; i++ {
		length, n := binary.Uvarint(data)
		if n <= 0 || length > uint64(len(data)-n) {
			return nil, fmt.Errorf("invalid temporary spill column")
		}
		data = data[n:]
		name := string(data[:int(length)])
		data = data[int(length):]
		if len(data) == 0 {
			return nil, fmt.Errorf("missing temporary spill value")
		}
		// Validate framing before the common SQL decoder, whose row-storage helper
		// deliberately tolerates short rows for ALTER ADD COLUMN.
		size := 1
		switch data[0] {
		case binTypeNull:
		case binTypeInt64, binTypeFloat64:
			size = 9
		case binTypeText, binTypeBlob:
			length, n := binary.Uvarint(data[1:])
			if n <= 0 || length > uint64(len(data)-1-n) {
				return nil, fmt.Errorf("invalid temporary spill value")
			}
			size = 1 + n + int(length)
		default:
			return nil, fmt.Errorf("invalid temporary spill type")
		}
		if size > len(data) {
			return nil, fmt.Errorf("truncated temporary spill value")
		}
		value, used := decodeValue(data[:size])
		if used != size {
			return nil, fmt.Errorf("invalid temporary spill value length")
		}
		row[name] = value
		data = data[size:]
	}
	if len(data) != 0 {
		return nil, fmt.Errorf("trailing temporary spill bytes")
	}
	return row, nil
}
func (h *diskHash) walk(key string, visit func(map[string]Value) error) error {
	if err := h.s.checkInterrupt(); err != nil {
		return err
	}
	if h.lastReady && h.lastKey == key {
		for _, row := range h.lastRows {
			if err := h.s.checkInterrupt(); err != nil {
				return err
			}
			if err := visit(row); err != nil {
				return err
			}
		}
		return nil
	}
	h.lastReady = false
	h.lastRows = nil
	prefix := spillBucketPrefix(key)
	iter := h.index.NewIteratorRange(prefix, prefixSuccessor(prefix), false)
	defer iter.Close()
	cache := true
	var retained int64
	var rows []map[string]Value
	for iter.Valid() {
		if err := h.s.checkInterrupt(); err != nil {
			return err
		}
		row, err := decodeSpillMap(iter.Value())
		if err != nil {
			return err
		}
		h.rowsRead++
		if h.s.profile != nil {
			h.s.profile.SpillRowsRead++
		}
		if cache {
			retained += rowMemory(row)
			if retained <= h.s.memoryBudget()/16 {
				rows = append(rows, row)
			} else {
				rows = nil
				cache = false
			}
		}
		if err = visit(row); err != nil {
			return err
		}
		iter.Next()
	}
	if err := storage.IteratorError(iter); err != nil {
		return err
	}
	if cache {
		h.lastKey = key
		h.lastRows = rows
		h.lastReady = true
	}
	return nil
}

func (s *Schema) sortBounded(rows []map[string]Value, keys [][]string, orders []OrderByClause, window int64) ([]map[string]Value, [][]string, error) {
	if window == 0 {
		return nil, nil, nil
	}
	sorter := newSpillSorter(s, stableRecordLess(s, orders))
	defer sorter.close()
	for i, row := range rows {
		var k []string
		if keys != nil {
			k = keys[i]
		}
		if e := sorter.add(row, k, ""); e != nil {
			return nil, nil, e
		}
	}
	var out []map[string]Value
	var outKeys [][]string
	err := sorter.walk(func(r spillRecord) error {
		out = append(out, r.Row)
		if keys != nil {
			outKeys = append(outKeys, r.Keys)
		}
		if window >= 0 && int64(len(out)) >= window {
			return errAccessStop
		}
		return nil
	})
	if errors.Is(err, errAccessStop) {
		err = nil
	}
	return out, outKeys, err
}

func (r *spillSorter) addRecord(rec spillRecord) error {
	if e := r.add(rec.Row, rec.Keys, rec.Key); e != nil {
		return e
	}
	r.rows[len(r.rows)-1].Seq = rec.Seq
	return nil
}

func (s *Schema) buildJoinBuckets(reader *joinReader) error {
	r := reader.relation
	reader.buckets = make(map[string][]map[string]Value)
	var used int64
	add := func(row map[string]Value) error {
		key, valid := joinRowKey(row, reader.pairs, true)
		if !valid {
			return nil
		}
		if reader.disk == nil && used+rowMemory(row)+int64(len(key)+64) > s.memoryBudget() {
			var e error
			reader.disk, e = newDiskHash(s)
			if e != nil {
				return e
			}
			for k, rows := range reader.buckets {
				for _, row := range rows {
					if e = reader.disk.add(k, row); e != nil {
						return e
					}
				}
			}
			reader.buckets = nil
		}
		if reader.disk != nil {
			return reader.disk.add(key, row)
		}
		owned := copyRow(row)
		reader.buckets[key] = append(reader.buckets[key], owned)
		used += rowMemory(owned) + int64(len(key)+64)
		return nil
	}
	if r.loaded {
		for _, row := range r.rows {
			if e := s.checkInterrupt(); e != nil {
				return e
			}
			if e := add(row); e != nil {
				return e
			}
		}
		r.rows = nil
	} else {
		e := s.walkAccess(r.info, r.needed, r.paths, func(row map[string]Value) error {
			q := row
			if r.qualifiedFilter {
				q = copyRow(row)
				qualifyAccessRow(q, r.table, r.alias)
			}
			if r.filter != nil {
				ok, e := s.evalCondition(r.filter, q)
				if e != nil {
					return e
				}
				if !ok {
					return nil
				}
			}
			return add(row)
		})
		if e != nil {
			return e
		}
	}

	return nil
}

var errAggregateSpill = errors.New("aggregate working set requires spilling")

func aggregateGroupMemory(g *aggregateGroup) int64 {
	n := int64(128 + len(g.states)*192 + len(g.baseValues)*96)
	for _, v := range g.baseValues {
		n += int64(len(v.Text) + len(v.Blob))
	}
	for _, a := range g.states {
		n += a.seenBytes + a.partsBytes + int64(len(a.best.Text)+len(a.best.Blob))
	}
	return n
}

func (s *Schema) addAggregateValue(a *aggregateState, f *FuncCallExpr, v Value) error {
	a.owner = s
	return addAggregateValue(a, f, v)
}
func (s *Schema) aggregateDistinct(a *aggregateState, v Value) (bool, error) {
	key := string(valueIdentity(v))
	if a.distinctDisk == nil {
		if a.seen == nil {
			a.seen = make(map[string]bool)
		}
		if a.seen[key] {
			return false, nil
		}
		if a.seenBytes+int64(len(key)+64) <= s.memoryBudget()/4 {
			a.seen[key] = true
			a.seenBytes += int64(len(key) + 64)
			return true, nil
		}
		disk, e := newDiskHash(s)
		if e != nil {
			return false, e
		}
		s.queryDisks = append(s.queryDisks, disk)
		a.distinctDisk = disk
		for k := range a.seen {
			if e = disk.add(k, nil); e != nil {
				return false, e
			}
		}
		a.seen = nil
		a.seenBytes = 0
	}
	found := false
	e := a.distinctDisk.walk(key, func(map[string]Value) error { found = true; return errAccessStop })
	if e != nil && !errors.Is(e, errAccessStop) {
		return false, e
	}
	if found {
		return false, nil
	}
	e = a.distinctDisk.add(key, nil)
	return e == nil, e
}

// DISTINCT switches to sorted equality records when its key set exceeds the
// budget. Keep first-occurrence order and all contributing locking row keys.
func (s *Schema) distinctBounded(rows []map[string]Value, cols []string, keys [][]string) ([]map[string]Value, [][]string, error) {
	seen := map[string]int{}
	var used int64
	var out []map[string]Value
	var outKeys [][]string
	identity := func(row map[string]Value) string {
		var key []byte
		for _, col := range cols {
			key = append(key, valueIdentity(row[col])...)
		}
		return string(key)
	}
	spill := false
	for i, row := range rows {
		if e := s.checkInterrupt(); e != nil {
			return nil, nil, e
		}
		key := identity(row)
		pos, exists := seen[key]
		if !exists {
			used += int64(len(key) + 64)
			if used > s.memoryBudget() {
				spill = true
				break
			}
			pos = len(out)
			seen[key] = pos
			out = append(out, row)
			if keys != nil {
				outKeys = append(outKeys, nil)
			}
		}
		if keys != nil {
			outKeys[pos] = append(outKeys[pos], keys[i]...)
		}
	}
	if !spill {
		return out, outKeys, nil
	}
	out = nil
	outKeys = nil
	seen = nil
	sorter := newSpillSorter(s, func(a, b spillRecord) bool {
		if a.Key != b.Key {
			return a.Key < b.Key
		}
		return a.Seq < b.Seq
	})
	defer sorter.close()
	for i, row := range rows {
		var k []string
		if keys != nil {
			k = keys[i]
		}
		if e := sorter.add(row, k, identity(row)); e != nil {
			return nil, nil, e
		}
	}
	var last string
	var sequences []int64
	err := sorter.walk(func(rec spillRecord) error {
		if len(out) == 0 || rec.Key != last {
			last = rec.Key
			out = append(out, rec.Row)
			sequences = append(sequences, rec.Seq)
			if keys != nil {
				outKeys = append(outKeys, nil)
			}
		}
		if keys != nil {
			outKeys[len(outKeys)-1] = append(outKeys[len(outKeys)-1], rec.Keys...)
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	order := make([]int, len(out))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool { return sequences[order[i]] < sequences[order[j]] })
	result := make([]map[string]Value, len(out))
	var resultKeys [][]string
	if keys != nil {
		resultKeys = make([][]string, len(out))
	}
	for i, pos := range order {
		result[i] = out[pos]
		if keys != nil {
			resultKeys[i] = outKeys[pos]
		}
	}
	return result, resultKeys, nil
}
