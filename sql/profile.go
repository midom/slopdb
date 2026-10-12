package sql

import (
	"bytes"
	"runtime"
	"strings"
	"time"

	"slopdb/storage"
)

// QueryProfile describes one execution, including retry work. Operator times
// are inclusive and must not be summed. Go heap counters are process-wide
// deltas; concurrent statements can contribute to those two counters.
type QueryProfile struct {
	SpillRowsRead         int64             `json:"spill_rows_read"`
	SpillBytes            int64             `json:"spill_bytes"`
	SpillRuns             int64             `json:"spill_runs"`
	MemoryLimitBytes      int64             `json:"memory_limit_bytes"`
	ElapsedMS             float64           `json:"elapsed_ms"`
	LockWaitMS            float64           `json:"lock_wait_ms"`
	RowsExamined          int64             `json:"rows_examined"`
	IndexEntriesExamined  int64             `json:"index_entries_examined"`
	PointLookups          int64             `json:"point_lookups"`
	BytesRead             int64             `json:"bytes_read"`
	ProcessAllocatedBytes uint64            `json:"process_allocated_bytes"`
	ProcessAllocations    uint64            `json:"process_allocations"`
	Operators             []OperatorProfile `json:"operators"`
	started               time.Time
	heap                  runtime.MemStats
	stack                 []int
	nodes                 map[profileNode]int
	overflow              int
	CollapsedInvocations  int64 `json:"collapsed_invocations"`
}

const maxProfileOperators = 1024

type profileNode struct {
	parent         int
	name, relation string
}

type OperatorProfile struct {
	Invocations          int64   `json:"invocations"`
	ID                   int     `json:"id"`
	ParentID             int     `json:"parent_id"`
	Operator             string  `json:"operator"`
	Relation             string  `json:"relation,omitempty"`
	RowsExamined         int64   `json:"rows_examined"`
	IndexEntriesExamined int64   `json:"index_entries_examined"`
	RowsReturned         int64   `json:"rows_returned"`
	PointLookups         int64   `json:"point_lookups"`
	BytesRead            int64   `json:"bytes_read"`
	ElapsedMS            float64 `json:"elapsed_ms"`
	Error                string  `json:"error,omitempty"`
}

// StartQueryProfile enables metrics only on this serialized session. A caller
// must stop profiling before releasing the session to a different execution.
func (s *Schema) StartQueryProfile() *QueryProfile {
	s.StopQueryProfile()
	p := &QueryProfile{MemoryLimitBytes: s.memoryBudget(), started: time.Now(), nodes: make(map[profileNode]int), overflow: -1}
	runtime.ReadMemStats(&p.heap)
	s.profile = p
	return p
}

func (s *Schema) QueryProfile() *QueryProfile { return s.profile }

func (p *QueryProfile) sample() {
	p.ElapsedMS = float64(time.Since(p.started)) / float64(time.Millisecond)
	var heap runtime.MemStats
	runtime.ReadMemStats(&heap)
	p.ProcessAllocatedBytes = heap.TotalAlloc - p.heap.TotalAlloc
	p.ProcessAllocations = heap.Mallocs - p.heap.Mallocs
}

func (s *Schema) StopQueryProfile() {
	if p := s.profile; p != nil {
		p.sample()
		p.stack = nil
		p.nodes = nil
		s.profile = nil
	}
}

func (s *Schema) RecordLockWait(elapsed time.Duration) {
	if s.profile != nil {
		s.profile.LockWaitMS += float64(elapsed) / float64(time.Millisecond)
	}
}

func (p *QueryProfile) parent() int {
	if len(p.stack) == 0 {
		return -1
	}
	return p.stack[len(p.stack)-1]
}

func (p *QueryProfile) operator(name, relation string) int {
	return p.operatorUnder(p.parent(), name, relation)
}
func (p *QueryProfile) operatorUnder(parent int, name, relation string) int {
	key := profileNode{parent, name, relation}
	if id, ok := p.nodes[key]; ok {
		p.Operators[id].Invocations++
		return id
	}
	if len(p.nodes) >= maxProfileOperators {
		if p.overflow < 0 {
			p.overflow = len(p.Operators)
			p.Operators = append(p.Operators, OperatorProfile{ID: p.overflow, ParentID: -1, Operator: "Collapsed"})
		}
		p.CollapsedInvocations++
		p.Operators[p.overflow].Invocations++
		return p.overflow
	}
	id := len(p.Operators)
	p.nodes[key] = id
	p.Operators = append(p.Operators, OperatorProfile{ID: id, ParentID: parent, Operator: name, Relation: relation, Invocations: 1})
	return id
}

func (s *Schema) profileSelect(stmt *SelectStmt) func(*ResultSet, error) {
	p := s.profile
	if p == nil {
		return nil
	}
	id := p.operator("SELECT", stmt.From)
	p.stack = append(p.stack, id)
	started := time.Now()
	return func(result *ResultSet, err error) {
		op := &p.Operators[id]
		op.ElapsedMS += float64(time.Since(started)) / float64(time.Millisecond)
		if result != nil {
			op.RowsReturned += int64(len(result.Rows))
		}
		if err != nil {
			op.Error = err.Error()
		}
		p.stack = p.stack[:len(p.stack)-1]
	}
}

// ProfileOperator records a logical execution stage. The returned finish
// function must run before the enclosing stage finishes. It is nil when
// profiling is disabled, so ordinary execution does not create timers/maps.
func (s *Schema) ProfileOperator(name, relation string) func(int64, int64, error) {
	p := s.profile
	if p == nil {
		return nil
	}
	id := p.operator(name, relation)
	p.stack = append(p.stack, id)
	started := time.Now()
	return func(input, output int64, err error) {
		op := &p.Operators[id]
		op.RowsExamined += input
		op.RowsReturned += output
		op.ElapsedMS += float64(time.Since(started)) / float64(time.Millisecond)
		if err != nil {
			op.Error = err.Error()
		}
		p.stack = p.stack[:len(p.stack)-1]
	}
}

func (s *Schema) profileRelation(key []byte) (string, bool) {
	if len(key) == 0 {
		return "catalog", false
	}
	if key[0] == 1 {
		for name := range s.tables {
			if bytes.HasPrefix(key, []byte(s.tableDataPrefix(name))) {
				return name, false
			}
		}
	}
	if key[0] == 2 {
		for name, index := range s.indexes {
			if bytes.HasPrefix(key, []byte(s.indexDataPrefix(name))) {
				return index.Table + "." + name, true
			}
		}
	}
	return "catalog", false
}

func (p *QueryProfile) read(id int, key, value []byte, index bool) {
	op := &p.Operators[id]
	if index {
		op.IndexEntriesExamined++
		p.IndexEntriesExamined++
	} else if len(key) != 0 && key[0] == 1 {
		op.RowsExamined++
		p.RowsExamined++
	}
	n := int64(len(key) + len(value))
	op.BytesRead += n
	p.BytesRead += n
}

func (s *Schema) profilePoint(key, value []byte, found bool, elapsed time.Duration) {
	p := s.profile
	if p == nil {
		return
	}
	relation, index := s.profileRelation(key)
	id := p.operator("PointLookup", relation)
	p.Operators[id].PointLookups++
	p.Operators[id].ElapsedMS += float64(elapsed) / float64(time.Millisecond)
	p.PointLookups++
	if found {
		p.read(id, key, value, index)
	}
}

type profileIterator struct {
	storage.Iterator
	schema   *Schema
	profile  *QueryProfile
	id       int
	parent   int
	name     string
	relation string
	visited  bool
	closed   bool
	started  time.Time
}

func (s *Schema) profileReader(it storage.Iterator, name string) storage.Iterator {
	if s.profile == nil {
		return it
	}
	return &profileIterator{Iterator: it, schema: s, profile: s.profile, id: -1, parent: s.profile.parent(), name: name, started: time.Now()}
}

func (it *profileIterator) visit() {
	if it.visited || !it.Iterator.Valid() {
		return
	}
	it.visited = true
	key := it.Iterator.Key().UserKey
	relation, index := it.schema.profileRelation(key)
	if it.id < 0 || it.relation != relation {
		it.id = it.profile.operatorUnder(it.parent, it.name, relation)
		it.relation = relation
	}
	it.profile.read(it.id, key, it.Iterator.Value(), index)
}

func (it *profileIterator) Key() storage.InternalKey { it.visit(); return it.Iterator.Key() }
func (it *profileIterator) Value() []byte            { it.visit(); return it.Iterator.Value() }
func (it *profileIterator) Next()                    { it.Iterator.Next(); it.visited = false }
func (it *profileIterator) Prev()                    { it.Iterator.Prev(); it.visited = false }
func (it *profileIterator) Seek(key []byte)          { it.Iterator.Seek(key); it.visited = false }
func (it *profileIterator) Err() error               { return storage.IteratorError(it.Iterator) }
func (it *profileIterator) Close() {
	if it.closed {
		return
	}
	it.closed = true
	it.Iterator.Close()
	if it.id < 0 {
		it.id = it.profile.operatorUnder(it.parent, it.name, "")
	}
	it.profile.Operators[it.id].ElapsedMS += float64(time.Since(it.started)) / float64(time.Millisecond)
}

func (s *Schema) appendProfileResult(rs *ResultSet) {
	p := s.profile
	if p == nil {
		return
	}
	p.sample()
	rs.Profile = p
	fields := []ColumnDef{
		{Name: "invocations", ColType: ColInteger}, {Name: "collapsed_invocations", ColType: ColInteger},
		{Name: "operator_id", ColType: ColInteger}, {Name: "parent_id", ColType: ColInteger},
		{Name: "rows_examined", ColType: ColInteger}, {Name: "index_entries_examined", ColType: ColInteger},
		{Name: "rows_returned", ColType: ColInteger}, {Name: "point_lookups", ColType: ColInteger},
		{Name: "bytes_read", ColType: ColInteger}, {Name: "elapsed_ms", ColType: ColReal},
		{Name: "spill_rows_read", ColType: ColInteger}, {Name: "spill_bytes", ColType: ColInteger}, {Name: "spill_runs", ColType: ColInteger}, {Name: "memory_limit_bytes", ColType: ColInteger},
		{Name: "lock_wait_ms", ColType: ColReal}, {Name: "process_allocated_bytes", ColType: ColInteger},
		{Name: "process_allocations", ColType: ColInteger},
	}
	rs.Columns = append(rs.Columns, fields...)
	for _, row := range rs.Rows {
		for _, field := range fields {
			row[field.Name] = NewNullValue()
		}
	}
	add := func(name, relation string, op OperatorProfile) map[string]Value {
		row := make(map[string]Value, len(rs.Columns))
		for _, field := range rs.Columns {
			row[field.Name] = NewNullValue()
		}
		row["addr"] = NewIntValue(int64(len(rs.Rows)))
		row["opcode"] = NewTextValue(name)
		row["p4"] = NewTextValue(relation)
		row["comment"] = NewTextValue("actual execution; inclusive operator time")
		row["invocations"] = NewIntValue(op.Invocations)
		row["operator_id"] = NewIntValue(int64(op.ID))
		row["parent_id"] = NewIntValue(int64(op.ParentID))
		row["rows_examined"] = NewIntValue(op.RowsExamined)
		row["index_entries_examined"] = NewIntValue(op.IndexEntriesExamined)
		row["rows_returned"] = NewIntValue(op.RowsReturned)
		row["point_lookups"] = NewIntValue(op.PointLookups)
		row["bytes_read"] = NewIntValue(op.BytesRead)
		row["elapsed_ms"] = NewRealValue(op.ElapsedMS)
		rs.Rows = append(rs.Rows, row)
		return row
	}
	for _, op := range p.Operators {
		add("Actual"+op.Operator, op.Relation, op)
	}
	row := add("QueryProfile", "", OperatorProfile{ID: -1, ParentID: -1, RowsExamined: p.RowsExamined, IndexEntriesExamined: p.IndexEntriesExamined, PointLookups: p.PointLookups, BytesRead: p.BytesRead, ElapsedMS: p.ElapsedMS})
	row["collapsed_invocations"] = NewIntValue(p.CollapsedInvocations)
	row["spill_rows_read"] = NewIntValue(p.SpillRowsRead)
	row["spill_bytes"] = NewIntValue(p.SpillBytes)
	row["spill_runs"] = NewIntValue(p.SpillRuns)
	row["memory_limit_bytes"] = NewIntValue(p.MemoryLimitBytes)
	row["lock_wait_ms"] = NewRealValue(p.LockWaitMS)
	row["process_allocated_bytes"] = NewIntValue(int64(p.ProcessAllocatedBytes))
	row["process_allocations"] = NewIntValue(int64(p.ProcessAllocations))
	row["comment"] = NewTextValue(strings.Join([]string{"actual reads include retries", "heap deltas include other concurrent Go work", "operator times are inclusive"}, "; "))
}
