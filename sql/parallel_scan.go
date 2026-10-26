package sql

import (
	"context"
	"runtime"
	"strings"
	"sync"

	"slopdb/storage"
)

// Extra workers borrow a fixed server-wide CPU budget without waiting. A busy
// server executes serially; a large lone query can use otherwise idle cores.
// The storage planner limits splits by physical size and never reads data to
// plan a query. Memory is divided before creating private worker programs.
func (s *Schema) parallelRanges(ctx context.Context, snapshot uint64, lower, upper []byte, bytesPerWorker int64) ([]storage.KeyRange, func(), error) {
	root := s.executionRoot()
	workers := runtime.GOMAXPROCS(0)
	active := root.parallelScanQueries.Load()
	if active > 1 {
		workers = max(1, workers/int(active))
	}
	if bytesPerWorker > 0 {
		workers = min(workers, int(s.memoryBudget()/bytesPerWorker))
	}
	if workers < 2 || s.profile != nil || (s.inTx && len(s.txLatest) > 0) {
		return nil, nil, nil
	}
	// Reserve the serial caller's CPU too. Saturated callers do not spin or wait.
	used := root.parallelScanSlots.Load()
	workers = min(workers, runtime.GOMAXPROCS(0)-int(used))
	if workers < 2 || !root.parallelScanSlots.CompareAndSwap(used, used+int64(workers)) {
		return nil, nil, nil
	}
	release := func() { root.parallelScanSlots.Add(-int64(workers)) }
	ranges, err := s.lsm.PartitionRangeAtContext(ctx, snapshot, lower, upper, s.snapshotReader, workers)
	if err != nil || len(ranges) < 2 {
		release()
		return nil, nil, err
	}
	if len(ranges) < workers {
		root.parallelScanSlots.Add(-int64(workers - len(ranges)))
		workers = len(ranges)
	}
	return ranges, release, nil
}

func (s *Schema) countRangeParallel(ctx context.Context, snapshot uint64, lower, upper []byte) (int64, error) {
	root := s.executionRoot()
	root.parallelScanQueries.Add(1)
	defer root.parallelScanQueries.Add(-1)
	if !s.statementActive {
		snapshot = s.lsm.SnapshotForReader(s.snapshotReader)
		defer s.lsm.ReleaseSnapshotForReader(s.snapshotReader, snapshot)
	}
	ranges, release, err := s.parallelRanges(ctx, snapshot, lower, upper, 0)
	if err != nil {
		return 0, err
	}
	if release == nil {
		return s.lsm.CountRangeAtContext(ctx, snapshot, lower, upper, s.snapshotReader)
	}
	defer release()
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	counts := make([]int64, len(ranges))
	errs := make([]error, len(ranges))
	var wg sync.WaitGroup
	for i, r := range ranges {
		wg.Add(1)
		reader := root.nextSnapshotReader.Add(1)
		go func(i int, r storage.KeyRange, reader uint64) {
			defer wg.Done()
			counts[i], errs[i] = s.lsm.CountRangeAtContext(workCtx, snapshot, r.Lower, r.Upper, reader)
			if errs[i] != nil {
				cancel()
			}
		}(i, r, reader)
	}
	wg.Wait()
	var n int64
	for i, e := range errs {
		if e != nil && e != context.Canceled {
			return 0, e
		}
		n += counts[i]
	}
	for _, e := range errs {
		if e != nil {
			return 0, e
		}
	}
	return n, nil
}

// Parallel reduction is deliberately restricted to associative aggregate
// states. DISTINCT and ordered/string aggregation retain their normal plans.
func (s *Schema) parallelTypedAggregate(t *typedAggregate, info *TableInfo, needed map[string]bool, paths []*accessPath, groups map[string]*aggregateGroup) (bool, error) {
	if paths != nil || len(t.group) != 0 || len(t.baseSlots) != 0 || !s.statementActive || s.profile != nil || s.isTemporaryTable(info.Name) {
		return false, nil
	}
	for _, f := range t.functions {
		if f.Distinct {
			return false, nil
		}
		switch strings.ToUpper(f.Name) {
		case "COUNT", "MIN", "MAX":
		default:
			return false, nil
		}
	}
	// Fixed-width inputs bound each private decode batch. Variable-width and
	// arithmetic reductions retain serial execution and its exact error/order
	// semantics until their retained memory and reduction rules are proven.
	for _, col := range t.columns {
		switch col.typ {
		case ColInteger, ColReal, ColUnsigned:
		default:
			return false, nil
		}
	}

	if !parallelProgramFixed(t.where) {
		return false, nil
	}
	for _, arg := range t.args {
		if !parallelProgramFixed(arg) {
			return false, nil
		}
	}
	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	root := s.executionRoot()
	root.parallelScanQueries.Add(1)
	defer root.parallelScanQueries.Add(-1)
	lower := []byte(info.storagePrefix(info.Name))
	upper := prefixSuccessor(lower)
	perWorker := int64(t.width)*typedBatchRows*256 + int64(len(t.functions))*256
	estimateProgram := func(p *typedProgram) {
		if p != nil {
			perWorker += int64(len(p.code)+len(p.registers)+len(p.literals)) * 256
		}
	}
	estimateProgram(t.where)
	estimateProgram(t.rawWhere)
	for _, p := range t.group {
		estimateProgram(p)
	}
	for _, p := range t.args {
		estimateProgram(p)
	}
	perWorker = max(int64(256<<10), perWorker)
	ranges, release, err := s.parallelRanges(ctx, s.statementSnapshot, lower, upper, perWorker)
	if err != nil {
		return true, err
	}
	if release == nil {
		return false, nil
	}
	defer release()
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	local := make([]*aggregateGroup, len(ranges))
	errs := make([]error, len(ranges))
	var wg sync.WaitGroup
	for i, r := range ranges {
		worker := &Schema{lsm: s.lsm, parent: root, ctx: workCtx, statementSnapshot: s.statementSnapshot, statementActive: true, snapshotReader: root.nextSnapshotReader.Add(1), queryMemoryBytes: s.memoryBudget() / int64(len(ranges)), dialect: s.dialect, expressionDialect: s.expressionDialect, collation: s.collation, parallelScanLower: r.Lower, parallelScanUpper: r.Upper}
		program := t.clonePrivate()
		g := &aggregateGroup{states: make([]aggregateState, len(t.functions))}
		local[i] = g
		wg.Add(1)
		go func(i int, worker *Schema, program *typedAggregate, g *aggregateGroup) {
			defer wg.Done()
			errs[i] = worker.walkTypedAggregate(program, info, needed, nil, map[string]*aggregateGroup{"": g}, func() *aggregateGroup { return g })
			if errs[i] != nil {
				cancel()
			}
		}(i, worker, program, g)
	}
	wg.Wait()
	for _, e := range errs {
		if e != nil && e != context.Canceled {
			return true, e
		}
	}
	for _, e := range errs {
		if e != nil {
			return true, e
		}
	}
	target := groups[""]
	for _, g := range local {
		target.inputs += g.inputs
		for i := range g.states {
			if err := s.mergeAggregateState(&target.states[i], &g.states[i], t.functions[i]); err != nil {
				return true, err
			}
		}
	}
	return true, nil
}

func parallelProgramFixed(p *typedProgram) bool {
	if p == nil {
		return true
	}
	for _, instruction := range p.code {
		if instruction.op == "||" {
			return false
		}
		if instruction.op == "literal" {
			v := instruction.value
			if instruction.literal != nil {
				v = instruction.literal.Value
			}
			if !v.Null {
				switch v.Type {
				case ColInteger, ColReal, ColUnsigned:
				default:
					return false
				}
			}
			if len(v.NumericText) > 256 {
				return false
			}
		}
	}
	return true
}

func (s *Schema) mergeAggregateState(dst, src *aggregateState, f *FuncCallExpr) error {
	if src.count == 0 {
		return nil
	}
	name := strings.ToUpper(f.Name)
	if name == "MIN" || name == "MAX" {
		if dst.count == 0 || (name == "MIN" && s.valueCompare(src.best, dst.best) < 0) || (name == "MAX" && s.valueCompare(src.best, dst.best) > 0) {
			dst.best = ownAggregateBest(src.best, dst.best.Blob)
		}
		dst.count += src.count
		return nil
	}
	if name == "COUNT" {
		dst.count += src.count
		return nil
	}
	panic("unsupported parallel aggregate")
}
