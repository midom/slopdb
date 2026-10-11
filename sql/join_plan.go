package sql

import (
	"fmt"
	"math"
	"strings"
	"time"
)

type joinRelation struct {
	table, alias    string
	info            *TableInfo
	needed          map[string]bool
	filter          Expression
	paths           []*accessPath
	rows            []map[string]Value
	loaded          bool
	count, total    float64
	left            bool
	qualifiedFilter bool
	distribution    *tableStatistics
}
type joinEdge struct {
	a, b   int
	ac, bc string
}
type joinPredicate struct {
	expr  Expression
	mask  uint64
	stage int
}
type joinPlan struct {
	relations  []*joinRelation
	edges      []joinEdge
	predicates []joinPredicate
	owners     map[string]int
	firstOwner map[string]int
	order      []int
	fixed      bool
	where      Expression
	keyDomains map[string]float64
}

func (s *Schema) expressionDependencies(expr Expression) ([]string, bool) {
	var names []string
	safe := true
	_ = walkExpression(expr, func(e Expression) (bool, error) {
		switch e := e.(type) {
		case *ColumnRefExpr:
			if e.Name != "*" {
				names = append(names, e.Name)
			}
		case *FuncCallExpr:
			if !deterministicFunction(e.Name) {
				safe = false
			}
		case *SubqueryExpr:
			deps, ok := s.subqueryDependencies(e.Stmt)
			names = append(names, deps...)
			safe = safe && ok
			return true, nil
		case *ExistsExpr:
			deps, ok := s.subqueryDependencies(e.Stmt)
			names = append(names, deps...)
			safe = safe && ok
			return true, nil
		}
		return false, nil
	})
	return names, safe
}

// Project necessary conditions through AND/OR. A condition belonging to a
// different relation becomes TRUE; this preserves NULL/three-valued semantics
// because the original predicate is always evaluated after its inputs exist.
func (s *Schema) relationPredicate(expr Expression, owner int, owners map[string]int) Expression {
	if b, ok := expr.(*BinaryExpr); ok && (b.Op == "AND" || b.Op == "OR") {
		a, c := s.relationPredicate(b.Left, owner, owners), s.relationPredicate(b.Right, owner, owners)
		if b.Op == "AND" {
			return booleanAnd(a, c)
		}
		if a == nil || c == nil {
			return nil
		}
		return &BinaryExpr{Op: "OR", Left: a, Right: c}
	}
	names, safe := s.expressionDependencies(expr)
	if !safe || len(names) == 0 {
		return nil
	}
	for _, name := range names {
		if i, ok := owners[name]; !ok || i != owner {
			return nil
		}
	}
	return expr
}

func bareRelationPredicate(expr Expression, owner int, owners map[string]int) Expression {
	out, _ := mapExpression(expr, func(e Expression) (Expression, bool, error) {
		switch e := e.(type) {
		case *ColumnRefExpr:
			if owners[e.Name] == owner {
				_, name, qualified := strings.Cut(e.Name, ".")
				if !qualified {
					name = e.Name
				}
				return &ColumnRefExpr{Name: name}, true, nil
			}
		// Nested query blocks have their own lexical scopes.
		case *SubqueryExpr, *ExistsExpr:
			return e, true, nil
		}
		return e, false, nil
	})
	return out
}

func (s *Schema) makeJoinPlan(stmt *SelectStmt) *joinPlan {
	if len(stmt.Joins) == 0 || len(stmt.Joins) > 62 || stmt.FromExpr != nil || stmt.Lock != nil || stmt.trackRows || stmt.Virtual != nil {
		return nil
	}
	p := &joinPlan{owners: make(map[string]int), firstOwner: make(map[string]int)}
	add := func(table, alias string) bool {
		info := s.tables[table]
		if info == nil {
			return false
		}
		if alias == "" {
			alias = table
		}
		for _, r := range p.relations {
			if r.alias == alias {
				return false
			}
		}
		i := len(p.relations)
		r := &joinRelation{table: table, alias: alias, info: info, needed: make(map[string]bool)}
		r.distribution = s.distributions[table]
		count, analyzed := s.stats[table]
		r.total = float64(count)
		if !analyzed || r.total < 0 {
			r.total = 1000
		}
		r.count = r.total
		p.relations = append(p.relations, r)
		for _, c := range info.Columns {
			if _, ok := p.firstOwner[c.Name]; !ok {
				p.firstOwner[c.Name] = i
			}
			if _, ok := p.owners[c.Name]; ok {
				p.owners[c.Name] = -1
			} else {
				p.owners[c.Name] = i
			}
			p.owners[alias+"."+c.Name] = i
		}
		return true
	}
	if !add(stmt.From, stmt.FromAlias) {
		return nil
	}
	all := stmt.Where
	for _, j := range stmt.Joins {
		if j.Type == "LEFT" {
			p.fixed = true
		}
	}
	if p.fixed {
		p.where = stmt.Where
		all = nil
		if _, safe := s.expressionDependencies(stmt.Where); !safe {
			return nil
		}
	}
	for _, j := range stmt.Joins {
		if (j.Type != "INNER" && j.Type != "CROSS" && j.Type != "LEFT" && j.Type != "") || j.Subquery != nil || len(j.Using) > 0 || !add(j.Table, j.Alias) {
			return nil
		}
		all = booleanAnd(all, j.On)
		p.relations[len(p.relations)-1].left = j.Type == "LEFT"
	}
	var terms []joinPredicate
	if p.fixed {
		for i, j := range stmt.Joins {
			for _, expr := range andTerms(j.On) {
				terms = append(terms, joinPredicate{expr: expr, stage: i + 1})
			}
		}
	} else {
		for _, expr := range andTerms(all) {
			terms = append(terms, joinPredicate{expr: expr, stage: -1})
		}
	}
	for _, term := range terms {
		expr := term.expr
		names, safe := s.expressionDependencies(expr)
		if !safe {
			return nil
		}
		var mask uint64
		for _, name := range names {
			i, ok := p.owners[name]
			if !ok {
				if _, outer := s.outerRow[name]; outer {
					continue
				}
				return nil
			}
			if i < 0 {
				return nil
			}
			mask |= 1 << i
		}
		if p.fixed && mask>>uint(term.stage+1) != 0 {
			return nil
		}
		p.predicates = append(p.predicates, joinPredicate{expr: expr, mask: mask, stage: term.stage})
		if b, ok := expr.(*BinaryExpr); ok && b.Op == "=" {
			a, aok := b.Left.(*ColumnRefExpr)
			c, cok := b.Right.(*ColumnRefExpr)
			if aok && cok {
				ai, aok := p.owners[a.Name]
				bi, bok := p.owners[c.Name]
				if aok && bok && ai >= 0 && bi >= 0 && ai != bi && (!p.fixed || ai == term.stage || bi == term.stage) {
					ac, bc := a.Name, c.Name
					if n := strings.IndexByte(ac, '.'); n >= 0 {
						ac = ac[n+1:]
					}
					if n := strings.IndexByte(bc, '.'); n >= 0 {
						bc = bc[n+1:]
					}
					p.edges = append(p.edges, joinEdge{ai, bi, ac, bc})
				}
			}
		}
	}
	// Volatility in projections can make reordered evaluation observable.
	for _, c := range stmt.Columns {
		if _, safe := s.expressionDependencies(c.Expr); !safe {
			return nil
		}
	}
	needed := joinRequiredNames(stmt)
	for i, r := range p.relations {
		for _, c := range r.info.Columns {
			if needed == nil || needed[c.Name] || needed[r.alias+"."+c.Name] {
				r.needed[c.Name] = true
			}
		}
		r.filter = s.relationPredicate(all, i, p.owners)
		if p.fixed {
			r.filter = nil
			if i > 0 {
				r.filter = s.relationPredicate(stmt.Joins[i-1].On, i, p.owners)
			}
		}
		bare := bareRelationPredicate(r.filter, i, p.owners)
		refs, _ := s.expressionDependencies(r.filter)
		for _, name := range refs {
			if strings.Contains(name, ".") {
				r.qualifiedFilter = true
			}
		}
		r.paths = s.accessPaths(bare, r.info)
		if r.filter != nil {
			r.count *= s.predicateSelectivity(r.table, bare)
		}
	}
	p.chooseOrder()
	return p
}

func (p *joinPlan) pairs(i int, mask uint64) []joinKeyPair {
	var out []joinKeyPair
	seen := make(map[joinKeyPair]bool)
	add := func(pair joinKeyPair) {
		if !seen[pair] {
			out = append(out, pair)
			seen[pair] = true
		}
	}
	for _, e := range p.edges {
		if e.a == i && mask&(1<<e.b) != 0 {
			add(joinKeyPair{p.relations[e.b].alias + "." + e.bc, e.ac})
		}
		if e.b == i && mask&(1<<e.a) != 0 {
			add(joinKeyPair{p.relations[e.a].alias + "." + e.ac, e.bc})
		}
	}
	return out
}

func (s *Schema) loadJoinRelation(r *joinRelation) error {
	if r.loaded {
		return nil
	}
	err := s.walkAccess(r.info, r.needed, r.paths, func(row map[string]Value) error {
		if r.filter != nil {
			q := row
			if r.qualifiedFilter {
				q = copyRow(row)
				qualifyAccessRow(q, r.table, r.alias)
			}
			ok, err := s.evalCondition(r.filter, q)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
		}
		r.rows = append(r.rows, copyRow(row))
		return nil
	})
	if err == nil {
		r.loaded = true
		r.count = float64(len(r.rows))
	}
	return err
}

func (p *joinPlan) fanout(i int, mask uint64) (factor float64) {
	r := p.relations[i]
	// A LEFT input emits a preserved outer row even when no match exists.
	defer func() {
		if r.left && factor < 1 {
			factor = 1
		}
	}()
	if r.total == 0 || r.count == 0 {
		return 0
	}
	pairs := p.costPairs(i, mask)
	if len(pairs) == 0 {
		return r.count
	}
	keys := make(map[string]bool)
	for _, pair := range pairs {
		keys[pair.right] = true
	}
	unique := len(r.info.PKColumns) > 0
	for _, name := range r.info.PKColumns {
		unique = unique && keys[name]
	}
	if unique {
		return math.Min(1, r.count/r.total)
	}
	selectivity := 1.0
	for _, pair := range pairs {
		var right, left *columnStatistics
		if r.distribution != nil {
			right = r.distribution.Columns[pair.right]
		}
		owner, exists := p.owners[pair.left]
		if exists && owner >= 0 && p.relations[owner].distribution != nil {
			_, name, _ := strings.Cut(pair.left, ".")
			left = p.relations[owner].distribution.Columns[name]
		}
		if right != nil {
			selectivity *= joinDistributionSelectivity(left, right)
		} else {
			selectivity /= p.equalityDomain(r.alias + "." + pair.right)
		}
	}
	return math.Max(0.001, r.count*selectivity)
}

func (p *joinPlan) chooseGreedyOrder() {
	if p.fixed {
		p.order = nil
		for i := range p.relations {
			p.order = append(p.order, i)
		}
		return
	}
	best := math.Inf(1)
	for start := range p.relations {
		order := []int{start}
		mask := uint64(1) << start
		rows := p.relations[start].count
		cost := rows
		for len(order) < len(p.relations) {
			next := -1
			factor := math.Inf(1)
			for i := range p.relations {
				if mask&(1<<i) == 0 {
					f := p.fanout(i, mask)
					if len(p.pairs(i, mask)) == 0 {
						f = joinCostProduct(f, 1000)
					}
					if f < factor {
						next = i
						factor = f
					}
				}
			}
			rows = joinCostProduct(rows, p.fanout(next, mask))
			cost = joinCostSum(cost, rows)
			order = append(order, next)
			mask |= 1 << next
		}
		if cost < best {
			best = cost
			p.order = order
		}
	}
}

type joinReader struct {
	disk       *diskHash
	cacheBytes int64
	relation   *joinRelation
	pairs      []joinKeyPair
	index      *IndexInfo
	columns    []string
	buckets    map[string][]map[string]Value
	cache      map[string][]map[string]Value
	cacheRows  int
}

func (s *Schema) joinReader(r *joinRelation, pairs []joinKeyPair) *joinReader {
	reader := &joinReader{relation: r, pairs: pairs, cache: make(map[string][]map[string]Value)}
	bound := make(map[string]bool)
	for _, pair := range pairs {
		bound[pair.right] = true
	}
	best := 0
	bestCost := math.Inf(1)
	consider := func(idx *IndexInfo, columns []string) {
		n := 0
		for _, c := range columns {
			if !bound[c] {
				break
			}
			n++
		}
		score := n * 16
		if n == len(columns) && idx == nil {
			score += 64
		}
		cost := math.Inf(1)
		if n > 0 && r.distribution != nil {
			rows := r.total
			known := true
			for _, name := range columns[:n] {
				c := r.distribution.Columns[name]
				if c == nil {
					known = false
					break
				}
				rows *= (1 - c.NullFraction) / math.Max(1, c.DistinctEstimate)
			}
			if idx == nil && n == len(columns) {
				rows = math.Min(rows, 1)
			}
			if known {
				cost = accessCost(math.Max(1, rows), idx != nil, false)
			}
		}
		if n > 0 && (cost < bestCost || cost == bestCost && score > best) {
			best = score
			bestCost = cost
			reader.index = idx
			reader.columns = columns[:n]
		}
	}
	consider(nil, r.info.PKColumns)
	for _, idx := range s.tableIndexes(r.table) {
		if !s.isClusteredIndex(idx) {
			consider(idx, idx.columnNames())
		}
	}
	return reader
}

func joinTypedValue(v Value, typ ColumnType) (Value, bool) {
	if v.Null {
		return v, true
	}
	if v.Type == typ {
		return v, typ != ColReal || v.Real != 0 && !math.IsNaN(v.Real)
	}
	if typ == ColInteger && v.Type == ColReal && v.Real >= -9223372036854775808.0 && v.Real < 9223372036854775808.0 && v.Real == float64(int64(v.Real)) {
		return NewIntValue(int64(v.Real)), true
	}
	if typ == ColReal && v.Type == ColInteger {
		r := float64(v.Int)
		if ValueCompare(v, NewRealValue(r)) == 0 && r != 0 {
			return NewRealValue(r), true
		}
	}
	return v, false
}

func (s *Schema) readerRows(reader *joinReader, left map[string]Value) ([]map[string]Value, error) {
	r := reader.relation
	key, valid := joinRowKey(left, reader.pairs, false)
	if !valid {
		return nil, nil
	}
	if reader.disk != nil {
		return nil, nil
	}
	if reader.buckets != nil {
		return reader.buckets[key], nil
	}
	if rows, ok := reader.cache[key]; ok {
		return rows, nil
	}
	if !r.loaded && len(reader.columns) > 0 {
		values := make(map[string]*tupleConstraint)
		typed := true
		for _, pair := range reader.pairs {
			v := left[pair.left]
			var typ ColumnType
			for _, c := range r.info.Columns {
				if c.Name == pair.right {
					typ = c.ColType
				}
			}
			v, ok := joinTypedValue(v, typ)
			typed = typed && ok
			values[pair.right] = &tupleConstraint{equal: &v}
		}
		if typed {
			path := buildAccessPath(r.info, reader.index, values)
			if path != nil {
				var rows []map[string]Value
				err := s.walkAccess(r.info, r.needed, []*accessPath{path}, func(row map[string]Value) error {
					rk, ok := joinRowKey(row, reader.pairs, true)
					if !ok || rk != key {
						return nil
					}
					if r.filter != nil {
						q := row
						if r.qualifiedFilter {
							q = copyRow(row)
							qualifyAccessRow(q, r.table, r.alias)
						}
						ok, err := s.evalCondition(r.filter, q)
						if err != nil {
							return err
						}
						if !ok {
							return nil
						}
					}
					rows = append(rows, copyRow(row))
					return nil
				})
				var bytes int64
				for _, row := range rows {
					bytes += rowMemory(row)
				}
				if err == nil && len(reader.cache) < 4096 && reader.cacheRows+len(rows) <= 8192 && reader.cacheBytes+bytes <= s.memoryBudget()/4 {
					reader.cache[key] = rows
					reader.cacheRows += len(rows)
					reader.cacheBytes += bytes
				}
				return rows, err
			}
		}
	}
	if reader.buckets == nil {
		if err := s.buildJoinBuckets(reader); err != nil {
			return nil, err
		}
	}
	return reader.buckets[key], nil
}

// Reuse one joined row while walking index probes/hash buckets. Only final
// result rows or group accumulators are retained, rather than every join stage.
func (s *Schema) walkJoinPlan(p *joinPlan, visit func(map[string]Value) error) error {
	// Exact local cardinalities guide ordering. Read selective indexed inputs
	// and small filtered dimensions once; unfiltered fact tables stay unopened.
	for _, r := range p.relations {
		load := r.filter != nil && r.total <= 10000
		if r.filter != nil && r.paths != nil && r.total > 10000 {
			n, err := s.countAccessPaths(r.paths, 65536)
			if err != nil {
				return err
			}
			r.count = float64(n)
			load = n <= 2048
		}
		if load && r.count*256 <= float64(s.memoryBudget()/4) {
			if err := s.loadJoinRelation(r); err != nil {
				return err
			}
		}
	}
	p.chooseOrder()
	row := make(map[string]Value)
	readers := make([]*joinReader, len(p.order))
	defer func() {
		for _, reader := range readers {
			if reader != nil && reader.disk != nil {
				reader.disk.close()
			}
		}
	}()
	predicates := make([][]Expression, len(p.order))
	var mask uint64
	estimatedLeft := 1.0
	for depth, i := range p.order {
		readers[depth] = s.joinReader(p.relations[i], p.pairs(i, mask))
		r := p.relations[i]
		hash := p.preferHashJoin(i, mask, estimatedLeft, readers[depth])
		if depth > 0 && len(readers[depth].pairs) > 0 && hash {
			// The cost decision must survive the memory guard. Build directly
			// from access paths rather than first retaining the entire relation;
			// buildJoinBuckets transitions to disk as the budget is reached.
			if err := s.buildJoinBuckets(readers[depth]); err != nil {
				return err
			}
		}
		if depth == 0 {
			estimatedLeft = r.count
		} else {
			estimatedLeft *= p.fanout(i, mask)
		}
		previous := mask
		mask |= 1 << i
		for _, pred := range p.predicates {
			if !p.fixed && pred.mask != 0 && pred.mask&(pred.mask-1) == 0 {
				local := true
				names, _ := s.expressionDependencies(pred.expr)
				for _, name := range names {
					owner, ok := p.owners[name]
					if !ok || owner < 0 || pred.mask != 1<<owner {
						local = false
						break
					}
				}
				if local {
					continue
				} // The complete local condition was checked by the reader.
			}
			if p.fixed && pred.stage == depth || !p.fixed && pred.mask&mask == pred.mask && (depth == 0 || pred.mask&previous != pred.mask) {
				predicates[depth] = append(predicates[depth], pred.expr)
			}
		}
	}
	var walk func(int) error
	var joinProfiles []int
	if s.profile != nil {
		parent := s.profile.parent()
		for _, i := range p.order {
			id := s.profile.operatorUnder(parent, "Join", p.relations[i].alias)
			parent = id
			joinProfiles = append(joinProfiles, id)
		}
	}
	walk = func(depth int) error {
		if err := s.checkInterrupt(); err != nil {
			return err
		}
		if depth == len(p.order) {
			if p.where != nil {
				ok, err := s.evalCondition(p.where, row)
				if err != nil {
					return err
				}
				if !ok {
					return nil
				}
			}
			return visit(row)
		}
		i := p.order[depth]
		var profile *OperatorProfile
		if len(joinProfiles) > 0 {
			id := joinProfiles[depth]
			profile = &s.profile.Operators[id]
			s.profile.stack = append(s.profile.stack, id)
			started := time.Now()
			defer func() {
				s.profile.Operators[id].ElapsedMS += float64(time.Since(started)) / float64(time.Millisecond)
				s.profile.stack = s.profile.stack[:len(s.profile.stack)-1]
			}()
		}
		reader := readers[depth]
		r := reader.relation
		var candidates []map[string]Value
		var err error
		if len(reader.pairs) > 0 {
			candidates, err = s.readerRows(reader, row)
		} else {
			err = s.loadJoinRelation(r)
			candidates = r.rows
		}
		if err != nil {
			return err
		}
		if profile != nil && profile.ID != s.profile.overflow {
			// readerRows can initialize a hash table on its first call.
			profile = &s.profile.Operators[joinProfiles[depth]]
			profile.Operator = "NestedLoopJoin"
			if depth == 0 {
				profile.Operator = "JoinInput"
			} else if reader.disk != nil {
				profile.Operator = "SpillHashJoin"
			} else if reader.buckets != nil {
				profile.Operator = "HashJoin"
			} else if len(reader.columns) > 0 {
				profile.Operator = "IndexProbeJoin"
			}
		}
		matchedAny := false
		acceptCandidate := func(candidate map[string]Value) error {
			if profile != nil {
				s.profile.Operators[joinProfiles[depth]].RowsExamined++
			}
			for k, v := range candidate {
				row[r.alias+"."+k] = v
				if p.firstOwner[k] == i {
					row[k] = v
				}
			}
			matched := true
			for _, expr := range predicates[depth] {
				ok, e := s.evalCondition(expr, row)
				if e != nil {
					return e
				}
				if !ok {
					matched = false
					break
				}
			}
			if matched {
				if profile != nil {
					s.profile.Operators[joinProfiles[depth]].RowsReturned++
				}
				matchedAny = true
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
			return nil
		}
		if reader.disk != nil {
			key, valid := joinRowKey(row, reader.pairs, false)
			if valid {
				if err := reader.disk.walk(key, acceptCandidate); err != nil {
					return err
				}
			}
		} else {
			for _, candidate := range candidates {
				if err := acceptCandidate(candidate); err != nil {
					return err
				}
			}
		}
		if r.left && !matchedAny {
			if profile != nil {
				s.profile.Operators[joinProfiles[depth]].RowsReturned++
			}
			for k := range r.needed {
				row[r.alias+"."+k] = NewNullValue()
				if p.firstOwner[k] == i {
					row[k] = NewNullValue()
				}
			}
			if err := walk(depth + 1); err != nil {
				return err
			}
		}
		for k := range r.needed {
			delete(row, r.alias+"."+k)
			if p.firstOwner[k] == i {
				delete(row, k)
			}
		}
		return nil
	}
	return walk(0)
}

func (p *joinPlan) description() string {
	var names []string
	var mask uint64
	for _, i := range p.order {
		r := p.relations[i]
		mode := "scan/hash"
		if len(p.pairs(i, mask)) > 0 {
			mode = "equality join"
		}
		if r.paths != nil {
			mode = "bounded index"
		}
		names = append(names, fmt.Sprintf("%s (%s; estimated rows=%.3g)", r.alias, mode, r.count))
		mask |= 1 << i
	}
	return strings.Join(names, " -> ")
}

func (p *joinPlan) preferHashJoin(i int, mask uint64, left float64, reader *joinReader) bool {
	r := p.relations[i]
	if r.distribution == nil || len(reader.columns) == 0 {
		return r.total > 256 && left > r.total/16
	}
	probeCost := left * (4 + 3*p.fanout(i, mask))
	hashCost := r.total + r.count + left
	return probeCost > hashCost
}
