package sql

import (
	"math"
	"sort"
)

// ON and WHERE often repeat the same equality. Additional sources for the same
// target key are also redundant if earlier joins have already equated them.
// Keep every original SQL predicate and physical reader key; only costing uses
// equivalence classes, restricted to columns of relations already joined.
func (p *joinPlan) costPairs(i int, mask uint64) []joinKeyPair {
	pairs := p.pairs(i, mask)
	targets := make(map[string]bool)
	repeated := false
	for _, pair := range pairs {
		repeated = repeated || targets[pair.right]
		targets[pair.right] = true
	}
	if !repeated {
		return pairs
	}
	parent := make(map[string]string)
	var find func(string) string
	find = func(x string) string {
		if next, ok := parent[x]; ok && next != x {
			parent[x] = find(next)
			return parent[x]
		}
		return x
	}
	for _, edge := range p.edges {
		if mask&(1<<edge.a) == 0 || mask&(1<<edge.b) == 0 {
			continue
		}
		a := find(p.relations[edge.a].alias + "." + edge.ac)
		b := find(p.relations[edge.b].alias + "." + edge.bc)
		if a != b {
			parent[b] = a
		}
	}
	type equality struct{ source, target string }
	seen := make(map[equality]bool)
	out := make([]joinKeyPair, 0, len(pairs))
	for _, pair := range pairs {
		key := equality{find(pair.left), pair.right}
		if !seen[key] {
			out = append(out, pair)
			seen[key] = true
		}
	}
	return out
}

// Equated columns describe one key domain, even when that domain's small
// dimension has not entered the physical order yet. Without ANALYZE, table
// cardinalities provide conservative distinct upper bounds; with statistics,
// distinct estimates improve them. These are cost hints, never constraints.
func (p *joinPlan) equalityDomain(column string) float64 {
	if p.keyDomains == nil {
		parent := make(map[string]string)
		var find func(string) string
		find = func(x string) string {
			if next, ok := parent[x]; ok && next != x {
				parent[x] = find(next)
				return parent[x]
			}
			return x
		}
		for _, edge := range p.edges {
			a := find(p.relations[edge.a].alias + "." + edge.ac)
			b := find(p.relations[edge.b].alias + "." + edge.bc)
			if a != b {
				parent[b] = a
			}
		}
		domains := make(map[string]float64)
		for _, r := range p.relations {
			for _, c := range r.info.Columns {
				root := find(r.alias + "." + c.Name)
				bound := math.Max(1, r.total)
				if r.distribution != nil {
					if d := r.distribution.Columns[c.Name]; d != nil {
						bound = math.Max(1, d.DistinctEstimate)
					}
				}
				if prior, ok := domains[root]; !ok || bound < prior {
					domains[root] = bound
				}
			}
		}
		p.keyDomains = make(map[string]float64)
		for _, r := range p.relations {
			for _, c := range r.info.Columns {
				name := r.alias + "." + c.Name
				p.keyDomains[name] = domains[find(name)]
			}
		}
	}
	return math.Max(1, p.keyDomains[column])
}

const joinOrderDPRelations = 12
const joinOrderFrontier = 4

type joinOrderState struct {
	cost, rows float64
	order      []int
}

// For a given subset, future fanouts depend on that subset, not its order.
// Discard states dominated in both accumulated work and output cardinality.
// Bound the remaining frontier, retaining the cheapest and smallest-output
// alternatives so estimation differences do not turn planning exponential.
func addJoinOrderState(frontier []joinOrderState, candidate joinOrderState) []joinOrderState {
	for _, state := range frontier {
		if state.cost <= candidate.cost && state.rows <= candidate.rows {
			return frontier
		}
	}
	kept := frontier[:0]
	for _, state := range frontier {
		if candidate.cost <= state.cost && candidate.rows <= state.rows {
			continue
		}
		kept = append(kept, state)
	}
	kept = append(kept, candidate)
	sort.SliceStable(kept, func(i, j int) bool { return kept[i].cost < kept[j].cost })
	if len(kept) > joinOrderFrontier {
		smallest := 0
		for i := range kept {
			if kept[i].rows < kept[smallest].rows {
				smallest = i
			}
		}
		if smallest >= joinOrderFrontier {
			kept[joinOrderFrontier-1] = kept[smallest]
		}
		kept = kept[:joinOrderFrontier]
	}
	return kept
}

func joinCostProduct(a, b float64) float64 {
	if a == 0 || b == 0 {
		return 0
	}
	if a > math.MaxFloat64/b {
		return math.MaxFloat64
	}
	return a * b
}
func joinCostSum(a, b float64) float64 {
	if a > math.MaxFloat64-b {
		return math.MaxFloat64
	}
	return a + b
}

// Search connected alternatives beyond one greedy next step. Small dimension
// branches can appear cheap individually but create a huge Cartesian fanout
// before their connecting fact table arrives (TPC-H Q5 is one example).
// LEFT joins retain SQL order; unusually large graphs keep bounded greedy work.
func (p *joinPlan) chooseOrder() {
	n := len(p.relations)
	if p.fixed || n > joinOrderDPRelations || n == 0 {
		p.chooseGreedyOrder()
		return
	}
	states := make([][]joinOrderState, 1<<n)
	for i, r := range p.relations {
		states[1<<i] = []joinOrderState{{cost: r.count, rows: r.count, order: []int{i}}}
	}
	for mask := 1; mask < len(states); mask++ {
		if len(states[mask]) == 0 {
			continue
		}
		for i := range p.relations {
			if mask&(1<<i) != 0 {
				continue
			}
			factor := p.fanout(i, uint64(mask))
			for _, state := range states[mask] {
				rows := joinCostProduct(state.rows, factor)
				order := make([]int, len(state.order)+1)
				copy(order, state.order)
				order[len(state.order)] = i
				next := mask | 1<<i
				states[next] = addJoinOrderState(states[next], joinOrderState{cost: joinCostSum(state.cost, rows), rows: rows, order: order})
			}
		}
	}
	full := states[len(states)-1]
	p.order = append(p.order[:0], full[0].order...)
}
