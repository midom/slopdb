package sql

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

func q5OrderPlan(distributions bool) *joinPlan {
	p := &joinPlan{owners: make(map[string]int)}
	names := []string{"customer", "orders", "lineitem", "supplier", "nation", "region"}
	columns := [][]string{{"id", "nation"}, {"id", "customer"}, {"order", "supplier", "line"}, {"id", "nation"}, {"id", "region"}, {"id"}}
	totals := []float64{30000, 300000, 1200000, 2000, 25, 5}
	distinct := [][]float64{{30000, 25}, {300000, 30000}, {300000, 2000, 7}, {2000, 25}, {25, 5}, {5}}
	for i, name := range names {
		info := &TableInfo{Name: name, PKColumns: []string{columns[i][0]}}
		if i == 2 {
			info.PKColumns = []string{"order", "line"}
		}
		r := &joinRelation{table: name, alias: name, info: info, total: totals[i], count: totals[i]}
		if distributions {
			r.distribution = &tableStatistics{Rows: int64(totals[i]), Columns: make(map[string]*columnStatistics)}
		}
		for j, column := range columns[i] {
			info.Columns = append(info.Columns, ColumnDef{Name: column, ColType: ColInteger, NotNull: true})
			p.owners[name+"."+column] = i
			if distributions {
				r.distribution.Columns[column] = &columnStatistics{Type: ColInteger, DistinctEstimate: distinct[i][j]}
			}
		}
		p.relations = append(p.relations, r)
	}
	p.relations[1].count = 60000
	p.relations[5].count = 1
	p.edges = []joinEdge{{0, 1, "id", "customer"}, {1, 2, "id", "order"}, {2, 3, "supplier", "id"}, {0, 3, "nation", "nation"}, {3, 4, "nation", "id"}, {4, 5, "region", "id"}}
	return p
}

func orderCost(p *joinPlan, order []int) float64 {
	rows, cost := 1.0, 0.0
	var mask uint64
	for _, i := range order {
		rows *= p.fanout(i, mask)
		cost += rows
		mask |= 1 << i
	}
	return cost
}

func TestJoinOrderLooksAheadPastDimensionFanout(t *testing.T) {
	for _, analyzed := range []bool{false, true} {
		p := q5OrderPlan(analyzed)
		// Q5 repeats its ON equalities in WHERE. Repeating SQL conditions must
		// neither square estimated selectivity nor duplicate physical hash keys.
		before := p.fanout(0, 1<<3)
		p.edges = append(p.edges, append([]joinEdge(nil), p.edges...)...)
		if after := p.fanout(0, 1<<3); before != after {
			t.Fatalf("repeated predicates changed fanout %g to %g", before, after)
		}
		if got := len(p.pairs(0, 1<<3)); got != 1 {
			t.Fatalf("duplicate physical keys: %d", got)
		}
		if before < 1000 {
			t.Fatalf("same-nation customer expansion underestimated: %g", before)
		}
		p.chooseOrder()
		cost := orderCost(p, p.order)
		// Exhaustive six-relation enumeration provides an independent optimal
		// cost under the model, without blessing one hand-picked SQL order.
		best := math.Inf(1)
		var enumerate func([]int, uint64)
		enumerate = func(order []int, mask uint64) {
			if len(order) == len(p.relations) {
				best = math.Min(best, orderCost(p, order))
				return
			}
			for i := range p.relations {
				if mask&(1<<i) == 0 {
					enumerate(append(order, i), mask|1<<i)
				}
			}
		}
		enumerate(nil, 0)
		if math.Abs(cost-best) > 0.00001 {
			t.Fatalf("DP cost %g, exhaustive optimum %g; order %v", cost, best, p.order)
		}
		positions := make(map[int]int)
		for i, relation := range p.order {
			positions[relation] = i
		}
		if positions[0] < positions[1] && positions[3] < positions[1] {
			t.Fatalf("customer/supplier product appears before its fact-table connection: %v", p.order)
		}
		chosen := append([]int(nil), p.order...)
		p.chooseOrder()
		if !reflect.DeepEqual(chosen, p.order) {
			t.Fatal("join order changed without changing input estimates")
		}
	}
}

func TestJoinCostEquivalentKeys(t *testing.T) {
	p := q5OrderPlan(true)
	// Connect customer/supplier nation keys before adding nation. They are
	// equivalent because their predicate was already enforced, so this is one
	// equality condition on nation.id rather than two independent conditions.
	pairs := p.costPairs(4, (1<<0)|(1<<3))
	if len(pairs) != 1 {
		t.Fatalf("equivalent sources produced %d selectivity factors", len(pairs))
	}
	p.edges = append(p.edges, joinEdge{0, 4, "nation", "id"})
	if got := len(p.costPairs(4, (1<<0)|(1<<3))); got != 1 {
		t.Fatalf("transitive equality counted %d times", got)
	}
	// The same target with independent source columns does impose a second
	// condition; only relations already joined may establish equivalence.
	p.edges = append(p.edges, joinEdge{1, 4, "customer", "id"})
	if got := len(p.costPairs(4, (1<<0)|(1<<1)|(1<<3))); got != 2 {
		t.Fatalf("independent equality sources collapsed: %d", got)
	}
}

func TestJoinOrderPreservesLeftAndBoundsLargeGraphs(t *testing.T) {
	p := q5OrderPlan(true)
	p.fixed = true
	p.chooseOrder()
	if !reflect.DeepEqual(p.order, []int{0, 1, 2, 3, 4, 5}) {
		t.Fatalf("LEFT order changed: %v", p.order)
	}
	large := &joinPlan{owners: make(map[string]int)}
	for i := 0; i < joinOrderDPRelations+1; i++ {
		large.relations = append(large.relations, &joinRelation{info: &TableInfo{}, total: 2, count: 2})
	}
	large.chooseOrder()
	if len(large.order) != len(large.relations) {
		t.Fatalf("large graph omitted relations: %v", large.order)
	}
	if joinCostProduct(math.MaxFloat64, 2) != math.MaxFloat64 || joinCostSum(math.MaxFloat64, math.MaxFloat64) != math.MaxFloat64 {
		t.Fatal("cost arithmetic overflowed")
	}
}

func TestJoinOrderAvoidsDimensionProductInExecution(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	statisticsSQL(t, s, "CREATE TABLE region(id INTEGER PRIMARY KEY,name TEXT);CREATE TABLE nation(id INTEGER PRIMARY KEY,region INTEGER,INDEX nr(region));CREATE TABLE supplier(id INTEGER PRIMARY KEY,nation INTEGER,INDEX sn(nation));CREATE TABLE customer(id INTEGER PRIMARY KEY,nation INTEGER,INDEX cn(nation));CREATE TABLE orders(id INTEGER PRIMARY KEY,customer INTEGER,created TEXT,INDEX oc(customer));CREATE TABLE lineitem(order_id INTEGER,supplier INTEGER,line INTEGER,amount INTEGER,PRIMARY KEY(order_id,line),INDEX ls(supplier))")
	statisticsSQL(t, s, "INSERT INTO region VALUES(0,'ASIA'),(1,'OTHER');INSERT INTO nation VALUES(0,0),(1,0),(2,1),(3,1)")
	var supplier, customer, orders, lines []string
	for id := 0; id < 400; id++ {
		supplier = append(supplier, fmt.Sprintf("(%d,%d)", id, id/100))
	}
	for id := 0; id < 200; id++ {
		country := id / 50
		customer = append(customer, fmt.Sprintf("(%d,%d)", id, country))
		orders = append(orders, fmt.Sprintf("(%d,%d,'1994-06-01')", id, id))
		lines = append(lines, fmt.Sprintf("(%d,%d,0,10)", id, country*100+id%50), fmt.Sprintf("(%d,%d,1,20)", id, ((country+1)%4)*100+id%50))
	}
	for _, load := range []struct {
		table string
		rows  []string
	}{{"supplier", supplier}, {"customer", customer}, {"orders", orders}, {"lineitem", lines}} {
		statisticsSQL(t, s, "INSERT INTO "+load.table+" VALUES"+strings.Join(load.rows, ","))
	}
	query := `SELECT COUNT(*) AS n,SUM(lineitem.amount) AS total FROM customer JOIN orders ON customer.id=orders.customer JOIN lineitem ON lineitem.order_id=orders.id JOIN supplier ON lineitem.supplier=supplier.id AND customer.nation=supplier.nation JOIN nation ON supplier.nation=nation.id JOIN region ON nation.region=region.id WHERE customer.id=orders.customer AND lineitem.order_id=orders.id AND lineitem.supplier=supplier.id AND customer.nation=supplier.nation AND supplier.nation=nation.id AND nation.region=region.id AND region.name='ASIA' AND orders.created>='1994-01-01' AND orders.created<'1995-01-01'`
	statisticsSQL(t, s, "ANALYZE")
	distributions := s.distributions
	for _, analyzed := range []bool{false, true} {
		if analyzed {
			s.distributions = distributions
		} else {
			s.distributions = nil
		}
		result := statisticsSQL(t, s, query)
		if len(result.Rows) != 1 || result.Rows[0]["n"].Int != 100 || result.Rows[0]["total"].Int != 1000 {
			t.Fatalf("analyzed=%v: incorrect joined answer %v", analyzed, result.Rows)
		}
		explanation := statisticsSQL(t, s, "EXPLAIN ANALYZE "+query)
		if explanation.Profile == nil {
			t.Fatal("missing actual execution evidence")
		}
		var candidates int64
		for _, operator := range explanation.Profile.Operators {
			if strings.Contains(operator.Operator, "Join") {
				candidates += operator.RowsExamined
			}
		}
		if candidates > 3000 {
			t.Fatalf("analyzed=%v: join examined %d candidates, implying supplier/customer product", analyzed, candidates)
		}
	}
}
