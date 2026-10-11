package sql

// Distribution statistics are planning hints, never constraints. They describe
// the ANALYZE statement snapshot and can be stale after later writes.
import (
	"bytes"
	"encoding/json"
	"math"
	"sort"
	"strings"
)

const distributionPrefix = "\x00__column_stats__"
const statisticsSampleSize = 2048
const statisticsDistinctSize = 256

// Use a struct encoding rather than Value's JSON methods: those methods do not
// preserve all int64, REAL and BLOB types when decoding catalog metadata.
type statisticValue Value
type statisticFrequency struct {
	Value    statisticValue `json:"value"`
	Fraction float64        `json:"fraction"`
}
type columnStatistics struct {
	Type             ColumnType           `json:"type"`
	NullFraction     float64              `json:"null_fraction"`
	DistinctEstimate float64              `json:"distinct_estimate"`
	SampleRows       int                  `json:"sample_rows"`
	Common           []statisticFrequency `json:"common,omitempty"`
	Histogram        []statisticValue     `json:"histogram,omitempty"`
}
type tableStatistics struct {
	Rows    int64                        `json:"rows"`
	Columns map[string]*columnStatistics `json:"columns"`
}
type distributionMetadata struct {
	Version int                         `json:"version"`
	Tables  map[string]*tableStatistics `json:"tables"`
}

func (s *Schema) loadDistributions() {
	data, ok, err := s.schemaGet([]byte(distributionPrefix))
	if err != nil || !ok {
		return
	}
	var meta distributionMetadata
	if json.Unmarshal(data, &meta) == nil && meta.Version == 1 && meta.Tables != nil {
		s.distributions = meta.Tables
	}
}

func (s *Schema) saveDistributions() error {
	if s.inTx {
		return nil
	}
	data, err := json.Marshal(distributionMetadata{1, s.distributions})
	if err != nil {
		return err
	}
	return s.write([]byte(distributionPrefix), data)
}

type distributionBuilder struct {
	definition ColumnDef
	nulls      int64
	distinct   map[uint64]bool
	maximum    uint64
	sample     []Value
	incomplete bool
}

func (b *distributionBuilder) addDistinct(hash uint64) {
	if len(b.distinct) == statisticsDistinctSize && hash >= b.maximum {
		return
	}
	if b.distinct[hash] {
		return
	}
	b.distinct[hash] = true
	if len(b.distinct) > statisticsDistinctSize {
		delete(b.distinct, b.maximum)
		b.maximum = 0
		for h := range b.distinct {
			if h > b.maximum {
				b.maximum = h
			}
		}
	} else if hash > b.maximum {
		b.maximum = hash
	}
}

// A fixed-size row reservoir, exact NULL counts and a KMV distinct sketch keep
// ANALYZE memory bounded by column count, independent of table cardinality.
func (s *Schema) analyzeTable(info *TableInfo) (*tableStatistics, error) {
	builders := make([]distributionBuilder, len(info.Columns))
	for i, c := range info.Columns {
		builders[i] = distributionBuilder{definition: c, distinct: make(map[uint64]bool)}
	}
	var count int64
	rng := uint64(0x9e3779b97f4a7c15)
	err := s.walkAccess(info, nil, nil, func(row map[string]Value) error {
		count++
		slot := int64(-1)
		if count <= statisticsSampleSize {
			slot = count - 1
		} else {
			rng ^= rng << 13
			rng ^= rng >> 7
			rng ^= rng << 17
			slot = int64(rng % uint64(count))
			if slot >= statisticsSampleSize {
				slot = -1
			}
		}
		for i := range builders {
			b := &builders[i]
			v := row[b.definition.Name]
			if v.Null {
				b.nulls++
			} else {
				// All values have column affinity. Canonicalize signed REAL zero.
				if v.Type == ColReal && v.Real == 0 {
					v.Real = 0
				}
				b.addDistinct(statisticsHash(v))
			}
			if slot >= 0 {
				if len(v.Text) > 512 || len(v.Blob) > 512 {
					b.incomplete = true
					v = NewNullValue()
				}
				v.Blob = copyBytes(v.Blob)
				if int(slot) == len(b.sample) {
					b.sample = append(b.sample, v)
				} else {
					b.sample[slot] = v
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := &tableStatistics{Rows: count, Columns: make(map[string]*columnStatistics)}
	for _, b := range builders {
		c := &columnStatistics{Type: b.definition.ColType, SampleRows: len(b.sample), DistinctEstimate: float64(len(b.distinct))}
		if count > 0 {
			c.NullFraction = float64(b.nulls) / float64(count)
		}
		if len(b.distinct) == statisticsDistinctSize && b.maximum > 0 {
			c.DistinctEstimate = math.Min(float64(count-b.nulls), float64(statisticsDistinctSize-1)*math.Exp2(64)/float64(b.maximum))
		}
		if !b.incomplete {
			values := make([]Value, 0, len(b.sample))
			for _, v := range b.sample {
				if !v.Null {
					values = append(values, v)
				}
			}
			sort.Slice(values, func(i, j int) bool { return ValueCompare(values[i], values[j]) < 0 })
			var frequencies []statisticFrequency
			for i := 0; i < len(values); {
				j := i + 1
				for j < len(values) && ValueCompare(values[i], values[j]) == 0 {
					j++
				}
				if j-i > 1 {
					frequencies = append(frequencies, statisticFrequency{statisticValue(values[i]), float64(j-i) / float64(len(b.sample))})
				}
				i = j
			}
			sort.Slice(frequencies, func(i, j int) bool { return frequencies[i].Fraction > frequencies[j].Fraction })
			if len(frequencies) > 16 {
				frequencies = frequencies[:16]
			}
			c.Common = frequencies
			// Store at most 128 quantiles, including endpoints. Repeated
			// quantiles deliberately preserve skew rather than deduplicating it.
			n := len(values)
			if n > 128 {
				n = 128
			}
			for i := 0; i < n; i++ {
				pos := 0
				if n > 1 {
					pos = i * (len(values) - 1) / (n - 1)
				}
				c.Histogram = append(c.Histogram, statisticValue(values[pos]))
			}
		}
		out.Columns[b.definition.Name] = c
	}
	return out, nil
}

func (s *Schema) columnDistribution(table, column string) *columnStatistics {
	if n := strings.LastIndexByte(column, '.'); n >= 0 {
		column = column[n+1:]
	}
	t := s.distributions[table]
	if t == nil {
		return nil
	}
	c := t.Columns[column]
	if c == nil {
		return nil
	}
	// DDL invalidates statistics, but validate types on load too.
	info := s.tables[table]
	if info == nil {
		return nil
	}
	for _, d := range info.Columns {
		if d.Name == column && d.ColType == c.Type {
			return c
		}
	}
	return nil
}

func clampSelectivity(f float64) float64 { return math.Max(0, math.Min(1, f)) }
func (c *columnStatistics) equality(v Value) float64 {
	if v.Null {
		return 0
	}
	mass := c.NullFraction
	for _, m := range c.Common {
		if ValueCompare(Value(m.Value), v) == 0 {
			return m.Fraction
		}
		mass += m.Fraction
	}
	return clampSelectivity((1 - mass) / math.Max(1, c.DistinctEstimate-float64(len(c.Common))))
}
func (c *columnStatistics) rangeFraction(op string, v Value) float64 {
	if v.Null {
		return 0
	}
	if len(c.Histogram) == 0 {
		return (1 - c.NullFraction) / 3
	}
	match := 0
	for _, sample := range c.Histogram {
		cmp := ValueCompare(Value(sample), v)
		if op == "<" && cmp < 0 || op == "<=" && cmp <= 0 || op == ">" && cmp > 0 || op == ">=" && cmp >= 0 {
			match++
		}
	}
	return float64(match) / float64(len(c.Histogram)) * (1 - c.NullFraction)
}

// Unknown predicates retain conservative defaults; estimates do not decide
// whether a predicate can be elided or whether an index bound is SQL-safe.
func (s *Schema) predicateSelectivity(table string, e Expression) float64 {
	if e == nil {
		return 1
	}
	if s.distributions[table] == nil {
		return 0.1
	}
	if b, ok := e.(*BinaryExpr); ok {
		if b.Op == "AND" || b.Op == "OR" {
			a := s.predicateSelectivity(table, b.Left)
			c := s.predicateSelectivity(table, b.Right)
			if b.Op == "AND" {
				return a * c
			}
			return clampSelectivity(a + c - a*c)
		}
		col, ok := b.Left.(*ColumnRefExpr)
		literal, lok := statisticLiteral(b.Right)
		op := b.Op
		if !ok || !lok {
			col, ok = b.Right.(*ColumnRefExpr)
			literal, lok = statisticLiteral(b.Left)
			switch op {
			case "<":
				op = ">"
			case ">":
				op = "<"
			case "<=":
				op = ">="
			case ">=":
				op = "<="
			}
		}
		if ok && lok {
			if c := s.columnDistribution(table, col.Name); c != nil {
				switch op {
				case "=":
					return c.equality(literal)
				case "!=", "<>":
					if literal.Null {
						return 0
					}
					return clampSelectivity(1 - c.NullFraction - c.equality(literal))
				case "<", ">", "<=", ">=":
					return c.rangeFraction(op, literal)
				case "IS":
					if literal.Null {
						return c.NullFraction
					}
					return c.equality(literal)
				case "IS NOT":
					if literal.Null {
						return 1 - c.NullFraction
					}
				}
			}
		}
	}
	if b, ok := e.(*BetweenExpr); ok {
		if col, ok := b.Expr.(*ColumnRefExpr); ok {
			lo, lok := statisticLiteral(b.Low)
			hi, hok := statisticLiteral(b.High)
			if c := s.columnDistribution(table, col.Name); c != nil && lok && hok {
				if lo.Null || hi.Null {
					return 0
				}
				fraction := clampSelectivity(c.rangeFraction("<=", hi) - c.rangeFraction("<", lo))
				if b.Not {
					return clampSelectivity(1 - c.NullFraction - fraction)
				}
				return fraction
			}
		}
	}
	if in, ok := e.(*InListExpr); ok {
		if col, ok := in.Expr.(*ColumnRefExpr); ok {
			if c := s.columnDistribution(table, col.Name); c != nil {
				f := 0.0
				seen := make(map[string]bool)
				for _, x := range in.List {
					v, ok := statisticLiteral(x)
					if !ok {
						return 0.1
					}
					if v.Null && in.Not {
						return 0
					}
					k := string(appendClusteredValue(nil, v))
					if !seen[k] {
						f += c.equality(v)
						seen[k] = true
					}
				}
				if in.Not {
					return clampSelectivity(1 - c.NullFraction - f)
				}
				return clampSelectivity(f)
			}
		}
	}
	return 0.1
}

// Estimate only the leading usable index components, not residual predicates.
func (s *Schema) accessEstimate(info *TableInfo, idx *IndexInfo, constraints map[string]*tupleConstraint) (float64, bool) {
	t := s.distributions[info.Name]
	if t == nil {
		return 0, false
	}
	columns := info.PKColumns
	if idx != nil {
		columns = idx.columnNames()
	}
	f := 1.0
	known := false
	for _, name := range columns {
		b := constraints[name]
		if b == nil {
			break
		}
		c := s.columnDistribution(info.Name, name)
		if c == nil {
			return 0, false
		}
		known = true
		if b.equal != nil {
			f *= c.equality(*b.equal)
			continue
		}
		if b.listTerm != nil {
			sum := 0.0
			for _, v := range b.list {
				sum += c.equality(v)
			}
			f *= clampSelectivity(sum)
			continue
		}
		low, high := 0.0, 1-c.NullFraction
		if b.lower != nil {
			op := "<"
			if b.lowerStrict {
				op = "<="
			}
			low = c.rangeFraction(op, *b.lower)
		}
		if b.upper != nil {
			op := "<="
			if b.upperStrict {
				op = "<"
			}
			high = c.rangeFraction(op, *b.upper)
		}
		fraction := clampSelectivity(high - low)
		if idx != nil {
			// Legacy secondary TEXT/BLOB keys do not terminate components.
			// Correct physical bounds can include much more than the SQL
			// interval, including proper-prefix delimiter bands. Cost those
			// actual encoded bounds rather than the residual SQL predicate.
			intervals, exact := accessRangeIntervals(nil, name, info, idx, b)
			if !exact {
				fraction = 1 - c.NullFraction
				if len(c.Histogram) > 0 {
					matched := 0
					for _, sample := range c.Histogram {
						key := appendAccessValue(nil, Value(sample), idx)
						for _, interval := range intervals {
							if bytes.Compare(key, interval.lower) >= 0 && bytes.Compare(key, interval.upper) < 0 {
								matched++
								break
							}
						}
					}
					fraction = float64(matched) / float64(len(c.Histogram)) * (1 - c.NullFraction)
				}
			}
		}
		f *= fraction
		break
	}
	return math.Max(1, float64(t.Rows)*f), known
}

func accessCost(rows float64, secondary, covered bool) float64 {
	if secondary && !covered {
		// Small probes often reuse cached blocks. Large non-covering ranges
		// revisit row blocks in secondary-key order; a sequential typed scan
		// is often cheaper for broad predicates. Keep the large-range weight
		// at six: twelve rejected useful ~15% ranges and scanned/decompressed
		// the whole table (TPC-H Q6), even with batched cached point lookups.
		lookupWeight := 3 + 3*math.Min(1, rows/4096)
		return 4 + rows*lookupWeight
	}
	return 2 + rows
}

// Legacy readers must respect a costed scan decision instead of rediscovering
// the rejected secondary index through an equality-only compatibility path.
func (s *Schema) distributionScanPreferred(stmt *SelectStmt, info *TableInfo) bool {
	t := s.distributions[info.Name]
	if t == nil || stmt.Where == nil || len(stmt.Joins) > 0 || len(stmt.OrderBy) > 0 {
		return false
	}
	constraints := s.accessConstraints(stmt.Where, info)
	best := math.Inf(1)
	consider := func(idx *IndexInfo) {
		if buildAccessPath(info, idx, constraints) == nil {
			return
		}
		rows, known := s.accessEstimate(info, idx, constraints)
		if !known {
			return
		}
		covered := idx == nil
		if idx != nil {
			if payload := s.lookups().covering[idx]; payload != nil {
				covered = queryCovered(stmt, info.Name, stmt.FromAlias, info, payload)
			}
		}
		best = math.Min(best, accessCost(rows, idx != nil, covered))
	}
	consider(nil)
	for _, idx := range s.tableIndexes(info.Name) {
		if !s.isClusteredIndex(idx) {
			consider(idx)
		}
	}
	return best > float64(t.Rows) && best < math.Inf(1)
}

func statisticLiteral(e Expression) (Value, bool) {
	literal, ok := tupleLiteral(e)
	if !ok {
		return Value{}, false
	}
	return literal.Value, true
}

// Stable FNV-1a plus a 64-bit avalanche gives reproducible KMV estimates and
// spreads related sortable integer encodings across the full hash domain.
func statisticsHash(v Value) uint64 {
	h := uint64(14695981039346656037)
	for _, b := range appendClusteredValue(nil, v) {
		h ^= uint64(b)
		h *= 1099511628211
	}
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31
	return h
}

func joinDistributionSelectivity(left, right *columnStatistics) float64 {
	if left == nil {
		return (1 - right.NullFraction) / math.Max(1, right.DistinctEstimate)
	}
	common := 0.0
	leftMass, rightMass := 0.0, 0.0
	for _, m := range left.Common {
		leftMass += m.Fraction
	}
	for _, m := range right.Common {
		rightMass += m.Fraction
	}
	leftTailUsed, rightTailUsed := 0.0, 0.0
	leftTailValues, rightTailValues := 0, 0
	for _, a := range left.Common {
		fraction := right.equality(Value(a.Value))
		common += a.Fraction * fraction
		found := false
		for _, b := range right.Common {
			if ValueCompare(Value(a.Value), Value(b.Value)) == 0 {
				found = true
				break
			}
		}
		if !found {
			rightTailUsed += fraction
			rightTailValues++
		}
	}
	for _, b := range right.Common {
		found := false
		for _, a := range left.Common {
			if ValueCompare(Value(a.Value), Value(b.Value)) == 0 {
				found = true
				break
			}
		}
		if !found {
			fraction := left.equality(Value(b.Value))
			common += b.Fraction * fraction
			leftTailUsed += fraction
			leftTailValues++
		}
	}
	leftTail := math.Max(0, 1-left.NullFraction-leftMass-leftTailUsed)
	rightTail := math.Max(0, 1-right.NullFraction-rightMass-rightTailUsed)
	domain := math.Max(1, math.Max(left.DistinctEstimate-float64(len(left.Common)+leftTailValues), right.DistinctEstimate-float64(len(right.Common)+rightTailValues)))
	return clampSelectivity(common + leftTail*rightTail/domain)
}
