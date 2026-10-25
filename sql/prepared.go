package sql

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
)

// PreparedQuery keeps an immutable parsed template. Each execution borrows its
// own AST and literal cells; binding neither reparses SQL nor shares mutable
// argument values between concurrent executions. Each private tree also retains
// compiled execution structure, invalidated against the current catalog and
// statistics; runtime bounds and costs use current arguments and snapshots.
type PreparedQuery struct {
	template Statement
	count    int
	pooled   bool
	pool     sync.Pool
	// Assign a counter shard only when building a private AST, never on a hit.
	nextCounter atomic.Uint64
	counters    [preparedCounterShards]preparedCounter
}

const preparedCounterShards = 128

type preparedCounter struct {
	builds  atomic.Uint64
	hits    atomic.Uint64
	padding [48]byte
}

// Prepare accepts one statement and anonymous positional '?' parameters in
// value expressions and LIMIT/OFFSET. Schema definitions cannot contain bound
// parameters because their expression SQL must survive database reopening.
func Prepare(query string) (*PreparedQuery, error) {
	return PrepareWithOptions(query, ParseOptions{})
}

func PrepareWithOptions(query string, options ParseOptions) (*PreparedQuery, error) {
	p := &Parser{tok: NewTokenizerWithOptions(query, options), parameters: true}
	statements, err := p.parse()
	if err != nil {
		return nil, err
	}
	if len(statements) != 1 {
		return nil, fmt.Errorf("Prepare requires exactly one statement")
	}
	// Identifiers and aliases are not parameter positions. Some dialect parsing
	// paths consume a name without parsePrimary, so check for swallowed markers.
	t := NewTokenizerWithOptions(query, options)
	markers := 0
	for tok := t.Next(); tok.Type != TokEOF; tok = t.Next() {
		if tok.Type == TokParameter {
			markers++
		}
	}
	if markers != p.parameterCount {
		return nil, fmt.Errorf("parameters are allowed only in value expressions and LIMIT/OFFSET")
	}
	q := &PreparedQuery{template: statements[0], count: p.parameterCount, pooled: reusableStatement(statements[0])}
	if q.count > 0 && !q.pooled {
		return nil, fmt.Errorf("parameters are not supported in schema definitions")
	}
	q.pool.New = func() any { return q.newBound() }
	return q, nil
}

func reusableStatement(stmt Statement) bool {
	switch s := stmt.(type) {
	case *SelectStmt, *InsertStmt, *UpdateStmt, *DeleteStmt, *UnionStmt, *ReplicationPositionStmt, *ReadBarrierStmt, *SetIsolationStmt, *SetConstraintsStmt,
		*ShowStmt, *PragmaStmt, *BeginStmt, *CommitStmt, *RollbackStmt,
		*SavepointStmt, *ReleaseStmt:
		return true
	case *ExplainStmt:
		return reusableStatement(s.Stmt)
	}
	return false
}

func (q *PreparedQuery) NumInput() int { return q.count }

type limitBinding struct {
	value  *int64
	index  int
	offset bool
}

// BoundQuery belongs to one execution until Release. Its AST must not be
// retained or modified by the caller after release.
type BoundQuery struct {
	query      *PreparedQuery
	statements []Statement
	literals   []*LiteralExpr
	limits     []limitBinding
}

func (q *PreparedQuery) Bind(args []Value) (*BoundQuery, error) {
	if len(args) != q.count {
		return nil, fmt.Errorf("expected %d arguments, got %d", q.count, len(args))
	}
	var b *BoundQuery
	if q.pooled {
		b = q.pool.Get().(*BoundQuery)
	} else {
		b = q.newBound()
	}
	for _, limit := range b.limits {
		v := args[limit.index]
		if v.Null || v.Type != ColInteger {
			b.Release()
			return nil, fmt.Errorf("LIMIT/OFFSET argument %d must be an integer", limit.index+1)
		}
		*limit.value = v.Int
		if limit.offset && *limit.value < 0 {
			*limit.value = 0
		}
	}
	for i, literal := range b.literals {
		if literal != nil {
			literal.Value = args[i]
		}
	}
	return b, nil
}

func (b *BoundQuery) Statements() []Statement { return b.statements }

func (b *BoundQuery) Release() {
	for _, statement := range b.statements {
		switch st := statement.(type) {
		case *SelectStmt:
			if st.prepared != nil {
				st.prepared.releaseValues()
			}
		case *UpdateStmt:
			if st.prepared != nil {
				st.prepared.releaseValues()
			}
		}
	}
	for _, literal := range b.literals {
		if literal != nil {
			literal.Value = Value{}
		}
	}
	if b.query.pooled {
		b.query.pool.Put(b)
	}
}

func (q *PreparedQuery) newBound() *BoundQuery {
	b := &BoundQuery{query: q, literals: make([]*LiteralExpr, q.count)}
	counter := &q.counters[(q.nextCounter.Add(1)-1)%preparedCounterShards]
	stmt := b.clone(reflect.ValueOf(q.template)).Interface().(Statement)
	switch st := stmt.(type) {
	case *SelectStmt:
		if preparedSelectEligible(st) {
			st.prepared = &preparedExecutionPlan{owner: q, counter: counter, where: st.Where, fromAlias: st.FromAlias}
		}
	case *UpdateStmt:
		if st.With == nil {
			st.prepared = &preparedExecutionPlan{owner: q, counter: counter, where: st.Where}
		}
	}
	b.statements = []Statement{stmt}
	return b
}

// Reflection is confined to building a private tree on a pool miss (and cold
// DDL executions). The hot binding path only fills precomputed literal/limit
// addresses, so its cost is proportional to the number of arguments.
func (b *BoundQuery) clone(v reflect.Value) reflect.Value {
	switch v.Kind() {
	case reflect.Interface:
		if v.IsNil() {
			return v
		}
		out := reflect.New(v.Type()).Elem()
		out.Set(b.clone(v.Elem()))
		return out
	case reflect.Pointer:
		if v.IsNil() {
			return v
		}
		if parameter, ok := v.Interface().(*ParameterExpr); ok {
			literal := b.literals[parameter.Index]
			if literal == nil {
				literal = &LiteralExpr{BoundParameter: true}
				b.literals[parameter.Index] = literal
			}
			return reflect.ValueOf(literal)
		}
		out := reflect.New(v.Type().Elem())
		out.Elem().Set(b.clone(v.Elem()))
		if out.Elem().Kind() == reflect.Struct {
			for _, name := range []string{"Limit", "Offset"} {
				param := out.Elem().FieldByName(name + "Param")
				if param.IsValid() && param.Int() > 0 {
					b.limits = append(b.limits, limitBinding{value: out.Elem().FieldByName(name).Addr().Interface().(*int64), index: int(param.Int()) - 1, offset: name == "Offset"})
				}
			}
		}
		return out
	case reflect.Struct:
		out := reflect.New(v.Type()).Elem()
		out.Set(v)
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				out.Field(i).Set(b.clone(v.Field(i)))
			}
		}
		return out
	case reflect.Slice:
		if v.IsNil() {
			return v
		}
		out := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		for i := 0; i < v.Len(); i++ {
			out.Index(i).Set(b.clone(v.Index(i)))
		}
		return out
	}
	return v
}
