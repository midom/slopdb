package sql

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
)

// DescribeOptions supplies logical parameter types, independent of wire IDs.
// ParameterPositions maps each native '?' occurrence to a client parameter;
// nil means identity. Repeated PostgreSQL $n markers share one logical slot.
type DescribeOptions struct {
	ParameterTypes     []LogicalType
	ParameterPositions []int
}
type PreparedDescription struct {
	Kind       string
	Parameters []LogicalType
	Results    []ColumnDef
}
type preparedTypeInference struct {
	types     []LogicalType
	positions []int
	changed   bool
}

func (i *preparedTypeInference) index(occurrence int) int {
	if occurrence < 0 || occurrence >= len(i.positions) {
		return -1
	}
	return i.positions[occurrence]
}
func (i *preparedTypeInference) column(occurrence int) ColumnDef {
	index := i.index(occurrence)
	if index < 0 {
		return ColumnDef{ColType: ColUnknown}
	}
	c := i.types[index].Column()
	c.ParameterIndex = occurrence + 1
	return c
}
func (i *preparedTypeInference) hint(occurrence int, t LogicalType) {
	index := i.index(occurrence)
	if index < 0 || !t.Known() || i.types[index].Known() {
		return
	}
	i.types[index] = t
	i.changed = true
}
func (s *Schema) describeParameter(occurrence int) ColumnDef {
	if s.describeTypes == nil {
		return ColumnDef{ColType: ColText}
	}
	return s.describeTypes.column(occurrence)
}
func (s *Schema) constrainDescription(expr Expression, want ColumnDef, env map[string]ColumnDef) {
	if s.describeTypes == nil {
		return
	}
	t := want.LogicalType()
	if !t.Known() {
		return
	}
	switch e := expr.(type) {
	case *ParameterExpr:
		s.describeTypes.hint(e.Index, t)
	case *LiteralExpr:
		if e.BoundParameter {
			s.describeTypes.hint(int(e.Value.Int)-1, t)
		}
	case *ColumnRefExpr:
		if c := env[e.Name]; c.ParameterIndex > 0 {
			s.describeTypes.hint(c.ParameterIndex-1, t)
		}
	case *UnaryExpr:
		if strings.ToUpper(e.Op) != "NOT" {
			s.constrainDescription(e.Right, want, env)
		}
	case *BinaryExpr:
		switch strings.ToUpper(e.Op) {
		case "+", "-", "*", "/", "%":
			s.constrainDescription(e.Left, want, env)
			s.constrainDescription(e.Right, want, env)
		}
	}
}

// DescribePrepared inspects a private AST using constraints only. It does not
// bind arbitrary values, execute functions/defaults, scan rows or obtain locks.
func (s *Schema) DescribePrepared(stmt Statement, options DescribeOptions) (PreparedDescription, error) {
	count := 0
	namespaceVisit(reflect.ValueOf(stmt), func(value any) {
		switch v := value.(type) {
		case *ParameterExpr:
			count = max(count, v.Index+1)
		case *SelectStmt:
			count = max(count, v.LimitParam, v.OffsetParam)
		case *UpdateStmt:
			count = max(count, v.LimitParam, v.OffsetParam)
		case *DeleteStmt:
			count = max(count, v.LimitParam, v.OffsetParam)
		case *UnionStmt:
			count = max(count, v.LimitParam, v.OffsetParam)
		}
	})
	positions := slices.Clone(options.ParameterPositions)
	if positions == nil {
		positions = make([]int, count)
		for j := range positions {
			positions[j] = j
		}
	} else if len(positions) != count {
		return PreparedDescription{}, fmt.Errorf("expected %d parameter positions, got %d", count, len(positions))
	}
	groups := 0
	for _, n := range positions {
		if n < 0 {
			return PreparedDescription{}, fmt.Errorf("invalid parameter position %d", n)
		}
		groups = max(groups, n+1)
	}
	if len(options.ParameterTypes) > groups {
		return PreparedDescription{}, fmt.Errorf("too many supplied parameter types")
	}
	inference := &preparedTypeInference{types: make([]LogicalType, groups), positions: positions}
	copy(inference.types, options.ParameterTypes)
	old := s.describeTypes
	s.describeTypes = inference
	defer func() { s.describeTypes = old }()
	var cols []ColumnDef
	var err error
	// Projection can precede constraints in WHERE/RETURNING or a dependent CTE.
	// Monotonic inference converges without evaluating any expression.
	for pass := 0; pass < min(128, count+2); pass++ {
		inference.changed = false
		cols, err = s.Describe(stmt)
		if err != nil {
			return PreparedDescription{}, err
		}
		if !inference.changed {
			break
		}
	}
	for j := range cols {
		cols[j].ParameterIndex = 0
	}
	return PreparedDescription{Kind: metadataStatementKind(stmt), Parameters: slices.Clone(inference.types), Results: cols}, nil
}
func (s *Schema) describeLimitParameters(limit, offset int) {
	if s.describeTypes == nil {
		return
	}
	t := logicalTypeID(TypeInt64)
	if limit > 0 {
		s.describeTypes.hint(limit-1, t)
	}
	if offset > 0 {
		s.describeTypes.hint(offset-1, t)
	}
}

func metadataStatementKind(stmt Statement) string {
	switch s := stmt.(type) {
	case *SelectStmt, *UnionStmt:
		return "SELECT"
	case *InsertStmt:
		return "INSERT"
	case *UpdateStmt:
		return "UPDATE"
	case *DeleteStmt:
		return "DELETE"
	case *ReadBarrierStmt:
		return "SET"
	case *ReplicationPositionStmt:
		if s.Operation == "wait" {
			return "WAIT"
		}
		return "POSITION"
	}
	return ""
}
