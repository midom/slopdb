package sql

import "strings"

// evalPredicate shares exactly the same instruction execution as eval, while
// consuming the final register without returning a full Value to the caller.
func (p *typedProgram) evalPredicate(slots []Value) (bool, error) {
	if err := p.run(slots); err != nil {
		return false, err
	}
	value := &p.registers[p.result]
	if value.Null {
		return false, nil
	}
	if value.EnumValue {
		return value.EnumOrdinal != 0, nil
	}
	switch value.Type {
	case ColInteger:
		return value.Int != 0, nil
	case ColUnsigned:
		return value.Uint != 0, nil
	case ColReal:
		return value.Real != 0, nil
	default:
		return truthValue(*value), nil
	}
}

func (t *typedAggregate) scalarCountEligible(group *aggregateGroup) bool {
	if group == nil || t.where == nil || t.rawWhere != nil || len(t.group) != 0 || len(t.baseSlots) != 0 || len(t.functions) != 1 || len(t.args) != 1 || t.args[0] != nil || t.functions[0].Distinct || !strings.EqualFold(t.functions[0].Name, "COUNT") {
		return false
	}
	for _, column := range t.columns {
		switch column.typ {
		case ColInteger, ColReal, ColUnsigned:
		default:
			return false
		}
	}
	return true
}

// An ungrouped fixed-width COUNT(*) needs only one private row and one scalar
// accumulator. The shared access walker owns snapshot/overlay/index/dedup,
// cancellation and profiling behavior; no row bytes survive its callback.
func (s *Schema) walkTypedScalarCount(t *typedAggregate, info *TableInfo, needed map[string]bool, paths []*accessPath, group *aggregateGroup) error {
	row := make([]Value, t.width)
	var lastLayout *scanColumn
	var layout []int
	visited := false
	err := s.walkAccessRaw(info, needed, paths, func(data []byte, columns []scanColumn) error {
		visited = true
		if len(columns) > 0 && lastLayout != &columns[0] {
			lastLayout = &columns[0]
			if cap(layout) < len(columns) {
				layout = make([]int, len(columns))
			} else {
				layout = layout[:len(columns)]
			}
			for i, column := range columns {
				layout[i] = -1
				if column.needed {
					if slot, ok := t.slots[strings.ToLower(column.name)]; ok {
						layout[i] = slot
					}
				}
			}
		}
		decodeTypedRow(data, layout, row)
		accepted, err := t.where.evalPredicate(row)
		if err != nil {
			return err
		}
		if accepted {
			group.inputs++
			group.states[0].count++
		}
		return nil
	})
	if err != nil {
		return err
	}
	if visited && aggregateGroupMemory(group) > s.memoryBudget() {
		return errAggregateSpill
	}
	return nil
}
