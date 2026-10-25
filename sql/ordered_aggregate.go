package sql

import "strings"

// Only one monotonically traversed clustered interval can establish grouping
// order here. Secondary indexes, IN/OR unions and point sets retain their normal
// planner paths; their visitation order need not match the table's primary key.
func orderedAggregateInput(stmt *SelectStmt, info *TableInfo, groups []Expression, paths []*accessPath) bool {
	if len(groups) == 0 || len(groups) > len(info.PKColumns) || len(stmt.Joins) > 0 {
		return false
	}
	for i, expr := range groups {
		ref, ok := expr.(*ColumnRefExpr)
		if !ok {
			return false
		}
		name := coveringColumn(ref.Name, stmt.From, stmt.FromAlias)
		if name != info.PKColumns[i] {
			return false
		}
	}
	if paths == nil {
		return true
	}
	return len(paths) == 1 && paths[0].index == nil && paths[0].points == nil && len(paths[0].intervals) <= 1
}
func resetOrderedAggregate(g *aggregateGroup) {
	states, values := g.states, g.baseValues
	clear(states)
	clear(values)
	*g = aggregateGroup{states: states, baseValues: values[:0]}
}

// Typed ordered aggregation retains one accumulator and one decode batch.
// WHERE runs before boundary detection so rejected rows cannot split groups.
func (s *Schema) walkTypedOrderedAggregate(t *typedAggregate, info *TableInfo, needed map[string]bool, paths []*accessPath, emit func(*aggregateGroup) error) error {
	batch := make([]Value, typedBatchRows*t.width)
	rows := 0
	var keyBuffer [128]byte
	keyScratch := keyBuffer[:0]
	var current *aggregateGroup
	var lastKey string
	flush := func() error {
		if err := s.checkInterrupt(); err != nil {
			return err
		}
		for n := 0; n < rows; n++ {
			row := batch[n*t.width : (n+1)*t.width]
			if t.where != nil {
				v, err := t.where.eval(row)
				if err != nil {
					return err
				}
				if !truthValue(v) {
					continue
				}
			}
			key := keyScratch[:0]
			for _, p := range t.group {
				v, err := p.eval(row)
				if err != nil {
					return err
				}
				key = appendValueIdentity(key, v)
			}
			if current == nil {
				current = &aggregateGroup{states: make([]aggregateState, len(t.functions))}
				lastKey = string(key)
			} else if string(key) != lastKey {
				if err := s.checkInterrupt(); err != nil {
					return err
				}
				if err := emit(current); err != nil {
					return err
				}
				resetOrderedAggregate(current)
				lastKey = string(key)
			}
			if cap(key) <= typedScratchLimit {
				keyScratch = key[:0]
			}
			if current.inputs == 0 {
				if cap(current.baseValues) < len(t.baseSlots) {
					current.baseValues = make([]Value, len(t.baseSlots))
				} else {
					current.baseValues = current.baseValues[:len(t.baseSlots)]
				}
				for i, slot := range t.baseSlots {
					current.baseValues[i] = row[slot]
					if row[slot].Type == ColBlob {
						current.baseValues[i].Blob = copyBytes(row[slot].Blob)
					}
				}
			}
			current.inputs++
			for i, p := range t.args {
				if p == nil {
					current.states[i].count++
					continue
				}
				v, err := p.eval(row)
				if err != nil {
					return err
				}
				if err = s.addAggregateValue(&current.states[i], t.functions[i], v); err != nil {
					return err
				}
			}
		}
		resetTypedBatch(batch)
		rows = 0
		return nil
	}
	var lastLayout *scanColumn
	var layout []int
	err := s.walkAccessRaw(info, needed, paths, func(data []byte, columns []scanColumn) error {
		if len(columns) > 0 && lastLayout != &columns[0] {
			lastLayout = &columns[0]
			if cap(layout) < len(columns) {
				layout = make([]int, len(columns))
			} else {
				layout = layout[:len(columns)]
			}
			for i, col := range columns {
				layout[i] = -1
				if col.needed {
					if slot, ok := t.slots[strings.ToLower(col.name)]; ok {
						layout[i] = slot
					}
				}
			}
		}
		decodeTypedRow(data, layout, batch[rows*t.width:(rows+1)*t.width])
		rows++
		if rows == typedBatchRows {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	if rows > 0 {
		if err = flush(); err != nil {
			return err
		}
	}
	if current != nil {
		return emit(current)
	}
	return nil
}

// Closing a group's DISTINCT stores also releases their statement ownership.
// The accumulator may then be reused without retaining prior temporary engines.
func closeAggregateDisks(g *aggregateGroup) {
	for i := range g.states {
		if disk := g.states[i].distinctDisk; disk != nil {
			disk.close()
			g.states[i].distinctDisk = nil
		}
	}
}
