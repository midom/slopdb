package sql

import (
	"bytes"
	"maps"
	"slices"
	"sort"
	"strings"

	"slopdb/storage"
)

// RowCursor incrementally evaluates a simple SELECT at the caller's statement
// snapshot. The caller must keep the statement alive until Close. It is not
// safe for concurrent calls.
type RowCursor struct {
	columnsComplete  bool
	fulltext         bool
	fulltextOpened   bool
	schema           *Schema
	stmt             *SelectStmt
	info             *TableInfo
	columns          []ColumnDef
	expressions      []selectExprCol
	physical         *preparedExecutionPlan
	sourceColumns    []cursorSourceColumn
	sourceFields     int
	source           map[string]Value
	condition        map[string]Value
	iterator         storage.Iterator
	intervals        []coveringInterval
	position         int
	reverse          bool
	skipped, emitted int64
	closed           bool
}

type cursorSourceColumn struct {
	pointDecodeMetadata
	aliasName, tableName string
	blobBuffer           []byte
}

// OpenRowCursor returns handled=false for operators which need the ordinary
// executor (joins, grouping, sorting, DISTINCT, views or subqueries). No rows
// are fetched while opening an eligible cursor.
func (s *Schema) OpenRowCursor(stmt *SelectStmt) (*RowCursor, bool, error) {
	if stmt.CalcFoundRows || s.cteScope != nil || stmt.With != nil || stmt.Compound != nil || stmt.materialized != nil {
		return nil, false, nil
	}
	if hasWindows(stmt) {
		return nil, false, nil
	}

	info := s.tables[strings.ToLower(stmt.From)]
	if info == nil || stmt.FromExpr != nil || stmt.Virtual != nil || len(stmt.Joins) != 0 || stmt.Lock != nil || stmt.Distinct || len(stmt.GroupBy) != 0 || stmt.Having != nil || hasAggregates(stmt) {
		return nil, false, nil
	}
	expandedStatement, err := s.expandQualifiedStars(stmt, nil, 0)
	if err != nil {
		return nil, true, err
	}
	stmt = expandedStatement
	if !cursorExpressionSafe(s, stmt.Where) {
		return nil, false, nil
	}
	for _, col := range stmt.Columns {
		if !cursorExpressionSafe(s, col.Expr) {
			return nil, false, nil
		}
	}
	reverse := false
	if len(stmt.OrderBy) > 0 {
		if len(stmt.OrderBy) > len(info.PKColumns) {
			return nil, false, nil
		}
		reverse = stmt.OrderBy[0].Desc
		for i, order := range stmt.OrderBy {
			ref, ok := order.Expr.(*ColumnRefExpr)
			if !ok || cursorOrderColumn(stmt, ref.Name) != info.PKColumns[i] || order.Desc != reverse {
				return nil, false, nil
			}
		}
	}
	if stmt.prepared != nil {
		defer s.restorePreparedPlan(s.usePreparedPlan(stmt.prepared, info))
	}
	if err := s.validateQuery(stmt, info); err != nil {
		return nil, true, err
	}
	c := &RowCursor{schema: s, stmt: stmt, info: info, reverse: reverse}
	c.columns, c.expressions, _ = s.selectProjection(stmt, info)
	c.columnsComplete = cursorCatalogProjection(stmt, info, c.columns)
	// Compile decoding metadata once. Simple physical projections can decode
	// straight into the caller's output buffers, keeping WHERE-only fields in
	// a private map. Expressions use a separate, reusable source row instead.
	physical := &preparedExecutionPlan{}
	if ok, err := s.preparePhysicalReadProjection(stmt, info, physical); err != nil {
		return nil, true, err
	} else if ok {
		c.physical = physical
		if physical.pointDecodeCount != len(physical.pointProjection) {
			c.condition = make(map[string]Value, physical.pointDecodeCount)
		}
	} else {
		c.compileSourceColumns()
	}
	if hasFulltextCondition(stmt.Where) {
		c.fulltext = true
		return c, true, nil
	}
	prefix := []byte(info.storagePrefix(info.Name))
	// Merge clustered intervals so OR/IN reads remain bounded, ordered and
	// duplicate-free. Any branch that cannot constrain the key keeps the scan.
	branches, expanded := accessBranches(stmt.Where)
	if !expanded {
		branches = []Expression{stmt.Where}
	}
	for _, branch := range branches {
		if key, found := s.primaryKeyLookup(branch, info.Name, info); found {
			c.intervals = append(c.intervals, coveringInterval{key, append(copyBytes(key), 0)})
			continue
		}
		copy := *stmt
		copy.Where = branch
		plan := buildTuplePlan(&copy, info, nil, tupleConstraints(branch, info.Name, stmt.FromAlias, info))
		if plan == nil {
			c.intervals = []coveringInterval{{prefix, prefixSuccessor(prefix)}}
			break
		}
		c.intervals = append(c.intervals, plan.intervals...)
	}
	sort.Slice(c.intervals, func(i, j int) bool { return bytes.Compare(c.intervals[i].lower, c.intervals[j].lower) < 0 })
	merged := c.intervals[:0]
	for _, interval := range c.intervals {
		if len(merged) > 0 && bytes.Compare(interval.lower, merged[len(merged)-1].upper) <= 0 {
			if bytes.Compare(interval.upper, merged[len(merged)-1].upper) > 0 {
				merged[len(merged)-1].upper = interval.upper
			}
		} else {
			merged = append(merged, interval)
		}
	}
	c.intervals = merged
	return c, true, nil
}

// Expression and aliased projections need a private source row, but only the
// columns referenced by their expressions or WHERE need decoding. Keep its
// qualification keys and declaration metadata stable for the cursor lifetime.
func (c *RowCursor) compileSourceColumns() {
	needed := make([]bool, len(c.info.Columns))
	mark := func(expression Expression) {
		_ = walkExpression(expression, func(expression Expression) (bool, error) {
			if ref, ok := expression.(*ColumnRefExpr); ok {
				name := strings.ToLower(ref.Name)
				for i, column := range c.info.Columns {
					if name == column.Name || strings.HasSuffix(name, "."+column.Name) {
						needed[i] = true
					}
				}
			}
			return false, nil
		})
	}
	if c.stmt.SelectAll {
		for i := range needed {
			needed[i] = true
		}
	} else {
		for _, expression := range c.expressions {
			mark(expression.expr)
		}
	}
	mark(c.stmt.Where)
	qualified := needsQualifiedRows(c.stmt)
	alias := c.stmt.FromAlias
	if alias == "" {
		alias = c.stmt.From
	}
	c.sourceColumns = make([]cursorSourceColumn, len(c.info.Columns))
	for i, column := range c.info.Columns {
		m := &c.sourceColumns[i]
		m.name, m.decode = column.Name, needed[i]
		if !m.decode {
			continue
		}
		m.kind, m.binary, m.collation, m.enums = column.LogicalType().Kind, column.Binary, column.Collation, column.EnumValues
		c.sourceFields++
		if qualified && !strings.Contains(column.Name, ".") {
			m.aliasName = alias + "." + column.Name
			c.sourceFields++
			if c.stmt.From != alias {
				m.tableName = c.stmt.From + "." + column.Name
				c.sourceFields++
			}
		}
	}
}

// The map is entirely cursor-private, so no field cleanup or qualification-key
// construction is needed per row. BLOB decoding reuses each source field's own
// buffer; projection copies it before anything reaches a caller-owned result.
func (c *RowCursor) decodeSourceRow(data []byte) map[string]Value {
	if c.source == nil {
		c.source = make(map[string]Value, c.sourceFields)
	}
	offset := 0
	for i := range c.sourceColumns {
		column := &c.sourceColumns[i]
		if !column.decode {
			if offset < len(data) {
				offset += skipPointValue(data[offset:])
			}
			continue
		}
		value := NewNullValue()
		if offset < len(data) {
			var n int
			previous := c.source[column.name]
			if previous.Blob == nil {
				previous.Blob = column.blobBuffer
			}
			value, n = decodePointValueInto(data[offset:], previous)
			value = decorateTypedValue(value, column.kind, column.binary, column.collation, column.enums)
			offset += n
		}
		// NULL clears the visible value, while a bounded, exclusively private
		// buffer can survive for the next non-NULL row in this cursor.
		if !value.Null && value.Type == ColBlob {
			column.blobBuffer = nil
			if cap(value.Blob) <= 64<<10 {
				column.blobBuffer = value.Blob
			}
		}
		c.source[column.name] = value
		if column.aliasName != "" {
			c.source[column.aliasName] = value
		}
		if column.tableName != "" {
			c.source[column.tableName] = value
		}
	}
	return c.source
}

func cursorOrderColumn(stmt *SelectStmt, name string) string {
	for _, selected := range stmt.Columns {
		if selected.Alias != "" && strings.EqualFold(selected.Alias, name) {
			ref, ok := selected.Expr.(*ColumnRefExpr)
			if !ok {
				return ""
			}
			return coveringColumn(ref.Name, stmt.From, stmt.FromAlias)
		}
	}
	return coveringColumn(name, stmt.From, stmt.FromAlias)
}

func (c *RowCursor) nextSourceRow(dst map[string]Value) (row, out map[string]Value, found bool, err error) {
	if c.fulltext && !c.fulltextOpened {
		c.fulltextOpened = true
		keys, used, err := c.schema.fulltextCandidates(c.stmt.Where, c.info)
		if err != nil {
			return nil, dst, false, err
		}
		if used {
			for key := range keys {
				k := []byte(key)
				c.intervals = append(c.intervals, coveringInterval{k, append(copyBytes(k), 0)})
			}
			sort.Slice(c.intervals, func(i, j int) bool { return bytes.Compare(c.intervals[i].lower, c.intervals[j].lower) < 0 })
		} else {
			prefix := []byte(c.info.storagePrefix(c.info.Name))
			c.intervals = []coveringInterval{{prefix, prefixSuccessor(prefix)}}
		}
	}
	for {
		if c.iterator == nil {
			if c.position >= len(c.intervals) {
				return nil, dst, false, nil
			}
			position := c.position
			if c.reverse {
				position = len(c.intervals) - 1 - position
			}
			interval := c.intervals[position]
			c.position++
			c.iterator = c.schema.newCursorRangeIterator(interval.lower, interval.upper, c.reverse)
		}
		if c.iterator.Valid() {
			// Decode before moving: streaming storage iterators may lend their
			// encoded value only until Next/Prev. Decoded values own their bytes.
			data := c.iterator.Value()
			if c.physical != nil {
				if c.condition == nil {
					out = decodePointRowInto(data, c.info, c.physical, dst)
					row = out
				} else {
					out = decodePointProjectionInto(data, c.info, c.physical, dst, c.condition)
					row = c.condition
				}
			} else {
				row, out = c.decodeSourceRow(data), dst
			}
			if c.reverse {
				c.iterator.Prev()
			} else {
				c.iterator.Next()
			}
			return row, out, true, nil
		}
		err := storage.IteratorError(c.iterator)
		c.iterator.Close()
		c.iterator = nil
		if err != nil {
			return nil, dst, false, err
		}
	}
}
func (c *RowCursor) Columns() []ColumnDef {
	columns := append([]ColumnDef(nil), c.columns...)
	for i := range columns {
		columns[i].Default = nil
		columns[i].Check = nil
		columns[i].RefColumns = append([]string(nil), columns[i].RefColumns...)
		columns[i].EnumValues = slices.Clone(columns[i].EnumValues)
		columns[i].ConstraintNames = maps.Clone(columns[i].ConstraintNames)
	}
	return columns
}

// ColumnsComplete reports that Columns already contains complete catalog
// declaration metadata, with no remaining expression metadata validation. A
// caller can avoid a second Describe traversal in the same execution view.
// More complex expressions retain Describe's full validation and inference.
func (c *RowCursor) ColumnsComplete() bool { return c.columnsComplete }

func cursorCatalogProjection(stmt *SelectStmt, info *TableInfo, columns []ColumnDef) bool {
	for _, column := range stmt.Columns {
		if !column.Star {
			if _, ok := column.Expr.(*ColumnRefExpr); !ok {
				return false
			}
		}
	}
	for _, column := range columns {
		if column.SemanticType == TypeUnknown {
			return false
		}
	}
	return cursorCatalogPredicate(stmt.Where, info, 0)
}

// Only expression shapes whose metadata cannot add errors or parameter
// inference after validateQuery are eligible. Functions, casts, qualified/outer
// references and decimal literal inference keep the full Describe path.
func cursorCatalogPredicate(expression Expression, info *TableInfo, depth int) bool {
	if depth > 64 {
		return false
	}
	switch e := expression.(type) {
	case nil:
		return true
	case *ColumnRefExpr:
		for _, column := range info.Columns {
			if e.Name == column.Name {
				return true
			}
		}
	case *LiteralExpr:
		return e.Value.Type != ColDecimal && e.Value.NumericText == ""
	case *BinaryExpr:
		switch strings.ToUpper(e.Op) {
		case "AND", "OR", "=", "==", "!=", "<>", "<", "<=", ">", ">=", "IS", "IS NOT":
			return cursorCatalogPredicate(e.Left, info, depth+1) && cursorCatalogPredicate(e.Right, info, depth+1)
		}
	case *UnaryExpr:
		return strings.EqualFold(e.Op, "NOT") && cursorCatalogPredicate(e.Right, info, depth+1)
	case *InListExpr:
		if !cursorCatalogPredicate(e.Expr, info, depth+1) {
			return false
		}
		for _, item := range e.List {
			if !cursorCatalogPredicate(item, info, depth+1) {
				return false
			}
		}
		return true
	case *BetweenExpr:
		return cursorCatalogPredicate(e.Expr, info, depth+1) && cursorCatalogPredicate(e.Low, info, depth+1) && cursorCatalogPredicate(e.High, info, depth+1)
	}
	return false
}

// Next returns an independently owned row whose map and BLOBs remain valid
// after cursor movement and Close.
func (c *RowCursor) Next() (map[string]Value, bool, error) {
	return c.NextInto(nil)
}

// NextInto reads a row using dst and its BLOB buffers as exclusive scratch.
// The caller relinquishes all previous contents and aliases of dst. Pass nil,
// or use Next, to retain independently owned rows across calls. Returned values
// never alias storage pages or the cursor's private source and condition rows.
// Each BLOB buffer in dst must be independently owned, without aliases to
// another destination field.
func (c *RowCursor) NextInto(dst map[string]Value) (map[string]Value, bool, error) {
	if c.closed {
		return nil, false, nil
	}
	if err := c.schema.checkInterrupt(); err != nil {
		c.Close()
		return nil, false, err
	}
	if c.stmt.Limit >= 0 && c.emitted >= c.stmt.Limit {
		c.Close()
		return nil, false, nil
	}
	out := dst
	for {
		if err := c.schema.checkInterrupt(); err != nil {
			c.Close()
			return nil, false, err
		}
		var row map[string]Value
		var found bool
		var err error
		row, out, found, err = c.nextSourceRow(out)
		if err != nil || !found {
			c.Close()
			return nil, false, err
		}
		if c.stmt.Where != nil {
			matches, err := c.schema.evalCondition(c.stmt.Where, row)
			if err != nil {
				c.Close()
				return nil, false, err
			}
			if !matches {
				continue
			}
		}
		if c.skipped < c.stmt.Offset {
			c.skipped++
			continue
		}
		if c.physical == nil {
			if out == nil {
				out = make(map[string]Value, len(c.columns))
			}
			for name := range out {
				if !slices.ContainsFunc(c.columns, func(col ColumnDef) bool { return col.Name == name }) {
					delete(out, name)
				}
			}
			for i, col := range c.columns {
				value := row[col.Name]
				if i < len(c.expressions) {
					value, err = c.schema.evalExpr(c.expressions[i].expr, row)
					if err != nil {
						c.Close()
						return nil, false, err
					}
				}
				if !value.Null && value.Type == ColBlob {
					buffer := out[col.Name].Blob
					value.Blob = append(buffer[:0], value.Blob...)
					if value.Blob == nil {
						value.Blob = []byte{}
					}
				}
				out[col.Name] = value
			}
		}
		// Selected fields belong to the caller, including when the caller
		// switches destinations on its next call. Retain only private fields.
		if c.condition != nil {
			for _, col := range c.physical.pointDecodeMetadata {
				if col.project {
					delete(c.condition, col.name)
				}
			}
		}
		c.emitted++
		return out, true, nil
	}
}
func (c *RowCursor) Close() {
	if !c.closed {
		c.closed = true
		if c.iterator != nil {
			c.iterator.Close()
			c.iterator = nil
		}
		c.source, c.condition = nil, nil
		c.sourceColumns = nil
		c.schema.clearSelectCaches()
	}
}

func cursorExpressionSafe(s *Schema, expression Expression) bool {
	_, safe := s.expressionDependencies(expression)
	if !safe {
		return false
	}
	_ = walkExpression(expression, func(e Expression) (bool, error) {
		switch e.(type) {
		case *SubqueryExpr, *ExistsExpr:
			safe = false
			return true, nil
		}
		return false, nil
	})
	return safe
}
