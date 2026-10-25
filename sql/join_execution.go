package sql

import (
	"bytes"
	"fmt"
	"slopdb/storage"
	"strings"
)

// A hash key has the same numeric equivalences as ValueCompare, including
// integral REALs and signed zero. NULL never matches an equality join.
func appendEqualityKey(dst []byte, value Value) []byte {
	if !value.Null && value.Type == ColReal && value.Real >= -9223372036854775808.0 && value.Real < 9223372036854775808.0 && value.Real == float64(int64(value.Real)) {
		value = NewIntValue(int64(value.Real))
	}
	return appendValue(dst, value)
}

func (s *Schema) appendEqualityKey(dst []byte, value Value) []byte {
	if !value.Null && value.Type == ColText && (!value.Binary || value.Collation != "") {
		value.Text = valueTextKey(value, s.collation)
	}
	if s.mysqlMode && value.Type == ColText && !value.Null {
		value = NewBlobValue([]byte(value.Text))
	}
	return appendEqualityKey(dst, value)
}

func andTerms(expr Expression) []Expression {
	var terms []Expression
	var collect func(Expression)
	collect = func(expr Expression) {
		if b, ok := expr.(*BinaryExpr); ok && b.Op == "AND" {
			collect(b.Left)
			collect(b.Right)
		} else if expr != nil {
			terms = append(terms, expr)
		}
	}
	collect(expr)
	return terms
}

func deterministicFunction(name string) bool {
	switch strings.ToUpper(name) {
	case "COUNT", "SUM", "AVG", "MIN", "MAX", "TOTAL", "GROUP_CONCAT", "COALESCE", "IFNULL", "NULLIF", "LENGTH", "UPPER", "LOWER", "ABS", "ROUND", "IIF", "IF", "GREATEST", "LEAST", "CONCAT", "__MYSQL_CONVERT", "CAST", "SUBSTR", "SUBSTRING", "REPLACE", "TRIM", "LTRIM", "RTRIM", "TYPEOF", "HEX", "UNICODE", "CHAR", "INSTR":
		return true
	}
	return false
}

func expressionNames(expr Expression) []string {
	var names []string
	_ = walkExpression(expr, func(e Expression) (bool, error) {
		if c, ok := e.(*ColumnRefExpr); ok && c.Name != "*" {
			names = append(names, strings.ToLower(c.Name))
		}
		return false, nil
	})
	return names
}

type joinKeyPair struct{ left, right string }

func rightJoinName(name, alias string, columns []ColumnDef, leftNames map[string]bool) (string, bool) {
	name = strings.ToLower(name)
	qualified := strings.Contains(name, ".")
	if qualified {
		prefix, rest, _ := strings.Cut(name, ".")
		if prefix != alias {
			return "", false
		}
		name = rest
	} else if leftNames[name] {
		return "", false
	}
	for _, c := range columns {
		if c.Name == name {
			return name, true
		}
	}
	return "", false
}

func joinKeyPairs(stmt *SelectStmt, jc JoinClause, alias string, columns []ColumnDef, leftNames map[string]bool) []joinKeyPair {
	var pairs []joinKeyPair
	using := jc.Using
	if jc.Type == "NATURAL" {
		for _, col := range columns {
			if leftNames[col.Name] {
				using = append(using, col.Name)
			}
		}
	}
	for _, name := range using {
		pairs = append(pairs, joinKeyPair{name, name})
	}
	expressions := andTerms(jc.On)
	if jc.Type != "LEFT" {
		expressions = append(expressions, andTerms(stmt.Where)...)
	}
	for _, term := range expressions {
		b, ok := term.(*BinaryExpr)
		if !ok || b.Op != "=" {
			continue
		}
		a, aOK := b.Left.(*ColumnRefExpr)
		c, cOK := b.Right.(*ColumnRefExpr)
		if !aOK || !cOK {
			continue
		}
		for _, side := range [][2]string{{a.Name, c.Name}, {c.Name, a.Name}} {
			right, ok := rightJoinName(side[1], alias, columns, leftNames)
			if ok && leftNames[side[0]] {
				pairs = append(pairs, joinKeyPair{side[0], right})
				break
			}
		}
	}
	return pairs
}

func (s *Schema) joinRowKey(row map[string]Value, pairs []joinKeyPair, right bool) (string, bool) {
	var local [64]byte
	key := local[:0]
	for _, pair := range pairs {
		name := pair.left
		if right {
			name = pair.right
		}
		value, ok := row[name]
		if !ok || value.Null {
			return "", false
		}
		key = s.appendEqualityKey(key, value)
	}
	return string(key), true
}

func (s *Schema) filterJoinInput(rows []map[string]Value, keys [][]string, terms []Expression, localNames map[string]bool, alias string) ([]map[string]Value, [][]string, error) {
	var filters []Expression
	for _, term := range terms {
		if !s.movableJoinExpression(term, localNames) {
			continue
		}
		names := expressionNames(term)
		if len(names) == 0 {
			continue
		}
		local := true
		for _, name := range names {
			if !localNames[name] {
				local = false
				break
			}
		}
		if local {
			filters = append(filters, term)
		}
	}
	if len(filters) == 0 {
		return rows, keys, nil
	}
	// Evaluate qualification using a reused scratch map; retain input rows only.
	var scratch map[string]Value
	if alias != "" {
		scratch = make(map[string]Value)
	}
	out := rows[:0]
	var outKeys [][]string
	if keys != nil {
		outKeys = keys[:0]
	}
	for i, row := range rows {
		if err := s.checkInterrupt(); err != nil {
			return nil, nil, err
		}
		evalRow := row
		if scratch != nil {
			clear(scratch)
			for k, v := range row {
				scratch[k] = v
				scratch[alias+"."+k] = v
			}
			evalRow = scratch
		}
		matched := true
		for _, term := range filters {
			ok, err := s.evalCondition(term, evalRow)
			if err != nil {
				return nil, nil, err
			}
			if !ok {
				matched = false
				break
			}
		}
		if matched {
			out = append(out, row)
			if keys != nil {
				outKeys = append(outKeys, keys[i])
			}
		}
	}
	return out, outKeys, nil
}

func (s *Schema) executeJoins(stmt *SelectStmt, baseRows []map[string]Value, baseKeys [][]string) ([]map[string]Value, [][]string, error) {
	var spills []*diskHash
	defer func() {
		for _, spill := range spills {
			spill.close()
		}
	}()
	needed := joinRequiredNames(stmt)
	pruneJoinRows(baseRows, needed, "")
	result, resultKeys := baseRows, baseKeys
	trackKeys := stmt.Lock != nil || stmt.trackRows
	leftNames := make(map[string]bool)
	if info := s.tables[stmt.From]; info != nil {
		for _, c := range info.Columns {
			leftNames[c.Name] = true
			leftNames[stmt.From+"."+c.Name] = true
			if stmt.FromAlias != "" {
				leftNames[stmt.FromAlias+"."+c.Name] = true
			}
		}
	}
	for _, row := range baseRows {
		for name := range row {
			leftNames[name] = true
		}
		break
	}
	allInner := !trackKeys
	for _, jc := range stmt.Joins {
		allInner = allInner && jc.Type != "LEFT" && jc.Type != "RIGHT"
	}
	whereTerms := andTerms(stmt.Where)
	if allInner {
		var err error
		result, resultKeys, err = s.filterJoinInput(result, resultKeys, whereTerms, leftNames, "")
		if err != nil {
			return nil, nil, err
		}
	}
	for _, jc := range stmt.Joins {
		if err := s.checkInterrupt(); err != nil {
			return nil, nil, err
		}
		joinRows, columns, joinKeys, err := s.getJoinRows(jc, trackKeys, needed)
		if err != nil {
			return nil, nil, err
		}
		alias := jc.Alias
		if alias == "" {
			alias = jc.Table
		}
		rightNames := make(map[string]bool)
		for _, c := range columns {
			rightNames[alias+"."+c.Name] = true
			if !leftNames[c.Name] {
				rightNames[c.Name] = true
			}
		}
		filters := andTerms(jc.On)
		if allInner {
			filters = append(filters, whereTerms...)
		}
		if !trackKeys {
			joinRows, joinKeys, err = s.filterJoinInput(joinRows, joinKeys, filters, rightNames, alias)
			if err != nil {
				return nil, nil, err
			}
		}
		pruneJoinRows(joinRows, needed, alias)
		pairs := joinKeyPairs(stmt, jc, alias, columns, leftNames)
		if s.collation != "" && s.collation != BinaryCollation {
			// A BINARY column can be compared to a collated column. Their
			// hash representations then differ even when the raw values match.
			pairs = nil
		}
		var buckets map[string][]int
		var disk *diskHash
		var bucketBytes int64
		if len(pairs) > 0 {
			buckets = make(map[string][]int)
			for j, row := range joinRows {
				if j%256 == 0 {
					if err := s.checkInterrupt(); err != nil {
						return nil, nil, err
					}
				}
				if key, valid := s.joinRowKey(row, pairs, true); valid {
					bucketBytes += int64(len(key) + 16)
					if disk == nil && bucketBytes > s.memoryBudget() {
						disk, err = newDiskHash(s)
						if err != nil {
							return nil, nil, err
						}
						spills = append(spills, disk)
						for k, indexes := range buckets {
							for _, index := range indexes {
								if err = disk.add(k, map[string]Value{"pos": NewIntValue(int64(index))}); err != nil {
									return nil, nil, err
								}
							}
						}
						buckets = nil
					}
					if disk != nil {
						if err = disk.add(key, map[string]Value{"pos": NewIntValue(int64(j))}); err != nil {
							return nil, nil, err
						}
					} else {
						buckets[key] = append(buckets[key], j)
					}
				}
			}
		}
		var next []map[string]Value
		var nextKeys [][]string
		for i, left := range result {
			if err := s.checkInterrupt(); err != nil {
				return nil, nil, err
			}
			var candidates []int
			if buckets != nil {
				if key, valid := s.joinRowKey(left, pairs, false); valid {
					candidates = buckets[key]
				}
			}
			matched := false
			count := len(joinRows)
			if buckets != nil {
				count = len(candidates)
			}
			acceptIndex := func(j int) error {
				if err := s.checkInterrupt(); err != nil {
					return err
				}
				right := joinRows[j]
				using := jc.Using
				if jc.Type == "NATURAL" {
					using = nil
					for _, col := range columns {
						if _, ok := left[col.Name]; ok {
							using = append(using, col.Name)
						}
					}
				}
				match := true
				for _, name := range using {
					a, b := left[name], right[name]
					if a.Null || b.Null || s.valueCompare(a, b) != 0 {
						match = false
						break
					}
				}
				if !match {
					return nil
				}
				combined := s.combineRowsWithAlias(left, right, alias)
				if jc.On != nil {
					match, err = s.evalCondition(jc.On, combined)
					if err != nil {
						return err
					}
				}
				if match {
					matched = true
					next = append(next, combined)
					if trackKeys {
						var keys []string
						if resultKeys != nil {
							keys = append(keys, resultKeys[i]...)
						}
						if joinKeys != nil {
							keys = append(keys, joinKeys[j]...)
						}
						nextKeys = append(nextKeys, keys)
					}
				}
				return nil
			}
			if disk != nil {
				if key, valid := s.joinRowKey(left, pairs, false); valid {
					if err := disk.walk(key, func(row map[string]Value) error { return acceptIndex(int(row["pos"].Int)) }); err != nil {
						return nil, nil, err
					}
				}
			} else {
				for k := 0; k < count; k++ {
					j := k
					if buckets != nil {
						j = candidates[k]
					}
					if err := acceptIndex(j); err != nil {
						return nil, nil, err
					}
				}
			}

			if !matched && jc.Type == "LEFT" {
				null := make(map[string]Value)
				for _, c := range columns {
					null[c.Name] = NewNullValue()
					if jc.Subquery != nil && jc.Subquery.JoinGroup {
						if _, name, ok := strings.Cut(c.Name, "."); ok {
							null[name] = NewNullValue()
						}
					}
				}
				next = append(next, s.combineRowsWithAlias(left, null, alias))
				if trackKeys {
					var keys []string
					if resultKeys != nil {
						keys = resultKeys[i]
					}
					nextKeys = append(nextKeys, keys)
				}
			}
		}
		if jc.Type == "RIGHT" {
			return nil, nil, fmt.Errorf("RIGHT JOIN is not supported")
		}
		result, resultKeys = next, nextKeys
		for _, c := range columns {
			leftNames[c.Name] = true
			leftNames[alias+"."+c.Name] = true
		}
		if allInner {
			result, resultKeys, err = s.filterJoinInput(result, resultKeys, whereTerms, leftNames, "")
			if err != nil {
				return nil, nil, err
			}
		}
	}
	return result, resultKeys, nil
}

// Keep only values referenced by this SELECT or its nested expressions. Names
// in nested scopes are over-collected deliberately: outer references must remain
// available when correlated subqueries are evaluated after the joins.
func joinRequiredNames(stmt *SelectStmt) map[string]bool {
	names := make(map[string]bool)
	safe := true
	var query func(*SelectStmt, bool)
	query = func(q *SelectStmt, root bool) {
		if q.SelectAll && root {
			safe = false
			return
		}
		visit := func(expr Expression) {
			_ = walkExpression(expr, func(e Expression) (bool, error) {
				switch e := e.(type) {
				case *ColumnRefExpr:
					if e.Name != "*" {
						names[e.Name] = true
					}
				case *SubqueryExpr:
					query(e.Stmt, false)
					return true, nil
				case *ExistsExpr:
					query(e.Stmt, false)
					return true, nil
				}
				return false, nil
			})
		}
		for _, c := range q.Columns {
			if c.Star && root {
				safe = false
			}
			visit(c.Expr)
		}
		visit(q.Where)
		visit(q.Having)
		for _, g := range q.GroupBy {
			visit(g)
		}
		for _, o := range q.OrderBy {
			visit(o.Expr)
		}
		for _, j := range q.Joins {
			if root && (j.Type == "NATURAL" || len(j.Using) > 0) {
				safe = false
			}
			visit(j.On)
		}
	}
	query(stmt, true)
	if !safe {
		return nil
	}
	return names
}

func pruneJoinRows(rows []map[string]Value, needed map[string]bool, alias string) {
	if needed == nil {
		return
	}
	for _, row := range rows {
		for name := range row {
			if !needed[name] && (alias == "" || !needed[alias+"."+name]) {
				delete(row, name)
			}
		}
	}
}

func (s *Schema) movableJoinExpression(expr Expression, names map[string]bool) bool {
	safe := true
	_ = walkExpression(expr, func(e Expression) (bool, error) {
		var query *SelectStmt
		switch e := e.(type) {
		case *FuncCallExpr:
			if !deterministicFunction(e.Name) {
				safe = false
			}
		case *SubqueryExpr:
			query = e.Stmt
		case *ExistsExpr:
			query = e.Stmt
		}
		if query != nil {
			deps, deterministic := s.subqueryDependencies(query)
			if !deterministic || len(deps) > 0 {
				// Correlated work stays after selective joins; pushing it into a
				// large input can multiply indexed probes before dimension filters.
				safe = false
			}
			for _, name := range deps {
				if !names[name] {
					safe = false
				}
			}
			return true, nil
		}
		return false, nil
	})
	return safe
}

// Project physical join inputs while decoding, so unused TEXT/BLOB columns
// and their map entries are never allocated.
func (s *Schema) scanJoinTable(table, alias string, info *TableInfo, needed map[string]bool) ([]map[string]Value, error) {
	if alias == "" {
		alias = table
	}
	columns := make([]scanColumn, len(info.Columns))
	count := 0
	for i, c := range info.Columns {
		use := needed[c.Name] || needed[alias+"."+c.Name]
		columns[i] = scanColumn{name: c.Name, needed: use, definition: c}
		if use {
			count++
		}
	}
	var iter storage.Iterator
	if s.inTx {
		iter = s.txNewIterator()
	} else {
		iter = s.newIterator()
	}
	defer iter.Close()
	prefix := []byte(s.tableDataPrefix(table))
	iter.Seek(prefix)
	var rows []map[string]Value
	for n := 0; iter.Valid() && bytes.HasPrefix(iter.Key().UserKey, prefix); iter.Next() {
		if n%128 == 0 {
			if err := s.checkInterrupt(); err != nil {
				return nil, err
			}
		}
		n++
		row := make(map[string]Value, count)
		decodeScanRow(iter.Value(), columns, row)
		rows = append(rows, row)
	}
	return rows, storage.IteratorError(iter)
}
