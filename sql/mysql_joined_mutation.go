package sql

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
)

func mysqlQualifiedAssignments(sets []UpdateSet) bool {
	for _, set := range sets {
		if strings.Contains(set.Column, ".") {
			return true
		}
	}
	return false
}

func mysqlMutationKey(alias string) string { return "\x00rowkey." + alias }

func mysqlMutationFromSQL(table, alias string, joins []JoinClause) string {
	query := &SelectStmt{From: table, FromAlias: alias, Joins: joins, SelectAll: true, Limit: -1}
	return strings.TrimPrefix(formatSelectSQL(query), "SELECT * FROM ")
}

func renameJoinedMutation(stmt Statement, table, newTable, oldCol, newCol string, trigger bool) bool {
	var query SelectStmt
	var update *UpdateStmt
	var deletion *DeleteStmt
	switch st := stmt.(type) {
	case *UpdateStmt:
		if st.Alias == "" && len(st.Joins) == 0 && !mysqlQualifiedAssignments(st.Set) {
			return false
		}
		update = st
		query = SelectStmt{From: st.Table, FromAlias: st.Alias, Joins: st.Joins, With: st.With, Where: st.Where, Limit: -1}
		for _, set := range st.Set {
			query.Columns = append(query.Columns, SelectColumn{Expr: set.Value})
		}
	case *DeleteStmt:
		if st.Alias == "" && len(st.Joins) == 0 && len(st.Targets) == 0 {
			return false
		}
		deletion = st
		query = SelectStmt{From: st.From, FromAlias: st.Alias, Joins: st.Joins, With: st.With, Where: st.Where, Limit: -1}
	default:
		return false
	}
	aliases := make(map[string]bool)
	if query.From == table {
		alias := query.FromAlias
		if alias == "" {
			alias = query.From
		}
		aliases[alias] = true
	}
	for _, join := range query.Joins {
		if join.Table == table {
			alias := join.Alias
			if alias == "" {
				alias = join.Table
			}
			aliases[alias] = true
		}
	}
	renameViewSelect(&query, table, newTable, oldCol, newCol, renameOperation(oldCol))
	if trigger {
		rewriteSelectTriggerRefs(&query, table, newTable, oldCol, newCol)
	}
	if update != nil {
		for i := range update.Set {
			set := &update.Set[i]
			qualifier, name, qualified := strings.Cut(set.Column, ".")
			if !qualified {
				name = set.Column
			}
			if oldCol != "" && name == oldCol && (qualified && aliases[qualifier] || !qualified && len(aliases) > 0) {
				name = newCol
			}
			if oldCol == "" && qualifier == table && aliases[qualifier] {
				qualifier = newTable
			}
			set.Column = name
			if qualified {
				set.Column = qualifier + "." + name
			}
			set.Value = query.Columns[i].Expr
		}
		update.Table, update.Alias, update.Joins, update.Where = query.From, query.FromAlias, query.Joins, query.Where
	} else {
		for i, target := range deletion.Targets {
			if oldCol == "" && target == table && aliases[target] {
				deletion.Targets[i] = newTable
			}
		}
		deletion.From, deletion.Alias, deletion.Joins, deletion.Where = query.From, query.FromAlias, query.Joins, query.Where
	}
	return true
}

type mysqlMutationTable struct {
	name, alias string
	info        *TableInfo
	sets        []UpdateSet
}

type mysqlMutationRow struct {
	table                *mysqlMutationTable
	key                  []byte
	original, evaluation map[string]Value
}

func (s *Schema) mysqlMutationTables(from, alias string, joins []JoinClause) ([]*mysqlMutationTable, error) {
	var result []*mysqlMutationTable
	seen := make(map[string]bool)
	add := func(name, alias string) error {
		if alias == "" {
			alias = name
		}
		if seen[alias] {
			return fmt.Errorf("duplicate mutation table alias %s", alias)
		}
		seen[alias] = true
		info := s.tables[name]
		if info == nil {
			return fmt.Errorf("physical table %q does not exist", name)
		}
		result = append(result, &mysqlMutationTable{name: name, alias: alias, info: info})
		return nil
	}
	if err := add(from, alias); err != nil {
		return nil, err
	}
	for _, join := range joins {
		if join.Subquery != nil {
			return nil, fmt.Errorf("derived mutation table is not supported")
		}
		if err := add(join.Table, join.Alias); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// mysqlJoinedRows closes every scan before returning any mutation candidate.
// Hidden alias identities distinguish self joins and tables without primary keys.
func (s *Schema) mysqlJoinedRows(stmt *SelectStmt, tables []*mysqlMutationTable) ([]map[string]Value, error) {
	if err := s.validateQuery(stmt, tables[0].info); err != nil {
		return nil, err
	}
	var keys [][]string
	rows, err := s.scanTableLimit(stmt.From, tables[0].info, s.tableDataPrefix(stmt.From), -1, false, &keys)
	if err != nil {
		return nil, err
	}
	for i, row := range rows {
		qualifyRow(row, tables[0].alias)
		if i < len(keys) && len(keys[i]) == 1 {
			row[mysqlMutationKey(tables[0].alias)] = NewBlobValue([]byte(keys[i][0]))
		}
	}
	stmt.trackRows, stmt.mutationRows = true, true
	rows, keys, err = s.executeJoins(stmt, rows, keys)
	if err != nil {
		return nil, err
	}
	if stmt.Where != nil {
		rows, _, err = s.filterRowsWithKeys(stmt.Where, rows, keys)
	}
	return rows, err
}

func (s *Schema) mysqlMutationCandidates(rows []map[string]Value, targets []*mysqlMutationTable, limit, offset int64, aliasUpdates bool) ([]mysqlMutationRow, error) {
	if offset > int64(len(rows)) {
		rows = nil
	} else if offset > 0 {
		rows = rows[int(offset):]
	}
	if limit >= 0 && int64(len(rows)) > limit {
		rows = rows[:int(limit)]
	}
	seen := make(map[string]bool)
	var result []mysqlMutationRow
	for _, row := range rows {
		for _, table := range targets {
			keyValue, exists := row[mysqlMutationKey(table.alias)]
			if !exists || keyValue.Null {
				continue
			} // NULL-extended outer side
			key := string(keyValue.Blob)
			identity := key
			if aliasUpdates {
				identity = table.alias + "\x00" + key
			}
			if seen[identity] {
				continue
			}
			seen[identity] = true
			encoded, found, err := s.schemaGet([]byte(key))
			if err != nil {
				return nil, err
			}
			if !found {
				continue
			}
			result = append(result, mysqlMutationRow{table: table, key: []byte(key), original: decodeRow(encoded, table.info), evaluation: row})
		}
	}
	// A stable physical order avoids unnecessary opposite-order lock waits.
	sort.SliceStable(result, func(i, j int) bool { return string(result[i].key) < string(result[j].key) })
	return result, nil
}

func (s *Schema) updateJoined(stmt *UpdateStmt) (int64, *ResultSet, error) {
	if len(stmt.Returning) != 0 {
		return 0, nil, fmt.Errorf("joined UPDATE does not support RETURNING")
	}
	if len(stmt.Joins) > 0 && (stmt.Limit >= 0 || stmt.Offset > 0) {
		return 0, nil, fmt.Errorf("multi-table UPDATE does not support LIMIT")
	}
	tables, err := s.mysqlMutationTables(stmt.Table, stmt.Alias, stmt.Joins)
	if err != nil {
		return 0, nil, err
	}
	query := &SelectStmt{From: stmt.Table, FromAlias: stmt.Alias, Joins: stmt.Joins, Where: stmt.Where, Limit: -1}
	for _, set := range stmt.Set {
		qualifier, name, qualified := strings.Cut(set.Column, ".")
		if !qualified {
			name = set.Column
		}
		var target *mysqlMutationTable
		for _, table := range tables {
			if qualified && qualifier != table.alias {
				continue
			}
			for _, column := range table.info.Columns {
				if column.Name == name {
					if target != nil {
						return 0, nil, fmt.Errorf("ambiguous UPDATE column %s", set.Column)
					}
					target = table
				}
			}
		}
		if target == nil {
			return 0, nil, fmt.Errorf("no such UPDATE column %s", set.Column)
		}
		for _, previous := range target.sets {
			if previous.Column == name {
				return 0, nil, fmt.Errorf("duplicate UPDATE column %s", set.Column)
			}
		}
		target.sets = append(target.sets, UpdateSet{Column: name, Value: set.Value})
		if err := validateAggregateExpr(set.Value, false); err != nil {
			return 0, nil, err
		}
		query.Columns = append(query.Columns, SelectColumn{Expr: set.Value})
	}
	if err := validateAggregateExpr(stmt.Where, false); err != nil {
		return 0, nil, err
	}
	// Validate all expressions before enabling SELECT *, which retains the full
	// namespace through the join operator for assignment evaluation.
	if err := s.validateQuery(query, tables[0].info); err != nil {
		return 0, nil, err
	}
	query.SelectAll, query.Columns = true, []SelectColumn{{Star: true}}
	rows, err := s.mysqlJoinedRows(query, tables)
	if err != nil {
		return 0, nil, err
	}
	var targets []*mysqlMutationTable
	for _, table := range tables {
		if len(table.sets) > 0 {
			targets = append(targets, table)
		}
	}
	for _, target := range targets {
		aliases := 0
		for _, other := range targets {
			if other.name == target.name {
				aliases++
			}
		}
		if aliases > 1 {
			for _, set := range target.sets {
				for _, primary := range target.info.PKColumns {
					if set.Column == primary {
						return 0, nil, fmt.Errorf("primary key update is not allowed when table %s is updated through multiple aliases", target.name)
					}
				}
			}
		}
	}
	candidates, err := s.mysqlMutationCandidates(rows, targets, stmt.Limit, stmt.Offset, true)
	if err != nil {
		return 0, nil, err
	}
	var changes int64
	for _, candidate := range candidates {
		key := candidate.key
		encoded, exists, err := s.schemaGet(key)
		if err != nil {
			return 0, nil, err
		}
		if !exists {
			continue
		}
		original := decodeRow(encoded, candidate.table.info)
		row, err := s.applyRowUpdate(candidate.table.name, candidate.table.info, key, original, candidate.table.sets, candidate.evaluation)
		if err != nil {
			return 0, nil, err
		}
		if !s.mysqlMode || s.mysqlFoundRows() || !bytes.Equal(encodeRow(original, candidate.table.info), encodeRow(row, candidate.table.info)) {
			changes++
		}
	}
	return changes, nil, nil
}

func (s *Schema) deleteJoined(stmt *DeleteStmt) (int64, *ResultSet, error) {
	if len(stmt.Returning) != 0 {
		return 0, nil, fmt.Errorf("joined DELETE does not support RETURNING")
	}
	if len(stmt.Targets) > 0 && (stmt.Limit >= 0 || stmt.Offset > 0) {
		return 0, nil, fmt.Errorf("multi-table DELETE does not support LIMIT")
	}
	tables, err := s.mysqlMutationTables(stmt.From, stmt.Alias, stmt.Joins)
	if err != nil {
		return 0, nil, err
	}
	targetNames := stmt.Targets
	if len(targetNames) == 0 {
		targetNames = []string{tables[0].alias}
	}
	var targets []*mysqlMutationTable
	seen := make(map[string]bool)
	for _, name := range targetNames {
		var target *mysqlMutationTable
		for _, table := range tables {
			if table.alias == name {
				target = table
			}
		}
		if target == nil {
			return 0, nil, fmt.Errorf("unknown DELETE target %s", name)
		}
		if seen[name] {
			return 0, nil, fmt.Errorf("duplicate DELETE target %s", name)
		}
		seen[name] = true
		targets = append(targets, target)
	}
	if err := validateAggregateExpr(stmt.Where, false); err != nil {
		return 0, nil, err
	}
	query := &SelectStmt{From: stmt.From, FromAlias: stmt.Alias, Joins: stmt.Joins, Where: stmt.Where, SelectAll: true, Columns: []SelectColumn{{Star: true}}, Limit: -1}
	rows, err := s.mysqlJoinedRows(query, tables)
	if err != nil {
		return 0, nil, err
	}
	candidates, err := s.mysqlMutationCandidates(rows, targets, stmt.Limit, stmt.Offset, false)
	if err != nil {
		return 0, nil, err
	}
	for _, candidate := range candidates {
		table, info, row := candidate.table.name, candidate.table.info, candidate.original
		if err := s.lockRow(table, info, row, candidate.key); err != nil {
			return 0, nil, err
		}
		if err := s.fireTriggers(table, "DELETE", "BEFORE", row, nil); err != nil {
			return 0, nil, err
		}
		if err := s.updateIndexes(table, info, row, candidate.key, false); err != nil {
			return 0, nil, err
		}
		if err := s.remove(candidate.key); err != nil {
			return 0, nil, err
		}
		if err := s.fireTriggers(table, "DELETE", "AFTER", row, nil); err != nil {
			return 0, nil, err
		}
	}
	return int64(len(candidates)), nil, nil
}
