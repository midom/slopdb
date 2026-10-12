package sql

import "fmt"

type referencingKey struct {
	table string
	key   ForeignKeyDef
}

func tableForeignKeys(info *TableInfo) []ForeignKeyDef {
	keys := append([]ForeignKeyDef(nil), info.ForeignKeys...)
	for _, col := range info.Columns {
		if col.RefTable != "" {
			keys = append(keys, ForeignKeyDef{Columns: []string{col.Name}, RefTable: col.RefTable, RefColumns: col.RefColumns})
		}
	}
	return keys
}

func (s *Schema) foreignDefinitions(info *TableInfo) error {
	for _, fk := range tableForeignKeys(info) {
		parent := s.tables[fk.RefTable]
		if parent == nil {
			return fmt.Errorf("referenced table %s does not exist", fk.RefTable)
		}
		ref := fk.RefColumns
		if len(ref) == 0 {
			ref = parent.PKColumns
		}
		if len(fk.Columns) == 0 || len(fk.Columns) != len(ref) {
			return fmt.Errorf("foreign key column count mismatch on table %s", info.Name)
		}
		if err := validateIndexColumns(info.Columns, fk.Columns); err != nil {
			return err
		}
		if err := validateIndexColumns(parent.Columns, ref); err != nil {
			return err
		}
		unique := candidateColumns(ref, parent.PKColumns)
		for _, group := range parent.TableUnique {
			unique = unique || candidateColumns(ref, group)
		}
		for _, col := range parent.Columns {
			unique = unique || (col.Unique && len(ref) == 1 && ref[0] == col.Name)
		}
		for _, idx := range s.indexes {
			unique = unique || (idx.Table == parent.Name && idx.IsUnique && candidateColumns(ref, idx.columnNames()))
		}
		if !unique {
			return fmt.Errorf("foreign key on %s must reference a PRIMARY KEY or UNIQUE key", info.Name)
		}
		for i, name := range fk.Columns {
			var sourceType, targetType ColumnType
			for _, col := range info.Columns {
				if col.Name == name {
					sourceType = col.ColType
				}
			}
			for _, col := range parent.Columns {
				if col.Name == ref[i] {
					targetType = col.ColType
				}
			}
			if sourceType != targetType {
				return fmt.Errorf("foreign key type mismatch: %s.%s and %s.%s", info.Name, name, parent.Name, ref[i])
			}
		}
	}
	return nil
}

func equalityPredicate(columns []string, values []Value) Expression {
	var where Expression
	for i, col := range columns {
		term := &BinaryExpr{Op: "=", Left: &ColumnRefExpr{Name: col}, Right: &LiteralExpr{Value: values[i]}}
		if where == nil {
			where = term
		} else {
			where = &BinaryExpr{Op: "AND", Left: where, Right: term}
		}
	}
	return where
}

func foreignValues(columns []string, row map[string]Value) ([]Value, bool) {
	values := make([]Value, len(columns))
	for i, col := range columns {
		values[i] = row[col]
		if values[i].Null {
			return nil, false
		}
	}
	return values, true
}

func (s *Schema) checkForeignRow(table string, row map[string]Value) error {
	for _, fk := range s.lookups().foreign[table] {
		values, present := foreignValues(fk.Columns, row)
		if !present {
			continue
		} // MATCH SIMPLE: any NULL exempts the reference.
		parent := s.tables[fk.RefTable]
		ref := fk.RefColumns
		if len(ref) == 0 {
			ref = parent.PKColumns
		}
		where := equalityPredicate(ref, values)
		var parentKey []byte
		if candidateColumns(ref, parent.PKColumns) {
			parentRow := make(map[string]Value, len(ref))
			for i, name := range ref {
				parentRow[name] = values[i]
			}
			parentKey = parent.encodePK(parent.Name, parentRow)
			// Parent changes hold an exclusive row lock. Shared references live
			// until transaction completion, preventing a concurrent orphan.
			if err := s.RequestLock(LockRequest{Resource: "row:" + string(parentKey)}); err != nil {
				return err
			}
			data, found, err := s.schemaGet(parentKey)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("FOREIGN KEY constraint failed: %s -> %s", table, parent.Name)
			}
			match, err := s.evalCondition(where, decodeRow(data, parent))
			if err != nil {
				return err
			}
			if !match {
				return fmt.Errorf("FOREIGN KEY constraint failed: %s", table)
			}
		} else {
			entries, err := s.matchingDMLRows(parent.Name, parent, where, 1, 0)
			if err != nil {
				return err
			}
			if len(entries) == 0 {
				return fmt.Errorf("FOREIGN KEY constraint failed: %s -> %s", table, parent.Name)
			}
			parentKey = entries[0].Key.UserKey
			if err := s.RequestLock(LockRequest{Resource: "row:" + string(parentKey)}); err != nil {
				return err
			}
			data, found, err := s.schemaGet(parentKey)
			if err != nil {
				return err
			}
			if !found {
				return ErrStatementRetry
			}
			match, err := s.evalCondition(where, decodeRow(data, parent))
			if err != nil {
				return err
			}
			if !match {
				return ErrStatementRetry
			}
		}
	}
	return nil
}

// Validate the final statement state, so self references and multi-row changes
// are checked atomically instead of rejecting a parent inserted later in VALUES.
func (s *Schema) checkStatementForeignKeys(pos int) error {
	lookup := s.lookups()
	if len(lookup.foreign) == 0 {
		return nil
	}
	// References must see commits completed before acquiring the parent lock,
	// rather than an older statement snapshot. Private writes remain visible.
	active := s.statementActive
	s.statementActive = false
	defer func() { s.statementActive = active }()
	checked := make(map[string]bool)
	for i := pos; i < len(s.txWrites); i++ {
		w := s.txWrites[i]
		name := string(w.key)
		if checked[name] {
			continue
		}
		checked[name] = true
		table, info := s.rowTable(w.key)
		if info == nil {
			continue
		}
		latest := s.txWrites[s.txLatest[name]]
		if !latest.del {
			if err := s.checkForeignRow(table, decodeRow(latest.value, info)); err != nil {
				return err
			}
		}
		if len(lookup.references[table]) == 0 {
			continue
		}
		var old []byte
		if w.previous >= 0 {
			before := s.txWrites[w.previous]
			if !before.del {
				old = before.value
			}
		} else {
			var found bool
			var err error
			old, found, err = s.committedGet(w.key)
			if err != nil {
				return err
			}
			if !found {
				continue
			}
		}
		if old == nil {
			continue
		}
		oldRow := decodeRow(old, info)
		var newRow map[string]Value
		if !latest.del {
			newRow = decodeRow(latest.value, info)
		}
		for _, ref := range lookup.references[table] {
			columns := ref.key.RefColumns
			if len(columns) == 0 {
				columns = info.PKColumns
			}
			before, present := foreignValues(columns, oldRow)
			if !present {
				continue
			}
			after, exists := foreignValues(columns, newRow)
			if !latest.del && exists && equalIndexValues(before, after) {
				continue
			}
			entries, err := s.matchingDMLRows(ref.table, s.tables[ref.table], equalityPredicate(ref.key.Columns, before), -1, 0)
			if err != nil {
				return err
			}
			for _, entry := range entries {
				if err := s.checkForeignRow(ref.table, decodeRow(entry.Value, s.tables[ref.table])); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *Schema) checkDropReferencedTable(table string) error {
	for _, ref := range s.lookups().references[table] {
		if ref.table != table {
			return fmt.Errorf("cannot drop table %s: foreign key from %s", table, ref.table)
		}
	}
	return nil
}

func (s *Schema) renameForeignKeys(table, newTable, oldColumn, newColumn string) {
	for _, info := range s.tables {
		for i := range info.Constraints {
			c := &info.Constraints[i]
			if c.Kind == "FOREIGN" && c.RefTable == table {
				c.RefTable = newTable
				for n, col := range c.RefColumns {
					if oldColumn != "" && col == oldColumn {
						c.RefColumns[n] = newColumn
					}
				}
			}
		}
		for i := range info.ForeignKeys {
			fk := &info.ForeignKeys[i]
			if info.Name == newTable && oldColumn != "" {
				for j, col := range fk.Columns {
					if col == oldColumn {
						fk.Columns[j] = newColumn
					}
				}
			}
			if fk.RefTable == table {
				fk.RefTable = newTable
				if oldColumn != "" {
					for j, col := range fk.RefColumns {
						if col == oldColumn {
							fk.RefColumns[j] = newColumn
						}
					}
				}
			}
		}
		for i := range info.Columns {
			col := &info.Columns[i]
			if col.RefTable == table {
				col.RefTable = newTable
				for j, name := range col.RefColumns {
					if name == oldColumn {
						col.RefColumns[j] = newColumn
					}
				}
			}
		}
	}
}

// A referenced UNIQUE index cannot be removed while its foreign key depends on
// it, unless another candidate key guarantees the same referenced columns.
func (s *Schema) checkDropForeignIndex(idx *IndexInfo) error {
	if !idx.IsUnique {
		return nil
	}
	parent := s.tables[idx.Table]
	for _, ref := range s.lookups().references[idx.Table] {
		columns := ref.key.RefColumns
		if len(columns) == 0 {
			columns = parent.PKColumns
		}
		if !candidateColumns(columns, idx.columnNames()) || candidateColumns(columns, parent.PKColumns) {
			continue
		}
		other := false
		for _, group := range parent.TableUnique {
			other = other || candidateColumns(group, columns)
		}
		for _, col := range parent.Columns {
			other = other || (col.Unique && len(columns) == 1 && columns[0] == col.Name)
		}
		for _, candidate := range s.indexes {
			other = other || (candidate != idx && candidate.Table == idx.Table && candidate.IsUnique && candidateColumns(candidate.columnNames(), columns))
		}
		if !other {
			return fmt.Errorf("cannot drop index %s: foreign key from %s", idx.Name, ref.table)
		}
	}
	return nil
}

func candidateColumns(columns, key []string) bool {
	if len(columns) == 0 || len(columns) != len(key) {
		return false
	}
	seen := make(map[string]bool, len(key))
	for _, col := range key {
		seen[col] = true
	}
	for _, col := range columns {
		if !seen[col] {
			return false
		}
		delete(seen, col)
	}
	return len(seen) == 0
}
