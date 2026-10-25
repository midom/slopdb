package sql

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"slopdb/storage"
)

func (s *Schema) AlterTable(a *AlterTableStmt) error {
	if !s.inTx {
		return s.ExecuteStatement(func() error { return s.AlterTable(a) })
	}
	s.ensureCatalogWritable()
	if a.ObjectKind == "INDEX" {
		return s.alterIndex(a)
	}
	if a.ObjectKind == "VIEW" {
		return s.alterView(a)
	}
	if a.Operation == "MULTI" && s.tables[a.Table] == nil {
		if a.TableIfExists {
			return nil
		}
		return fmt.Errorf("table %s does not exist", a.Table)
	}
	if a.Operation == "MULTI" || a.Operation == "RENAME_TABLES" {
		name := a.Table
		for _, action := range a.Actions {
			c := *action
			if a.Operation == "MULTI" {
				c.Table = name
			}
			if err := s.AlterTable(&c); err != nil {
				return err
			}
			if c.Operation == "RENAME_TO" {
				name = c.NewName
			}
		}
		return nil
	}
	info := s.tables[a.Table]
	if info == nil {
		if a.TableIfExists {
			return nil
		}
		return fmt.Errorf("table %q does not exist", a.Table)
	}
	if a.Operation == "ADD_COLUMN" && a.IfNotExists && slices.ContainsFunc(info.Columns, func(c ColumnDef) bool { return c.Name == a.Column.Name }) {
		return nil
	}
	if a.Operation == "DROP_COLUMN" && a.IfExists && !slices.ContainsFunc(info.Columns, func(c ColumnDef) bool { return c.Name == a.OldColName }) {
		return nil
	}
	if a.Operation == "DROP_CONSTRAINT" && a.IfExists && !slices.ContainsFunc(info.Constraints, func(c ConstraintDef) bool { return c.Name == a.NewName }) {
		return nil
	}
	if a.Operation == "RENAME_TO" {
		return s.renameTable(a.Table, a.NewName)
	}
	if a.Operation == "RENAME_COLUMN" {
		return s.renameColumn(info, a.OldColName, a.NewColName)
	}
	if a.Operation == "RENAME_INDEX" {
		idx := s.indexes[a.OldColName]
		if idx == nil || idx.Table != a.Table {
			return fmt.Errorf("index %s does not belong to %s", a.OldColName, a.Table)
		}
		return s.renameIndex(a.OldColName, a.NewColName)
	}
	if a.Operation == "RENAME_CONSTRAINT" {
		for i := range info.Constraints {
			if info.Constraints[i].Name == a.NewColName {
				return fmt.Errorf("constraint %s already exists", a.NewColName)
			}
		}
		for i := range info.Constraints {
			if info.Constraints[i].Name == a.OldColName {
				info.Constraints[i].Name = a.NewColName
				return s.saveSchema()
			}
		}
		return fmt.Errorf("constraint %s does not exist", a.OldColName)
	}
	column := func(name string) (*ColumnDef, error) {
		for i := range info.Columns {
			if info.Columns[i].Name == name {
				return &info.Columns[i], nil
			}
		}
		return nil, fmt.Errorf("column %q does not exist", name)
	}
	switch a.Operation {
	case "SET_DEFAULT", "DROP_DEFAULT", "DROP_NOT_NULL", "SET_COLLATION":
		col, err := column(a.OldColName)
		if err != nil {
			return err
		}
		switch a.Operation {
		case "SET_DEFAULT":
			col.Default = a.Default
			col.DefaultSQL = formatExpr(a.Default)
		case "DROP_DEFAULT":
			col.Default = nil
			col.DefaultSQL = ""
		case "DROP_NOT_NULL":
			if slices.Contains(info.PKColumns, col.Name) {
				return fmt.Errorf("PRIMARY KEY columns cannot allow NULL")
			}
			col.NotNull = false
			delete(col.ConstraintNames, "NOT_NULL")
			info.Constraints = slices.DeleteFunc(info.Constraints, func(c ConstraintDef) bool { return c.Kind == "NOT_NULL" && slices.Contains(c.Columns, col.Name) })
		case "SET_COLLATION":
			if a.NewName != "binary" {
				return fmt.Errorf("unsupported collation: %s", a.NewName)
			}
			if col.ColType != ColText {
				return fmt.Errorf("BINARY collation requires a TEXT column")
			}
			if !col.Binary {
				col.Binary = true
				if err := s.rebuildCollation(s.collation); err != nil {
					return err
				}
			}
		}
		if err = validateConstraintExpressions(info); err != nil {
			return err
		}
		delete(s.distributions, a.Table)
		return s.saveSchema()
	case "ADD_INDEX":
		c := a.Constraint
		name := c.Name
		if name == "" {
			name = info.Name + "_" + strings.Join(c.Columns, "_") + "_idx"
		}
		return s.CreateIndex(&CreateIndexStmt{Table: info.Name, Index: name, Columns: c.Columns, Unique: c.Kind == "UNIQUE_INDEX", Fulltext: c.Fulltext})
	case "DROP_INDEX":
		idx := s.indexes[a.NewName]
		if idx == nil {
			if a.IfExists {
				return nil
			}
			return fmt.Errorf("index %s does not exist", a.NewName)
		}
		if idx.Table != info.Name {
			return fmt.Errorf("index %s does not belong to %s", a.NewName, info.Name)
		}
		return s.DropIndex(&DropIndexStmt{Index: a.NewName})
	}
	if a.Operation == "ALTER_TYPE" {
		col, err := column(a.OldColName)
		if err != nil {
			return err
		}
		lengthOnly := a.Column != nil && a.Column.ColType == col.ColType &&
			(col.ColType == ColText || col.ColType == ColBlob) &&
			a.Column.Length > 0 && a.Using == nil &&
			!(a.Column.Binary && !col.Binary)
		if a.AllowExisting && (!lengthOnly || (col.Length > 0 && a.Column.Length >= col.Length)) {
			return fmt.Errorf("ALLOW EXISTING requires a shorter VARCHAR or VARBINARY length without conversion")
		}
		if lengthOnly && ((col.Length > 0 && a.Column.Length >= col.Length) || a.AllowExisting) {
			col.Length = a.Column.Length
			if a.AllowExisting {
				col.AllowExistingLength = true
			}
			delete(s.distributions, a.Table)
			return s.saveSchema()
		}
	}
	oldInfo := *info
	oldInfo.Columns = slices.Clone(info.Columns)
	oldInfo.PKColumns = slices.Clone(info.PKColumns)
	if a.Using != nil {
		if err := s.validateDMLExpressions(&oldInfo, nil, []UpdateSet{{Value: a.Using}}, nil); err != nil {
			return err
		}
	}
	entries, err := s.matchingDMLRows(info.Name, info, nil, -1, 0)
	if err != nil {
		return err
	}
	rows := make([]map[string]Value, len(entries))
	preserveExisting := slices.ContainsFunc(info.Columns, func(c ColumnDef) bool { return c.AllowExistingLength })
	var originalRows []map[string]Value
	if preserveExisting {
		originalRows = make([]map[string]Value, len(entries))
	}
	for i, e := range entries {
		rows[i] = decodeRow(e.Value, &oldInfo)
		if preserveExisting {
			originalRows[i] = maps.Clone(rows[i])
		}
	}
	// Remove every old entry in the private overlay before validating/rekeying.
	for i, e := range entries {
		if err = s.updateIndexes(info.Name, &oldInfo, rows[i], e.Key.UserKey, false); err != nil {
			return err
		}
		if err = s.remove(e.Key.UserKey); err != nil {
			return err
		}
	}
	if a.Operation == "ADD_COLUMN" {
		if _, e := column(a.Column.Name); e == nil {
			if a.IfNotExists {
				return s.restoreAlterRows(&oldInfo, entries, rows)
			}
			return fmt.Errorf("duplicate column: %s", a.Column.Name)
		}
		col := *a.Column
		if col.Default != nil {
			col.DefaultSQL = formatExpr(col.Default)
		}
		if col.Check != nil {
			col.CheckSQL = formatExpr(col.Check)
		}
		info.Columns = append(info.Columns, col)
		for _, row := range rows {
			v := NewNullValue()
			if col.Default != nil {
				v, err = s.evalExpr(col.Default, nil)
				if err != nil {
					return err
				}
			}
			row[col.Name] = applyAffinity(v, col.ColType)
		}
		if col.PrimaryKey {
			if len(info.PKColumns) > 0 {
				return fmt.Errorf("multiple PRIMARY KEY declarations")
			}
			info.PKColumns = []string{col.Name}
		}
		if col.Unique {
			if err = s.addConstraintIndex(info, &ConstraintDef{Kind: "UNIQUE", Columns: []string{col.Name}, Inline: true}); err != nil {
				return err
			}
		}
		if err = s.positionColumn(info, col.Name, a.Position, a.After); err != nil {
			return err
		}
	} else if a.Operation == "DROP_COLUMN" {
		pos := slices.IndexFunc(info.Columns, func(c ColumnDef) bool { return c.Name == a.OldColName })
		if pos < 0 {
			if a.IfExists {
				return s.restoreAlterRows(&oldInfo, entries, rows)
			}
			return fmt.Errorf("column %s does not exist", a.OldColName)
		}
		if len(info.Columns) == 1 {
			return fmt.Errorf("cannot drop the last column")
		}
		if err = s.dropColumnDependencies(info, a.OldColName, a.Cascade); err != nil {
			return err
		}
		info.Columns = append(info.Columns[:pos], info.Columns[pos+1:]...)
		for _, row := range rows {
			delete(row, a.OldColName)
		}
	} else if a.Operation == "ALTER_TYPE" || a.Operation == "MODIFY_COLUMN" {
		col, err := column(a.OldColName)
		if err != nil {
			return err
		}
		target := a.Column.ColType
		for _, row := range rows {
			v := row[a.OldColName]
			if a.Using != nil {
				v, err = s.evalExpr(a.Using, row)
				if err != nil {
					return err
				}
			}
			if !v.Null {
				v, err = castSQLValue(v, target.String())
				if err != nil {
					return err
				}
			}
			row[a.OldColName] = v
		}
		if a.Operation == "MODIFY_COLUMN" {
			replacement := *a.Column
			replacement.PrimaryKey = col.PrimaryKey
			replacement.Unique = col.Unique || replacement.Unique
			replacement.AutoIncrement = col.AutoIncrement || replacement.AutoIncrement
			replacement.Name = a.OldColName
			// MODIFY/CHANGE supply a new column definition. Retain independent
			// key constraints, but replace inline checks, references and nullability.
			if replacement.ConstraintNames == nil {
				replacement.ConstraintNames = make(map[string]string)
			}
			for kind, name := range col.ConstraintNames {
				present := kind == "PRIMARY" || kind == "UNIQUE" ||
					(kind == "NOT_NULL" && replacement.NotNull) ||
					(kind == "CHECK" && replacement.Check != nil) ||
					(kind == "FOREIGN" && replacement.RefTable != "")
				if present && replacement.ConstraintNames[kind] == "" {
					replacement.ConstraintNames[kind] = name
				}
			}
			info.Constraints = slices.DeleteFunc(info.Constraints, func(c ConstraintDef) bool {
				return c.Inline && slices.Equal(c.Columns, []string{a.OldColName}) &&
					(c.Kind == "CHECK" || c.Kind == "FOREIGN" || c.Kind == "NOT_NULL")
			})
			*col = replacement
			if col.Default != nil {
				col.DefaultSQL = formatExpr(col.Default)
			}
			if col.Check != nil {
				col.CheckSQL = formatExpr(col.Check)
			}
		} else {
			col.ColType = target
			col.Length = a.Column.Length
			col.AllowExistingLength = false
			if target != ColText {
				col.Binary = false
			} else if a.Column.Binary {
				col.Binary = true
			}
			if col.Default != nil {
				col.Default = &FuncCallExpr{Name: "CAST", Args: []Expression{col.Default, &LiteralExpr{Value: NewTextValue(target.String())}}}
				col.DefaultSQL = formatExpr(col.Default)
			}
		}
		if a.Column.Name != a.OldColName {
			if err = s.renameColumn(info, a.OldColName, a.Column.Name); err != nil {
				return err
			}
			for _, row := range rows {
				row[a.Column.Name] = row[a.OldColName]
				delete(row, a.OldColName)
			}
		}
		if err = s.positionColumn(info, a.Column.Name, a.Position, a.After); err != nil {
			return err
		}
	} else if a.Operation == "SET_NOT_NULL" {
		col, err := column(a.OldColName)
		if err != nil {
			return err
		}
		col.NotNull = true
	} else if a.Operation == "ADD_CONSTRAINT" {
		if err = s.addTableConstraint(info, *a.Constraint); err != nil {
			return err
		}
	} else if a.Operation == "DROP_CONSTRAINT" || a.Operation == "DROP_PRIMARY" {
		name := a.NewName
		if a.Operation == "DROP_PRIMARY" {
			for _, c := range info.Constraints {
				if c.Kind == "PRIMARY" {
					name = c.Name
				}
			}
		}
		if err = s.dropTableConstraint(info, name, a.IfExists, a.Cascade); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("unsupported ALTER operation %s", a.Operation)
	}
	if err = primaryKeyColumns(info); err != nil {
		return err
	}
	if err = validateConstraintExpressions(info); err != nil {
		return err
	}
	s.catalogLookup = nil
	for _, t := range s.tables {
		if err = s.foreignDefinitions(t); err != nil {
			return err
		}
	}
	if err = s.initializeConstraintNames(info); err != nil {
		return err
	}
	groups := s.uniqueGroups(info.Name, info)
	seen := make([]map[string]bool, len(groups))
	for i := range seen {
		seen[i] = make(map[string]bool)
	}
	keys := make([][]byte, len(rows))
	pkChanged := !slices.Equal(oldInfo.PKColumns, info.PKColumns) || a.Operation == "ALTER_TYPE" || a.Operation == "MODIFY_COLUMN"
	for i, row := range rows {
		var assigned map[string]bool
		var original map[string]Value
		if a.Operation == "ALTER_TYPE" || a.Operation == "MODIFY_COLUMN" {
			assigned = map[string]bool{a.OldColName: true}
		}
		if preserveExisting {
			original = originalRows[i]
		}
		if err = s.checkNonUniqueConstraintsPreserving(info.Name, info, row, original, assigned); err != nil {
			return err
		}
		for n, g := range groups {
			values, hasNull := indexValues(g, row)
			if hasNull {
				continue
			}
			var buf []byte
			for i, v := range values {
				if info.binaryColumn(g[i]) {
					v.Binary = true
				}
				buf = s.appendValueIdentity(buf, v)
			}
			if seen[n][string(buf)] {
				return fmt.Errorf("UNIQUE constraint failed: %s.%s", info.Name, strings.Join(g, ","))
			}
			seen[n][string(buf)] = true
		}
		key := entries[i].Key.UserKey
		if pkChanged {
			if len(info.PKColumns) > 0 {
				key = info.encodePK(info.Name, row)
			} else if len(oldInfo.PKColumns) > 0 {
				id, e := s.nextRowID(info.Name)
				if e != nil {
					return e
				}
				key = s.makeRowKey(info.Name, info, row, id)
			}
		}
		keys[i] = key
		if err = s.write(key, encodeRow(row, info)); err != nil {
			return err
		}
		if err = s.updateIndexes(info.Name, info, row, key, true); err != nil {
			return err
		}
	}
	for _, row := range rows {
		if err = s.checkForeignRow(info.Name, row); err != nil {
			return err
		}
	}
	for _, child := range s.tables {
		if child.Name == info.Name {
			continue
		}
		referenced := false
		for _, fk := range tableForeignKeys(child) {
			referenced = referenced || fk.RefTable == info.Name
		}
		if !referenced {
			continue
		}
		children, e := s.matchingDMLRows(child.Name, child, nil, -1, 0)
		if e != nil {
			return e
		}
		for _, entry := range children {
			if e = s.checkForeignRow(child.Name, decodeRow(entry.Value, child)); e != nil {
				return e
			}
		}
	}
	delete(s.distributions, info.Name)
	s.catalogLookup = nil
	if err = s.initializeConstraintNames(info); err != nil {
		return err
	}
	if err = s.saveIndexes(); err != nil {
		return err
	}
	return s.saveSchema()
}

func (s *Schema) restoreAlterRows(info *TableInfo, entries []storage.KeyValue, rows []map[string]Value) error {
	for i, e := range entries {
		if err := s.write(e.Key.UserKey, e.Value); err != nil {
			return err
		}
		if err := s.updateIndexes(info.Name, info, rows[i], e.Key.UserKey, true); err != nil {
			return err
		}
	}
	return nil
}
func (s *Schema) positionColumn(info *TableInfo, name, position, after string) error {
	if position == "" {
		return nil
	}
	pos := slices.IndexFunc(info.Columns, func(c ColumnDef) bool { return c.Name == name })
	if pos < 0 {
		return fmt.Errorf("column %s does not exist", name)
	}
	col := info.Columns[pos]
	list := append(slices.Clone(info.Columns[:pos]), info.Columns[pos+1:]...)
	target := 0
	if position == "AFTER" {
		if after == name {
			return fmt.Errorf("cannot position a column after itself")
		}
		target = slices.IndexFunc(list, func(c ColumnDef) bool { return c.Name == after })
		if target < 0 {
			return fmt.Errorf("column %s does not exist", after)
		}
		target++
	}
	info.Columns = slices.Insert(list, target, col)
	return nil
}

func (s *Schema) renameColumn(info *TableInfo, old, name string) error {
	if old == name {
		for _, c := range info.Columns {
			if c.Name == old {
				return nil
			}
		}
		return fmt.Errorf("column %s does not exist", old)
	}
	found := false
	for i := range info.Columns {
		if info.Columns[i].Name == name {
			return fmt.Errorf("duplicate column: %s", name)
		}
		if info.Columns[i].Name == old {
			info.Columns[i].Name = name
			found = true
		}
	}
	if !found {
		return fmt.Errorf("column %s does not exist", old)
	}
	rename := func(cols []string) {
		for i, c := range cols {
			if c == old {
				cols[i] = name
			}
		}
	}
	rename(info.PKColumns)
	for _, g := range info.TableUnique {
		rename(g)
	}
	for i := range info.Columns {
		col := &info.Columns[i]
		col.Check = renameDependencyExpr(col.Check, info.Name, info.Name, old, name, true, false)
		if col.Check != nil {
			col.CheckSQL = formatExpr(col.Check)
		}
	}
	for i, e := range info.TableCheck {
		info.TableCheck[i] = renameDependencyExpr(e, info.Name, info.Name, old, name, true, false)
		info.TableCheckSQL[i] = formatExpr(info.TableCheck[i])
	}
	for i := range info.Constraints {
		c := &info.Constraints[i]
		rename(c.Columns)
		if c.CheckSQL != "" {
			e, err := parseExpr(c.CheckSQL)
			if err != nil {
				return err
			}
			c.Check = renameDependencyExpr(e, info.Name, info.Name, old, name, true, false)
			c.CheckSQL = formatExpr(c.Check)
		}
	}
	for _, idx := range s.indexes {
		if idx.Table == info.Name {
			if idx.Column == old {
				idx.Column = name
			}
			rename(idx.Columns)
			s.bindIndexCollation(idx)
		}
	}
	s.renameForeignKeys(info.Name, info.Name, old, name)
	if err := s.renameDependencies(info.Name, info.Name, old, name); err != nil {
		return err
	}
	delete(s.distributions, info.Name)
	s.catalogLookup = nil
	if err := s.saveIndexes(); err != nil {
		return err
	}
	if err := s.saveViews(); err != nil {
		return err
	}
	if err := s.saveTriggers(); err != nil {
		return err
	}
	return s.saveSchema()
}

func (s *Schema) renameIndex(old, name string) error {
	idx := s.indexes[old]
	if idx == nil {
		return fmt.Errorf("index %s does not exist", old)
	}
	if old == name {
		return nil
	}
	if s.indexes[name] != nil || name == "primary" {
		return fmt.Errorf("index %s already exists or is reserved", name)
	}
	idx.Name = name
	s.indexes[name] = idx
	delete(s.indexes, old)
	return s.saveIndexes()
}
func (s *Schema) alterIndex(a *AlterTableStmt) error {
	idx := s.indexes[a.Table]
	if idx == nil {
		if a.TableIfExists {
			return nil
		}
		return fmt.Errorf("index %s does not exist", a.Table)
	}
	if a.Operation == "RENAME_TO" {
		return s.renameIndex(a.Table, a.NewName)
	}
	if a.Operation != "REBUILD_INDEX" {
		return fmt.Errorf("unsupported ALTER INDEX action")
	}
	if s.isClusteredIndex(idx) {
		return nil
	}
	prefix := []byte(idx.storagePrefix())
	it := s.newIteratorSeek(prefix)
	var keys [][]byte
	for it.Valid() && bytes.HasPrefix(it.Key().UserKey, prefix) {
		keys = append(keys, append([]byte(nil), it.Key().UserKey...))
		it.Next()
	}
	err := storage.IteratorError(it)
	it.Close()
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err = s.remove(key); err != nil {
			return err
		}
	}
	info := s.tables[idx.Table]
	entries, err := s.matchingDMLRows(info.Name, info, nil, -1, 0)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		row := decodeRow(entry.Value, info)
		if err = s.write(encodeRowIndexKey(idx, row, entry.Key.UserKey), s.encodeIndexValue(idx, row)); err != nil {
			return err
		}
	}
	return nil
}
func (s *Schema) alterView(a *AlterTableStmt) error {
	v := s.views[a.Table]
	if v == nil {
		if a.TableIfExists {
			return nil
		}
		return fmt.Errorf("view %s does not exist", a.Table)
	}
	if a.Operation == "RENAME_TO" {
		if s.tables[a.NewName] != nil || s.views[a.NewName] != nil {
			return fmt.Errorf("object %s already exists", a.NewName)
		}
		v.Name = a.NewName
		s.views[a.NewName] = v
		delete(s.views, a.Table)
		if err := s.renameDependencies(a.Table, a.NewName, "", ""); err != nil {
			return err
		}
		if err := s.saveTriggers(); err != nil {
			return err
		}
	} else if a.Operation == "SET_VIEW" {
		parsed, err := Parse(a.ViewSQL)
		if err != nil {
			return err
		}
		v.SQL = a.ViewSQL
		v.Parsed = selectQuery(parsed[0])
		v.Columns = slices.Clone(a.ViewColumns)
	} else {
		return fmt.Errorf("unsupported ALTER VIEW action")
	}
	return s.saveViews()
}

func (s *Schema) addConstraintIndex(info *TableInfo, c *ConstraintDef) error {
	id, err := s.allocateObjectID()
	if err != nil {
		return err
	}
	name := c.Name
	if name == "" || s.indexes[name] != nil {
		name = "__constraint_" + strconv.FormatUint(id, 10)
	}
	idx := newIndexInfo(name, info.Name, c.Columns, true)
	s.bindIndexCollation(idx)
	idx.ID = id
	idx.Auto = true
	s.indexes[name] = idx
	c.IndexID = id
	s.catalogLookup = nil
	return nil
}
