package sql

import (
	"fmt"
	"maps"
	"strconv"
	"strings"
)

type foreignChange struct {
	table         string
	key           []byte
	before, after map[string]Value
	position      int
}

func (s *Schema) recordForeignChange(table string, key []byte, before, after map[string]Value) {
	if len(s.lookups().references[table]) == 0 {
		return
	}
	clone := func(row map[string]Value) map[string]Value {
		if row == nil {
			return nil
		}
		out := maps.Clone(row)
		for name, value := range out {
			if value.Type == ColBlob && !value.Null {
				value.Blob = copyBytes(value.Blob)
				out[name] = value
			}
		}
		return out
	}
	s.foreignChanges = append(s.foreignChanges, foreignChange{table: table, key: copyBytes(key), before: clone(before), after: clone(after), position: len(s.txWrites)})
}

func (s *Schema) processForeignActions(start int) error {
	if start >= len(s.foreignChanges) {
		return nil
	}
	active := s.statementActive
	s.statementActive = false
	defer func() { s.statementActive = active }()
	visited := map[string]bool{}
	for current := start; current < len(s.foreignChanges); current++ {
		if err := s.checkInterrupt(); err != nil {
			return err
		}
		change := s.foreignChanges[current]
		parent := s.tables[change.table]
		if parent == nil || change.before == nil {
			continue
		}
		for _, reference := range s.lookups().references[change.table] {
			columns := reference.key.RefColumns
			if len(columns) == 0 {
				columns = parent.PKColumns
			}
			before, exists := foreignValues(columns, change.before)
			if !exists {
				continue
			}
			after, afterExists := foreignValues(columns, change.after)
			if change.after != nil && afterExists && s.foreignValuesEqual(before, after) {
				continue
			}
			action := reference.key.OnDelete
			if change.after != nil {
				action = reference.key.OnUpdate
			}
			if action == "" || action == "NO ACTION" {
				continue
			}
			child := s.tables[reference.table]
			if child == nil {
				continue
			}
			entries, err := s.matchingDMLRows(reference.table, child, equalityPredicate(reference.key.Columns, before), -1, 0)
			if err != nil {
				return err
			}
			if action == "RESTRICT" && len(entries) > 0 {
				return fmt.Errorf("FOREIGN KEY RESTRICT failed: %s -> %s", reference.table, change.table)
			}
			for _, entry := range entries {
				row := decodeRow(entry.Value, child)
				if err := s.lockRow(reference.table, child, row, entry.Key.UserKey); err != nil {
					return err
				}
				data, found, err := s.schemaGet(entry.Key.UserKey)
				if err != nil {
					return err
				}
				if !found {
					continue
				}
				row = decodeRow(data, child)
				values, present := foreignValues(reference.key.Columns, row)
				if !present || !s.foreignValuesEqual(values, before) {
					continue
				}
				identity := reference.table + "\x00" + reference.key.Name + "\x00" + action + "\x00" + string(entry.Key.UserKey)
				for _, value := range after {
					identity += string(s.valueIdentity(value))
				}
				if visited[identity] {
					continue
				}
				visited[identity] = true
				if action == "CASCADE" && change.after == nil {
					if err := s.deletePhysicalRow(reference.table, child, entry.Key.UserKey, row); err != nil {
						return err
					}
					continue
				}
				var assignments []UpdateSet
				for i, name := range reference.key.Columns {
					var value Value
					switch action {
					case "CASCADE":
						if !afterExists {
							return fmt.Errorf("invalid cascaded parent key")
						}
						value = after[i]
					case "SET NULL":
						value = NewNullValue()
					case "SET DEFAULT":
						value = NewNullValue()
						for _, column := range child.Columns {
							if column.Name == name && column.Default != nil {
								var err error
								value, err = s.evalDefinitionExpression(column.Default, column.DefaultDialect, nil)
								if err != nil {
									return err
								}
								break
							}
						}
					default:
						return fmt.Errorf("unsupported foreign key action %s", action)
					}
					assignments = append(assignments, UpdateSet{Column: name, Value: &LiteralExpr{Value: value}})
				}
				if _, err := s.applyRowUpdate(reference.table, child, entry.Key.UserKey, row, assignments, row); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *Schema) deletePhysicalRow(table string, info *TableInfo, key []byte, row map[string]Value) error {
	if err := s.lockRow(table, info, row, key); err != nil {
		return err
	}
	if err := s.fireTriggers(table, "DELETE", "BEFORE", row, nil); err != nil {
		return err
	}
	if err := s.updateIndexes(table, info, row, key, false); err != nil {
		return err
	}
	if err := s.remove(key); err != nil {
		return err
	}
	s.recordForeignChange(table, key, row, nil)
	return s.fireTriggers(table, "DELETE", "AFTER", row, nil)
}

func foreignConstraintID(info *TableInfo, fk ForeignKeyDef) string {
	name := fk.Name
	if name == "" {
		name = strings.Join(fk.Columns, ",") + "->" + fk.RefTable + "(" + strings.Join(fk.RefColumns, ",") + ")"
	}
	return strconv.FormatUint(info.ID, 10) + ":" + name
}

func (s *Schema) foreignKeyDeferred(table string, fk ForeignKeyDef) bool {
	if !s.inTx || s.checkingDeferred || !fk.Deferrable {
		return false
	}
	if mode, ok := s.foreignConstraintModes[foreignConstraintID(s.tables[table], fk)]; ok {
		return mode
	}
	return fk.InitiallyDeferred
}

func (s *Schema) checkDeferredForeignKeys() error {
	if !s.inTx {
		return nil
	}
	pending := false
	for table, keys := range s.lookups().foreign {
		for _, fk := range keys {
			pending = pending || s.foreignKeyDeferred(table, fk)
		}
	}
	if !pending {
		return nil
	}
	s.checkingDeferred = true
	defer func() { s.checkingDeferred = false }()
	return s.checkStatementForeignKeys(0)
}

func (s *Schema) SetConstraints(statement *SetConstraintsStmt) error {
	if !s.inTx {
		return fmt.Errorf("SET CONSTRAINTS requires an active transaction")
	}
	before := maps.Clone(s.foreignConstraintModes)
	if s.foreignConstraintModes == nil {
		s.foreignConstraintModes = map[string]bool{}
	}
	matched := map[string]bool{}
	// Bare constraint names follow the session search path. An explicit schema
	// selects that namespace even when a different namespace is current.
	selectedNames := map[string]string{}
	for _, name := range statement.Names {
		if strings.Contains(name, ".") {
			schema, object := SplitSchemaName(name)
			selectedNames[name] = s.namespaceAlias(schema) + "." + object
			continue
		}
		for _, schema := range s.SearchPath() {
			found := false
			for table, keys := range s.lookups().foreign {
				tableSchema, _ := SplitSchemaName(table)
				if tableSchema != s.namespaceAlias(schema) {
					continue
				}
				for _, fk := range keys {
					found = found || fk.Name == name
				}
			}
			if found {
				selectedNames[name] = s.namespaceAlias(schema) + "." + name
				break
			}
		}
	}
	for table, keys := range s.lookups().foreign {
		info := s.tables[table]
		for _, fk := range keys {
			selected := statement.All
			for _, name := range statement.Names {
				schema, _ := SplitSchemaName(table)
				qualified := schema + "." + fk.Name
				if selectedNames[name] == qualified {
					selected = true
					matched[name] = true
				}
			}
			if !selected {
				continue
			}
			if statement.Deferred && !fk.Deferrable {
				if statement.All {
					continue
				}
				s.foreignConstraintModes = before
				return fmt.Errorf("constraint %s is not DEFERRABLE", fk.Name)
			}
			s.foreignConstraintModes[foreignConstraintID(info, fk)] = statement.Deferred
		}
	}
	for _, name := range statement.Names {
		if !matched[name] {
			s.foreignConstraintModes = before
			return fmt.Errorf("constraint %s does not exist", name)
		}
	}
	if !statement.Deferred {
		if err := s.checkStatementForeignKeys(0); err != nil {
			s.foreignConstraintModes = before
			return err
		}
	}
	return nil
}

func (s *Schema) foreignValuesEqual(a, b []Value) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if s.valueCompare(a[i], b[i]) != 0 {
			return false
		}
	}
	return true
}
