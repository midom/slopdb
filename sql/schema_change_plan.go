package sql

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

const schemaExclusiveLock = "exclusive catalog lock; may wait for active transactions"

// explainSchemaChange reads catalog metadata only. It describes execution work,
// rather than executing DDL or checking every constraint against stored rows.
func (s *Schema) explainSchemaChange(stmt Statement) (*ResultSet, error) {
	p := &schemaChangePlanner{schema: s, tables: maps.Clone(s.tables), indexes: maps.Clone(s.indexes)}
	for name, info := range p.tables {
		clone := *info
		clone.Columns = slices.Clone(info.Columns)
		p.tables[name] = &clone
	}
	p.result.Columns = []ColumnDef{
		{Name: "action", ColType: ColText}, {Name: "object", ColType: ColText},
		{Name: "work", ColType: ColText}, {Name: "scan_rows", ColType: ColInteger},
		{Name: "rewrite_rows", ColType: ColInteger}, {Name: "rebuild_indexes", ColType: ColInteger},
		{Name: "locking", ColType: ColText}, {Name: "detail", ColType: ColText},
	}
	var err error
	switch st := stmt.(type) {
	case *CreateIndexStmt:
		err = p.createIndex(st, !s.InTransaction())
	case *DropIndexStmt:
		err = p.dropIndex(st.Index, "", st.PublicationAbortAfter)
	case *AlterTableStmt:
		err = p.alter(st, !s.InTransaction())
	default:
		err = fmt.Errorf("schema-change explanation is unsupported for %T", stmt)
	}
	return &p.result, err
}

type schemaChangePlanner struct {
	schema  *Schema
	tables  map[string]*TableInfo
	indexes map[string]*IndexInfo
	result  ResultSet
}

func (p *schemaChangePlanner) add(action, object, work, locking, detail string, scan, rewrite, indexes bool) {
	flag := func(v bool) Value {
		if v {
			return NewIntValue(1)
		}
		return NewIntValue(0)
	}
	p.result.Rows = append(p.result.Rows, map[string]Value{
		"action": NewTextValue(action), "object": NewTextValue(object), "work": NewTextValue(work),
		"scan_rows": flag(scan), "rewrite_rows": flag(rewrite), "rebuild_indexes": flag(indexes),
		"locking": NewTextValue(locking), "detail": NewTextValue(detail),
	})
}

func (p *schemaChangePlanner) createIndex(st *CreateIndexStmt, online bool) error {
	if err := p.validatePublicationPolicy(st.PublicationAbortAfter, online); err != nil {
		return err
	}
	name, table := strings.ToLower(st.Index), strings.ToLower(st.Table)
	if p.indexes[name] != nil {
		if !st.IfNotExists {
			return fmt.Errorf("index %q already exists", name)
		}
		locking := schemaExclusiveLock
		if online {
			locking = "shared catalog lock; no publication needed"
		}
		p.add("CREATE INDEX", name, "no-op", locking, "The index already exists.", false, false, false)
		return nil
	}
	info := p.tables[table]
	if info == nil {
		return fmt.Errorf("table %q does not exist", table)
	}
	columns := normalizedIndexColumns(st.Columns, st.Column)
	if err := validateIndexColumns(info.Columns, columns); err != nil {
		return err
	}
	work, locking, detail := "index build", schemaExclusiveLock, "Scans existing rows and builds index entries while holding the catalog lock."
	if online {
		work = "online index build"
		locking = "shared catalog lock during scan; exclusive catalog lock for publication"
		detail = "Writers continue during the snapshot scan and catch-up; publication may wait for active transactions."
	}
	if st.Unique {
		detail += " Publication fails if committed rows contain duplicate non-NULL keys."
	}
	locking, detail = publicationPolicyDescription(locking, detail, st.PublicationAbortAfter)
	p.add("CREATE INDEX", name, work, locking, detail, true, false, true)
	p.indexes[name] = newIndexInfo(name, table, columns, st.Unique)
	return nil
}

func (p *schemaChangePlanner) dropIndex(name, table string, abortAfter *time.Duration) error {
	if err := p.validatePublicationPolicy(abortAfter, !p.schema.InTransaction()); err != nil {
		return err
	}
	name = strings.ToLower(name)
	idx := p.indexes[name]
	if idx == nil {
		return fmt.Errorf("index %q does not exist", name)
	}
	if table != "" && idx.Table != table {
		return fmt.Errorf("index %s does not belong to %s", name, table)
	}
	if idx.Auto {
		return fmt.Errorf("cannot drop index backing a constraint: %s", name)
	}
	locking, detail := publicationPolicyDescription(schemaExclusiveLock,
		"Removes the index from the catalog without scanning rows or index entries; physical cleanup follows separately.", abortAfter)
	p.add("DROP INDEX", name, "catalog-only", locking, detail, false, false, false)
	delete(p.indexes, name)
	return nil
}

func (p *schemaChangePlanner) validatePublicationPolicy(abortAfter *time.Duration, online bool) error {
	if abortAfter == nil {
		return nil
	}
	if *abortAfter < 0 {
		return fmt.Errorf("index publication abort threshold must be nonnegative")
	}
	if !online {
		return fmt.Errorf("KILL TRANSACTIONS AFTER requires a standalone autocommit index change")
	}
	return nil
}

func publicationPolicyDescription(locking, detail string, abortAfter *time.Duration) (string, string) {
	if abortAfter != nil {
		locking += fmt.Sprintf("; abort blocking transactions after %s", *abortAfter)
		detail += fmt.Sprintf(" After waiting %s for publication, blocking transactions are cancelled and rolled back before publication continues.", *abortAfter)
	}
	return locking, detail
}

func (p *schemaChangePlanner) alter(st *AlterTableStmt, online bool) error {
	if st.PublicationAbortAfter != nil && (st.Operation != "ADD_INDEX" && st.Operation != "DROP_INDEX" || st.ObjectKind == "INDEX" || st.ObjectKind == "VIEW") {
		return fmt.Errorf("KILL TRANSACTIONS AFTER requires a single ALTER TABLE ADD INDEX or DROP INDEX action")
	}
	if err := p.validatePublicationPolicy(st.PublicationAbortAfter, online); err != nil {
		return err
	}
	if st.Operation == "MULTI" {
		table := st.Table
		for _, action := range st.Actions {
			copy := *action
			copy.Table = table
			// Every action stays in the same atomic transaction, so index
			// additions use the ordinary transactional build path.
			if err := p.alter(&copy, false); err != nil {
				return err
			}
			if copy.Operation == "RENAME_TO" {
				table = copy.NewName
			}
		}
		return nil
	}
	if st.ObjectKind == "INDEX" {
		idx := p.indexes[st.Table]
		if idx == nil {
			if st.TableIfExists {
				p.add(st.Operation, st.Table, "no-op", schemaExclusiveLock, "The index does not exist.", false, false, false)
				return nil
			}
			return fmt.Errorf("index %q does not exist", st.Table)
		}
		if st.Operation == "RENAME_TO" {
			p.add(st.Operation, st.Table, "catalog-only", schemaExclusiveLock, "Changes the index name and preserves physical keys.", false, false, false)
		} else if p.schema.isClusteredIndex(idx) {
			p.add(st.Operation, st.Table, "no-op", schemaExclusiveLock, "The clustered primary index already uses row keys.", false, false, false)
		} else {
			p.add(st.Operation, st.Table, "index rebuild", schemaExclusiveLock, "Scans table rows and replaces index entries.", true, false, true)
		}
		return nil
	}
	if st.ObjectKind == "VIEW" {
		if p.schema.views[st.Table] == nil {
			if st.TableIfExists {
				p.add(st.Operation, st.Table, "no-op", schemaExclusiveLock, "The view does not exist.", false, false, false)
				return nil
			}
			return fmt.Errorf("view %q does not exist", st.Table)
		}
		p.add(st.Operation, st.Table, "catalog-only", schemaExclusiveLock, "Updates stored view metadata.", false, false, false)
		return nil
	}
	info := p.tables[st.Table]
	if info == nil {
		if !st.TableIfExists {
			return fmt.Errorf("table %q does not exist", st.Table)
		}
		locking := schemaExclusiveLock
		if st.Operation == "ADD_INDEX" && online {
			locking = "shared catalog lock; no publication needed"
		}
		p.add(st.Operation, st.Table, "no-op", locking, "The table does not exist.", false, false, false)
		return nil
	}
	column := func() (*ColumnDef, error) {
		for i := range info.Columns {
			if info.Columns[i].Name == st.OldColName {
				return &info.Columns[i], nil
			}
		}
		return nil, fmt.Errorf("column %q does not exist", st.OldColName)
	}
	work, detail := "table rewrite", "Scans and rewrites table rows and maintained indexes; cost grows with table size."
	scan, rewrite, indexes := true, true, true
	switch st.Operation {
	case "IGNORE_ENGINE":
		p.add(st.Operation, st.Table, "no-op", schemaExclusiveLock, "ENGINE options are ignored; tables use the LSM engine.", false, false, false)
		return nil
	case "ADD_INDEX":
		name := st.Constraint.Name
		if name == "" {
			name = info.Name + "_" + strings.Join(st.Constraint.Columns, "_") + "_idx"
		}
		return p.createIndex(&CreateIndexStmt{Table: st.Table, Index: name, Columns: st.Constraint.Columns, Unique: st.Constraint.Kind == "UNIQUE_INDEX", PublicationAbortAfter: st.PublicationAbortAfter}, online)
	case "DROP_INDEX":
		if p.indexes[st.NewName] == nil && st.IfExists {
			p.add(st.Operation, st.NewName, "no-op", schemaExclusiveLock, "The index does not exist.", false, false, false)
			return nil
		}
		return p.dropIndex(st.NewName, st.Table, st.PublicationAbortAfter)
	case "RENAME_TO", "RENAME_COLUMN", "RENAME_INDEX", "RENAME_CONSTRAINT", "SET_DEFAULT", "DROP_DEFAULT", "DROP_NOT_NULL":
		work, detail = "catalog-only", "Updates metadata while preserving existing row and index encodings."
		scan, rewrite, indexes = false, false, false
		if st.Operation == "RENAME_TO" {
			if p.tables[st.NewName] != nil && st.NewName != st.Table {
				return fmt.Errorf("table %q already exists", st.NewName)
			}
			delete(p.tables, st.Table)
			info.Name = st.NewName
			p.tables[st.NewName] = info
			for name, idx := range p.indexes {
				if idx.Table == st.Table {
					copy := *idx
					copy.Table = st.NewName
					p.indexes[name] = &copy
				}
			}
		} else if st.Operation == "RENAME_COLUMN" {
			col, err := column()
			if err != nil {
				return err
			}
			col.Name = st.NewColName
			info.PKColumns = slices.Clone(info.PKColumns)
			for i, name := range info.PKColumns {
				if name == st.OldColName {
					info.PKColumns[i] = st.NewColName
				}
			}
		} else if st.Operation == "RENAME_INDEX" {
			idx := p.indexes[st.OldColName]
			if idx == nil || idx.Table != st.Table {
				return fmt.Errorf("index %s does not belong to %s", st.OldColName, st.Table)
			}
			if p.indexes[st.NewColName] != nil && st.NewColName != st.OldColName {
				return fmt.Errorf("index %s already exists", st.NewColName)
			}
			copy := *idx
			copy.Name = st.NewColName
			delete(p.indexes, st.OldColName)
			p.indexes[st.NewColName] = &copy
		} else if st.Operation != "RENAME_CONSTRAINT" {
			col, err := column()
			if err != nil {
				return err
			}
			if st.Operation == "DROP_NOT_NULL" && slices.Contains(info.PKColumns, col.Name) {
				return fmt.Errorf("PRIMARY KEY columns cannot allow NULL")
			}
		}
	case "ALTER_TYPE":
		col, err := column()
		if err != nil {
			return err
		}
		lengthOnly := st.Column.ColType == col.ColType && (col.ColType == ColText || col.ColType == ColBlob) &&
			st.Column.Length > 0 && st.Using == nil && !(st.Column.Binary && !col.Binary)
		if st.AllowExisting && (!lengthOnly || (col.Length > 0 && st.Column.Length >= col.Length)) {
			return fmt.Errorf("ALLOW EXISTING requires a shorter VARCHAR or VARBINARY length without conversion")
		}
		if lengthOnly && ((col.Length > 0 && st.Column.Length >= col.Length) || st.AllowExisting) {
			work, detail = "catalog-only", "Changes the length limit while preserving row and index encoding."
			scan, rewrite, indexes = false, false, false
		}
		copyColumnType(col, st.Column)
		col.ColType, col.Length = st.Column.ColType, st.Column.Length
		col.Binary = col.ColType == ColText && (col.Binary || st.Column.Binary)
	case "SET_COLLATION":
		col, err := column()
		if err != nil {
			return err
		}
		if st.NewName != "binary" || col.ColType != ColText {
			return fmt.Errorf("COLLATE BINARY requires a TEXT or VARCHAR column")
		}
		if col.Binary {
			work, detail = "catalog-only", "The column already uses BINARY collation."
			scan, rewrite, indexes = false, false, false
		} else {
			work, detail = "database rewrite", "Rebuilds clustered and secondary keys across the database for changed collation semantics."
			col.Binary = true
		}
	case "ADD_COLUMN":
		if slices.ContainsFunc(info.Columns, func(col ColumnDef) bool { return col.Name == st.Column.Name }) {
			if !st.IfNotExists {
				return fmt.Errorf("duplicate column: %s", st.Column.Name)
			}
			p.add(st.Operation, st.Table, "no-op", schemaExclusiveLock, "The column already exists.", false, false, false)
			return nil
		}
		info.Columns = append(info.Columns, *st.Column)
	case "DROP_COLUMN":
		if _, err := column(); err != nil {
			if !st.IfExists {
				return err
			}
			p.add(st.Operation, st.Table, "no-op", schemaExclusiveLock, "The column does not exist.", false, false, false)
			return nil
		}
		info.Columns = slices.DeleteFunc(info.Columns, func(c ColumnDef) bool { return c.Name == st.OldColName })
	case "MODIFY_COLUMN":
		col, err := column()
		if err != nil {
			return err
		}
		*col = *st.Column
	}
	p.add(st.Operation, st.Table, work, schemaExclusiveLock, detail, scan, rewrite, indexes)
	return nil
}
