package sql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

func sameConstraint(a, b ConstraintDef) bool {
	return a.Kind == b.Kind && (a.Kind == "PRIMARY" || a.Inline == b.Inline) && slices.Equal(a.Columns, b.Columns) && slices.Equal(a.PrefixLengths, b.PrefixLengths) && a.CheckSQL == b.CheckSQL && a.RefTable == b.RefTable && slices.Equal(a.RefColumns, b.RefColumns)
}
func (s *Schema) initializeConstraintNames(info *TableInfo, createIndexes ...bool) error {
	build := len(createIndexes) == 0 || createIndexes[0]
	used := make(map[int]bool)
	names := make(map[string]bool)
	for _, c := range info.Constraints {
		if c.Name != "" {
			if names[c.Name] {
				return fmt.Errorf("duplicate constraint: %s", c.Name)
			}
			names[c.Name] = true
		}
	}
	ensure := func(c ConstraintDef) {
		if c.Inline && len(c.Columns) == 1 {
			for _, col := range info.Columns {
				if col.Name == c.Columns[0] {
					c.Name = col.ConstraintNames[c.Kind]
				}
			}
		}
		for i, existing := range info.Constraints {
			if !used[i] && sameConstraint(c, existing) {
				used[i] = true
				return
			}
		}
		info.Constraints = append(info.Constraints, c)
		used[len(info.Constraints)-1] = true
	}
	if len(info.PKColumns) > 0 {
		inline := len(info.PKColumns) == 1
		ensure(ConstraintDef{Kind: "PRIMARY", Columns: slices.Clone(info.PKColumns), Inline: inline})
	}
	for _, col := range info.Columns {
		if col.ConstraintNames["NOT_NULL"] != "" && col.NotNull {
			ensure(ConstraintDef{Kind: "NOT_NULL", Columns: []string{col.Name}, Inline: true})
		}
		if col.Unique {
			ensure(ConstraintDef{Kind: "UNIQUE", Columns: []string{col.Name}, Inline: true})
		}
		if col.Check != nil {
			ensure(ConstraintDef{Kind: "CHECK", Columns: []string{col.Name}, Check: col.Check, CheckSQL: col.CheckSQL, Inline: true})
		}
		if col.RefTable != "" {
			ensure(ConstraintDef{Kind: "FOREIGN", Columns: []string{col.Name}, RefTable: col.RefTable, RefColumns: slices.Clone(col.RefColumns), Inline: true})
		}
	}
	for _, cols := range info.TableUnique {
		ensure(ConstraintDef{Kind: "UNIQUE", Columns: slices.Clone(cols)})
	}
	for i, sql := range info.TableCheckSQL {
		ensure(ConstraintDef{Kind: "CHECK", Check: info.TableCheck[i], CheckSQL: sql})
	}
	for _, fk := range info.ForeignKeys {
		ensure(ConstraintDef{Kind: "FOREIGN", Columns: slices.Clone(fk.Columns), RefTable: fk.RefTable, RefColumns: slices.Clone(fk.RefColumns)})
	}
	for i := range info.Constraints {
		c := &info.Constraints[i]
		if c.Name == "" {
			suffix := strings.ToLower(c.Kind)
			if suffix == "primary" {
				suffix = "pkey"
			}
			base := info.Name + "_" + strings.Join(c.Columns, "_") + "_" + suffix
			c.Name = base
			for n := 2; names[c.Name]; n++ {
				c.Name = fmt.Sprintf("%s_%d", base, n)
			}
			names[c.Name] = true
		}
		if c.CheckSQL != "" && c.Check == nil {
			e, err := parseExpr(c.CheckSQL)
			if err != nil {
				return err
			}
			c.Check = e
		}
		if (c.Kind == "UNIQUE" || c.Kind == "PRIMARY") && c.IndexID == 0 {
			for _, idx := range s.indexes {
				if idx.Table == info.Name && idx.Auto && slices.Equal(idx.columnNames(), c.Columns) && slices.Equal(idx.PrefixLengths, c.PrefixLengths) {
					c.IndexID = idx.ID
					break
				}
			}
			if c.IndexID == 0 && c.Kind == "UNIQUE" && build {
				if err := s.addConstraintIndex(info, c); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *Schema) addTableConstraint(info *TableInfo, c ConstraintDef) error {
	for _, old := range info.Constraints {
		if c.Name != "" && old.Name == c.Name {
			return fmt.Errorf("constraint %s already exists", c.Name)
		}
	}
	if c.Kind != "CHECK" {
		if err := validateIndexColumns(info.Columns, c.Columns); err != nil {
			return err
		}
	}
	switch c.Kind {
	case "PRIMARY":
		if len(info.PKColumns) > 0 {
			return fmt.Errorf("multiple PRIMARY KEY declarations")
		}
		info.PKColumns = slices.Clone(c.Columns)
	case "UNIQUE":
		if !(&IndexInfo{PrefixLengths: c.PrefixLengths}).hasPrefixes() {
			info.TableUnique = append(info.TableUnique, slices.Clone(c.Columns))
		}
		if err := s.addConstraintIndex(info, &c); err != nil {
			return err
		}
	case "CHECK":
		info.TableCheck = append(info.TableCheck, c.Check)
		info.TableCheckSQL = append(info.TableCheckSQL, c.CheckSQL)
	case "FOREIGN":
		info.ForeignKeys = append(info.ForeignKeys, ForeignKeyDef{Columns: slices.Clone(c.Columns), RefTable: c.RefTable, RefColumns: slices.Clone(c.RefColumns)})
	default:
		return fmt.Errorf("unsupported constraint %s", c.Kind)
	}
	info.Constraints = append(info.Constraints, c)
	s.catalogLookup = nil
	return s.initializeConstraintNames(info)
}

func (s *Schema) dropTableConstraint(info *TableInfo, name string, ifExists, cascade bool) error {
	pos := slices.IndexFunc(info.Constraints, func(c ConstraintDef) bool { return c.Name == name })
	if pos < 0 {
		if ifExists {
			return nil
		}
		return fmt.Errorf("constraint %s does not exist", name)
	}
	c := info.Constraints[pos]
	if c.Inline && len(c.Columns) == 1 {
		for i := range info.Columns {
			if info.Columns[i].Name == c.Columns[0] {
				delete(info.Columns[i].ConstraintNames, c.Kind)
			}
		}
	}
	info.Constraints = append(info.Constraints[:pos], info.Constraints[pos+1:]...)
	switch c.Kind {
	case "NOT_NULL":
		for i := range info.Columns {
			if info.Columns[i].Name == c.Columns[0] {
				if slices.Contains(info.PKColumns, c.Columns[0]) {
					return fmt.Errorf("PRIMARY KEY columns cannot allow NULL")
				}
				info.Columns[i].NotNull = false
				delete(info.Columns[i].ConstraintNames, "NOT_NULL")
			}
		}
	case "PRIMARY":
		info.PKColumns = nil
		for i := range info.Columns {
			info.Columns[i].PrimaryKey = false
			info.Columns[i].AutoIncrement = false
		}
	case "UNIQUE":
		if c.Inline {
			for i := range info.Columns {
				if info.Columns[i].Name == c.Columns[0] {
					info.Columns[i].Unique = false
				}
			}
		} else if !(&IndexInfo{PrefixLengths: c.PrefixLengths}).hasPrefixes() {
			for i, g := range info.TableUnique {
				if slices.Equal(g, c.Columns) {
					info.TableUnique = append(info.TableUnique[:i], info.TableUnique[i+1:]...)
					break
				}
			}
		}
	case "CHECK":
		if c.Inline {
			for i := range info.Columns {
				if info.Columns[i].Name == c.Columns[0] {
					info.Columns[i].Check = nil
					info.Columns[i].CheckSQL = ""
				}
			}
		} else {
			for i, e := range info.TableCheckSQL {
				if e == c.CheckSQL {
					info.TableCheck = append(info.TableCheck[:i], info.TableCheck[i+1:]...)
					info.TableCheckSQL = append(info.TableCheckSQL[:i], info.TableCheckSQL[i+1:]...)
					break
				}
			}
		}
	case "FOREIGN":
		if c.Inline {
			for i := range info.Columns {
				if info.Columns[i].Name == c.Columns[0] {
					info.Columns[i].RefTable = ""
					info.Columns[i].RefColumns = nil
				}
			}
		} else {
			for i, fk := range info.ForeignKeys {
				if fk.RefTable == c.RefTable && slices.Equal(fk.Columns, c.Columns) && slices.Equal(fk.RefColumns, c.RefColumns) {
					info.ForeignKeys = append(info.ForeignKeys[:i], info.ForeignKeys[i+1:]...)
					break
				}
			}
		}
	}
	if c.IndexID != 0 {
		shared := false
		for _, other := range info.Constraints {
			shared = shared || other.IndexID == c.IndexID
		}
		if !shared {
			for idxName, idx := range s.indexes {
				if idx.ID == c.IndexID {
					delete(s.indexes, idxName)
				}
			}
		}
	}
	s.catalogLookup = nil
	if cascade && (c.Kind == "PRIMARY" || c.Kind == "UNIQUE") {
		s.dropIncomingForeignKeys(info.Name, c.Columns)
	}
	return nil
}

func exprUsesColumn(expr Expression, column string) bool {
	found := false
	_ = walkExpression(expr, func(e Expression) (bool, error) {
		if ref, ok := e.(*ColumnRefExpr); ok {
			parts := strings.Split(ref.Name, ".")
			found = found || parts[len(parts)-1] == column
		}
		return false, nil
	})
	return found
}

func (s *Schema) dropColumnDependencies(info *TableInfo, column string, cascade bool) error {
	var constraints []string
	for _, c := range info.Constraints {
		uses := slices.Contains(c.Columns, column)
		if c.Check != nil {
			uses = uses || exprUsesColumn(c.Check, column)
		}
		if uses {
			if !cascade {
				return fmt.Errorf("column %s is used by constraint %s", column, c.Name)
			}
			constraints = append(constraints, c.Name)
		}
	}
	for _, name := range constraints {
		if err := s.dropTableConstraint(info, name, false, true); err != nil {
			return err
		}
	}
	for name, idx := range s.indexes {
		if idx.Table == info.Name && slices.Contains(idx.columnNames(), column) {
			if !cascade {
				return fmt.Errorf("column %s is used by index %s", column, name)
			}
			delete(s.indexes, name)
		}
	}
	for _, child := range s.tables {
		for _, fk := range tableForeignKeys(child) {
			ref := fk.RefColumns
			if len(ref) == 0 {
				ref = info.PKColumns
			}
			if fk.RefTable == info.Name && slices.Contains(ref, column) {
				if !cascade {
					return fmt.Errorf("column %s is referenced by foreign key on %s", column, child.Name)
				}
			}
		}
	}
	if cascade {
		s.dropIncomingForeignKeys(info.Name, []string{column})
	}
	for name, v := range s.views {
		parsed, err := Parse(v.SQL)
		if err != nil {
			return err
		}
		if len(parsed) != 1 {
			return fmt.Errorf("invalid view")
		}
		sel := selectQuery(parsed[0])
		if selectUsesTableColumn(sel, info.Name, column) {
			if !cascade {
				return fmt.Errorf("column %s is used by view %s", column, name)
			}
			delete(s.views, name)
			s.dropViewsUsing(name)
		}
	}
	for name, tr := range s.triggers {
		uses := false
		context := tr.Table == info.Name || tr.View == info.Name
		if tr.When != "" {
			e, err := parseExpr(tr.When)
			if err != nil {
				return err
			}
			uses = triggerExprDepends(e, info.Name, column, false, context)
		}
		for _, query := range tr.Body {
			parsed, err := Parse(query)
			if err != nil {
				return err
			}
			for _, stmt := range parsed {
				uses = uses || triggerStatementDepends(stmt, info.Name, column, context)
			}
		}
		if uses {
			if !cascade {
				return fmt.Errorf("column %s is used by trigger %s", column, name)
			}
			delete(s.triggers, name)
		}
	}
	s.catalogLookup = nil
	return nil
}

func (s *Schema) dropIncomingForeignKeys(table string, columns []string) {
	for _, child := range s.tables {
		for i := len(child.Constraints) - 1; i >= 0; i-- {
			c := child.Constraints[i]
			if c.Kind != "FOREIGN" || c.RefTable != table {
				continue
			}
			ref := c.RefColumns
			if len(ref) == 0 && s.tables[table] != nil {
				ref = s.tables[table].PKColumns
			}
			uses := len(ref) == 0
			for _, name := range columns {
				uses = uses || slices.Contains(ref, name)
			}
			if uses && !s.candidateKeyAvailable(s.tables[table], ref) {
				_ = s.dropTableConstraint(child, c.Name, false, false)
			}
		}
	}
	s.catalogLookup = nil
}
func selectUsesTableColumn(q *SelectStmt, table, column string) bool {
	return queryDependsOnColumn(q, table, column, nil, nil)
}
func (s *Schema) dropViewsUsing(table string) {
	for name, v := range s.views {
		parsed, err := Parse(v.SQL)
		if err != nil {
			continue
		}
		if len(parsed) > 0 && queryContainsTable(selectQuery(parsed[0]), table) {
			delete(s.views, name)
			s.dropViewsUsing(name)
		}
	}
	for name, t := range s.triggers {
		if t.Table == table || t.View == table {
			delete(s.triggers, name)
		}
	}
}
func queryContainsTable(q *SelectStmt, table string) bool {
	return queryDependsOnColumn(q, table, "", nil, nil)
}

func (s *Schema) normalizeConstraintCatalog() error {
	before, err := json.Marshal(s.tables)
	if err != nil {
		return err
	}
	for _, info := range s.tables {
		if err = s.initializeConstraintNames(info, false); err != nil {
			return err
		}
	}
	after, err := json.Marshal(s.tables)
	if err != nil {
		return err
	}
	if bytes.Equal(before, after) {
		return nil
	}
	return s.lsm.Put([]byte(schemaPrefix), after)
}

func (s *Schema) candidateKeyAvailable(info *TableInfo, columns []string) bool {
	if info == nil || len(columns) == 0 {
		return false
	}
	if candidateColumns(columns, info.PKColumns) {
		return true
	}
	for _, g := range info.TableUnique {
		if candidateColumns(columns, g) {
			return true
		}
	}
	for _, c := range info.Columns {
		if c.Unique && len(columns) == 1 && columns[0] == c.Name {
			return true
		}
	}
	for _, idx := range s.indexes {
		if idx.Table == info.Name && idx.IsUnique && candidateColumns(columns, idx.columnNames()) {
			return true
		}
	}
	return false
}
