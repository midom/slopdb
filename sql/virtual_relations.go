package sql

import "strings"

// Virtual catalogs share relation substitution and one native metadata view.
// Adapters supply only their columns, naming rules and row projections.
func (s *Schema) catalogFacts() *CatalogSnapshot {
	if s.catalogView != nil {
		return s.catalogView
	}
	return s.CatalogSnapshot()
}
func (s *Schema) virtualRelationColumns(name string) ([]ColumnDef, bool, error) {
	if strings.HasPrefix(strings.ToLower(name), "information_schema.") {
		if cols, known := informationSchemaColumns(name); known {
			return cols, true, nil
		}
	}

	if s.PostgresMode() {
		if n, ok := postgresCatalogName(name); ok {
			return pgCatalogFor(n).Columns, true, nil
		}
		return nil, false, nil
	}
	if opts, ok := MySQLSession(s.ctx); ok {
		name = strings.ToLower(name)
		if columns, known := mysqlCatalogColumns(name); known {
			return columns, true, nil
		}
		if strings.HasPrefix(name, "information_schema.") || name == "mysql.user" {
			r, e := s.mysqlCatalogTable(name, opts)
			if e != nil {
				return nil, true, e
			}
			if r != nil {
				return r.Columns, true, nil
			}
		}
	}
	return nil, false, nil
}
func (s *Schema) resolveCatalogTables(stmt *SelectStmt) (*SelectStmt, error) {
	mysql, _ := MySQLSession(s.ctx)
	pg := s.PostgresMode()
	cp := *stmt
	cp.Joins = append([]JoinClause(nil), stmt.Joins...)
	old := s.catalogView
	defer func() { s.catalogView = old }()
	resolve := func(name, alias string) (string, *SelectStmt, error) {
		var data *ResultSet
		var err error
		_, shared := informationSchemaName(name)
		shared = shared && strings.HasPrefix(strings.ToLower(name), "information_schema.")
		virtual := shared
		if !shared && pg {
			_, virtual = postgresCatalogName(name)
		} else if !shared {
			lower := strings.ToLower(name)
			virtual = strings.HasPrefix(lower, "information_schema.") || lower == "mysql.user"
		}
		if virtual {
			if s.catalogView == nil {
				s.catalogView = s.CatalogSnapshot()
			}
			if shared {
				filter := catalogFilter{}
				if s.collation == "" || s.collation == BinaryCollation {
					filter = catalogPredicateFilter(stmt.Where, name, alias, len(stmt.Joins) == 0)
				}
				data, err = s.informationSchemaTable(name, filter)
			} else if pg {
				data, err = s.PostgresCatalogTable(name)
			} else {
				data, err = s.mysqlCatalogTable(strings.ToLower(name), mysql)
			}
			if err != nil {
				return "", nil, err
			}
			return "", &SelectStmt{materialized: data}, nil
		}
		if !pg && strings.Contains(name, ".") {
			resolved, e := s.namespaceName(name, false, nil)
			return resolved, nil, e
		}
		return name, nil, nil
	}
	var sub *SelectStmt
	var err error
	if cp.From != "" {
		cp.From, sub, err = resolve(cp.From, cp.FromAlias)
		if err != nil {
			return nil, err
		}
		if sub != nil {
			cp.FromExpr = &SubqueryExpr{Stmt: sub}
		}
	}
	for i := range cp.Joins {
		j := &cp.Joins[i]
		if j.Table == "" {
			continue
		}
		j.Table, sub, err = resolve(j.Table, j.Alias)
		if err != nil {
			return nil, err
		}
		if sub != nil {
			j.Subquery = sub
		}
	}
	return &cp, nil
}

// Only independent equality conjuncts may reduce catalog enumeration. OR,
// functions, parameters and other predicates remain in normal SELECT evaluation.
func catalogPredicateFilter(where Expression, table, alias string, unqualified bool) catalogFilter {
	f := catalogFilter{}
	var visit func(Expression)
	visit = func(e Expression) {
		b, ok := e.(*BinaryExpr)
		if !ok {
			return
		}
		if strings.EqualFold(b.Op, "AND") {
			visit(b.Left)
			visit(b.Right)
			return
		}
		if b.Op != "=" {
			return
		}
		col, ok := b.Left.(*ColumnRefExpr)
		lit, valid := b.Right.(*LiteralExpr)
		if !ok || !valid {
			col, ok = b.Right.(*ColumnRefExpr)
			lit, valid = b.Left.(*LiteralExpr)
		}
		if !ok || !valid || lit.Value.Null || lit.Value.Type != ColText {
			return
		}
		name := col.Name
		if dot := strings.LastIndexByte(name, '.'); dot >= 0 {
			qual := name[:dot]
			if qual != alias && qual != table {
				return
			}
			name = name[dot+1:]
		} else if !unqualified {
			return
		}
		switch strings.ToLower(name) {
		case "table_schema", "constraint_schema", "trigger_schema", "event_object_schema", "schema_name":
			f.Schema = lit.Value.Text
		case "table_name", "event_object_table":
			f.Table = lit.Value.Text
		}
	}
	visit(where)
	return f
}
