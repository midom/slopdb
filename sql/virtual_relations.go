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
	mysql, hasMySQL := MySQLSession(s.ctx)
	pg := s.PostgresMode()
	if !pg && !hasMySQL {
		return stmt, nil
	}
	cp := *stmt
	cp.Joins = append([]JoinClause(nil), stmt.Joins...)
	old := s.catalogView
	defer func() { s.catalogView = old }()
	resolve := func(name string) (string, *SelectStmt, error) {
		var data *ResultSet
		var err error
		virtual := false
		if pg {
			_, virtual = postgresCatalogName(name)
		} else {
			lower := strings.ToLower(name)
			virtual = strings.HasPrefix(lower, "information_schema.") || lower == "mysql.user"
		}
		if virtual {
			if s.catalogView == nil {
				s.catalogView = s.CatalogSnapshot()
			}
			if pg {
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
		cp.From, sub, err = resolve(cp.From)
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
		j.Table, sub, err = resolve(j.Table)
		if err != nil {
			return nil, err
		}
		if sub != nil {
			j.Subquery = sub
		}
	}
	return &cp, nil
}
