package sql

import (
	"fmt"
	"strings"
)

type projectionSource struct {
	table, alias string
	columns      []ColumnDef
}

func (s *Schema) expandQualifiedStars(stmt *SelectStmt, scope map[string][]ColumnDef, depth int) (*SelectStmt, error) {
	qualified := false
	for _, c := range stmt.Columns {
		qualified = qualified || c.Star && c.StarQualifier != ""
	}
	if !qualified {
		return stmt, nil
	}
	if depth > 64 {
		return nil, fmt.Errorf("star metadata nesting limit exceeded")
	}
	var sources []projectionSource
	add := func(table, alias string, sub *SelectStmt) error {
		var cols []ColumnDef
		var err error
		if sub != nil {
			cols, err = s.describeStatement(sub, scope, depth+1)
		} else {
			cols, err = s.describeTable(table, scope, depth+1)
		}
		if err != nil {
			return err
		}
		sources = append(sources, projectionSource{table: table, alias: alias, columns: cols})
		return nil
	}
	var sub *SelectStmt
	if expr, ok := stmt.FromExpr.(*SubqueryExpr); ok {
		sub = expr.Stmt
	}
	if stmt.From != "" || sub != nil {
		if err := add(stmt.From, stmt.FromAlias, sub); err != nil {
			return nil, err
		}
	}
	for _, j := range stmt.Joins {
		if err := add(j.Table, j.Alias, j.Subquery); err != nil {
			return nil, err
		}
	}
	columns, err := expandStarColumns(stmt.Columns, sources)
	if err != nil {
		return nil, err
	}
	copy := *stmt
	copy.Columns = columns
	copy.SelectAll = false
	return &copy, nil
}

func expandStarColumns(columns []SelectColumn, sources []projectionSource) ([]SelectColumn, error) {
	var out []SelectColumn
	for _, c := range columns {
		if !c.Star || c.StarQualifier == "" {
			out = append(out, c)
			continue
		}
		var source *projectionSource
		for i := range sources {
			candidate := &sources[i]
			_, bare := SplitSchemaName(candidate.table)
			if c.StarQualifier == candidate.alias || c.StarQualifier == candidate.table || c.StarQualifier == bare && candidate.alias == "" {
				if source != nil {
					return nil, fmt.Errorf("ambiguous star qualifier %q", c.StarQualifier)
				}
				source = candidate
			}
		}
		if source == nil {
			return nil, fmt.Errorf("unknown star qualifier %q", c.StarQualifier)
		}
		qualifier := source.alias
		if qualifier == "" {
			qualifier = source.table
		}
		for _, col := range source.columns {
			ref := col.Name
			if qualifier != "" {
				ref = qualifier + "." + ref
			}
			out = append(out, SelectColumn{Expr: &ColumnRefExpr{Name: ref}, Alias: col.Name, starLabel: col.ResultName()})
		}
	}
	return out, nil
}

func (s *Schema) canonicalStarQualifier(name string) string {
	if ns, obj, qualified := strings.Cut(name, "."); qualified {
		return namespaceObject(s.namespaceAlias(ns), obj)
	}
	return name
}
