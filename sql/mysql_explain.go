package sql

import (
	"context"
	"math"
	"strings"
)

type nativeExplainKey struct{}

// mysqlExplain adapts the native planner to MySQL's tabular wire contract. The
// estimates use ANALYZE or bounded storage sampling; EXPLAIN never runs the query.
func (s *Schema) mysqlExplain(stmt *ExplainStmt) (*ResultSet, bool, error) {
	if _, ok := MySQLSession(s.ctx); !ok || stmt.Analyze {
		return nil, false, nil
	}
	if native, _ := s.ctx.Value(nativeExplainKey{}).(bool); native {
		return nil, false, nil
	}
	sel, ok := stmt.Stmt.(*SelectStmt)
	if !ok {
		return nil, false, nil
	}
	resolved, err := s.resolveMySQLTables(sel)
	if err != nil {
		return nil, true, err
	}
	copy := *stmt
	copy.Stmt = resolved
	old := s.ctx
	s.ctx = context.WithValue(old, nativeExplainKey{}, true)
	native, err := s.Explain(&copy)
	s.ctx = old
	if err != nil {
		return nil, true, err
	}
	r := &ResultSet{Columns: []ColumnDef{{Name: "id", ColType: ColInteger}, {Name: "select_type", ColType: ColText}, {Name: "table", ColType: ColText}, {Name: "partitions", ColType: ColText}, {Name: "type", ColType: ColText}, {Name: "possible_keys", ColType: ColText}, {Name: "key", ColType: ColText}, {Name: "key_len", ColType: ColText}, {Name: "ref", ColType: ColText}, {Name: "rows", ColType: ColInteger}, {Name: "filtered", ColType: ColReal}, {Name: "Extra", ColType: ColText}}}
	type relation struct {
		name, alias string
		sub         *SelectStmt
	}
	relations := []relation{{resolved.From, resolved.FromAlias, nil}}
	for _, j := range resolved.Joins {
		relations = append(relations, relation{j.Table, j.Alias, j.Subquery})
	}
	for _, rel := range relations {
		row := map[string]Value{"id": NewIntValue(1), "select_type": NewTextValue("SIMPLE"), "table": NewNullValue(), "partitions": NewNullValue(), "type": NewTextValue("ALL"), "possible_keys": NewNullValue(), "key": NewNullValue(), "key_len": NewNullValue(), "ref": NewNullValue(), "rows": NewIntValue(1), "filtered": NewRealValue(100), "Extra": NewTextValue("")}
		if rel.name == "" {
			row["Extra"] = NewTextValue("No tables used")
			if rel.sub != nil || resolved.FromExpr != nil {
				row["select_type"] = NewTextValue("DERIVED")
				row["Extra"] = NewTextValue("Materialized derived input")
			}
			r.Rows = append(r.Rows, row)
			continue
		}
		name := rel.alias
		if name == "" {
			name = rel.name
		}
		row["table"] = NewTextValue(name)
		info := s.tables[rel.name]
		if info == nil {
			r.Rows = append(r.Rows, row)
			continue
		}
		estimate, err := s.estimateStatusRange(info.storagePrefix(rel.name), true)
		if err != nil {
			return nil, true, err
		}
		rows := estimate.Rows
		if n, ok := s.stats[rel.name]; ok {
			rows = n
		}
		rows = max(int64(1), rows)
		var possible []string
		for _, idx := range s.tableIndexes(rel.name) {
			possible = append(possible, idx.Name)
		}
		if len(info.PKColumns) > 0 {
			possible = append([]string{"PRIMARY"}, possible...)
		}
		if len(possible) > 0 {
			row["possible_keys"] = NewTextValue(strings.Join(possible, ","))
		}
		if len(resolved.Joins) == 0 {
			if _, point := s.primaryKeyLookup(resolved.Where, rel.name, info); point {
				row["type"] = NewTextValue("const")
				row["key"] = NewTextValue("PRIMARY")
				rows = 1
			} else if index := s.fulltextPlanIndex(resolved.Where, info); index != nil {
				row["type"] = NewTextValue("fulltext")
				row["key"] = NewTextValue(index.SQLName())
			} else if plan := s.tuplePlan(resolved, info); plan != nil {
				row["type"] = NewTextValue("range")
				key := "PRIMARY"
				if plan.index != nil {
					key = plan.index.Name
				}
				row["key"] = NewTextValue(key)
				rows = int64(math.Ceil(float64(rows) * s.predicateSelectivity(rel.name, resolved.Where)))
			} else if index := s.findWhereIndex(resolved.Where, rel.name); index != nil {
				row["type"] = NewTextValue("ref")
				row["key"] = NewTextValue(index.Name)
			}
		}
		var extra []string
		for _, step := range native.Rows {
			switch step["opcode"].Text {
			case "Filter":
				extra = append(extra, "Using where")
			case "SorterOpen":
				extra = append(extra, "Using filesort")
			}
		}
		row["rows"] = NewIntValue(max(int64(1), rows))
		row["Extra"] = NewTextValue(strings.Join(extra, "; "))
		r.Rows = append(r.Rows, row)
	}
	return r, true, nil
}
