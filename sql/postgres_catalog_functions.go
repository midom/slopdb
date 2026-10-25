package sql

import (
	"fmt"
	"strconv"
	"strings"
)

func postgresCatalogFunctions() map[string]mysqlFunctionSpec {
	out := map[string]mysqlFunctionSpec{}
	add := func(name string, min, max int, typ ColumnType, declared string, fn func(*Schema, []Value) (Value, error)) {
		out[name] = mysqlFunctionSpec{Min: min, Max: max, Return: typ, Volatile: true, Metadata: func([]ColumnDef) ColumnDef { return ColumnDef{ColType: typ, Declared: declared, Postgres: true} }, Eval: func(s *Schema, a []Value) (Value, error) { v, e := fn(s, a); v.PGType = declared; return v, e }}
	}
	add("TO_REGCLASS", 1, 1, ColInteger, "BIGINT", func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		name := strings.ReplaceAll(a[0].String(), "\"", "")
		resolved, e := s.namespaceName(name, false, nil)
		if e != nil {
			return NewNullValue(), nil
		}
		if t := s.tables[resolved]; t != nil {
			return NewIntValue(pgObjectOID(t.ID)), nil
		}
		if idx := s.indexes[resolved]; idx != nil {
			return NewIntValue(pgObjectOID(idx.ID)), nil
		}
		if s.views[resolved] != nil {
			return NewIntValue(pgSyntheticOID("view:" + resolved)), nil
		}
		return NewNullValue(), nil
	})
	add("TO_REGNAMESPACE", 1, 1, ColInteger, "BIGINT", func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		n := strings.Trim(a[0].String(), "\"")
		if n == "pg_catalog" || n == "information_schema" || s.namespaces[s.namespaceAlias(n)] {
			return NewIntValue(pgNamespaceOID(n)), nil
		}
		return NewNullValue(), nil
	})
	add("FORMAT_TYPE", 2, 2, ColText, "TEXT", func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		oid, e := pgCatalogInt(a[0])
		if e != nil {
			return NewNullValue(), e
		}
		t := pgTypeFor(uint32(oid))
		if t.name == "unknown" {
			return NewTextValue("???"), nil
		}
		name := t.display
		if !a[1].Null {
			mod, e := pgCatalogInt(a[1])
			if e != nil {
				return NewNullValue(), e
			}
			if mod >= 4 {
				switch oid {
				case 1043:
					name += fmt.Sprintf("(%d)", mod-4)
				case 1700:
					m := mod - 4
					name += fmt.Sprintf("(%d,%d)", m>>16, m&65535)
				}
			}
		}
		return NewTextValue(name), nil
	})
	add("PG_TABLE_IS_VISIBLE", 1, 1, ColInteger, "BOOLEAN", func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		oid, e := pgCatalogInt(a[0])
		if e != nil {
			return NewNullValue(), e
		}
		for n, t := range s.tables {
			if pgObjectOID(t.ID) == oid {
				_, obj := pgObjectNames(n)
				resolved, e := s.namespaceName(obj, false, nil)
				return pgBool(e == nil && resolved == n), nil
			}
		}
		for n := range s.views {
			if pgSyntheticOID("view:"+n) == oid {
				_, obj := pgObjectNames(n)
				resolved, e := s.namespaceName(obj, false, nil)
				return pgBool(e == nil && resolved == n), nil
			}
		}
		return NewNullValue(), nil
	})
	add("PG_GET_VIEWDEF", 1, 2, ColText, "TEXT", func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		for n, v := range s.views {
			if (a[0].Type == ColInteger && pgSyntheticOID("view:"+n) == a[0].Int) || a[0].String() == n || a[0].String() == "public."+n {
				return NewTextValue(v.SQL), nil
			}
		}
		return NewNullValue(), nil
	})
	add("PG_GET_INDEXDEF", 1, 3, ColText, "TEXT", func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		oid, e := pgCatalogInt(a[0])
		if e != nil {
			return NewNullValue(), e
		}
		for n, idx := range s.indexes {
			if pgObjectOID(idx.ID) != oid {
				continue
			}
			if len(a) > 1 && !a[1].Null {
				col, e := pgCatalogInt(a[1])
				if e != nil {
					return NewNullValue(), e
				}
				if col != 0 {
					if col < 1 || col > int64(len(idx.columnNames())) {
						return NewTextValue(""), nil
					}
					return NewTextValue(quoteIdent(idx.columnNames()[col-1])), nil
				}
			}
			return NewTextValue(s.GetIndexSQL(n)), nil
		}
		return NewNullValue(), nil
	})
	add("PG_GET_EXPR", 2, 3, ColText, "TEXT", func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		return NewTextValue(a[0].String()), nil
	})
	add("PG_GET_CONSTRAINTDEF", 1, 2, ColText, "TEXT", func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		oid, e := pgCatalogInt(a[0])
		if e != nil {
			return NewNullValue(), e
		}
		for _, t := range s.tables {
			for _, c := range t.Constraints {
				if pgSyntheticOID(fmt.Sprintf("constraint:%d:%s", t.ID, c.Name)) == oid {
					return NewTextValue(pgConstraintSQL(c)), nil
				}
			}
		}
		return NewNullValue(), nil
	})
	add("PG_GET_SERIAL_SEQUENCE", 2, 2, ColText, "TEXT", func(s *Schema, a []Value) (Value, error) { return NewNullValue(), nil })
	add("PG_ENCODING_TO_CHAR", 1, 1, ColText, "TEXT", func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		n, e := pgCatalogInt(a[0])
		if e != nil {
			return NewNullValue(), e
		}
		switch n {
		case 6:
			return NewTextValue("UTF8"), nil
		case 0:
			return NewTextValue("SQL_ASCII"), nil
		}
		return NewTextValue(""), nil
	})

	add("CURRENT_SETTING", 1, 2, ColText, "TEXT", func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		name := strings.ToLower(a[0].String())
		value := ""
		switch name {
		case "server_version":
			value = "15.0"
		case "server_version_num":
			value = "150000"
		case "server_encoding", "client_encoding":
			value = "UTF8"
		case "datestyle":
			value = "ISO, MDY"
		case "timezone":
			value = "UTC"
		case "standard_conforming_strings":
			value = "on"
		case "integer_datetimes":
			value = "on"
		case "transaction_isolation", "default_transaction_isolation":
			value = strings.ToLower(string(s.IsolationLevel()))
		case "search_path":
			path := s.SessionSearchPath()
			for i := range path {
				path[i] = pgNamespaceName(path[i])
			}
			value = strings.Join(path, ", ")
		default:
			if opts, ok := PostgresSession(s.ctx); ok && opts.Function != nil {
				if v, handled, e := opts.Function(s.ctx, "CURRENT_SETTING", a); handled || e != nil {
					return v, e
				}
			}
			missing := false
			if len(a) > 1 && !a[1].Null {
				v, e := CastPostgresValue(a[1], "BOOLEAN")
				if e != nil {
					return NewNullValue(), e
				}
				missing = v.Int != 0
			}
			if missing {
				return NewNullValue(), nil
			}
			return NewNullValue(), fmt.Errorf("unrecognized configuration parameter %q", name)
		}
		return NewTextValue(value), nil
	})
	add("PG_CLIENT_ENCODING", 0, 0, ColText, "TEXT", func(*Schema, []Value) (Value, error) { return NewTextValue("UTF8"), nil })
	add("PG_GET_USERBYID", 1, 1, ColText, "TEXT", func(s *Schema, a []Value) (Value, error) {
		if a[0].Null {
			return NewNullValue(), nil
		}
		id, e := pgCatalogInt(a[0])
		if e != nil {
			return NewNullValue(), e
		}
		if id == 10 {
			opts, _ := PostgresSession(s.ctx)
			name := opts.User
			if name == "" {
				name = "slopdb"
			}
			return NewTextValue(name), nil
		}
		return NewTextValue(fmt.Sprintf("unknown (OID=%d)", id)), nil
	})
	return out
}
func pgConstraintSQL(c ConstraintDef) string {
	cols := func(names []string) string {
		q := make([]string, len(names))
		for i, n := range names {
			q[i] = quoteIdent(n)
		}
		return "(" + strings.Join(q, ", ") + ")"
	}
	kind := pgConstraintKind(c.Kind)
	out := kind
	switch kind {
	case "CHECK":
		out += " (" + c.CheckSQL + ")"
	case "PRIMARY KEY", "UNIQUE":
		out += " " + cols(c.Columns)
	case "FOREIGN KEY":
		out += " " + cols(c.Columns) + " REFERENCES " + quoteIdent(c.RefTable) + "" + cols(c.RefColumns)
		if c.OnUpdate != "" {
			out += " ON UPDATE " + c.OnUpdate
		}
		if c.OnDelete != "" {
			out += " ON DELETE " + c.OnDelete
		}
	}
	if c.Deferrable {
		out += " DEFERRABLE"
		if c.InitiallyDeferred {
			out += " INITIALLY DEFERRED"
		}
	}
	return out
}

// PostgresTypeDefinition returns a static declaration for a supported wire OID.
func PostgresTypeDefinition(oid uint32) ColumnDef {
	t := pgTypeFor(oid)
	c := ColumnDef{Declared: strings.ToUpper(t.display), Postgres: true, ColType: ColText}
	switch oid {
	case 16, 20, 21, 23:
		c.ColType = ColInteger
	case 700, 701:
		c.ColType = ColReal
	case 17, 2950:
		c.ColType = ColBlob
	case 1700:
		c.ColType = ColDecimal
	case 114, 3802:
		c.ColType = ColJSON
	}
	return c
}

func pgCatalogInt(v Value) (int64, error) { r, e := CastPostgresValue(v, "BIGINT"); return r.Int, e }

// CastPostgresCatalogValue resolves catalog name casts against this execution's
// namespace snapshot; it does not re-enter the session or execute SQL.
func (s *Schema) CastPostgresCatalogValue(v Value, target string) (Value, bool, error) {
	target = strings.ToUpper(strings.TrimSpace(strings.TrimPrefix(strings.ToLower(target), "pg_catalog.")))
	if target != "REGCLASS" && target != "REGNAMESPACE" && target != "REGTYPE" {
		return Value{}, false, nil
	}
	if v.Null {
		v.PGType = target
		return v, true, nil
	}
	name := v.String()
	if _, e := strconv.ParseUint(name, 10, 32); e == nil {
		r, e := CastPostgresValue(v, "BIGINT")
		r.PGType = target
		return r, true, e
	}
	fn := "TO_REGCLASS"
	if target == "REGNAMESPACE" {
		fn = "TO_REGNAMESPACE"
	}
	if target == "REGTYPE" {
		name = strings.TrimPrefix(strings.ToLower(name), "pg_catalog.")
		for _, t := range pgTypes {
			if name == t.name || name == t.display {
				r := NewIntValue(int64(t.oid))
				r.PGType = target
				return r, true, nil
			}
		}
		return NewNullValue(), true, fmt.Errorf("type %q does not exist", name)
	}
	r, e := postgresCatalogFunctions()[fn].Eval(s, []Value{NewTextValue(name)})
	if e == nil && r.Null {
		e = fmt.Errorf("relation or schema %q does not exist", name)
	}
	r.PGType = target
	return r, true, e
}

func pgConstraintKind(kind string) string {
	switch strings.ToUpper(kind) {
	case "PRIMARY":
		return "PRIMARY KEY"
	case "FOREIGN":
		return "FOREIGN KEY"
	}
	return strings.ToUpper(kind)
}
