package sql

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
)

// These virtual relations are projections of this session's actual catalog.
// They use the ordinary SELECT pipeline and never scan user table contents.
var postgresCatalogNames = map[string]bool{
	"pg_namespace": true, "pg_class": true, "pg_attribute": true, "pg_type": true,
	"pg_index": true, "pg_constraint": true, "pg_attrdef": true, "pg_tables": true,
	"pg_indexes": true, "pg_views": true, "pg_database": true, "pg_am": true,
}

func postgresCatalogName(name string) (string, bool) {
	name = strings.ToLower(name)
	if strings.HasPrefix(name, "pg_catalog.") {
		name = strings.TrimPrefix(name, "pg_catalog.")
	}
	if postgresCatalogNames[name] {
		return name, true
	}
	switch name {
	case "information_schema.schemata", "information_schema.tables", "information_schema.columns", "information_schema.views", "information_schema.table_constraints", "information_schema.key_column_usage", "information_schema.referential_constraints":
		return name, true
	}
	return "", false
}
func pgSyntheticOID(name string) int64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	return int64(h.Sum32() | 0x80000000)
}
func pgObjectOID(id uint64) int64 {
	if id == 0 || id >= 0x7fff0000 {
		return 0
	}
	return int64(id) + 16384
}
func pgNamespaceName(name string) string {
	if name == "main" {
		return "public"
	}
	return name
}
func pgNamespaceOID(name string) int64 {
	switch pgNamespaceName(name) {
	case "public":
		return 2200
	case "pg_catalog":
		return 11
	case "information_schema":
		return 13207
	}
	return pgSyntheticOID("namespace:" + name)
}
func pgObjectNames(name string) (string, string) {
	ns, n := SplitSchemaName(name)
	return pgNamespaceName(ns), n
}
func pgBool(v bool) Value {
	n := int64(0)
	if v {
		n = 1
	}
	r := NewIntValue(n)
	r.PGType = "BOOLEAN"
	return r
}
func pgCatalog(spec string) *ResultSet {
	r := &ResultSet{}
	for _, part := range strings.Fields(spec) {
		name, typ, _ := strings.Cut(part, ":")
		c := ColumnDef{Name: name, ColType: ColText, Declared: "TEXT", Postgres: true}
		switch typ {
		case "i":
			c.ColType = ColInteger
			c.Declared = "BIGINT"
		case "o":
			c.ColType = ColInteger
			c.Declared = "OID"
		case "n":
			c.Declared = "NAME"
		case "b":
			c.ColType = ColInteger
			c.Declared = "BOOLEAN"
		case "r":
			c.ColType = ColReal
			c.Declared = "DOUBLE PRECISION"
		}
		r.Columns = append(r.Columns, c)
	}
	return r
}
func pgCatalogRow(r *ResultSet, values ...any) {
	row := make(map[string]Value, len(r.Columns))
	for i, c := range r.Columns {
		v := NewNullValue()
		if i < len(values) {
			switch x := values[i].(type) {
			case string:
				v = NewTextValue(x)
			case int:
				v = NewIntValue(int64(x))
			case int64:
				v = NewIntValue(x)
			case uint32:
				v = NewIntValue(int64(x))
			case bool:
				v = pgBool(x)
			case float64:
				v = NewRealValue(x)
			case Value:
				v = x
			}
		}
		v.PGType = c.Declared
		row[c.Name] = v
	}
	r.Rows = append(r.Rows, row)
}
func pgTypeOID(c ColumnDef) uint32 {
	if oid := PostgresColumnOID(c); oid != 0 && c.Postgres {
		return oid
	}
	switch c.ColType {
	case ColInteger:
		return 20
	case ColUnsigned, ColDecimal:
		return 1700
	case ColReal:
		return 701
	case ColBlob:
		return 17
	case ColJSON:
		return 114
	}
	return 25
}

type pgTypeInfo struct {
	oid           uint32
	name, display string
	length        int
	category      string
}

var pgTypes = []pgTypeInfo{
	{19, "name", "name", 64, "S"}, {26, "oid", "oid", 4, "N"}, {2205, "regclass", "regclass", 4, "N"}, {2206, "regtype", "regtype", 4, "N"}, {4089, "regnamespace", "regnamespace", 4, "N"},
	{16, "bool", "boolean", 1, "B"}, {17, "bytea", "bytea", -1, "U"}, {20, "int8", "bigint", 8, "N"}, {21, "int2", "smallint", 2, "N"}, {23, "int4", "integer", 4, "N"}, {25, "text", "text", -1, "S"},
	{114, "json", "json", -1, "U"}, {700, "float4", "real", 4, "N"}, {701, "float8", "double precision", 8, "N"}, {1043, "varchar", "character varying", -1, "S"},
	{1082, "date", "date", 4, "D"}, {1083, "time", "time without time zone", 8, "D"}, {1114, "timestamp", "timestamp without time zone", 8, "D"}, {1184, "timestamptz", "timestamp with time zone", 8, "D"}, {1186, "interval", "interval", 16, "T"}, {1266, "timetz", "time with time zone", 12, "D"}, {1700, "numeric", "numeric", -1, "N"}, {2950, "uuid", "uuid", 16, "U"}, {3802, "jsonb", "jsonb", -1, "U"},
}

func pgTypeFor(oid uint32) pgTypeInfo {
	for _, t := range pgTypes {
		if t.oid == oid {
			return t
		}
	}
	return pgTypeInfo{oid: oid, name: "unknown", display: "unknown", length: -1, category: "X"}
}
func pgTypeMod(c ColumnDef) int64 {
	if c.ColType == ColDecimal && c.Precision > 0 {
		return int64((c.Precision<<16)|c.Scale) + 4
	}
	if c.Length > 0 && c.ColType == ColText {
		return c.Length + 4
	}
	return -1
}
func pgYes(v bool) string {
	if v {
		return "YES"
	}
	return "NO"
}
func (s *Schema) resolvePostgresTables(stmt *SelectStmt) (*SelectStmt, error) {
	if !s.postgresMode {
		return stmt, nil
	}
	cp := *stmt
	cp.Joins = append([]JoinClause(nil), stmt.Joins...)
	resolve := func(name string) (*SelectStmt, error) {
		if _, ok := postgresCatalogName(name); !ok {
			return nil, nil
		}
		r, e := s.PostgresCatalogTable(name)
		return &SelectStmt{materialized: r}, e
	}
	if sub, e := resolve(cp.From); e != nil {
		return nil, e
	} else if sub != nil {
		cp.From = ""
		cp.FromExpr = &SubqueryExpr{Stmt: sub}
	}
	for i := range cp.Joins {
		j := &cp.Joins[i]
		if sub, e := resolve(j.Table); e != nil {
			return nil, e
		} else if sub != nil {
			j.Table = ""
			j.Subquery = sub
		}
	}
	return &cp, nil
}

// PostgresCatalogTable returns nil for an ordinary user relation. Callers must
// hold their normal session/catalog execution view, just as for Describe.
func (s *Schema) PostgresCatalogTable(name string) (*ResultSet, error) {
	name, known := postgresCatalogName(name)
	if !known {
		return nil, nil
	}
	opts, _ := PostgresSession(s.ctx)
	database := opts.Database
	if database == "" {
		database = "slopdb"
	}
	owner := opts.User
	if owner == "" {
		owner = "slopdb"
	}
	tables := make([]string, 0, len(s.tables))
	for n := range s.tables {
		tables = append(tables, n)
	}
	sort.Strings(tables)
	indexes := make([]string, 0, len(s.indexes))
	for n := range s.indexes {
		indexes = append(indexes, n)
	}
	sort.Strings(indexes)
	views := make([]string, 0, len(s.views))
	for n := range s.views {
		views = append(views, n)
	}
	sort.Strings(views)
	switch name {
	case "pg_namespace", "information_schema.schemata":
		r := pgCatalog("oid:o nspname nspowner:o nspacl")
		if name != "pg_namespace" {
			r = pgCatalog("catalog_name schema_name schema_owner default_character_set_catalog default_character_set_schema default_character_set_name sql_path")
		}
		names := append([]string{"pg_catalog", "information_schema"}, s.SchemaNames()...)
		sort.Strings(names)
		for _, n := range names {
			n = pgNamespaceName(n)
			if name == "pg_namespace" {
				pgCatalogRow(r, pgNamespaceOID(n), n, int64(10), nil)
			} else {
				pgCatalogRow(r, database, n, owner, nil, nil, nil, nil)
			}
		}
		return r, nil
	case "pg_type":
		r := pgCatalog("oid:o typname typnamespace:o typowner:o typlen:i typbyval:b typtype typcategory typispreferred:b typisdefined:b typrelid:o typelem:o typarray:o typbasetype:o typtypmod:i typnotnull:b typdefault")
		for _, t := range pgTypes {
			pgCatalogRow(r, t.oid, t.name, int64(11), int64(10), t.length, t.length > 0, "b", t.category, false, true, 0, 0, 0, 0, -1, false, nil)
		}
		return r, nil
	case "pg_am":
		r := pgCatalog("oid:o amname amtype")
		pgCatalogRow(r, 8000, "lsm", "t")
		pgCatalogRow(r, 8001, "lsm_index", "i")
		return r, nil
	case "pg_database":
		r := pgCatalog("oid:o datname datdba:o encoding:i datistemplate:b datallowconn:b datconnlimit:i datcollate datctype")
		pgCatalogRow(r, 16384, database, 10, 6, false, true, -1, "C", "C")
		return r, nil
	case "pg_class":
		r := pgCatalog("oid:o relname relnamespace:o reltype:o reloftype:o relowner:o relam:o relfilenode:o reltablespace:o relpages:i reltuples:r relallvisible:i reltoastrelid:o relhasindex:b relisshared:b relpersistence relkind relnatts:i relchecks:i relhasrules:b relhastriggers:b relhassubclass:b relrowsecurity:b relforcerowsecurity:b relispopulated:b relreplident relispartition:b relrewrite:o relacl reloptions")
		add := func(oid int64, n, kind string, cols int, hasindex, temp bool) {
			ns, obj := pgObjectNames(n)
			p := "p"
			if temp {
				p = "t"
			}
			am := 8000
			if kind == "i" {
				am = 8001
			}
			if kind == "v" {
				am = 0
			}
			count := -1.0
			if v, ok := s.stats[n]; ok {
				count = float64(v)
			}
			pgCatalogRow(r, oid, obj, pgNamespaceOID(ns), 0, 0, 10, am, 0, 0, 0, count, 0, 0, hasindex, false, p, kind, cols, 0, false, false, false, false, false, true, "d", false, 0, nil, nil)
		}
		for _, n := range tables {
			t := s.tables[n]
			has := len(t.PKColumns) > 0
			for _, idx := range s.indexes {
				has = has || idx.Table == n
			}
			add(pgObjectOID(t.ID), n, "r", len(t.Columns), has, s.isTemporaryTable(n))
			if pgPrimaryOID(t) != 0 {
				ns, _ := pgObjectNames(n)
				add(pgPrimaryOID(t), ns+"."+pgPrimaryName(t), "i", len(t.PKColumns), false, s.isTemporaryTable(n))
			}
		}
		for _, n := range indexes {
			idx := s.indexes[n]
			ns, _ := pgObjectNames(idx.Table)
			add(pgObjectOID(idx.ID), ns+"."+idx.SQLName(), "i", len(idx.columnNames()), false, s.isTemporaryTable(idx.Table))
		}
		for _, n := range views {
			add(pgSyntheticOID("view:"+n), n, "v", len(s.views[n].Columns), false, false)
		}
		return r, nil
	case "pg_attribute":
		r := pgCatalog("attrelid:o attname atttypid:o attstattarget:i attlen:i attnum:i attndims:i attcacheoff:i atttypmod:i attbyval:b attstorage attalign attnotnull:b atthasdef:b atthasmissing:b attidentity attgenerated attisdropped:b attislocal:b attinhcount:i attcollation:o attacl attoptions attfdwoptions attmissingval")
		for _, n := range tables {
			t := s.tables[n]
			for i, c := range t.Columns {
				typ := pgTypeFor(pgTypeOID(c))
				pgCatalogRow(r, pgObjectOID(t.ID), c.Name, typ.oid, -1, typ.length, i+1, 0, -1, pgTypeMod(c), typ.length > 0, "p", "i", c.NotNull || c.PrimaryKey, c.DefaultSQL != "", false, "", "", false, true, 0, 0, nil, nil, nil, nil)
			}
		}
		return r, nil
	case "pg_attrdef":
		r := pgCatalog("oid:o adrelid:o adnum:i adbin")
		for _, n := range tables {
			t := s.tables[n]
			for i, c := range t.Columns {
				if c.DefaultSQL != "" {
					pgCatalogRow(r, pgSyntheticOID(fmt.Sprintf("default:%d:%d", t.ID, i)), pgObjectOID(t.ID), i+1, c.DefaultSQL)
				}
			}
		}
		return r, nil
	case "pg_index":
		r := pgCatalog("indexrelid:o indrelid:o indnatts:i indnkeyatts:i indisunique:b indnullsnotdistinct:b indisprimary:b indisexclusion:b indimmediate:b indisclustered:b indisvalid:b indcheckxmin:b indisready:b indislive:b indisreplident:b indkey indexprs indpred")
		for _, n := range indexes {
			idx := s.indexes[n]
			t := s.tables[idx.Table]
			if t == nil {
				continue
			}
			primary := false
			for _, c := range t.Constraints {
				primary = primary || c.IndexID == idx.ID && strings.EqualFold(c.Kind, "PRIMARY")
			}
			var key []string
			for _, cn := range idx.columnNames() {
				for i, c := range t.Columns {
					if c.Name == cn {
						key = append(key, fmt.Sprint(i+1))
					}
				}
			}
			pgCatalogRow(r, pgObjectOID(idx.ID), pgObjectOID(t.ID), len(key), len(key), idx.IsUnique, false, primary, false, true, false, true, false, true, true, false, strings.Join(key, " "), nil, nil)
		}

		for _, n := range tables {
			t := s.tables[n]
			if pgPrimaryOID(t) == 0 {
				continue
			}
			var key []string
			for _, cn := range t.PKColumns {
				for i, c := range t.Columns {
					if c.Name == cn {
						key = append(key, fmt.Sprint(i+1))
					}
				}
			}
			pgCatalogRow(r, pgPrimaryOID(t), pgObjectOID(t.ID), len(key), len(key), true, false, true, false, true, true, true, false, true, true, false, strings.Join(key, " "), nil, nil)
		}
		return r, nil
	case "pg_tables", "information_schema.tables":
		r := pgCatalog("schemaname tablename tableowner tablespace hasindexes:b hasrules:b hastriggers:b rowsecurity:b")
		if name != "pg_tables" {
			r = pgCatalog("table_catalog table_schema table_name table_type self_referencing_column_name reference_generation user_defined_type_catalog user_defined_type_schema user_defined_type_name is_insertable_into is_typed commit_action")
		}
		for _, n := range tables {
			ns, obj := pgObjectNames(n)
			has := len(s.tables[n].PKColumns) > 0
			for _, idx := range s.indexes {
				has = has || idx.Table == n
			}
			if name == "pg_tables" {
				pgCatalogRow(r, ns, obj, owner, nil, has, false, false, false)
			} else {
				kind := "BASE TABLE"
				if s.isTemporaryTable(n) {
					kind = "LOCAL TEMPORARY"
				}
				pgCatalogRow(r, database, ns, obj, kind, nil, nil, nil, nil, nil, "YES", "NO", nil)
			}
		}
		if name != "pg_tables" {
			for _, n := range views {
				ns, obj := pgObjectNames(n)
				pgCatalogRow(r, database, ns, obj, "VIEW", nil, nil, nil, nil, nil, "NO", "NO", nil)
			}
		}
		return r, nil
	case "pg_indexes":
		r := pgCatalog("schemaname tablename indexname tablespace indexdef")
		for _, n := range indexes {
			idx := s.indexes[n]
			ns, obj := pgObjectNames(idx.Table)
			pgCatalogRow(r, ns, obj, idx.SQLName(), nil, s.GetIndexSQL(n))
		}

		for _, n := range tables {
			t := s.tables[n]
			if pgPrimaryOID(t) != 0 {
				ns, obj := pgObjectNames(n)
				pgCatalogRow(r, ns, obj, pgPrimaryName(t), nil, pgPrimarySQL(t))
			}
		}
		return r, nil
	case "pg_views", "information_schema.views":
		r := pgCatalog("schemaname viewname viewowner definition")
		if name != "pg_views" {
			r = pgCatalog("table_catalog table_schema table_name view_definition check_option is_updatable is_insertable_into is_trigger_updatable is_trigger_deletable is_trigger_insertable_into")
		}
		for _, n := range views {
			ns, obj := pgObjectNames(n)
			if name == "pg_views" {
				pgCatalogRow(r, ns, obj, owner, s.views[n].SQL)
			} else {
				pgCatalogRow(r, database, ns, obj, s.views[n].SQL, "NONE", "NO", "NO", "NO", "NO", "NO")
			}
		}
		return r, nil
	case "information_schema.columns":
		r := pgCatalog("table_catalog table_schema table_name column_name ordinal_position:i column_default is_nullable data_type character_maximum_length:i character_octet_length:i numeric_precision:i numeric_precision_radix:i numeric_scale:i datetime_precision:i interval_type interval_precision:i character_set_catalog character_set_schema character_set_name collation_catalog collation_schema collation_name domain_catalog domain_schema domain_name udt_catalog udt_schema udt_name scope_catalog scope_schema scope_name maximum_cardinality:i dtd_identifier is_self_referencing is_identity identity_generation identity_start identity_increment identity_maximum identity_minimum identity_cycle is_generated generation_expression is_updatable")
		for _, n := range tables {
			t := s.tables[n]
			ns, obj := pgObjectNames(n)
			for i, c := range t.Columns {
				typ := pgTypeFor(pgTypeOID(c))
				var length, octets, precision, radix, scale, datetime, def any
				if c.Length > 0 {
					length = c.Length
					octets = c.Length * 4
				}
				if c.DefaultSQL != "" {
					def = c.DefaultSQL
				}
				switch typ.category {
				case "N":
					switch typ.oid {
					case 20:
						precision = 64
						radix = 2
						scale = 0
					case 21:
						precision = 16
						radix = 2
						scale = 0
					case 23:
						precision = 32
						radix = 2
						scale = 0
					case 700:
						precision = 24
						radix = 2
					case 701:
						precision = 53
						radix = 2
					case 1700:
						if c.Precision > 0 {
							precision = c.Precision
							scale = c.Scale
						}
						radix = 10
					}
				case "D", "T":
					datetime = 6
					if typ.oid == 1082 {
						datetime = 0
					}
				}
				pgCatalogRow(r, database, ns, obj, c.Name, i+1, def, pgYes(!c.NotNull && !c.PrimaryKey), typ.display, length, octets, precision, radix, scale, datetime, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, database, "pg_catalog", typ.name, nil, nil, nil, nil, fmt.Sprint(i+1), "NO", "NO", nil, nil, nil, nil, nil, "NO", "NEVER", nil, "YES")
			}
		}
		return r, nil
	case "pg_constraint", "information_schema.table_constraints", "information_schema.key_column_usage", "information_schema.referential_constraints":
		return s.postgresConstraints(name, tables, database), nil
	}
	return nil, fmt.Errorf("unsupported PostgreSQL catalog %s", name)
}
func (s *Schema) postgresConstraints(name string, tables []string, database string) *ResultSet {
	r := pgCatalog("oid:o conname connamespace:o contype condeferrable:b condeferred:b convalidated:b conrelid:o contypid:o conindid:o conparentid:o confrelid:o confupdtype confdeltype confmatchtype conislocal:b coninhcount:i connoinherit:b conkey confkey conbin")
	switch name {
	case "information_schema.table_constraints":
		r = pgCatalog("constraint_catalog constraint_schema constraint_name table_catalog table_schema table_name constraint_type is_deferrable initially_deferred enforced nulls_distinct")
	case "information_schema.key_column_usage":
		r = pgCatalog("constraint_catalog constraint_schema constraint_name table_catalog table_schema table_name column_name ordinal_position:i position_in_unique_constraint:i")
	case "information_schema.referential_constraints":
		r = pgCatalog("constraint_catalog constraint_schema constraint_name unique_constraint_catalog unique_constraint_schema unique_constraint_name match_option update_rule delete_rule")
	}
	for _, n := range tables {
		t := s.tables[n]
		ns, obj := pgObjectNames(n)
		for _, c := range t.Constraints {
			kind := pgConstraintKind(c.Kind)
			typ := ""
			switch kind {
			case "PRIMARY KEY":
				typ = "p"
			case "UNIQUE":
				typ = "u"
			case "FOREIGN KEY":
				typ = "f"
			case "CHECK":
				typ = "c"
			default:
				continue
			}
			oid := pgSyntheticOID(fmt.Sprintf("constraint:%d:%s", t.ID, c.Name))
			ref := s.tables[c.RefTable]
			refid := int64(0)
			if ref != nil {
				refid = pgObjectOID(ref.ID)
			}
			keys := func(cols []string, table *TableInfo) string {
				var ids []string
				if table != nil {
					for _, n := range cols {
						for i, c := range table.Columns {
							if c.Name == n {
								ids = append(ids, fmt.Sprint(i+1))
							}
						}
					}
				}
				return "{" + strings.Join(ids, ",") + "}"
			}
			switch name {
			case "pg_constraint":
				indexOID := pgObjectOID(c.IndexID)
				if typ == "p" {
					indexOID = pgPrimaryOID(t)
				}
				pgCatalogRow(r, oid, c.Name, pgNamespaceOID(ns), typ, c.Deferrable, c.InitiallyDeferred, true, pgObjectOID(t.ID), 0, indexOID, 0, refid, pgAction(c.OnUpdate), pgAction(c.OnDelete), "s", true, 0, true, keys(c.Columns, t), keys(c.RefColumns, ref), c.CheckSQL)
			case "information_schema.table_constraints":
				pgCatalogRow(r, database, ns, c.Name, database, ns, obj, kind, pgYes(c.Deferrable), pgYes(c.InitiallyDeferred), "YES", "YES")
			case "information_schema.key_column_usage":
				for i, col := range c.Columns {
					var pos any
					if typ == "f" {
						pos = i + 1
					}
					pgCatalogRow(r, database, ns, c.Name, database, ns, obj, col, i+1, pos)
				}
			case "information_schema.referential_constraints":
				if typ != "f" {
					continue
				}
				refns, _ := pgObjectNames(c.RefTable)
				var refname any
				if ref != nil {
					for _, candidate := range ref.Constraints {
						if strings.Join(candidate.Columns, "\x00") == strings.Join(c.RefColumns, "\x00") && (candidate.Kind == "PRIMARY" || candidate.Kind == "UNIQUE") {
							refname = candidate.Name
							break
						}
					}
				}
				up, del := c.OnUpdate, c.OnDelete
				if up == "" {
					up = "NO ACTION"
				}
				if del == "" {
					del = "NO ACTION"
				}
				pgCatalogRow(r, database, ns, c.Name, database, refns, refname, "NONE", up, del)
			}
		}
	}
	return r
}
func pgAction(action string) string {
	switch strings.ToUpper(action) {
	case "RESTRICT":
		return "r"
	case "CASCADE":
		return "c"
	case "SET NULL":
		return "n"
	case "SET DEFAULT":
		return "d"
	}
	return "a"
}

// A clustered primary key is a real maintained LSM index even though it has no
// separate secondary IndexInfo. Publish it as a logical catalog index too.
func pgPrimaryOID(t *TableInfo) int64 {
	if len(t.PKColumns) == 0 {
		return 0
	}
	return pgSyntheticOID(fmt.Sprintf("primary:%d", t.ID))
}
func pgPrimaryName(t *TableInfo) string {
	for _, c := range t.Constraints {
		if c.Kind == "PRIMARY" {
			return c.Name
		}
	}
	_, name := pgObjectNames(t.Name)
	return name + "_pkey"
}
func pgPrimarySQL(t *TableInfo) string {
	cols := make([]string, len(t.PKColumns))
	for i, c := range t.PKColumns {
		cols[i] = quoteIdent(c)
	}
	return "CREATE UNIQUE INDEX " + quoteIdent(pgPrimaryName(t)) + " ON " + quoteIdent(t.Name) + "(" + strings.Join(cols, ", ") + ")"
}
