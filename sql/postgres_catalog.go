package sql

import (
	"fmt"
	"hash/fnv"
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
	if _, known := informationSchemaName(name); strings.HasPrefix(name, "information_schema.") && known {
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
	return DialectPostgreSQL.NamespacePolicy().Present(name)
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
	r = r.WithDeclaredType("BOOLEAN")
	return r
}
func pgCatalog(spec string) *ResultSet {
	r := catalogResult(spec)
	for i := range r.Columns {
		r.Columns[i].Postgres = true
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
		v = v.WithDeclaredType(c.Declared)
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

// PostgresCatalogTable projects the shared native facts through PostgreSQL OIDs
// and PostgreSQL's catalog layouts. It never executes a query for introspection.
func (s *Schema) PostgresCatalogTable(name string) (*ResultSet, error) {
	name, known := postgresCatalogName(name)
	if !known {
		return nil, nil
	}
	if strings.HasPrefix(name, "information_schema.") {
		return s.informationSchemaTable(name, catalogFilter{})
	}
	opts, _ := PostgresSession(s.ctx)
	database := opts.Database
	if database == "" {
		database = "slopdb"
	}
	view := s.catalogFacts()
	old := s.catalogView
	s.catalogView = view
	defer func() { s.catalogView = old }()
	r := pgCatalogFor(name)
	switch name {
	case "pg_type":
		for _, t := range pgTypes {
			pgCatalogRow(r, t.oid, t.name, int64(11), int64(10), t.length, t.length > 0, "b", t.category, false, true, 0, 0, 0, 0, -1, false, nil)
		}
		return r, nil
	case "pg_am":
		pgCatalogRow(r, 8000, "lsm", "t")
		pgCatalogRow(r, 8001, "lsm_index", "i")
		return r, nil
	case "pg_database":
		pgCatalogRow(r, 16384, database, 10, 6, false, true, -1, "C", "C")
		return r, nil
	case "pg_namespace":
		schemas, e := s.informationSchemaTable("schemata", catalogFilter{})
		if e != nil {
			return nil, e
		}
		for _, row := range schemas.Rows {
			n := row["schema_name"].Text
			pgCatalogRow(r, pgNamespaceOID(n), n, int64(10), nil)
		}
		return r, nil
	}
	facts, e := s.catalogRelations(catalogFilter{}, name == "pg_attribute" || name == "pg_class")
	if e != nil {
		return nil, e
	}
	// Wire OIDs are intentionally narrower than durable native IDs. Detect
	// collisions rather than publishing a wrapped or ambiguous identifier.
	seen := map[int64]string{}
	claim := func(oid int64, key string) error {
		if oid == 0 {
			return fmt.Errorf("PostgreSQL OID range exhausted for %s", key)
		}
		if old, ok := seen[oid]; ok && old != key {
			return fmt.Errorf("PostgreSQL virtual OID collision between %s and %s", old, key)
		}
		seen[oid] = key
		return nil
	}
	for _, fact := range facts {
		rel := fact.Relation
		oid := pgObjectOID(rel.ID)
		if rel.Kind == "view" {
			oid = pgSyntheticOID("view:" + fact.Key)
		}
		if e = claim(oid, rel.Kind+":"+fact.Key); e != nil {
			return nil, e
		}
		if t := rel.Definition; t != nil && pgPrimaryIsSynthetic(t) {
			if e = claim(pgPrimaryOID(t), "primary:"+fact.Key); e != nil {
				return nil, e
			}
		}
		for _, idx := range rel.Indexes {
			if e = claim(pgObjectOID(idx.ID), "index:"+idx.Name); e != nil {
				return nil, e
			}
		}
	}
	addClass := func(oid int64, ns, obj, kind string, columns int, hasindex, hastriggers, temp bool, checks int, count float64) {
		persistence := "p"
		if temp {
			persistence = "t"
		}
		am := 8000
		if kind == "i" {
			am = 8001
		} else if kind == "v" {
			am = 0
		}
		pgCatalogRow(r, oid, obj, pgNamespaceOID(ns), 0, 0, 10, am, 0, 0, 0, count, 0, 0, hasindex, false, persistence, kind, columns, checks, false, hastriggers, false, false, false, true, "d", false, 0, nil, nil)
	}
	for _, fact := range facts {
		rel := fact.Relation
		t := rel.Definition
		ns := pgNamespaceName(rel.Schema)
		oid := pgObjectOID(rel.ID)
		if rel.Kind == "view" {
			oid = pgSyntheticOID("view:" + fact.Key)
		}
		hasindex := len(rel.Indexes) > 0
		checks := 0
		hastriggers := false
		if t != nil {
			hasindex = hasindex || len(t.PKColumns) > 0
			for _, c := range t.Constraints {
				if c.Kind == "CHECK" {
					checks++
				}
			}
		}
		for _, tr := range view.triggers {
			if tr.Table == fact.Key || tr.View == fact.Key {
				hastriggers = true
				break
			}
		}
		switch name {
		case "pg_class":
			kind := "r"
			if rel.Kind == "view" {
				kind = "v"
			}
			count := -1.0
			if v, ok := view.RowEstimate(fact.Key); ok {
				count = float64(v)
			}
			addClass(oid, ns, rel.Name, kind, len(rel.Columns), hasindex, hastriggers, rel.Temporary, checks, count)
			for _, idx := range rel.Indexes {
				addClass(pgObjectOID(idx.ID), ns, idx.SQLName(), "i", len(idx.columnNames()), false, false, rel.Temporary, 0, -1)
			}
			if t != nil && pgPrimaryIsSynthetic(t) {
				addClass(pgPrimaryOID(t), ns, pgPrimaryName(t), "i", len(t.PKColumns), false, false, rel.Temporary, 0, -1)
			}
		case "pg_attribute":
			for i, c := range rel.Columns {
				typ := pgTypeFor(pgTypeOID(c))
				pgCatalogRow(r, oid, c.Name, typ.oid, -1, typ.length, i+1, 0, -1, pgTypeMod(c), typ.length > 0, "p", "i", c.NotNull || c.PrimaryKey, c.DefaultSQL != "", false, "", "", false, true, 0, 0, nil, nil, nil, nil)
			}
		case "pg_attrdef":
			if t == nil {
				continue
			}
			for i, c := range rel.Columns {
				def := catalogDefault(c, true)
				if !def.Null {
					pgCatalogRow(r, pgSyntheticOID(fmt.Sprintf("default:%d:%d", rel.ID, i)), oid, i+1, def)
				}
			}
		case "pg_tables":
			if t != nil {
				pgCatalogRow(r, ns, rel.Name, nil, nil, hasindex, false, hastriggers, false)
			}
		case "pg_views":
			if rel.Kind == "view" {
				pgCatalogRow(r, ns, rel.Name, nil, rel.ViewSQL)
			}
		case "pg_indexes", "pg_index":
			if t == nil {
				continue
			}
			addIndex := func(indexOID int64, indexName string, columns []string, unique, primary, clustered bool, definition string) {
				if name == "pg_indexes" {
					pgCatalogRow(r, ns, rel.Name, indexName, nil, definition)
					return
				}
				var key []string
				for _, cn := range columns {
					for i, c := range t.Columns {
						if c.Name == cn {
							key = append(key, fmt.Sprint(i+1))
						}
					}
				}
				pgCatalogRow(r, indexOID, oid, len(key), len(key), unique, false, primary, false, true, clustered, true, false, true, true, false, strings.Join(key, " "), nil, nil)
			}
			for _, idx := range rel.Indexes {
				primary := false
				for _, c := range t.Constraints {
					primary = primary || c.Kind == "PRIMARY" && c.IndexID == idx.ID
				}
				addIndex(pgObjectOID(idx.ID), idx.SQLName(), idx.columnNames(), idx.IsUnique, primary, false, s.GetIndexSQL(idx.Name))
			}
			if pgPrimaryIsSynthetic(t) {
				addIndex(pgPrimaryOID(t), pgPrimaryName(t), t.PKColumns, true, true, true, pgPrimarySQL(t))
			}
		case "pg_constraint":
			if t == nil {
				continue
			}
			for _, c := range t.Constraints {
				typ := ""
				switch catalogConstraintKind(c.Kind) {
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
				ref := view.tables[c.RefTable]
				refid := int64(0)
				if ref != nil {
					refid = pgObjectOID(ref.ID)
				}
				keys := func(columns []string, table *TableInfo) string {
					var positions []string
					if table != nil {
						for _, cn := range columns {
							for i, col := range table.Columns {
								if col.Name == cn {
									positions = append(positions, fmt.Sprint(i+1))
								}
							}
						}
					}
					return "{" + strings.Join(positions, ",") + "}"
				}
				indexOID := pgObjectOID(c.IndexID)
				if typ == "p" {
					indexOID = pgPrimaryOID(t)
				}
				pgCatalogRow(r, pgSyntheticOID(fmt.Sprintf("constraint:%d:%s", t.ID, c.Name)), catalogConstraintName(t, c), pgNamespaceOID(ns), typ, c.Deferrable, c.InitiallyDeferred, true, oid, 0, indexOID, 0, refid, pgAction(c.OnUpdate), pgAction(c.OnDelete), "s", true, 0, true, keys(c.Columns, t), keys(c.RefColumns, ref), c.CheckSQL)
			}
		}
	}
	return r, nil
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

	for _, c := range t.Constraints {
		if c.Kind == "PRIMARY" && c.IndexID != 0 {
			return pgObjectOID(c.IndexID)
		}
	}
	if len(t.PKColumns) == 0 {
		return 0
	}
	return pgSyntheticOID(fmt.Sprintf("primary:%d", t.ID))
}
func pgPrimaryName(t *TableInfo) string {
	for _, c := range t.Constraints {
		if c.Kind == "PRIMARY" {
			return pgConstraintName(t, c)
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

func pgConstraintName(t *TableInfo, c ConstraintDef) string { return catalogConstraintName(t, c) }

func pgPrimaryIsSynthetic(t *TableInfo) bool {
	if len(t.PKColumns) == 0 {
		return false
	}
	for _, c := range t.Constraints {
		if c.Kind == "PRIMARY" && c.IndexID != 0 {
			return false
		}
	}
	return true
}
