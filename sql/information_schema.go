package sql

import (
	"fmt"
	"sort"
	"strings"
)

// The standard catalog and its native extensions have one typed layout shared
// by execution, preparation and wire adapters. Missing facts stay NULL; these
// relations never evaluate defaults or infer permissions from connection names.
var informationSchemaSchemas = map[string]string{
	"schemata":                              "catalog_name schema_name default_character_set_name default_collation_name sql_path schema_owner default_character_set_catalog default_character_set_schema",
	"tables":                                "table_catalog table_schema table_name table_type engine version:i row_format table_rows:i avg_row_length:i data_length:i max_data_length:i index_length:i data_free:i auto_increment:i create_time update_time check_time table_collation checksum:i create_options table_comment self_referencing_column_name reference_generation user_defined_type_catalog user_defined_type_schema user_defined_type_name is_insertable_into is_typed commit_action slopdb_table_id:u slopdb_temporary",
	"columns":                               "table_catalog table_schema table_name column_name ordinal_position:i column_default is_nullable data_type column_type column_key extra character_set_name collation_name character_maximum_length:i character_octet_length:i numeric_precision:i numeric_precision_radix:i numeric_scale:i datetime_precision:i interval_type interval_precision:i character_set_catalog character_set_schema collation_catalog collation_schema domain_catalog domain_schema domain_name udt_catalog udt_schema udt_name scope_catalog scope_schema scope_name maximum_cardinality:i dtd_identifier is_self_referencing is_identity identity_generation identity_start identity_increment identity_maximum identity_minimum identity_cycle is_generated generation_expression is_updatable privileges column_comment slopdb_default_expression slopdb_declared_type slopdb_column_ordinal:i",
	"views":                                 "table_catalog table_schema table_name view_definition check_option is_updatable is_insertable_into is_trigger_updatable is_trigger_deletable is_trigger_insertable_into definer security_type character_set_client collation_connection",
	"statistics":                            "table_catalog table_schema table_name non_unique:i index_schema index_name seq_in_index:i column_name collation cardinality:i sub_part:i packed nullable index_type comment index_comment is_visible expression slopdb_index_id:u",
	"table_indexes":                         "table_catalog table_schema table_name index_schema index_name index_type is_unique is_primary slopdb_index_id:u",
	"table_constraints":                     "constraint_catalog constraint_schema constraint_name table_catalog table_schema table_name constraint_type is_deferrable initially_deferred enforced nulls_distinct",
	"key_column_usage":                      "constraint_catalog constraint_schema constraint_name table_catalog table_schema table_name column_name ordinal_position:i position_in_unique_constraint:i referenced_table_schema referenced_table_name referenced_column_name",
	"referential_constraints":               "constraint_catalog constraint_schema constraint_name unique_constraint_catalog unique_constraint_schema unique_constraint_name match_option update_rule delete_rule table_name referenced_table_name",
	"check_constraints":                     "constraint_catalog constraint_schema constraint_name check_clause",
	"constraint_column_usage":               "table_catalog table_schema table_name column_name constraint_catalog constraint_schema constraint_name",
	"triggers":                              "trigger_catalog trigger_schema trigger_name event_manipulation event_object_catalog event_object_schema event_object_table action_order:i action_condition action_statement action_orientation action_timing action_reference_old_table action_reference_new_table action_reference_old_row action_reference_new_row created sql_mode definer character_set_client collation_connection database_collation",
	"character_sets":                        "character_set_name default_collate_name description maxlen:i character_set_catalog character_set_schema character_repertoire form_of_use default_collate_catalog default_collate_schema",
	"collations":                            "collation_name character_set_name id:i is_default is_compiled sortlen:i pad_attribute collation_catalog collation_schema pad_attribute_description",
	"collation_character_set_applicability": "collation_name character_set_name collation_catalog collation_schema character_set_catalog character_set_schema",
	"engines":                               "engine support comment transactions xa savepoints",
	"routines":                              "specific_catalog specific_schema specific_name routine_catalog routine_schema routine_name routine_type data_type routine_definition external_language is_deterministic sql_data_access security_type",
	"parameters":                            "specific_catalog specific_schema specific_name ordinal_position:i parameter_mode is_result as_locator parameter_name data_type character_maximum_length:i character_octet_length:i numeric_precision:i numeric_scale:i parameter_default",
}

func informationSchemaName(name string) (string, bool) {
	name = strings.ToLower(name)
	name = strings.TrimPrefix(name, "information_schema.")
	_, ok := informationSchemaSchemas[name]
	return name, ok
}
func informationSchemaColumns(name string) ([]ColumnDef, bool) {
	n, ok := informationSchemaName(name)
	if !ok {
		return nil, false
	}
	cols := catalogResult(informationSchemaSchemas[n]).Columns
	return cols, true
}

type catalogFilter struct{ Schema, Table string }
type catalogRelationFact struct {
	Key      string
	Relation CatalogRelation
}

func (s *Schema) catalogNamespace(name string) string {
	if name != "main" {
		return name
	}
	if s.PostgresMode() {
		return "public"
	}
	if opts, ok := MySQLSession(s.ctx); ok {
		if opts.RootDatabase != "" {
			return opts.RootDatabase
		}
		if opts.Database != "" {
			return opts.Database
		}
	}
	return "main"
}
func (s *Schema) catalogDatabase() string {
	if s.PostgresMode() {
		opts, _ := PostgresSession(s.ctx)
		if opts.Database != "" {
			return opts.Database
		}
		return "slopdb"
	}
	if _, ok := MySQLSession(s.ctx); ok {
		return "def"
	}
	return "main"
}

// Filter before deriving view columns or building result rows. Point catalog
// queries do work proportional to the selected relations, not every column.
func (s *Schema) catalogRelations(filter catalogFilter, describeViews bool) ([]catalogRelationFact, error) {
	view := s.catalogFacts()
	var names []string
	if filter.Table != "" && filter.Schema != "" {
		ns := s.namespaceAlias(filter.Schema)
		key := filter.Table
		if ns != "main" {
			key = ns + "." + key
		}
		if _, ok := view.relation(key); ok {
			names = []string{key}
		}
	} else {
		names = append(view.TableNames(), view.ViewNames()...)
		sort.Strings(names)
	}
	out := make([]catalogRelationFact, 0, len(names))
	for _, n := range names {
		ns, bare := SplitSchemaName(n)
		if filter.Table != "" && bare != filter.Table || filter.Schema != "" && s.catalogNamespace(ns) != filter.Schema {
			continue
		}
		if err := s.checkInterrupt(); err != nil {
			return nil, err
		}
		relation, _ := view.relation(n)
		if relation.Kind == "view" && describeViews {
			cols, e := s.describeTable(n, nil, 0)
			if e != nil {
				return nil, e
			}
			for i := range cols {
				cols[i].NotNull = false
				cols[i].PrimaryKey = false
				cols[i].Unique = false
				cols[i].AutoIncrement = false
				cols[i].MySQLAutoIncrement = false
				cols[i].Default = nil
				cols[i].DefaultSQL = ""
			}
			relation.Columns = cols
		}
		out = append(out, catalogRelationFact{Key: n, Relation: relation})
	}
	return out, nil
}
func catalogEmptyRow(r *ResultSet) map[string]Value {
	row := make(map[string]Value, len(r.Columns))
	for _, c := range r.Columns {
		row[c.Name] = NewNullValue()
	}
	return row
}
func catalogSet(row map[string]Value, fields map[string]any) {
	for k, x := range fields {
		switch v := x.(type) {
		case string:
			row[k] = NewTextValue(v)
		case int:
			row[k] = NewIntValue(int64(v))
		case int64:
			row[k] = NewIntValue(v)
		case uint64:
			row[k] = NewUnsignedValue(v)
		case Value:
			row[k] = v
		}
	}
}
func catalogDefault(c ColumnDef, expression bool) Value {
	text := c.DefaultSQL
	if text == "" && c.Default != nil {
		text = formatExpr(c.Default)
	}
	if text == "" {
		return NewNullValue()
	}
	if !expression {
		if lit, ok := c.Default.(*LiteralExpr); ok {
			if lit.Value.Null {
				return NewNullValue()
			}
			return NewTextValue(lit.Value.String())
		}
		// The sign is syntax, not an invitation to execute a user expression.
		if u, ok := c.Default.(*UnaryExpr); ok && (u.Op == "+" || u.Op == "-") {
			if lit, ok := u.Right.(*LiteralExpr); ok && !lit.Value.Null && (lit.Value.Type == ColInteger || lit.Value.Type == ColReal || lit.Value.Type == ColDecimal) {
				prefix := u.Op
				if prefix == "+" {
					prefix = ""
				}
				return NewTextValue(prefix + lit.Value.String())
			}
		}
	}
	return NewTextValue(text)
}
func catalogColumnKey(c ColumnDef, keys []showKey) string {
	key := ""
	for _, k := range keys {
		for i, n := range k.columns {
			if n != c.Name {
				continue
			}
			if k.name == "PRIMARY" {
				return "PRI"
			}
			if i == 0 {
				if k.unique && len(k.columns) == 1 {
					key = "UNI"
				} else if key == "" {
					key = "MUL"
				}
			}
		}
	}
	return key
}

func (s *Schema) informationSchemaTable(name string, filter catalogFilter) (*ResultSet, error) {
	n, known := informationSchemaName(name)
	if !known {
		return nil, fmt.Errorf("unknown information_schema table %q", name)
	}
	cols, _ := informationSchemaColumns(n)
	r := &ResultSet{Columns: cols}
	add := func(fields map[string]any) {
		row := catalogEmptyRow(r)
		catalogSet(row, fields)
		r.Rows = append(r.Rows, row)
	}
	database := s.catalogDatabase()
	view := s.catalogFacts()
	old := s.catalogView
	s.catalogView = view
	defer func() { s.catalogView = old }()
	switch n {
	case "routines", "parameters":
		return r, nil // Stored routines are not implemented.
	case "engines":
		add(map[string]any{"engine": "LSM", "support": "DEFAULT", "comment": "SlopDB transactional LSM storage", "transactions": "YES", "xa": "NO", "savepoints": "YES"})
		return r, nil
	case "schemata":
		names := append([]string{"information_schema"}, view.SchemaNames()...)
		if s.PostgresMode() {
			names = append(names, "pg_catalog")
		} else if _, ok := MySQLSession(s.ctx); ok {
			names = append(names, "mysql")
		}
		sort.Strings(names)
		seen := map[string]bool{}
		for _, ns := range names {
			ns = s.catalogNamespace(ns)
			if seen[ns] || filter.Schema != "" && filter.Schema != ns {
				continue
			}
			seen[ns] = true
			add(map[string]any{"catalog_name": database, "schema_name": ns, "default_character_set_name": "utf8mb4", "default_collation_name": "utf8mb4_bin"})
		}
		return r, nil
	case "character_sets", "collations", "collation_character_set_applicability":
		s.informationSchemaCharsets(r, n)
		return r, nil
	}
	relationFilter := filter
	// This relation describes referenced columns, so table predicates cannot
	// filter the owning constraint relation before projection.
	if n == "constraint_column_usage" {
		relationFilter = catalogFilter{}
	}
	relations, err := s.catalogRelations(relationFilter, n == "columns")
	if err != nil {
		return nil, err
	}
	for _, f := range relations {
		rel := f.Relation
		ns := s.catalogNamespace(rel.Schema)
		t := rel.Definition
		base := func() map[string]any {
			return map[string]any{"table_catalog": database, "table_schema": ns, "table_name": rel.Name}
		}
		var keys []showKey
		if t != nil {
			keys = view.keys(t)
		}
		switch n {
		case "tables":
			row := base()
			kind := "BASE TABLE"
			if rel.Temporary {
				kind = "LOCAL TEMPORARY"
			}
			if rel.Kind == "view" {
				kind = "VIEW"
			} else {
				row["engine"] = "LSM"
				row["row_format"] = "LSM"
				row["table_comment"] = "SlopDB transactional LSM storage"
				row["table_collation"] = strings.ToLower(string(s.catalogTextCollation(ColumnDef{}, t)))
				if count, ok := view.RowEstimate(f.Key); ok {
					row["table_rows"] = count
				}
				row["slopdb_table_id"] = rel.ID
			}
			row["table_type"] = kind
			row["is_insertable_into"] = catalogYes(rel.Kind == "table")
			row["is_typed"] = "NO"
			row["create_options"] = ""
			row["slopdb_temporary"] = catalogYes(rel.Temporary)
			add(row)
		case "views":
			if rel.Kind != "view" {
				continue
			}
			row := base()
			row["view_definition"] = rel.ViewSQL
			row["check_option"] = "NONE"
			row["is_updatable"] = "NO"
			row["is_insertable_into"] = "NO"
			row["is_trigger_updatable"] = "NO"
			row["is_trigger_deletable"] = "NO"
			row["is_trigger_insertable_into"] = "NO"
			add(row)
		case "columns":
			for i, c := range rel.Columns {
				row := base()
				decl := c.DeclaredType()
				dt := strings.ToLower(decl)
				if pos := strings.IndexAny(dt, "( "); pos >= 0 {
					dt = dt[:pos]
				}
				row["column_name"] = c.DeclaredName()
				row["ordinal_position"] = i + 1
				row["slopdb_column_ordinal"] = i + 1
				row["column_default"] = catalogDefault(c, s.PostgresMode())
				row["slopdb_default_expression"] = catalogDefault(c, true)
				row["slopdb_declared_type"] = decl
				row["is_nullable"] = catalogYes(!c.NotNull && !c.PrimaryKey)
				row["data_type"] = dt
				row["column_type"] = strings.ToLower(decl)
				row["column_key"] = catalogColumnKey(c, keys)
				row["extra"] = ""
				row["column_comment"] = ""
				row["dtd_identifier"] = fmt.Sprint(i + 1)
				row["is_self_referencing"] = "NO"
				row["is_identity"] = "NO"
				row["is_generated"] = "NEVER"
				row["is_updatable"] = catalogYes(rel.Kind == "table")
				if c.AutoIncrement || c.MySQLAutoIncrement {
					row["extra"] = "auto_increment"
				}
				logical := c.LogicalType()
				if s.PostgresMode() {
					typ := pgTypeFor(pgTypeOID(c))
					row["data_type"] = typ.display
					row["udt_catalog"] = database
					row["udt_schema"] = "pg_catalog"
					row["udt_name"] = typ.name
				}
				switch logical.Kind {
				case TypeText, TypeVarchar, TypeEnum, TypeIdentifier:
					charset := "utf8mb4"
					if t != nil && t.Charset != "" {
						charset = strings.ToLower(t.Charset)
					}
					collation := strings.ToLower(string(s.catalogTextCollation(c, t)))
					if prefix, _, ok := strings.Cut(collation, "_"); ok && (prefix == "utf8" || prefix == "utf8mb3" || prefix == "utf8mb4") {
						charset = prefix
					}
					row["character_set_name"] = charset
					row["collation_name"] = collation
				}
				if c.Length > 0 {
					row["character_maximum_length"] = c.Length
					octets := c.Length
					if c.ColType == ColText && octets <= int64(^uint64(0)>>1)/4 {
						row["character_octet_length"] = octets * 4
					} else if c.ColType == ColBlob {
						row["character_octet_length"] = octets
					}
				}
				switch c.ColType {
				case ColInteger, ColUnsigned:
					bits := c.IntegerBits
					if bits == 0 {
						bits = 64
					}
					row["numeric_precision"] = bits
					row["numeric_precision_radix"] = 2
					row["numeric_scale"] = 0
				case ColReal:
					precision := 53
					if logical.Kind == TypeFloat32 {
						precision = 24
					}
					row["numeric_precision"] = precision
					row["numeric_precision_radix"] = 2
				case ColDecimal:
					row["numeric_precision_radix"] = 10
					if c.Precision > 0 {
						row["numeric_precision"] = c.Precision
						row["numeric_scale"] = c.Scale
					}
				}
				switch logical.Kind {
				case TypeDate:
					row["datetime_precision"] = 0
				case TypeTime, TypeTimeTZ, TypeTimestamp, TypeTimestampTZ, TypeInterval:
					precision := c.TemporalPrecision
					if c.Postgres && !c.TypeModifiers {
						precision = 6
					}
					row["datetime_precision"] = precision
				}

				add(row)
			}
		case "statistics", "table_indexes":
			if t == nil {
				continue
			}
			for _, key := range keys {
				id := uint64(0)
				var index *IndexInfo
				for _, idx := range rel.Indexes {
					if idx.SQLName() == key.name || key.name == "PRIMARY" && colsEqual(idx.columnNames(), t.PKColumns) {
						index = idx
						id = idx.ID
						break
					}
				}
				if n == "table_indexes" {
					row := base()
					row["index_schema"] = ns
					row["index_name"] = key.name
					row["index_type"] = key.method
					row["is_unique"] = catalogYes(key.unique)
					row["is_primary"] = catalogYes(key.name == "PRIMARY")
					if id > 0 {
						row["slopdb_index_id"] = id
					}
					add(row)
					continue
				}
				for i, cn := range key.columns {
					row := base()
					row["non_unique"] = 1
					if key.unique {
						row["non_unique"] = 0
					}
					row["index_schema"] = ns
					row["index_name"] = key.name
					row["seq_in_index"] = i + 1
					row["column_name"] = catalogDeclaredColumnName(t, cn)
					row["collation"] = "A"
					row["nullable"] = "YES"
					for _, c := range t.Columns {
						if c.Name == cn && (c.NotNull || c.PrimaryKey) {
							row["nullable"] = ""
						}
					}
					row["index_type"] = key.method
					row["comment"] = ""
					row["index_comment"] = ""
					row["is_visible"] = "YES"
					if index != nil && i < len(index.PrefixLengths) && index.PrefixLengths[i] > 0 {
						row["sub_part"] = index.PrefixLengths[i]
					}
					if id > 0 {
						row["slopdb_index_id"] = id
					}
					add(row)
				}
			}
		case "table_constraints", "key_column_usage", "referential_constraints", "check_constraints", "constraint_column_usage":
			if t == nil {
				continue
			}
			for _, c := range t.Constraints {
				kind := catalogConstraintKind(c.Kind)
				constraint := catalogConstraintName(t, c)
				common := func() map[string]any {
					row := base()
					row["constraint_catalog"] = database
					row["constraint_schema"] = ns
					row["constraint_name"] = constraint
					return row
				}
				switch n {
				case "table_constraints":
					row := common()
					row["constraint_type"] = kind
					row["is_deferrable"] = catalogYes(c.Deferrable)
					row["initially_deferred"] = catalogYes(c.InitiallyDeferred)
					row["enforced"] = "YES"
					row["nulls_distinct"] = "YES"
					add(row)
				case "check_constraints":
					if kind == "CHECK" {
						row := common()
						row["check_clause"] = c.CheckSQL
						add(row)
					}
				case "key_column_usage":
					if kind == "CHECK" {
						continue
					}
					for i, cn := range c.Columns {
						row := common()
						row["column_name"] = catalogDeclaredColumnName(t, cn)
						row["ordinal_position"] = i + 1
						if kind == "FOREIGN KEY" {
							rns, rname := SplitSchemaName(c.RefTable)
							row["position_in_unique_constraint"] = i + 1
							row["referenced_table_schema"] = s.catalogNamespace(rns)
							row["referenced_table_name"] = rname
							if i < len(c.RefColumns) {
								row["referenced_column_name"] = catalogDeclaredColumnName(view.tables[c.RefTable], c.RefColumns[i])
							}
						}
						add(row)
					}
				case "referential_constraints":
					if kind != "FOREIGN KEY" {
						continue
					}
					row := common()
					rns, rname := SplitSchemaName(c.RefTable)
					row["unique_constraint_catalog"] = database
					row["unique_constraint_schema"] = s.catalogNamespace(rns)
					row["referenced_table_name"] = rname
					row["match_option"] = "NONE"
					up, del := c.OnUpdate, c.OnDelete
					if up == "" {
						up = "NO ACTION"
					}
					if del == "" {
						del = "NO ACTION"
					}
					row["update_rule"] = up
					row["delete_rule"] = del
					if parent := view.tables[c.RefTable]; parent != nil {
						for _, candidate := range parent.Constraints {
							if (candidate.Kind == "PRIMARY" || candidate.Kind == "UNIQUE") && colsEqual(candidate.Columns, c.RefColumns) {
								row["unique_constraint_name"] = catalogConstraintName(parent, candidate)
								break
							}
						}
					}
					add(row)
				case "constraint_column_usage":
					target := t
					cols := c.Columns
					if kind == "CHECK" {
						seen := map[string]bool{}
						_ = walkExpression(c.Check, func(expr Expression) (bool, error) {
							if ref, ok := expr.(*ColumnRefExpr); ok {
								_, cn := SplitSchemaName(ref.Name)
								if !seen[cn] {
									seen[cn] = true
									cols = append(cols, cn)
								}
							}
							return true, nil
						})
					}
					if kind == "FOREIGN KEY" {
						target = view.tables[c.RefTable]
						cols = c.RefColumns
					}
					if target == nil {
						continue
					}
					tns, tn := SplitSchemaName(target.Name)
					for _, cn := range cols {
						row := common()
						row["table_schema"] = s.catalogNamespace(tns)
						row["table_name"] = tn
						row["column_name"] = catalogDeclaredColumnName(target, cn)
						add(row)
					}
				}
			}
		}
	}
	if n == "triggers" {
		for _, name := range view.TriggerNames() {
			tr := view.triggers[name]
			target := tr.Table
			if target == "" {
				target = tr.View
			}
			ns, bare := SplitSchemaName(target)
			ns = s.catalogNamespace(ns)
			if filter.Schema != "" && ns != filter.Schema || filter.Table != "" && bare != filter.Table {
				continue
			}
			_, triggerName := SplitSchemaName(name)
			timing := tr.Timing
			if tr.InsteadOf {
				timing = "INSTEAD OF"
			} else if timing == "" {
				timing = "AFTER"
			}
			orientation := "STATEMENT"
			if tr.ForEachRow {
				orientation = "ROW"
			}
			condition := NewNullValue()
			if tr.When != "" {
				condition = NewTextValue(tr.When)
			}
			add(map[string]any{"trigger_catalog": database, "trigger_schema": ns, "trigger_name": triggerName, "event_manipulation": tr.Event, "event_object_catalog": database, "event_object_schema": ns, "event_object_table": bare, "action_order": 1, "action_condition": condition, "action_statement": strings.Join(tr.Body, "; "), "action_orientation": orientation, "action_timing": timing})
		}
	}
	if n == "constraint_column_usage" && (filter.Schema != "" || filter.Table != "") {
		rows := r.Rows[:0]
		for _, row := range r.Rows {
			if filter.Schema != "" && row["table_schema"].Text != filter.Schema || filter.Table != "" && row["table_name"].Text != filter.Table {
				continue
			}
			rows = append(rows, row)
		}
		r.Rows = rows
	}
	return r, nil
}
func (s *Schema) informationSchemaCharsets(r *ResultSet, name string) {
	add := func(fields map[string]any) {
		row := catalogEmptyRow(r)
		catalogSet(row, fields)
		r.Rows = append(r.Rows, row)
	}
	for _, charset := range []string{"binary", "utf8", "utf8mb3", "utf8mb4"} {
		if name == "character_sets" {
			max := 4
			description := "Unicode UTF-8 (full Unicode)"
			collation := charset + "_bin"
			if charset == "binary" {
				max = 1
				description = "Binary bytes"
				collation = "binary"
			}
			add(map[string]any{"character_set_name": charset, "default_collate_name": collation, "description": description, "maxlen": max, "character_repertoire": "UCS", "form_of_use": "UTF8"})
			continue
		}
		names := []string{"binary"}
		if charset != "binary" {
			names = []string{charset + "_bin", charset + "_general_ci", charset + "_unicode_ci", charset + "_unicode_520_ci"}
			if charset == "utf8mb4" {
				names = append(names, "utf8mb4_0900_ai_ci")
			}
		}
		for _, collation := range names {
			if _, err := ParseCollation(collation); err != nil {
				continue
			}
			row := map[string]any{"collation_name": collation, "character_set_name": charset, "is_default": "", "is_compiled": "YES", "sortlen": 1, "pad_attribute": "PAD SPACE"}
			if collation == "binary" || strings.HasSuffix(collation, "_bin") {
				row["is_default"] = "YES"
			}
			if collation == "binary" || strings.Contains(collation, "0900") {
				row["pad_attribute"] = "NO PAD"
			}
			add(row)
		}
	}
}

func catalogYes(v bool) string {
	if v {
		return "YES"
	}
	return "NO"
}
func catalogConstraintKind(kind string) string {
	switch strings.ToUpper(kind) {
	case "PRIMARY":
		return "PRIMARY KEY"
	case "FOREIGN":
		return "FOREIGN KEY"
	}
	return strings.ToUpper(kind)
}
func catalogConstraintName(t *TableInfo, c ConstraintDef) string {
	ns, _ := SplitSchemaName(t.Name)
	return strings.TrimPrefix(c.Name, ns+".")
}

func catalogDeclaredColumnName(table *TableInfo, name string) string {
	if table != nil {
		for _, col := range table.Columns {
			if col.Name == name {
				return col.DeclaredName()
			}
		}
	}
	return name
}
func (s *Schema) catalogTextCollation(c ColumnDef, table *TableInfo) Collation {
	if c.Binary {
		return BinaryCollation
	}
	if c.Collation != "" {
		return c.Collation
	}
	if table != nil && table.DefaultCollation != "" {
		return table.DefaultCollation
	}
	if s.collation != "" {
		return s.collation
	}
	return "UTF8MB4_BIN"
}
