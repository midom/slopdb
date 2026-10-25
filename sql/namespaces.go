package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sort"
	"strings"
)

const namespacesPrefix = "\x00__namespaces_v1__"

// SplitSchemaName separates canonical catalog names. Legacy unqualified objects
// belong to main; virtual namespaces share that database's object allocator/LSM.
func SplitSchemaName(name string) (schema, object string) {
	if a, b, ok := strings.Cut(name, "."); ok {
		return a, b
	}
	return "main", name
}
func namespaceObject(schema, name string) string {
	if schema == "main" {
		return name
	}
	return schema + "." + name
}
func (s *Schema) namespaceAlias(name string) string {
	name = strings.ToLower(name)
	if options, ok := MySQLSession(s.ctx); ok {
		root := options.RootDatabase
		if root == "" && !s.namespaces[options.Database] {
			root = options.Database
		}
		if strings.EqualFold(root, name) {
			return "main"
		}
	}
	if name == "public" {
		return "main"
	}
	if canonical := s.schemaAliases[name]; canonical != "" {
		return canonical
	}
	return name
}
func (s *Schema) CurrentSchema() string {
	if s.defaultSchema == "" {
		return "main"
	}
	return s.defaultSchema
}
func (s *Schema) SearchPath() []string {
	if s.ctx != nil {
		if path, ok := s.ctx.Value(namespaceSearchPathKey{}).([]string); ok && len(path) > 0 {
			return slices.Clone(path)
		}
	}
	if len(s.searchPath) > 0 {
		return slices.Clone(s.searchPath)
	}
	return []string{s.CurrentSchema()}
}
func (s *Schema) RegisterRootSchemaAlias(name string) {
	if s.schemaAliases == nil {
		s.schemaAliases = make(map[string]string)
	}
	s.schemaAliases[strings.ToLower(name)] = "main"
}
func (s *Schema) SchemaNames() []string {
	out := make([]string, 0, len(s.namespaces))
	for name := range s.namespaces {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
func (s *Schema) CommittedSchemaNames() []string {
	root := s
	if s.parent != nil {
		root = s.parent
	}
	root.commitMu.RLock()
	defer root.commitMu.RUnlock()
	return root.SchemaNames()
}
func (s *Schema) DefaultSchemaNeedsResolution() bool {
	path := s.SearchPath()
	return path[0] != "main" || len(path) > 1
}
func (s *Schema) loadNamespaces() error {
	s.namespaces = map[string]bool{"main": true}
	data, ok, err := s.schemaGet([]byte(namespacesPrefix))
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if err = json.Unmarshal(data, &s.namespaces); err != nil {
		return fmt.Errorf("invalid namespace catalog: %w", err)
	}
	if !s.namespaces["main"] {
		return fmt.Errorf("namespace catalog missing main")
	}
	return nil
}
func (s *Schema) saveNamespaces() error {
	if s.inTx {
		return nil
	}
	data, err := json.Marshal(s.namespaces)
	if err != nil {
		return err
	}
	return s.write([]byte(namespacesPrefix), data)
}
func (s *Schema) ExecuteNamespace(stmt *NamespaceStmt) (*ResultSet, error) {
	if len(stmt.Names) == 0 {
		return nil, fmt.Errorf("schema name required")
	}
	name := s.namespaceAlias(stmt.Names[0])
	switch stmt.Operation {
	case "create":
		if name == "" || strings.ContainsAny(name, ".\x00") || name == "information_schema" || name == "mysql" {
			return nil, fmt.Errorf("invalid or reserved schema name")
		}
		if s.namespaces[name] {
			if stmt.IfNotExists {
				return nil, nil
			}
			return nil, fmt.Errorf("schema %q already exists", name)
		}
		s.namespaces = maps.Clone(s.namespaces)
		s.namespaces[name] = true
		return nil, s.saveNamespaces()
	case "drop":
		if s.temporary != nil {
			for table := range s.temporary.tables {
				schema, _ := SplitSchemaName(table)
				if schema == name {
					return nil, fmt.Errorf("drop temporary tables before dropping schema %q", name)
				}
			}
		}
		if name == "main" {
			return nil, fmt.Errorf("cannot drop default schema")
		}
		if !s.namespaces[name] {
			if stmt.IfExists {
				return nil, nil
			}
			return nil, fmt.Errorf("schema %q does not exist", name)
		}
		var tables, views []string
		for n := range s.tables {
			schema, _ := SplitSchemaName(n)
			if schema == name {
				tables = append(tables, n)
			}
		}
		for n := range s.views {
			schema, _ := SplitSchemaName(n)
			if schema == name {
				views = append(views, n)
			}
		}
		if !stmt.Cascade && (len(tables) > 0 || len(views) > 0) {
			return nil, fmt.Errorf("schema %q is not empty", name)
		}
		sort.Strings(tables)
		sort.Strings(views)
		for _, n := range views {
			if err := s.DropView(&DropViewStmt{View: n}); err != nil {
				return nil, err
			}
		}
		for _, n := range tables {
			if err := s.DropTable(&DropTableStmt{Table: n}); err != nil {
				return nil, err
			}
		}
		s.namespaces = maps.Clone(s.namespaces)
		delete(s.namespaces, name)
		return nil, s.saveNamespaces()
	case "use":
		if !s.namespaces[name] {
			return nil, fmt.Errorf("unknown schema %q", stmt.Names[0])
		}
		s.defaultSchema = name
		s.searchPath = []string{name}
		return nil, nil
	case "search_path":
		var path []string
		for _, n := range stmt.Names {
			n = s.namespaceAlias(n)
			if n == "$user" {
				continue
			}
			if s.namespaces[n] {
				path = append(path, n)
			}
		}
		if len(path) == 0 {
			return nil, fmt.Errorf("search_path contains no existing schema")
		}
		s.defaultSchema = path[0]
		s.searchPath = path
		return nil, nil
	}
	return nil, fmt.Errorf("unsupported namespace operation")
}
func (s *Schema) namespaceName(name string, create bool, scope map[string]bool) (string, error) {
	if name == "" {
		return "", nil
	}
	name = strings.ToLower(name)
	if s.PostgresMode() && !scope[name] {
		if _, known := postgresCatalogName(name); known {
			if create {
				return "", fmt.Errorf("system catalog %q is read only", name)
			}
			return name, nil
		}
	}
	if scope[name] || name == "sqlite_master" || name == "sqlite_stat1" || strings.HasPrefix(name, "information_schema.") || name == "mysql.user" {
		return name, nil
	}
	if schema, object, qualified := strings.Cut(name, "."); qualified {
		schema = s.namespaceAlias(schema)
		if !s.namespaces[schema] {
			if s.tables[name] != nil || s.views[name] != nil {
				return name, nil
			}
			return "", fmt.Errorf("unknown schema %q", schema)
		}
		return namespaceObject(schema, object), nil
	}
	path := s.SearchPath()
	if !create {
		for _, schema := range path {
			key := namespaceObject(schema, name)
			if s.tables[key] != nil || s.views[key] != nil || s.indexes[key] != nil || s.triggers[key] != nil {
				return key, nil
			}
		}
	}
	return namespaceObject(path[0], name), nil
}

// ResolveNamespaces copies only when name resolution can change an AST. A
// prepared statement's immutable names are resolved afresh for each execution,
// so switching defaults cannot leave a cached binding pointed at another table.
func (s *Schema) ResolveNamespaces(stmt Statement) (Statement, error) {
	needed := s.DefaultSchemaNeedsResolution() || len(s.namespaces) > 1
	if !needed {
		namespaceVisit(reflect.ValueOf(stmt), func(value any) {
			switch st := value.(type) {
			case *FuncCallExpr:
				needed = needed || st.PostgresBare && !s.PostgresMode()
			case *SelectStmt:
				needed = needed || strings.Contains(st.From, ".")
			case JoinClause:
				needed = needed || strings.Contains(st.Table, ".")
			case *ShowStmt:
				needed = needed || st.Database != "" || strings.Contains(st.Object, ".")
			case *AlterTableStmt:
				needed = needed || strings.Contains(st.Table, ".")
			case *CreateViewStmt:
				needed = needed || strings.Contains(st.View, ".")
			case *DropViewStmt:
				needed = needed || strings.Contains(st.View, ".")
			case *InsertStmt:
				needed = needed || strings.Contains(st.Table, ".")
			case *CreateTableStmt:
				needed = needed || strings.Contains(st.Table, ".")
			case *UpdateStmt:
				needed = needed || strings.Contains(st.Table, ".")
			case *DeleteStmt:
				needed = needed || strings.Contains(st.From, ".")
			case *DropTableStmt:
				needed = needed || strings.Contains(st.Table, ".")
			case *CreateIndexStmt:
				needed = needed || strings.Contains(st.Table, ".")
			}
		})
	}
	if !needed {
		return stmt, nil
	}
	copy := (&BoundQuery{}).clone(reflect.ValueOf(stmt)).Interface().(Statement)
	if !s.PostgresMode() {
		resolveNativeBareExpressions(reflect.ValueOf(copy))
	}
	namespaceVisit(reflect.ValueOf(copy), func(value any) {
		switch st := value.(type) {
		case *SelectStmt:
			st.prepared = nil
		case *UpdateStmt:
			st.prepared = nil
		}
	})
	if err := s.resolveNamespaceStatement(copy, nil); err != nil {
		return nil, err
	}
	return copy, nil
}

// Parsing is shared across dialects. PostgreSQL's bare session keywords remain
// ordinary column identifiers for native and MySQL sessions. Resolve a private
// AST before expression validation or read-plan column selection.
func resolveNativeBareExpressions(v reflect.Value) {
	if !v.IsValid() {
		return
	}
	switch v.Kind() {
	case reflect.Interface:
		if v.IsNil() {
			return
		}
		if f, ok := v.Interface().(*FuncCallExpr); ok && f.PostgresBare && v.CanSet() {
			v.Set(reflect.ValueOf(&ColumnRefExpr{Name: strings.ToLower(f.Name)}))
			return
		}
		resolveNativeBareExpressions(v.Elem())
	case reflect.Pointer:
		if !v.IsNil() {
			resolveNativeBareExpressions(v.Elem())
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				resolveNativeBareExpressions(v.Field(i))
			}
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			resolveNativeBareExpressions(v.Index(i))
		}
	}
}
func namespaceVisit(v reflect.Value, visit func(any)) {
	if !v.IsValid() {
		return
	}
	switch v.Kind() {
	case reflect.Interface, reflect.Pointer:
		if v.IsNil() {
			return
		}
		if v.CanInterface() {
			visit(v.Interface())
		}
		namespaceVisit(v.Elem(), visit)
	case reflect.Struct:
		if v.CanInterface() {
			visit(v.Interface())
		}
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				namespaceVisit(v.Field(i), visit)
			}
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			namespaceVisit(v.Index(i), visit)
		}
	}
}
func (s *Schema) resolveNamespaceStatement(stmt Statement, outer map[string]bool) error {
	scope := maps.Clone(outer)
	if scope == nil {
		scope = make(map[string]bool)
	}
	var with *WithClause
	switch st := stmt.(type) {
	case *SelectStmt:
		with = st.With
	case *UnionStmt:
		with = st.With
	case *InsertStmt:
		with = st.With
	case *UpdateStmt:
		with = st.With
	case *DeleteStmt:
		with = st.With
	}
	if with != nil {
		for i := range with.Entries {
			cte := &with.Entries[i]
			scope[cte.Name] = true
			if err := s.resolveNamespaceStatement(cte.Query, scope); err != nil {
				return err
			}
		}
	}
	resolve := func(name *string, create bool) error {
		value, err := s.namespaceName(*name, create, scope)
		if err == nil {
			*name = value
		}
		return err
	}
	references := func(table string, columns []ColumnDef, foreign []ForeignKeyDef, constraints []ConstraintDef) error {
		schema, _ := SplitSchemaName(table)
		ref := func(name string) (string, error) {
			if name != "" && !strings.Contains(name, ".") {
				name = namespaceObject(schema, name)
			}
			return s.namespaceName(name, false, nil)
		}
		for i := range columns {
			if columns[i].RefTable != "" {
				n, err := ref(columns[i].RefTable)
				if err != nil {
					return err
				}
				columns[i].RefTable = n
			}
		}
		for i := range foreign {
			n, err := ref(foreign[i].RefTable)
			if err != nil {
				return err
			}
			foreign[i].RefTable = n
		}
		for i := range constraints {
			if constraints[i].RefTable != "" {
				n, err := ref(constraints[i].RefTable)
				if err != nil {
					return err
				}
				constraints[i].RefTable = n
			}
		}
		return nil
	}
	var joins []JoinClause
	var aliases map[string]string
	relation := func(name *string, alias *string) error {
		original := *name
		if err := resolve(name, false); err != nil {
			return err
		}
		if *alias == "" {
			_, bare := SplitSchemaName(original)
			*alias = bare
		}
		if aliases != nil {
			aliases[original] = *alias
			aliases[*name] = *alias
		}
		return nil
	}
	switch st := stmt.(type) {
	case *NamespaceStmt:
		return nil
	case *SelectStmt:
		aliases = make(map[string]string)
		if st.From != "" {
			if err := relation(&st.From, &st.FromAlias); err != nil {
				return err
			}
		}

		joins = st.Joins
	case *UnionStmt:
		if err := s.resolveNamespaceStatement(st.Left, scope); err != nil {
			return err
		}
		return s.resolveNamespaceStatement(st.Right, scope)
	case *CreateTableStmt:
		if err := resolve(&st.Table, true); err != nil {
			return err
		}
		if st.Like != "" {
			if err := resolve(&st.Like, false); err != nil {
				return err
			}
		}
		if err := references(st.Table, st.Columns, st.ForeignKeys, st.Constraints); err != nil {
			return err
		}
		schema, _ := SplitSchemaName(st.Table)
		for i := range st.Indexes {
			idx := &st.Indexes[i]
			if idx.Name != "" && schema != "main" && !strings.Contains(idx.Name, ".") {
				if idx.DisplayName == "" {
					idx.DisplayName = idx.Name
				}
				idx.Name = namespaceObject(schema, idx.Name)
			}
		}
	case *InsertStmt:
		if err := resolve(&st.Table, false); err != nil {
			return err
		}
		if st.Select != nil {
			if err := s.resolveNamespaceStatement(st.Select, scope); err != nil {
				return err
			}
		}
	case *UpdateStmt:
		aliases = make(map[string]string)
		if err := relation(&st.Table, &st.Alias); err != nil {
			return err
		}
		joins = st.Joins
	case *DeleteStmt:
		aliases = make(map[string]string)
		if err := relation(&st.From, &st.Alias); err != nil {
			return err
		}
		joins = st.Joins
	case *DropTableStmt:
		return resolve(&st.Table, false)
	case *CreateIndexStmt:
		if err := resolve(&st.Table, false); err != nil {
			return err
		}
		schema, _ := SplitSchemaName(st.Table)
		if a, b, qualified := strings.Cut(st.Index, "."); qualified {
			if s.namespaceAlias(a) != schema {
				return fmt.Errorf("index must belong to table schema")
			}
			st.Index = namespaceObject(schema, b)
		} else {
			st.Index = namespaceObject(schema, st.Index)
		}
		return nil
	case *DropIndexStmt:
		if st.Table != "" {
			if err := resolve(&st.Table, false); err != nil {
				return err
			}
			return nil
		}
		return resolve(&st.Index, false)
	case *CreateViewStmt:
		if err := resolve(&st.View, true); err != nil {
			return err
		}
		if err := s.resolveNamespaceStatement(st.Select, scope); err != nil {
			return err
		}
		st.SQL = formatSelectSQL(st.Select)
		return nil
	case *DropViewStmt:
		return resolve(&st.View, false)
	case *CreateTriggerStmt:
		if err := resolve(&st.Name, true); err != nil {
			return err
		}
		if err := resolve(&st.View, false); err != nil {
			return err
		}
		for i, body := range st.Body {
			if err := s.resolveNamespaceStatement(body, scope); err != nil {
				return err
			}
			text, err := formatDMLStatement(body)
			if err != nil {
				return err
			}
			st.BodySQL[i] = text
		}
		return nil
	case *DropTriggerStmt:
		return resolve(&st.Name, false)
	case *AnalyzeStmt:
		if st.Table != "" {
			return resolve(&st.Table, false)
		}
		return nil
	case *AlterTableStmt:
		if err := resolve(&st.Table, false); err != nil {
			return err
		}
		if st.Operation == "RENAME_TO" && !strings.Contains(st.NewName, ".") {
			schema, _ := SplitSchemaName(st.Table)
			st.NewName = namespaceObject(schema, st.NewName)
		}
		for _, action := range st.Actions {
			if action.Table == "" {
				action.Table = st.Table
			}
			if err := s.resolveNamespaceStatement(action, scope); err != nil {
				return err
			}
		}
		if st.Constraint != nil {
			if st.Operation == "ADD_INDEX" && st.Constraint.Name != "" && !strings.Contains(st.Constraint.Name, ".") {
				schema, _ := SplitSchemaName(st.Table)
				st.Constraint.Name = namespaceObject(schema, st.Constraint.Name)
			}
			refs := []ConstraintDef{*st.Constraint}
			if err := references(st.Table, nil, nil, refs); err != nil {
				return err
			}
			*st.Constraint = refs[0]
		}
		return nil
	case *ShowStmt:
		if st.Database != "" {
			st.Database = s.namespaceAlias(st.Database)
			if !s.namespaces[st.Database] {
				return fmt.Errorf("unknown schema %q", st.Database)
			}
			if st.Object != "" && !strings.Contains(st.Object, ".") {
				st.Object = namespaceObject(st.Database, st.Object)
			}
		}
		if st.Object != "" {
			return resolve(&st.Object, false)
		}
		return nil
	case *ExplainStmt:
		return s.resolveNamespaceStatement(st.Stmt, scope)
	case *PragmaStmt:
		if st.Name == "table_info" || st.Name == "index_list" || st.Name == "foreign_key_list" {
			for _, arg := range st.Args {
				if literal, ok := arg.(*LiteralExpr); ok && literal.Value.Type == ColText {
					value, err := s.namespaceName(literal.Value.Text, false, scope)
					if err != nil {
						return err
					}
					literal.Value.Text = value
				}
			}
		}
		return nil
	}
	for i := range joins {
		j := &joins[i]
		if j.Table != "" {
			if err := relation(&j.Table, &j.Alias); err != nil {
				return err
			}
		}
		if j.Subquery != nil {
			if err := s.resolveNamespaceStatement(j.Subquery, scope); err != nil {
				return err
			}
		}
	}
	if update, ok := stmt.(*UpdateStmt); ok {
		for i := range update.Set {
			for table, alias := range aliases {
				if strings.Count(update.Set[i].Column, ".") >= 2 && strings.HasPrefix(update.Set[i].Column, table+".") {
					update.Set[i].Column = alias + strings.TrimPrefix(update.Set[i].Column, table)
					break
				}
			}
		}
	}
	var failure error
	namespaceExpressionVisit(reflect.ValueOf(stmt), true, func(value any) {
		if failure != nil {
			return
		}
		switch node := value.(type) {
		case *SubqueryExpr:
			failure = s.resolveNamespaceStatement(node.Stmt, scope)
		case *ExistsExpr:
			failure = s.resolveNamespaceStatement(node.Stmt, scope)
		case *ColumnRefExpr:
			for table, alias := range aliases {
				if strings.Count(node.Name, ".") >= 2 && strings.HasPrefix(node.Name, table+".") {
					node.Name = alias + strings.TrimPrefix(node.Name, table)
					break
				}
			}
		}
	})
	return failure
}

func namespaceExpressionVisit(v reflect.Value, root bool, visit func(any)) {
	if !v.IsValid() {
		return
	}
	if !root && v.CanInterface() {
		if _, statement := v.Interface().(Statement); statement {
			return
		}
	}
	switch v.Kind() {
	case reflect.Interface, reflect.Pointer:
		if v.IsNil() {
			return
		}
		if v.CanInterface() {
			visit(v.Interface())
		}
		namespaceExpressionVisit(v.Elem(), root, visit)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				namespaceExpressionVisit(v.Field(i), false, visit)
			}
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			namespaceExpressionVisit(v.Index(i), false, visit)
		}
	}
}

type namespaceSearchPathKey struct{}

// WithNamespaceSearchPath binds a prepared statement to its preparation-time
// default namespaces, without changing the connection's current USE setting.
func WithNamespaceSearchPath(ctx context.Context, path []string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, namespaceSearchPathKey{}, slices.Clone(path))
}
func (s *Schema) SessionSearchPath() []string {
	if len(s.searchPath) > 0 {
		return slices.Clone(s.searchPath)
	}
	return []string{s.CurrentSchema()}
}
