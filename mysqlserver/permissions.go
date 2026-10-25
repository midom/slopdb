package mysqlserver

import (
	"fmt"
	"reflect"
	"strings"

	"slopdb/sql"
)

func (c *client) authorize(statement sql.Statement) error {
	if c.isBootstrap() {
		return nil
	}
	private, err := c.session.TemporaryCatalog(c.ctx)
	if err != nil {
		return err
	}
	tableName := func(name string) string {
		name = strings.ToLower(name)
		if schema, table, qualified := strings.Cut(name, "."); qualified {
			if !strings.EqualFold(schema, c.selectedDatabaseName()) {
				return name
			}
			name = table
		}
		return name
	}
	isTemporary := func(table string) bool { _, exists := private.Tables[tableName(table)]; return exists }
	required := make(map[string]bool)
	require := func(table, privilege string) {
		if table != "" && isTemporary(table) {
			return
		}
		if privilege == "SELECT" && strings.HasPrefix(strings.ToLower(table), "information_schema.") {
			return
		}
		required[privilege] = true
	}
	relations := func(table, alias string, joins []sql.JoinClause) map[string]string {
		names := map[string]string{}
		if alias == "" {
			alias = table
		}
		names[alias] = table
		for _, join := range joins {
			name := join.Alias
			if name == "" {
				name = join.Table
			}
			names[name] = join.Table
		}
		return names
	}
	switch stmt := statement.(type) {
	case *sql.InsertStmt:
		require(stmt.Table, "INSERT")
		if stmt.Replace {
			require(stmt.Table, "DELETE")
		}
		if stmt.OnConflict != nil && !stmt.OnConflict.DoNothing {
			require(stmt.Table, "UPDATE")
		}
	case *sql.UpdateStmt:
		names := relations(stmt.Table, stmt.Alias, stmt.Joins)
		if len(stmt.Joins) == 0 {
			require(stmt.Table, "UPDATE")
		} else {
			for _, table := range names {
				require(table, "SELECT")
			}
			for _, set := range stmt.Set {
				alias, column, qualified := strings.Cut(set.Column, ".")
				if qualified {
					table, exists := names[alias]
					if !exists {
						required["UPDATE"] = true
					} else {
						require(table, "UPDATE")
					}
					continue
				}
				// An unqualified assignment that matches a private column either updates
				// that table or fails SQL's ambiguity check. Otherwise a persistent target
				// requires UPDATE. No hidden permanent table is selected through its shadow.
				column = alias
				temporaryTarget := false
				for _, table := range names {
					for _, name := range private.Tables[tableName(table)] {
						temporaryTarget = temporaryTarget || name == column
					}
				}
				if !temporaryTarget {
					required["UPDATE"] = true
				}
			}
		}
	case *sql.DeleteStmt:
		names := relations(stmt.From, stmt.Alias, stmt.Joins)
		if len(stmt.Joins) > 0 {
			for _, table := range names {
				require(table, "SELECT")
			}
		}
		if len(stmt.Targets) == 0 {
			require(stmt.From, "DELETE")
		} else {
			for _, target := range stmt.Targets {
				table, exists := names[target]
				if !exists {
					required["DELETE"] = true
				} else {
					require(table, "DELETE")
				}
			}
		}
	case *sql.CreateTableStmt:
		if stmt.Temporary {
			required["CREATE TEMPORARY TABLES"] = true
		} else {
			required["CREATE"] = true
		}
		if stmt.Like != "" {
			require(stmt.Like, "SELECT")
		}
	case *sql.DropTableStmt:
		require(stmt.Table, "DROP")
	case *sql.AlterTableStmt:
		var alter func(*sql.AlterTableStmt, string)
		alter = func(action *sql.AlterTableStmt, table string) {
			if action.Table != "" {
				table = action.Table
			}
			if action.ObjectKind == "VIEW" {
				required["ALTER"] = true
			} else if len(action.Actions) == 0 {
				require(table, "ALTER")
			}
			for _, child := range action.Actions {
				alter(child, table)
			}
		}
		alter(stmt, "")
	case *sql.CreateIndexStmt:
		require(stmt.Table, "INDEX")
	case *sql.DropIndexStmt:
		table := stmt.Table
		if table == "" {
			table = private.IndexTables[strings.ToLower(stmt.Index)]
		}
		require(table, "INDEX")
	case *sql.CreateViewStmt:
		required["CREATE VIEW"] = true
		required["SELECT"] = true
	case *sql.DropViewStmt:
		required["DROP"] = true
	case *sql.CreateTriggerStmt, *sql.DropTriggerStmt:
		for _, p := range []string{"TRIGGER", "SELECT", "INSERT", "UPDATE", "DELETE"} {
			required[p] = true
		}
	}
	var walk func(reflect.Value)
	walk = func(v reflect.Value) {
		if !v.IsValid() {
			return
		}
		if v.Kind() == reflect.Interface {
			if !v.IsNil() {
				walk(v.Elem())
			}
			return
		}
		if v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return
			}
			if selectStmt, ok := v.Interface().(*sql.SelectStmt); ok && selectStmt.From != "" {
				require(selectStmt.From, "SELECT")
			}
			walk(v.Elem())
			return
		}
		switch v.Kind() {
		case reflect.Struct:
			if join, ok := v.Interface().(sql.JoinClause); ok && join.Table != "" {
				require(join.Table, "SELECT")
			}
			for i := 0; i < v.NumField(); i++ {
				if v.Type().Field(i).IsExported() {
					walk(v.Field(i))
				}
			}
		case reflect.Slice:
			for i := 0; i < v.Len(); i++ {
				walk(v.Index(i))
			}
		}
	}
	walk(reflect.ValueOf(statement))
	for privilege := range required {
		if !c.hasPrivilege(c.selectedDatabaseName(), privilege, false) {
			return accessDenied(privilege)
		}
	}
	return nil
}

func (c *client) switchDatabase(name string) error {
	if err := c.server.refreshAccounts(c.ctx); err != nil {
		return err
	}
	a := &c.server.accounts
	a.mu.Lock()
	database := a.databases[name]
	actual := name
	if database == nil {
		for n, d := range a.databases {
			if strings.EqualFold(n, name) {
				database = d
				actual = n
				break
			}
		}
	}
	a.mu.Unlock()
	if database == nil {
		return &protocolError{1049, "42000", fmt.Sprintf("unknown database %q", name)}
	}
	allowed := c.isBootstrap()
	if !allowed {
		for _, schema := range c.availableSchemas() {
			allowed = allowed || schema == actual
		}
	}
	if !allowed {
		return &protocolError{1044, "42000", "access denied for database"}
	}
	if database == c.currentDatabase() {
		c.selected = actual
		return nil
	}
	if c.session != nil && c.session.InTransaction() {
		return &protocolError{1235, "42000", "switching databases inside a transaction is not supported"}
	}
	session, err := database.NewSession()
	if err != nil {
		return err
	}
	if err = enableMySQLSession(session); err != nil {
		session.Close()
		return err
	}
	c.closeStatements()
	if c.session != nil {
		c.session.Close()
	}
	c.session = session
	c.database = database
	c.selected = actual
	return nil
}
