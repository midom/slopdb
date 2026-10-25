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
	required := make(map[string]bool)
	switch stmt := statement.(type) {
	case *sql.InsertStmt:
		required["INSERT"] = true
		if stmt.Replace {
			required["DELETE"] = true
		}
		if stmt.OnConflict != nil && !stmt.OnConflict.DoNothing {
			required["UPDATE"] = true
		}
	case *sql.UpdateStmt:
		required["UPDATE"] = true
	case *sql.DeleteStmt:
		required["DELETE"] = true
	case *sql.CreateTableStmt:
		if stmt.Temporary {
			required["CREATE TEMPORARY TABLES"] = true
		} else {
			required["CREATE"] = true
		}
		if stmt.Like != "" {
			required["SELECT"] = true
		}
	case *sql.DropTableStmt:
		required["DROP"] = true
	case *sql.AlterTableStmt:
		required["ALTER"] = true
	case *sql.CreateIndexStmt, *sql.DropIndexStmt:
		required["INDEX"] = true
	case *sql.CreateViewStmt:
		required["CREATE VIEW"] = true
		required["SELECT"] = true
	case *sql.DropViewStmt:
		required["DROP"] = true
	case *sql.CreateTriggerStmt, *sql.DropTriggerStmt:
		// Database-level grants intentionally require the trigger definer to
		// have all ordinary data privileges; we do not fabricate definer ACLs.
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
			if selectStmt, ok := v.Interface().(*sql.SelectStmt); ok && selectStmt.From != "" && !strings.HasPrefix(strings.ToLower(selectStmt.From), "information_schema.") {
				required["SELECT"] = true
			}
			walk(v.Elem())
			return
		}
		switch v.Kind() {
		case reflect.Struct:
			if join, ok := v.Interface().(sql.JoinClause); ok && join.Table != "" && !strings.HasPrefix(strings.ToLower(join.Table), "information_schema.") {
				required["SELECT"] = true
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
