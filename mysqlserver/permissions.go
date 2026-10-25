package mysqlserver

import (
	"fmt"
	"strings"

	"slopdb/sql"
)

// SQL privilege enforcement is deliberately disabled. Authentication still
// establishes a connection identity; schemas are isolated by catalog names.
func (c *client) authorize(statement sql.Statement) error { return nil }
func (c *client) switchDatabase(name string) error {
	if err := c.session.RegisterRootSchemaAlias(c.ctx, c.server.config.Database); err != nil {
		return err
	}
	_, err := c.session.ExecContext(c.mysqlContext(), "USE "+"\""+strings.ReplaceAll(name, "\"", "\"\"")+"\"")
	if err != nil {
		return &protocolError{1049, "42000", fmt.Sprintf("unknown database %q: %v", name, err)}
	}
	c.database = c.server.database
	c.selected = name
	c.syncActivityIdentity()
	return nil
}
