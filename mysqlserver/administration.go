package mysqlserver

import (
	"fmt"
	"strings"

	"slopdb/db"
	"slopdb/sql"
)

func tokenWord(tokens []sql.Token, i int, word string) bool {
	return i < len(tokens) && strings.EqualFold(tokens[i].Value, word)
}
func accountTokens(tokens []sql.Token, i int) (string, string, int, error) {
	if i >= len(tokens) || tokens[i].Type != sql.TokString && tokens[i].Type != sql.TokIdent && tokens[i].Type != sql.TokKeyword {
		return "", "", i, fmt.Errorf("expected account name")
	}
	user := tokens[i].Value
	i++
	host := "%"
	if i < len(tokens) && tokens[i].Value == "@" {
		i++
		if i >= len(tokens) || tokens[i].Type != sql.TokString && tokens[i].Type != sql.TokIdent {
			return "", "", i, fmt.Errorf("expected account host")
		}
		host = tokens[i].Value
		i++
	}
	if user == "" || len(user) > 128 || host == "" || len(host) > 255 || strings.ContainsRune(user, 0) || strings.ContainsRune(host, 0) {
		return "", "", i, fmt.Errorf("invalid account name or host")
	}
	return user, host, i, nil
}
func (c *client) adminCommit() error {
	if c.session.InTransaction() {
		_, err := c.session.ExecContext(c.mysqlContext(), "COMMIT")
		return err
	}
	return nil
}
func accessDenied(privilege string) error {
	return &protocolError{1142, "42000", fmt.Sprintf("%s command denied", privilege)}
}

func (c *client) administration(query string) (*db.ExecResult, bool, error) {
	first := sql.NewTokenizerWithOptions(query, c.currentDatabase().ParseOptions()).Next()
	kind := strings.ToUpper(first.Value)
	if kind != "CREATE" && kind != "ALTER" && kind != "DROP" && kind != "GRANT" && kind != "REVOKE" && kind != "SHOW" {
		return nil, false, nil
	}
	tokens := sqlTokens(query, c.currentDatabase().ParseOptions())
	for _, t := range tokens {
		if t.Type == sql.TokSemicolon {
			return nil, false, nil
		}
	}
	if len(tokens) < 2 {
		return nil, false, nil
	}
	if kind == "CREATE" && tokenWord(tokens, 1, "DATABASE") {
		return c.createDatabase(tokens)
	}
	if kind == "SHOW" && tokenWord(tokens, 1, "DATABASES") {
		return c.showDatabases(tokens)
	}
	if kind == "SHOW" && tokenWord(tokens, 1, "GRANTS") {
		return c.showGrants(tokens)
	}
	if (kind == "CREATE" || kind == "ALTER" || kind == "DROP") && tokenWord(tokens, 1, "USER") {
		return c.accountCommand(kind, tokens)
	}
	if kind == "GRANT" || kind == "REVOKE" {
		return c.grantCommand(kind, tokens)
	}
	return nil, false, nil
}

func (c *client) accountCommand(kind string, tokens []sql.Token) (*db.ExecResult, bool, error) {
	if !c.hasPrivilege("*", "CREATE USER", false) {
		return nil, true, accessDenied("CREATE USER")
	}
	i := 2
	conditional := false
	if tokenWord(tokens, i, "IF") {
		i++
		if kind == "CREATE" {
			if !tokenWord(tokens, i, "NOT") {
				return nil, true, fmt.Errorf("expected NOT EXISTS")
			}
			i++
		}
		if !tokenWord(tokens, i, "EXISTS") {
			return nil, true, fmt.Errorf("expected EXISTS")
		}
		i++
		conditional = true
	}
	type change struct{ user, host, hash string }
	var changes []change
	for i < len(tokens) {
		user, host, next, err := accountTokens(tokens, i)
		if err != nil {
			return nil, true, err
		}
		i = next
		if user == c.server.config.Username {
			return nil, true, fmt.Errorf("the configured bootstrap account is managed by server configuration")
		}
		hash := ""
		if kind != "DROP" {
			if tokenWord(tokens, i, "IDENTIFIED") {
				i++
				if tokenWord(tokens, i, "WITH") {
					i++
					if !tokenWord(tokens, i, authPlugin) {
						return nil, true, fmt.Errorf("only mysql_native_password accounts are supported")
					}
					i++
				}
				if tokenWord(tokens, i, "BY") {
					i++
					if i >= len(tokens) || tokens[i].Type != sql.TokString {
						return nil, true, fmt.Errorf("expected password string")
					}
					hash = passwordHash(tokens[i].Value)
					i++
				} else {
					return nil, true, fmt.Errorf("expected IDENTIFIED BY password")
				}
			} else if kind == "ALTER" {
				return nil, true, fmt.Errorf("ALTER USER requires IDENTIFIED BY")
			}
		}
		changes = append(changes, change{user, host, hash})
		if i == len(tokens) {
			break
		}
		if tokens[i].Type != sql.TokComma {
			return nil, true, fmt.Errorf("unsupported account clause %q", tokens[i].Value)
		}
		i++
		if i == len(tokens) {
			return nil, true, fmt.Errorf("expected account after comma")
		}
	}
	if len(changes) == 0 {
		return nil, true, fmt.Errorf("expected account")
	}
	if err := c.adminCommit(); err != nil {
		return nil, true, err
	}
	a := &c.server.accounts
	a.mu.Lock()
	defer a.mu.Unlock()
	next := cloneAdministrativeState(a.state)
	for _, change := range changes {
		key := accountKey(change.user, change.host)
		account, exists := next.Accounts[key]
		switch kind {
		case "CREATE":
			if exists {
				if conditional {
					continue
				}
				return nil, true, &protocolError{1396, "HY000", "account already exists"}
			}
			account = mysqlAccount{User: change.user, Host: change.host, PasswordHash: change.hash, Grants: make(map[string]map[string]bool)}
			next.Accounts[key] = account
		case "ALTER":
			if !exists {
				return nil, true, &protocolError{1396, "HY000", "account does not exist"}
			}
			account.PasswordHash = change.hash
			next.Accounts[key] = account
		case "DROP":
			if !exists && !conditional {
				return nil, true, &protocolError{1396, "HY000", "account does not exist"}
			}
			delete(next.Accounts, key)
		}
	}
	return nil, true, c.server.saveAccountsLocked(c.ctx, next)
}

func (c *client) grantCommand(kind string, tokens []sql.Token) (*db.ExecResult, bool, error) {
	on := -1
	for i := 1; i < len(tokens); i++ {
		if tokenWord(tokens, i, "ON") {
			on = i
			break
		}
	}
	if on < 0 {
		return nil, true, fmt.Errorf("expected ON in privilege statement")
	}
	var privs []string
	start := 1
	for start < on {
		end := start
		for end < on && tokens[end].Type != sql.TokComma {
			end++
		}
		var words []string
		for _, t := range tokens[start:end] {
			words = append(words, strings.ToUpper(t.Value))
		}
		name := strings.Join(words, " ")
		if name == "ALL" || name == "ALL PRIVILEGES" {
			privs = append(privs, mysqlPrivileges...)
		} else if name == "USAGE" {
		} else {
			valid := name == "GRANT OPTION"
			for _, p := range mysqlPrivileges {
				valid = valid || p == name
			}
			if !valid {
				return nil, true, fmt.Errorf("unsupported privilege %q", name)
			}
			privs = append(privs, name)
		}
		start = end + 1
	}
	i := on + 1
	if i+2 >= len(tokens) || tokens[i+1].Type != sql.TokDot || tokens[i+2].Type != sql.TokStar {
		return nil, true, fmt.Errorf("privileges require database.* or *.* scope")
	}
	scope := strings.ToLower(tokens[i].Value)
	i += 3
	if !tokenWord(tokens, i, map[string]string{"GRANT": "TO", "REVOKE": "FROM"}[kind]) {
		return nil, true, fmt.Errorf("expected account target")
	}
	i++
	var targets []string
	for {
		user, host, next, err := accountTokens(tokens, i)
		if err != nil {
			return nil, true, err
		}
		i = next
		targets = append(targets, accountKey(user, host))
		if i < len(tokens) && tokens[i].Type == sql.TokComma {
			i++
			continue
		}
		break
	}
	grantOption := false
	if i < len(tokens) {
		if kind != "GRANT" || !tokenWord(tokens, i, "WITH") || !tokenWord(tokens, i+1, "GRANT") || !tokenWord(tokens, i+2, "OPTION") || i+3 != len(tokens) {
			return nil, true, fmt.Errorf("unsupported grant clause")
		}
		grantOption = true
		i += 3
	}
	for _, priv := range privs {
		if !c.hasPrivilege(scope, priv, true) {
			return nil, true, accessDenied("GRANT " + priv)
		}
	}
	if grantOption && !c.isBootstrap() && !c.hasPrivilege(scope, "CREATE", true) {
		return nil, true, accessDenied("GRANT OPTION")
	}
	if err := c.adminCommit(); err != nil {
		return nil, true, err
	}
	a := &c.server.accounts
	a.mu.Lock()
	defer a.mu.Unlock()
	next := cloneAdministrativeState(a.state)
	for _, key := range targets {
		account, ok := next.Accounts[key]
		if !ok {
			return nil, true, &protocolError{1410, "42000", "create the account before granting privileges"}
		}
		if account.Grants[scope] == nil {
			account.Grants[scope] = make(map[string]bool)
		}
		for _, priv := range privs {
			if kind == "GRANT" {
				account.Grants[scope][priv] = true
			} else {
				delete(account.Grants[scope], priv)
			}
		}
		if grantOption {
			account.Grants[scope]["GRANT OPTION"] = true
		}
		next.Accounts[key] = account
	}
	return nil, true, c.server.saveAccountsLocked(c.ctx, next)
}

func (c *client) showDatabases(tokens []sql.Token) (*db.ExecResult, bool, error) {
	pattern := "%"
	if len(tokens) != 2 {
		if len(tokens) != 4 || !tokenWord(tokens, 2, "LIKE") || tokens[3].Type != sql.TokString {
			return nil, true, fmt.Errorf("SHOW DATABASES supports LIKE string")
		}
		pattern = tokens[3].Value
	}
	r := &db.ExecResult{Columns: []sql.ColumnDef{{Name: "Database", ColType: sql.ColText}}}
	for _, name := range c.availableSchemas() {
		if variableLike(name, pattern) {
			r.Rows = append(r.Rows, map[string]sql.Value{"Database": sql.NewTextValue(name)})
		}
	}
	return r, true, nil
}
func (c *client) createDatabase(tokens []sql.Token) (*db.ExecResult, bool, error) {
	i := 2
	conditional := false
	if tokenWord(tokens, i, "IF") {
		if !tokenWord(tokens, i+1, "NOT") || !tokenWord(tokens, i+2, "EXISTS") {
			return nil, true, fmt.Errorf("expected IF NOT EXISTS")
		}
		i += 3
		conditional = true
	}
	if i >= len(tokens) || tokens[i].Type != sql.TokIdent && tokens[i].Type != sql.TokKeyword {
		return nil, true, fmt.Errorf("expected database name")
	}
	name := tokens[i].Value
	i++
	if name == "" || len(name) > 64 || strings.ContainsRune(name, 0) || strings.EqualFold(name, "information_schema") || strings.EqualFold(name, "mysql") {
		return nil, true, fmt.Errorf("invalid or reserved database name")
	}
	// Binary UTF-8 options match the native server's default comparisons.
	for i < len(tokens) {
		if tokenWord(tokens, i, "DEFAULT") {
			i++
		}
		if tokenWord(tokens, i, "CHARACTER") {
			i++
			if !tokenWord(tokens, i, "SET") {
				return nil, true, fmt.Errorf("expected CHARACTER SET")
			}
			i++
		} else if tokenWord(tokens, i, "CHARSET") || tokenWord(tokens, i, "COLLATE") {
			i++
		} else {
			return nil, true, fmt.Errorf("unsupported database option")
		}
		if i < len(tokens) && tokens[i].Type == sql.TokEq {
			i++
		}
		if i >= len(tokens) {
			return nil, true, fmt.Errorf("missing database option value")
		}
		switch strings.ToLower(tokens[i].Value) {
		case "binary", "utf8", "utf8mb3", "utf8mb4", "utf8mb4_bin", "utf8_bin":
		default:
			return nil, true, fmt.Errorf("unsupported database charset/collation")
		}
		i++
	}
	if !c.hasPrivilege(name, "CREATE", false) {
		return nil, true, accessDenied("CREATE DATABASE")
	}
	if err := c.adminCommit(); err != nil {
		return nil, true, err
	}
	a := &c.server.accounts
	a.mu.Lock()
	defer a.mu.Unlock()
	exists := false
	for current := range a.databases {
		exists = exists || strings.EqualFold(current, name)
	}
	if exists {
		if conditional {
			return nil, true, nil
		}
		return nil, true, &protocolError{1007, "HY000", "database already exists"}
	}
	database, err := c.server.database.OpenMySQLDatabase(c.ctx, name, true)
	if err != nil {
		return nil, true, err
	}
	next := cloneAdministrativeState(a.state)
	next.Databases = append(next.Databases, name)
	if err = c.server.saveAccountsLocked(c.ctx, next); err != nil {
		database.Close()
		return nil, true, err
	}
	a.databases[name] = database
	return &db.ExecResult{Changes: 1}, true, nil
}

func (c *client) showGrants(tokens []sql.Token) (*db.ExecResult, bool, error) {
	key := c.account
	if len(tokens) > 2 {
		if !tokenWord(tokens, 2, "FOR") {
			return nil, true, fmt.Errorf("expected FOR account")
		}
		user, host, next, err := accountTokens(tokens, 3)
		if err != nil || next != len(tokens) {
			return nil, true, fmt.Errorf("invalid SHOW GRANTS account")
		}
		key = accountKey(user, host)
	}
	if !c.isBootstrap() && key != c.account {
		return nil, true, accessDenied("SHOW GRANTS")
	}
	a := &c.server.accounts
	a.mu.Lock()
	defer a.mu.Unlock()
	account, exists := a.state.Accounts[key]
	if key == accountKey(c.server.config.Username, "%") {
		grants := map[string]bool{"GRANT OPTION": true}
		for _, p := range mysqlPrivileges {
			grants[p] = true
		}
		account = mysqlAccount{User: c.server.config.Username, Host: "%", Grants: map[string]map[string]bool{"*": grants}}
		exists = true
	}
	if !exists {
		return nil, true, &protocolError{1141, "42000", "account has no grants"}
	}
	name := "Grants for " + account.User + "@" + account.Host
	r := &db.ExecResult{Columns: []sql.ColumnDef{{Name: name, ColType: sql.ColText}}}
	for scope, privs := range account.Grants {
		var granted []string
		for _, priv := range mysqlPrivileges {
			if privs[priv] {
				granted = append(granted, priv)
			}
		}
		if len(granted) == 0 {
			continue
		}
		database := "`" + strings.ReplaceAll(scope, "`", "``") + "`"
		if scope == "*" {
			database = "*"
		}
		text := "GRANT " + strings.Join(granted, ", ") + " ON " + database + ".* TO '" + strings.ReplaceAll(account.User, "'", "''") + "'@'" + strings.ReplaceAll(account.Host, "'", "''") + "'"
		if privs["GRANT OPTION"] {
			text += " WITH GRANT OPTION"
		}
		r.Rows = append(r.Rows, map[string]sql.Value{name: sql.NewTextValue(text)})
	}
	if len(r.Rows) == 0 {
		r.Rows = append(r.Rows, map[string]sql.Value{name: sql.NewTextValue("GRANT USAGE ON *.* TO '" + account.User + "'@'" + account.Host + "'")})
	}
	return r, true, nil
}
