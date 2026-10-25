package mysqlserver

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"

	"slopdb/db"
	"slopdb/sql"
)

type mysqlAccount struct {
	User         string `json:"user"`
	Host         string `json:"host"`
	PasswordHash string `json:"password_hash"`
	// Grants map a database name (or *) to privilege names and grant option.
	Grants map[string]map[string]bool `json:"grants"`
}
type administrativeState struct {
	Version   int                     `json:"version"`
	Databases []string                `json:"databases"`
	Accounts  map[string]mysqlAccount `json:"accounts"`
}
type serverAccounts struct {
	mu        sync.Mutex
	writeMu   sync.Mutex
	state     administrativeState
	persisted []byte
	databases map[string]*db.Database
}

var mysqlPrivileges = []string{"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "INDEX", "ALTER", "CREATE TEMPORARY TABLES", "CREATE VIEW", "SHOW VIEW", "TRIGGER", "REFERENCES", "CREATE USER"}

func (s *Server) initAccounts() error {
	data, err := s.database.MySQLServerState()
	if err != nil {
		return err
	}
	a := &s.accounts
	a.persisted = append([]byte(nil), data...)
	a.state = administrativeState{Version: 1, Accounts: make(map[string]mysqlAccount)}
	a.databases = map[string]*db.Database{s.config.Database: s.database}
	if len(data) > 0 {
		if err = json.Unmarshal(data, &a.state); err != nil {
			return fmt.Errorf("MySQL account metadata: %w", err)
		}
		if a.state.Version != 1 {
			return fmt.Errorf("unsupported MySQL account metadata version")
		}
		if a.state.Accounts == nil {
			a.state.Accounts = make(map[string]mysqlAccount)
		}
	}
	for _, name := range a.state.Databases {
		if name == s.config.Database {
			continue
		}
		database, e := s.database.OpenMySQLDatabase(context.Background(), name, false)
		if e != nil {
			for n, d := range a.databases {
				if n != s.config.Database {
					d.Close()
				}
			}
			return fmt.Errorf("open MySQL database %s: %w", name, e)
		}
		a.databases[name] = database
	}
	return nil
}
func (s *Server) saveAccountsLocked(ctx context.Context, next administrativeState) error {
	data, err := json.Marshal(next)
	if err != nil {
		return err
	}
	// SQL catalog readers can consult accounts while holding the execution gate.
	// Never hold the account mutex while waiting for that gate. writeMu serializes
	// administration handlers across this persistence/publication interval.
	s.accounts.mu.Unlock()
	err = s.database.SetMySQLServerState(ctx, data)
	s.accounts.mu.Lock()
	if err != nil {
		return err
	}
	s.accounts.state = next
	s.accounts.persisted = append([]byte(nil), data...)
	return nil
}

// Refresh before SQL starts, never from a virtual-table callback while a
// session already holds the execution gate. Replication applies administration
// records through the same LSM hook as ordinary writes, including after failover.
func (s *Server) refreshAccounts(ctx context.Context) error {
	a := &s.accounts
	for {
		a.mu.Lock()
		previous := append([]byte(nil), a.persisted...)
		a.mu.Unlock()
		data, err := s.database.MySQLServerStateContext(ctx)
		if err != nil {
			return err
		}
		a.mu.Lock()
		if !bytes.Equal(previous, a.persisted) {
			a.mu.Unlock()
			continue
		}
		if bytes.Equal(data, a.persisted) {
			a.mu.Unlock()
			return nil
		}
		next := administrativeState{Version: 1, Accounts: make(map[string]mysqlAccount)}
		if len(data) > 0 {
			if err = json.Unmarshal(data, &next); err != nil {
				a.mu.Unlock()
				return fmt.Errorf("MySQL account metadata: %w", err)
			}
			if next.Version != 1 {
				a.mu.Unlock()
				return fmt.Errorf("unsupported MySQL account metadata version")
			}
		}
		if next.Accounts == nil {
			next.Accounts = make(map[string]mysqlAccount)
		}
		// Additional database directories are owned/opened by this Server and are
		// standalone-only; refreshing replicated metadata changes credentials/grants.
		a.state.Accounts = next.Accounts
		a.persisted = append([]byte(nil), data...)
		a.mu.Unlock()
		return nil
	}
}

func cloneAdministrativeState(state administrativeState) administrativeState {
	next := administrativeState{Version: 1, Databases: append([]string(nil), state.Databases...), Accounts: make(map[string]mysqlAccount, len(state.Accounts))}
	for key, a := range state.Accounts {
		copy := a
		copy.Grants = make(map[string]map[string]bool)
		for database, privileges := range a.Grants {
			m := make(map[string]bool, len(privileges))
			for p, yes := range privileges {
				m[p] = yes
			}
			copy.Grants[database] = m
		}
		next.Accounts[key] = copy
	}
	return next
}
func accountKey(user, host string) string { return user + "\x00" + host }
func passwordHash(password string) string {
	if password == "" {
		return ""
	}
	first := sha1.Sum([]byte(password))
	second := sha1.Sum(first[:])
	return hex.EncodeToString(second[:])
}
func checkPasswordHash(encoded string, salt, response []byte) bool {
	if encoded == "" {
		return len(response) == 0
	}
	hash, err := hex.DecodeString(encoded)
	if err != nil || len(hash) != sha1.Size || len(response) != sha1.Size {
		return false
	}
	h := sha1.New()
	h.Write(salt)
	h.Write(hash)
	scramble := h.Sum(nil)
	for i := range scramble {
		scramble[i] ^= response[i]
	}
	stage2 := sha1.Sum(scramble)
	return subtle.ConstantTimeCompare(hash, stage2[:]) == 1
}
func clientHost(conn net.Conn) string {
	if conn.LocalAddr().Network() == "unix" {
		return "localhost"
	}
	host, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		return conn.RemoteAddr().String()
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return "localhost"
	}
	return host
}
func (s *Server) authenticate(user, host string, salt, response []byte) (string, bool) {
	if user == s.config.Username {
		return accountKey(user, "%"), checkPassword(s.config.Password, salt, response)
	}
	s.accounts.mu.Lock()
	defer s.accounts.mu.Unlock()
	for _, h := range []string{host, "%"} {
		key := accountKey(user, h)
		if a, ok := s.accounts.state.Accounts[key]; ok {
			return key, checkPasswordHash(a.PasswordHash, salt, response)
		}
	}
	return "", false
}
func (c *client) isBootstrap() bool {
	return c.account == accountKey(c.server.config.Username, "%") || c.account == "" && c.loginName() == c.server.config.Username
}
func (c *client) hasPrivilege(database, privilege string, grant bool) bool {
	if c.isBootstrap() {
		return true
	}
	a := &c.server.accounts
	a.mu.Lock()
	defer a.mu.Unlock()
	account, ok := a.state.Accounts[c.account]
	if !ok {
		return false
	}
	for _, scope := range []string{"*", strings.ToLower(database)} {
		p := account.Grants[scope]
		if p[privilege] && (!grant || p["GRANT OPTION"]) {
			return true
		}
	}
	return false
}
func (c *client) availableSchemas() []string {
	a := &c.server.accounts
	a.mu.Lock()
	defer a.mu.Unlock()
	var names []string
	for name := range a.databases {
		if c.isBootstrap() {
			names = append(names, name)
			continue
		}
		account := a.state.Accounts[c.account]
		if len(account.Grants[strings.ToLower(name)]) > 0 || len(account.Grants["*"]) > 0 {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}
func (c *client) currentDatabase() *db.Database {
	if c.database != nil {
		return c.database
	}
	return c.server.database
}

func (c *client) accountTables() map[string]*sql.ResultSet {
	user := &sql.ResultSet{Columns: []sql.ColumnDef{{Name: "GRANTEE", ColType: sql.ColText}, {Name: "TABLE_CATALOG", ColType: sql.ColText}, {Name: "PRIVILEGE_TYPE", ColType: sql.ColText}, {Name: "IS_GRANTABLE", ColType: sql.ColText}}}
	schema := &sql.ResultSet{Columns: []sql.ColumnDef{{Name: "GRANTEE", ColType: sql.ColText}, {Name: "TABLE_CATALOG", ColType: sql.ColText}, {Name: "TABLE_SCHEMA", ColType: sql.ColText}, {Name: "PRIVILEGE_TYPE", ColType: sql.ColText}, {Name: "IS_GRANTABLE", ColType: sql.ColText}}}
	a := &c.server.accounts
	a.mu.Lock()
	defer a.mu.Unlock()
	accounts := make([]mysqlAccount, 0, len(a.state.Accounts)+1)
	grants := map[string]bool{"GRANT OPTION": true}
	for _, p := range mysqlPrivileges {
		grants[p] = true
	}
	accounts = append(accounts, mysqlAccount{User: c.server.config.Username, Host: "%", Grants: map[string]map[string]bool{"*": grants}})
	for _, v := range a.state.Accounts {
		accounts = append(accounts, v)
	}
	for _, account := range accounts {
		if !c.isBootstrap() && accountKey(account.User, account.Host) != c.account {
			continue
		}
		grantee := "'" + strings.ReplaceAll(account.User, "'", "''") + "'@'" + strings.ReplaceAll(account.Host, "'", "''") + "'"
		for database, privs := range account.Grants {
			for p, yes := range privs {
				if !yes || p == "GRANT OPTION" {
					continue
				}
				row := map[string]sql.Value{"GRANTEE": sql.NewTextValue(grantee), "TABLE_CATALOG": sql.NewTextValue("def"), "PRIVILEGE_TYPE": sql.NewTextValue(p), "IS_GRANTABLE": sql.NewTextValue("NO")}
				if privs["GRANT OPTION"] {
					row["IS_GRANTABLE"] = sql.NewTextValue("YES")
				}
				if database == "*" {
					user.Rows = append(user.Rows, row)
				} else {
					row["TABLE_SCHEMA"] = sql.NewTextValue(database)
					schema.Rows = append(schema.Rows, row)
				}
			}
		}
	}
	return map[string]*sql.ResultSet{"information_schema.user_privileges": user, "information_schema.schema_privileges": schema}
}

func (c *client) accountTable(name string) (*sql.ResultSet, error) {
	if name == "mysql.user" {
		if !c.isBootstrap() {
			return nil, accessDenied("SELECT mysql.user")
		}
		result := &sql.ResultSet{Columns: []sql.ColumnDef{{Name: "Host", ColType: sql.ColText}, {Name: "User", ColType: sql.ColText}}}
		result.Rows = append(result.Rows, map[string]sql.Value{"Host": sql.NewTextValue("%"), "User": sql.NewTextValue(c.server.config.Username)})
		a := &c.server.accounts
		a.mu.Lock()
		defer a.mu.Unlock()
		for _, account := range a.state.Accounts {
			result.Rows = append(result.Rows, map[string]sql.Value{"Host": sql.NewTextValue(account.Host), "User": sql.NewTextValue(account.User)})
		}
		return result, nil
	}
	if name != "information_schema.user_privileges" && name != "information_schema.schema_privileges" {
		return nil, nil
	}
	return c.accountTables()[name], nil
}
