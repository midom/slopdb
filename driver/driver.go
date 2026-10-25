// Package driver provides the embedded database/sql driver named "slopdb".
// Each SQL connection owns an independent native session. Path-based pools
// share one physical engine until their last connection closes.
package driver

import (
	"context"
	stdsql "database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"slopdb/db"
)

func init() { stdsql.Register("slopdb", registeredDriver) }

var registeredDriver = &Driver{}

// Driver opens native SlopDB database directories; DSNs are plain paths.
type Driver struct{}

func (*Driver) Open(name string) (sqldriver.Conn, error) {
	connector, e := registeredDriver.OpenConnector(name)
	if e != nil {
		return nil, e
	}
	return connector.Connect(context.Background())
}
func (*Driver) OpenConnector(name string) (sqldriver.Connector, error) {
	path, e := canonicalPath(name)
	if e != nil {
		return nil, e
	}
	return &Connector{path: path}, nil
}

// Connector embeds an existing Database or opens a shared directory. A
// connector created by NewConnector never closes the caller's Database.
type Connector struct {
	database *db.Database
	path     string
}

func NewConnector(database *db.Database) (*Connector, error) {
	if database == nil {
		return nil, errors.New("slopdb: nil database")
	}
	return &Connector{database: database}, nil
}

// OpenDB creates a standard pool over a caller-owned native Database.
func OpenDB(database *db.Database) (*stdsql.DB, error) {
	connector, e := NewConnector(database)
	if e != nil {
		return nil, e
	}
	return stdsql.OpenDB(connector), nil
}
func (*Connector) Driver() sqldriver.Driver { return registeredDriver }
func (c *Connector) Connect(ctx context.Context) (sqldriver.Conn, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	database := c.database
	var release func() error
	if database == nil {
		var e error
		database, release, e = acquirePath(c.path)
		if e != nil {
			return nil, e
		}
	}
	session, e := database.NewSession()
	if e != nil {
		if release != nil {
			_ = release()
		}
		return nil, e
	}
	if e = ctx.Err(); e != nil {
		_ = session.Close()
		if release != nil {
			_ = release()
		}
		return nil, e
	}
	return &conn{database: database, session: session, release: release, statements: make(map[*statement]struct{})}, nil
}

type pathDatabase struct {
	database   *db.Database
	references int
}

var paths = struct {
	sync.Mutex
	engines map[string]*pathDatabase
}{engines: make(map[string]*pathDatabase)}

func acquirePath(path string) (*db.Database, func() error, error) {
	paths.Lock()
	defer paths.Unlock()
	entry := paths.engines[path]
	if entry == nil {
		database, e := db.Open(path, nil)
		if e != nil {
			return nil, nil, e
		}
		entry = &pathDatabase{database: database}
		paths.engines[path] = entry
	}
	entry.references++
	var once sync.Once
	var closeErr error
	release := func() error {
		once.Do(func() {
			paths.Lock()
			defer paths.Unlock()
			entry.references--
			if entry.references == 0 {
				delete(paths.engines, path)
				closeErr = entry.database.Close()
			}
		})
		return closeErr
	}
	return entry.database, release, nil
}
func canonicalPath(name string) (string, error) {
	if name == "" {
		return "", errors.New("slopdb: database directory is empty")
	}
	path, e := filepath.Abs(name)
	if e != nil {
		return "", e
	}
	path = filepath.Clean(path)
	current := path
	var suffix []string
	for {
		resolved, e := filepath.EvalSymlinks(current)
		if e == nil {
			for i := len(suffix) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, suffix[i])
			}
			return resolved, nil
		}
		if !os.IsNotExist(e) {
			return "", fmt.Errorf("slopdb database path: %w", e)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", e
		}
		suffix = append(suffix, filepath.Base(current))
		current = parent
	}
}

var _ sqldriver.DriverContext = (*Driver)(nil)
var _ sqldriver.Connector = (*Connector)(nil)
