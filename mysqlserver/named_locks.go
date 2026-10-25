package mysqlserver

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"slopdb/sql"
)

type namedLock struct {
	owner uint32
	count uint64
}
type namedLocks struct {
	mu      sync.Mutex
	held    map[string]namedLock
	changed chan struct{}
}

func (l *namedLocks) init() {
	if l.held == nil {
		l.held = make(map[string]namedLock)
		l.changed = make(chan struct{})
	}
}
func (l *namedLocks) notify() { close(l.changed); l.changed = make(chan struct{}) }
func (l *namedLocks) releaseOwner(owner uint32) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.init()
	changed := false
	var released uint64
	for name, lock := range l.held {
		if lock.owner == owner {
			released += lock.count
			delete(l.held, name)
			changed = true
		}
	}
	if changed {
		l.notify()
	}
	return released
}

func (l *namedLocks) call(ctx context.Context, owner uint32, operation string, args []sql.Value) (sql.Value, error) {
	if len(args) == 0 || args[0].Null {
		return sql.NewNullValue(), nil
	}
	if args[0].Type != sql.ColText || len(args[0].Text) == 0 || len(args[0].Text) > 64 {
		return sql.NewNullValue(), fmt.Errorf("lock name must contain 1 to 64 bytes")
	}
	name := args[0].Text
	var timer *time.Timer
	var timeout <-chan time.Time
	if operation == "GET_LOCK" {
		if len(args) != 2 || args[1].Null {
			return sql.NewNullValue(), nil
		}
		var seconds float64
		switch args[1].Type {
		case sql.ColInteger:
			seconds = float64(args[1].Int)
		case sql.ColUnsigned:
			seconds = float64(args[1].Uint)
		case sql.ColDecimal:
			var err error
			seconds, err = strconv.ParseFloat(args[1].Text, 64)
			if err != nil {
				return sql.NewNullValue(), fmt.Errorf("invalid GET_LOCK timeout")
			}
		case sql.ColReal:
			seconds = args[1].Real
		default:
			return sql.NewNullValue(), fmt.Errorf("GET_LOCK timeout must be numeric")
		}
		if math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds < 0 || seconds > float64(math.MaxInt64)/float64(time.Second) {
			return sql.NewNullValue(), fmt.Errorf("invalid GET_LOCK timeout")
		}
		timer = time.NewTimer(time.Duration(seconds * float64(time.Second)))
		defer timer.Stop()
		timeout = timer.C
	}
	for {
		if err := ctx.Err(); err != nil {
			return sql.NewNullValue(), err
		}
		l.mu.Lock()
		l.init()
		lock, exists := l.held[name]
		switch operation {
		case "IS_FREE_LOCK":
			l.mu.Unlock()
			if exists {
				return sql.NewIntValue(0), nil
			}
			return sql.NewIntValue(1), nil
		case "IS_USED_LOCK":
			l.mu.Unlock()
			if !exists {
				return sql.NewNullValue(), nil
			}
			return sql.NewIntValue(int64(lock.owner)), nil
		case "RELEASE_LOCK":
			if !exists {
				l.mu.Unlock()
				return sql.NewNullValue(), nil
			}
			if lock.owner != owner {
				l.mu.Unlock()
				return sql.NewIntValue(0), nil
			}
			lock.count--
			if lock.count == 0 {
				delete(l.held, name)
				l.notify()
			} else {
				l.held[name] = lock
			}
			l.mu.Unlock()
			return sql.NewIntValue(1), nil
		case "GET_LOCK":
			if !exists || lock.owner == owner {
				lock.owner = owner
				lock.count++
				l.held[name] = lock
				l.mu.Unlock()
				return sql.NewIntValue(1), nil
			}
			changed := l.changed
			l.mu.Unlock()
			select {
			case <-ctx.Done():
				return sql.NewNullValue(), ctx.Err()
			case <-timeout:
				return sql.NewIntValue(0), nil
			case <-changed:
			}
		default:
			l.mu.Unlock()
			return sql.NewNullValue(), fmt.Errorf("unknown named lock operation %s", operation)
		}
	}
}

func (c *client) mysqlFunction(ctx context.Context, name string, args []sql.Value) (sql.Value, bool, error) {
	switch strings.ToUpper(name) {
	case "RELEASE_ALL_LOCKS":
		return sql.NewUnsignedValue(c.server.namedLocks.releaseOwner(c.id)), true, nil
	case "GET_LOCK", "IS_FREE_LOCK", "IS_USED_LOCK", "RELEASE_LOCK":
		v, err := c.server.namedLocks.call(ctx, c.id, strings.ToUpper(name), args)
		return v, true, err
	case "CURRENT_USER":
		host := "%"
		if fields := strings.SplitN(c.account, "\x00", 2); len(fields) == 2 {
			host = fields[1]
		}
		return sql.NewTextValue(c.loginName() + "@" + host), true, nil
	case "USER":
		return sql.NewTextValue(c.loginName() + "@" + clientHost(c.conn)), true, nil
	case "DATABASE", "SCHEMA":
		if c.selected == "" {
			return sql.NewNullValue(), true, nil
		}
		return sql.NewTextValue(c.selected), true, nil
	case "CONNECTION_ID":
		return sql.NewIntValue(int64(c.id)), true, nil
	case "VERSION":
		return sql.NewTextValue(serverVersion), true, nil
	}
	return sql.Value{}, false, nil
}

func (c *client) loginName() string {
	if c.username != "" {
		return c.username
	}
	return c.server.config.Username
}
