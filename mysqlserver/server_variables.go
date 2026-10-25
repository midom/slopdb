package mysqlserver

import (
	"encoding/hex"
	"encoding/json"
	"hash/crc32"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"slopdb/db"
	"slopdb/sql"
)

type stateObservation struct {
	state     *db.ServerState
	raft      *db.RaftState
	variables map[string]string
}

func (c *client) observeRaft() (*db.RaftState, error) {
	if c.observation != nil {
		if c.observation.state != nil {
			return &c.observation.state.Raft, nil
		}
		if c.observation.raft != nil {
			return c.observation.raft, nil
		}
	}
	state, e := c.server.database.RaftState(c.ctx)
	if e != nil {
		return nil, e
	}
	if c.observation != nil {
		c.observation.raft = &state
	}
	return &state, nil
}

func (c *client) observeServer() (*db.ServerState, error) {
	if c.observation != nil && c.observation.state != nil {
		return c.observation.state, nil
	}
	v, e := c.server.database.ServerState(c.ctx)
	if e != nil {
		return nil, e
	}
	if c.observation != nil {
		if c.observation.raft != nil {
			v.Raft = *c.observation.raft
			v.ReadOnly = !v.Raft.Writable
		}
		c.observation.state = &v
	}
	return &v, nil
}

func (c *client) observedVariables() (map[string]string, error) {
	if c.observation != nil && c.observation.variables != nil {
		return c.observation.variables, nil
	}
	s, e := c.observeServer()
	if e != nil {
		return nil, e
	}
	v := s.Variables()
	if c.observation != nil {
		c.observation.variables = v
	}
	return v, nil
}
func systemBoolVariable(name string) bool {
	switch name {
	case "read_only", "super_read_only", "log_bin", "slopdb_wal_enabled", "slopdb_wal_fsync", "slopdb_deadlock_detection", "slopdb_cdc_enabled", "slopdb_raft_enabled", "slopdb_raft_ready", "slopdb_raft_writable", "slopdb_raft_recovering", "slopdb_raft_non_voting", "slopdb_raft_logs_only", "slopdb_raft_durable_read_ready", "slopdb_raft_replica_read_ready":
		return true
	}
	return false
}
func serverStateValue(name, value string) sql.Value {
	if systemBoolVariable(name) {
		if value == "ON" {
			return sql.NewIntValue(1)
		}
		return sql.NewIntValue(0)
	}
	if value != "" {
		if n, e := strconv.ParseInt(value, 10, 64); e == nil {
			return sql.NewIntValue(n)
		}
		if n, e := strconv.ParseUint(value, 10, 64); e == nil {
			return sql.NewUnsignedValue(n)
		}
	}
	return sql.NewTextValue(value)
}
func (c *client) serverVariableNames() ([]string, error) {
	values, e := c.observedVariables()
	if e != nil {
		return nil, e
	}
	names := make([]string, 0, len(values)+8)
	for n := range values {
		names = append(names, n)
	}
	return append(names, "slopdb_uptime_seconds", "slopdb_connections", "slopdb_max_prepared_statements", "slopdb_listeners", "version_compile_os", "version_compile_machine", "protocol_version"), nil
}
func (c *client) serverVariable(name string) (sql.Value, bool, error) {
	name = strings.ToLower(name)
	if name == "read_only" || name == "super_read_only" || strings.HasPrefix(name, "slopdb_raft_") {
		state, e := c.observeRaft()
		if e != nil {
			return sql.Value{}, true, e
		}
		if name == "read_only" || name == "super_read_only" {
			if state.Writable {
				return sql.NewIntValue(0), true, nil
			}
			return sql.NewIntValue(1), true, nil
		}
		if value, ok := state.Variables()[strings.TrimPrefix(name, "slopdb_raft_")]; ok {
			return serverStateValue(name, value), true, nil
		}
		return sql.Value{}, false, nil
	}
	switch name {
	case "version_compile_os":
		return sql.NewTextValue(runtime.GOOS), true, nil
	case "version_compile_machine":
		return sql.NewTextValue(runtime.GOARCH), true, nil
	case "protocol_version":
		return sql.NewIntValue(10), true, nil
	case "slopdb_uptime_seconds":
		return sql.NewIntValue(int64(time.Since(c.server.startedAt) / time.Second)), true, nil
	case "slopdb_max_prepared_statements":
		return sql.NewIntValue(int64(c.server.config.MaxPreparedStatements)), true, nil
	case "slopdb_connections":
		c.server.mu.Lock()
		n := len(c.server.clients)
		c.server.mu.Unlock()
		return sql.NewIntValue(int64(n)), true, nil
	case "slopdb_listeners":
		c.server.mu.Lock()
		var names []string
		for listener := range c.server.listeners {
			names = append(names, listener.Addr().String())
		}
		c.server.mu.Unlock()
		sort.Strings(names)
		data, _ := json.Marshal(names)
		return sql.NewTextValue(string(data)), true, nil
	}
	stateName := strings.HasPrefix(name, "slopdb_") || name == "read_only" || name == "super_read_only" || name == "log_bin" || name == "server_id" || name == "server_uuid" || name == "gtid_executed" || name == "gtid_purged"
	if !stateName {
		return sql.Value{}, false, nil
	}
	state, e := c.observeServer()
	if e != nil {
		return sql.Value{}, true, e
	}
	switch name {
	case "log_bin":
		if state.Changefeed.Enabled {
			return sql.NewIntValue(1), true, nil
		}
		return sql.NewIntValue(0), true, nil
	case "server_id":
		if state.Changefeed.SourceID == "" {
			return sql.NewIntValue(0), true, nil
		}
		sid, e := hex.DecodeString(state.Changefeed.SourceID)
		if e != nil {
			return sql.Value{}, true, e
		}
		id := crc32.ChecksumIEEE(sid)
		if id == 0 {
			id = 1
		}
		return sql.NewIntValue(int64(id)), true, nil
	case "server_uuid", "gtid_executed", "gtid_purged":
		if name == "gtid_purged" || len(state.Changefeed.SourceID) != 32 {
			return sql.NewTextValue(""), true, nil
		}
		source := state.Changefeed.SourceID
		uuid := source[:8] + "-" + source[8:12] + "-" + source[12:16] + "-" + source[16:20] + "-" + source[20:]
		if name == "gtid_executed" {
			if state.Changefeed.Sequence == 0 {
				return sql.NewTextValue(""), true, nil
			}
			uuid += ":1-" + strconv.FormatUint(state.Changefeed.Sequence, 10)
		}
		return sql.NewTextValue(uuid), true, nil
	}
	values, e := c.observedVariables()
	if e != nil {
		return sql.Value{}, true, e
	}
	value, ok := values[name]
	if !ok {
		return sql.Value{}, false, nil
	}
	return serverStateValue(name, value), true, nil
}
