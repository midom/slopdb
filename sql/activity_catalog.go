package sql

import (
	"context"
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// ActivityRelationName recognizes only relations that this dialect resolves to
// the live activity provider. A user table named pg_stat_activity in another
// dialect must never acquire passive inspection admission.
func ActivityRelationName(name string, dialect Dialect) (string, bool) {
	name = strings.ToLower(name)
	if name == "information_schema.processlist" {
		return name, true
	}
	if dialect == DialectPostgreSQL && (name == "pg_stat_activity" || name == "pg_catalog.pg_stat_activity") {
		return "pg_stat_activity", true
	}
	return "", false
}

func (s *Schema) snapshotActivities() ([]SessionActivity, error) {
	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.activitySnapshot == nil {
		return nil, nil
	}
	return s.activitySnapshot(ctx)
}

// ProjectSessionActivity is a pure catalog adapter. The caller owns collection
// and lifecycle tracking; no session gates, row locks or storage reads occur.
// Input order and strings remain caller-owned and are never mutated.
func ProjectSessionActivity(name string, activities []SessionActivity, now time.Time) (*ResultSet, error) {
	name = strings.TrimPrefix(strings.ToLower(name), "pg_catalog.")
	var result *ResultSet
	switch name {
	case "information_schema.processlist":
		cols, _ := informationSchemaColumns(name)
		result = &ResultSet{Columns: cols}
	case "pg_stat_activity":
		result = pgCatalogFor(name)
	default:
		return nil, fmt.Errorf("unknown activity relation %q", name)
	}
	ordered := append([]SessionActivity(nil), activities...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].SessionID != ordered[j].SessionID {
			return ordered[i].SessionID < ordered[j].SessionID
		}
		return ordered[i].ID < ordered[j].ID
	})
	for _, activity := range ordered {
		row := catalogEmptyRow(result)
		row["slopdb_session_id"] = NewUnsignedValue(activity.SessionID)
		row["slopdb_backend_id"] = NewUnsignedValue(activity.ID)
		row["slopdb_protocol"] = NewTextValue(activity.Dialect.String())
		if activity.Waiting {
			row["slopdb_wait_kind"] = activityText(activity.WaitKind)
			row["slopdb_wait_resource"] = activityText(activity.WaitResource)
		}
		query := activityQueryText(activity.Query)
		if name == "information_schema.processlist" {
			row["id"] = NewUnsignedValue(activity.ID)
			row["user"] = activityText(activity.User)
			row["host"] = activityText(activityHost(activity))
			row["db"] = activityText(activity.Database)
			command := "Sleep"
			state := ""
			switch activity.State {
			case ActivityActive:
				command = "Query"
				state = "executing"
				row["info"] = NewTextValue(query)
			case ActivityIdleInTransaction:
				state = "idle in transaction"
			case ActivityIdleInTransactionAborted:
				state = "idle in transaction (aborted)"
			}
			if activity.Waiting {
				state = "Waiting for lock"
				if activity.WaitKind != "" {
					state = "Waiting for " + activity.WaitKind + " lock"
				}
			}
			since := activity.StateChange
			if activity.State == ActivityActive && !activity.QueryStart.IsZero() {
				since = activity.QueryStart
			}
			if since.IsZero() {
				since = activity.StartedAt
			}
			seconds := int64(0)
			if !since.IsZero() && now.After(since) {
				seconds = int64(now.Sub(since) / time.Second)
			}
			row["command"] = NewTextValue(command)
			row["time"] = NewIntValue(seconds)
			row["state"] = NewTextValue(state)
			row["slopdb_application_name"] = activityText(activity.ApplicationName)
		} else {
			// Native callers can outgrow PostgreSQL's signed 32-bit PID range; the
			// unabridged public ID remains available in slopdb_backend_id.
			if activity.ID > 0 && activity.ID <= math.MaxInt32 {
				row["pid"] = NewIntValue(int64(activity.ID)).WithDeclaredType("INTEGER")
			}
			row["datname"] = activityText(activity.Database)
			row["usename"] = activityText(activity.User)
			row["application_name"] = NewTextValue(activity.ApplicationName)
			row["client_addr"] = activityText(activity.ClientAddress)
			if activity.ClientPort != 0 {
				row["client_port"] = NewIntValue(int64(activity.ClientPort)).WithDeclaredType("INTEGER")
			}
			row["backend_start"] = activityTimestamp(activity.StartedAt)
			row["xact_start"] = activityTimestamp(activity.TransactionStart)
			row["query_start"] = activityTimestamp(activity.QueryStart)
			row["state_change"] = activityTimestamp(activity.StateChange)
			row["state"] = activityState(activity.State)
			if activity.Waiting {
				row["wait_event_type"] = NewTextValue("Lock")
				row["wait_event"] = activityText(activity.WaitKind)
			}
			if query != "" {
				row["query"] = NewTextValue(query)
			}
			row["backend_type"] = NewTextValue("client backend")
		}
		for _, column := range result.Columns {
			if !row[column.Name].Null {
				row[column.Name] = row[column.Name].WithDeclaredType(column.Declared)
			}
		}
		result.Rows = append(result.Rows, row)
	}
	return result, nil
}

// ProcesslistResult projects the same live catalog frame into MySQL's SHOW
// labels. FULL preserves retained text; normal SHOW limits Info to 100 Unicode
// code points. Both retain the core's 64 KiB source-text bound.
func ProcesslistResult(activities []SessionActivity, full bool, now time.Time) *ResultSet {
	source, _ := ProjectSessionActivity("information_schema.processlist", activities, now)
	result := catalogResult(showSchemas["PROCESSLIST"])
	fields := map[string]string{"Id": "id", "User": "user", "Host": "host", "db": "db", "Command": "command", "Time": "time", "State": "state", "Info": "info"}
	for _, row := range source.Rows {
		projected := catalogEmptyRow(result)
		for output, input := range fields {
			projected[output] = row[input]
		}
		if !full && !projected["Info"].Null {
			projected["Info"] = NewTextValue(activityCharacters(projected["Info"].Text, 100))
		}
		result.Rows = append(result.Rows, projected)
	}
	return result
}
func activityText(value string) Value {
	if value == "" {
		return NewNullValue()
	}
	return NewTextValue(value)
}
func activityTimestamp(value time.Time) Value {
	if value.IsZero() {
		return NewNullValue()
	}
	return NewTextValue(value.UTC().Format(time.RFC3339Nano)).WithDeclaredType("TIMESTAMPTZ")
}
func activityState(value ActivityState) Value {
	switch value {
	case ActivityIdle, ActivityActive, ActivityIdleInTransaction, ActivityIdleInTransactionAborted:
		return NewTextValue(string(value))
	default:
		return NewNullValue()
	}
}
func activityHost(activity SessionActivity) string {
	if activity.Host != "" {
		return activity.Host
	}
	if activity.ClientAddress != "" && activity.ClientPort > 0 {
		return net.JoinHostPort(activity.ClientAddress, strconv.Itoa(activity.ClientPort))
	}
	return activity.ClientAddress
}
func activityQueryText(value string) string {
	if len(value) <= ActivityQueryLimit {
		return value
	}
	end := ActivityQueryLimit
	for end > 0 && !utf8.RuneStart(value[end]) {
		end--
	}
	return value[:end]
}
func activityCharacters(value string, limit int) string {
	count := 0
	for index := range value {
		if count == limit {
			return value[:index]
		}
		count++
	}
	return value
}
