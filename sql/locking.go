package sql

import (
	"errors"
	"fmt"
	"strconv"
)

// ErrStatementRetry asks the driver to restart from a fresh statement snapshot.
// It is internal retry control, rather than a transaction conflict.
var ErrStatementRetry = errors.New("statement snapshot changed before row locking")

func (s *Schema) validateLockedRow(key []byte) error {
	if !s.statementActive || s.lockHandler == nil {
		return nil
	}
	if string(key) == s.validatedRowKey {
		return nil
	}
	if _, ok := s.txLatest[string(key)]; ok {
		return nil
	}
	changed, err := s.lsm.ChangedSince(key, s.statementSnapshot)
	if err != nil {
		return err
	}
	if changed {
		return ErrStatementRetry
	}
	s.validatedRowKey = string(key)
	return nil
}

// ErrLockNotAvailable identifies row contention on a NOWAIT locking read.
var ErrLockNotAvailable = errors.New("row lock is not available")

type LockWaitPolicy uint8

const (
	LockWait LockWaitPolicy = iota
	LockNoWait
	LockSkipLocked
)

func frameLockName(name string) string { return strconv.Itoa(len(name)) + ":" + name }

// LockRequest describes a row, unique-value predicate, or catalog lock. Values
// are compared using SQL equality, so integer/real equivalents share a lock.
type LockRequest struct {
	Resource  string
	Values    []Value
	Exclusive bool
	Wait      LockWaitPolicy
}

// SetLockHandler installs pessimistic validation before a transaction starts.
// A successful handler must keep
// the requested locks until transaction completion; without one, commit checks
// row versions and unique predicates optimistically.
func (s *Schema) SetLockHandler(handler func(LockRequest) error) { s.lockHandler = handler }

// SetLockGroupHandler installs atomic acquisition for rows in a join result.
func (s *Schema) SetLockGroupHandler(handler func([]LockRequest) error) { s.lockGroupHandler = handler }

func (s *Schema) requestRowLocks(keys []string, lock *SelectLock) error {
	if s.replicaReadOnly {
		return fmt.Errorf("locking reads require the Raft leader")
	}
	requests := make([]LockRequest, len(keys))
	for i, key := range keys {
		requests[i] = LockRequest{Resource: "row:" + key, Exclusive: lock.Exclusive, Wait: lock.Wait}
	}
	if s.lockGroupHandler != nil {
		if err := s.lockGroupHandler(requests); err != nil {
			return err
		}
		for _, key := range keys {
			if err := s.validateLockedRow([]byte(key)); err != nil {
				return err
			}
		}
		return nil
	}
	for _, request := range requests {
		if err := s.RequestLock(request); err != nil {
			return err
		}
		if err := s.validateLockedRow([]byte(request.Resource[len("row:"):])); err != nil {
			return err
		}
	}
	return nil
}

// Keep physical row keys separate from SQL values: projection aliases may
// overwrite primary-key columns, and tables without a PK use hidden row IDs.
type selectRowsWithKeys struct {
	rows []map[string]Value
	keys [][]string
	less func(map[string]Value, map[string]Value) bool
}

func (r selectRowsWithKeys) Len() int           { return len(r.rows) }
func (r selectRowsWithKeys) Less(i, j int) bool { return r.less(r.rows[i], r.rows[j]) }
func (r selectRowsWithKeys) Swap(i, j int) {
	r.rows[i], r.rows[j] = r.rows[j], r.rows[i]
	if r.keys != nil {
		r.keys[i], r.keys[j] = r.keys[j], r.keys[i]
	}
}

func (s *Schema) lockSelectRows(stmt *SelectStmt, rows []map[string]Value, keys [][]string, bounded bool) ([]map[string]Value, [][]string, error) {
	if stmt.Limit == 0 {
		return nil, nil, nil
	}
	var result []map[string]Value
	var resultKeys [][]string
	for i, row := range rows {
		if bounded && stmt.Limit >= 0 && int64(len(result)) >= stmt.Offset && int64(len(result))-stmt.Offset >= stmt.Limit {
			break
		}
		if err := s.checkInterrupt(); err != nil {
			return nil, nil, err
		}
		var rowKeys []string
		if keys != nil {
			rowKeys = keys[i]
		}
		if err := s.requestRowLocks(rowKeys, stmt.Lock); err != nil {
			if stmt.Lock.Wait == LockSkipLocked && errors.Is(err, ErrLockNotAvailable) {
				continue
			}
			return nil, nil, err
		}
		result = append(result, row)
		if keys != nil {
			resultKeys = append(resultKeys, rowKeys)
		}
	}
	return result, resultKeys, nil
}

func (s *Schema) RequestLock(request LockRequest) error {
	if s.lockHandler == nil {
		return nil
	}
	return s.lockHandler(request)
}

func (s *Schema) lockRow(table string, info *TableInfo, row map[string]Value, key []byte) error {
	if err := s.RequestLock(LockRequest{Resource: "row:" + string(key), Exclusive: true}); err != nil {
		return err
	}
	if err := s.validateLockedRow(key); err != nil {
		return err
	}
	for _, group := range s.uniqueGroups(table, info) {
		// Integer primary keys have a canonical physical row key. The row lock
		// already serializes every insert, delete and primary-key change for this
		// SQL value; a second predicate lock would serialize unrelated inserts.
		if len(info.PKColumns) == 1 && len(group) == 1 && group[0] == info.PKColumns[0] {
			if col := s.getPKColumn(info); col != nil && col.ColType == ColInteger {
				if v := row[col.Name]; !v.Null && v.Type == ColInteger {
					continue
				}
			}
		}
		values := make([]Value, 0, len(group))
		null := false
		for _, column := range group {
			v := row[column]
			if v.Null {
				null = true
				break
			}
			if !columnBinary(info, column) {
				v = s.collation.keyValue(v)
			}
			values = append(values, v)
		}
		if null {
			continue
		}
		// Length framing prevents quoted/binary identifiers from aliasing.
		resource := "unique:" + frameLockName(table)
		for _, column := range group {
			resource += frameLockName(column)
		}
		if err := s.RequestLock(LockRequest{Resource: resource, Values: values, Exclusive: true}); err != nil {
			return err
		}
	}
	return nil
}
