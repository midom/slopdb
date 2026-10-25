package storage

import "fmt"

// PointValue is one result of an ordered multi-key read. Missing and deleted
// keys have Found=false; returned values are owned by the caller.
type PointValue struct {
	Value []byte
	Found bool
}

// GetMulti reads all keys from one committed view, in input order. Duplicate
// keys remain separate results. GetMultiAt uses a caller-pinned snapshot.
func (l *LSMTree) GetMulti(keys [][]byte) ([]PointValue, error) { return l.getMulti(keys, 0, true) }
func (l *LSMTree) GetMultiAt(keys [][]byte, snapshot uint64) ([]PointValue, error) {
	return l.getMulti(keys, snapshot, false)
}

// GetMultiAtForReader reads an ordered key batch using one sharded read-view
// reference for a stable reader ID. The caller must keep snapshot pinned, as
// with GetAtForReader. Values belong to the caller, including separate copies
// for duplicate keys; missing/deleted keys retain their input positions.
func (l *LSMTree) GetMultiAtForReader(keys [][]byte, snapshot, reader uint64) ([]PointValue, error) {
	values, _, err := l.getMultiAtForReader(keys, snapshot, reader, -1)
	return values, err
}

// GetMultiAtForReaderInto reads one ordered batch using a sharded read-view
// reference, reusing dst and each result's value capacity. The caller gives up
// all previous contents and aliases of dst. Each Value buffer must be
// exclusively caller-owned, independent of keys and other destination slots.
// Returned values remain owned across subsequent engine calls and closure.
// Duplicate keys get independent copies; missing keys have Found=false.
// As with GetMultiAtForReader, the caller must keep snapshot pinned. An error
// returns nil and may have overwritten part of dst; no borrowed data escapes.
func (l *LSMTree) GetMultiAtForReaderInto(keys [][]byte, snapshot, reader uint64, dst []PointValue) ([]PointValue, error) {
	view, committed, shard := l.acquireViewForReader(reader)
	if view == nil {
		return nil, fmt.Errorf("database closed")
	}
	defer view.releaseReader(shard)
	if snapshot > committed {
		snapshot = committed
	}
	workspace := pointWorkspaces.Get().(*pointWorkspace)
	defer releasePointWorkspace(workspace)
	if len(dst) > len(keys) {
		clear(dst[len(keys):])
	}
	if cap(dst) < len(keys) {
		grown := make([]PointValue, len(keys))
		copy(grown, dst)
		dst = grown
	} else {
		dst = dst[:len(keys)]
	}
	for i, key := range keys {
		value, found, _, err := readPointWithWorkspaceMode(view, key, snapshot, 0, false, workspace, false)
		if err != nil {
			return nil, err
		}
		dst[i].Found = found
		if found {
			dst[i].Value = append(dst[i].Value[:0], value...)
		} else {
			dst[i].Value = dst[i].Value[:0]
		}
	}
	return dst, nil
}

// GetMultiAtForReaderBounded applies a byte budget before copying each value.
// The budget counts every found value, including duplicate keys. Missing and
// empty values consume no bytes. If the batch exceeds maxBytes it returns
// nil, false, nil, without exposing partial results or borrowed storage data.
// Negative budgets fail; snapshots and ownership match GetMultiAtForReader.
func (l *LSMTree) GetMultiAtForReaderBounded(keys [][]byte, snapshot, reader uint64, maxBytes int) ([]PointValue, bool, error) {
	if maxBytes < 0 {
		return nil, false, fmt.Errorf("multi-get byte budget must not be negative")
	}
	return l.getMultiAtForReader(keys, snapshot, reader, maxBytes)
}

// A negative internal budget selects the original unlimited contract.
func (l *LSMTree) getMultiAtForReader(keys [][]byte, snapshot, reader uint64, remaining int) ([]PointValue, bool, error) {
	view, committed, shard := l.acquireViewForReader(reader)
	if view == nil {
		return nil, false, fmt.Errorf("database closed")
	}
	defer view.releaseReader(shard)
	if snapshot > committed {
		snapshot = committed
	}
	workspace := pointWorkspaces.Get().(*pointWorkspace)
	defer releasePointWorkspace(workspace)
	values := make([]PointValue, len(keys))
	for i, key := range keys {
		value, found, _, err := readPointWithWorkspaceMode(view, key, snapshot, 0, false, workspace, false)
		if err != nil {
			return nil, false, err
		}
		if found {
			if remaining >= 0 {
				if len(value) > remaining {
					return nil, false, nil
				}
				remaining -= len(value)
			}
			values[i] = PointValue{Value: append([]byte(nil), value...), Found: true}
		}
	}
	return values, true, nil
}

func (l *LSMTree) getMulti(keys [][]byte, snapshot uint64, latest bool) ([]PointValue, error) {
	view, committed := l.acquireView()
	if view == nil {
		return nil, fmt.Errorf("database closed")
	}
	defer view.release()
	if latest || snapshot > committed {
		snapshot = committed
	}
	values := make([]PointValue, len(keys))
	var workspace pointWorkspace
	defer workspace.releaseBlocks()
	for i, key := range keys {
		v, found, _, err := readPointWithWorkspace(view, key, snapshot, 0, false, &workspace)
		if err != nil {
			return nil, err
		}
		values[i] = PointValue{Value: v, Found: found}
	}
	return values, nil
}
