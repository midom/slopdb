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
	for i, key := range keys {
		v, found, _, err := readPointWithWorkspace(view, key, snapshot, 0, false, &workspace)
		if err != nil {
			return nil, err
		}
		values[i] = PointValue{Value: v, Found: found}
	}
	return values, nil
}
