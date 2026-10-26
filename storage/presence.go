package storage

import "fmt"

// ExistsAtForReader tests visibility without copying a row value. The caller
// keeps snapshot pinned, just as for GetAtForReader. Zero is a real snapshot.
func (l *LSMTree) ExistsAtForReader(key []byte, snapshot, reader uint64) (bool, error) {
	view, committed, shard := l.acquireViewForReader(reader)
	if view == nil {
		return false, fmt.Errorf("database closed")
	}
	defer view.releaseReader(shard)
	if snapshot > committed {
		snapshot = committed
	}
	w := pointWorkspaces.Get().(*pointWorkspace)
	defer releasePointWorkspace(w)
	_, found, _, err := readPointWithWorkspaceMode(view, key, snapshot, 0, false, w, false)
	return found, err
}
