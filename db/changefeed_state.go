package db

import (
	"context"
	"slopdb/storage"
)

type ChangefeedState struct {
	Enabled                 bool
	SourceID                string
	StartSequence, Sequence uint64
}
type changefeedIdentity struct {
	tree   *storage.LSMTree
	source string
	start  uint64
}

// ChangefeedState observes the journal without activation or SQL rendering.
func (d *Database) ChangefeedState(ctx context.Context) (ChangefeedState, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if e := ctx.Err(); e != nil {
		return ChangefeedState{}, e
	}
	if d.closed.Load() {
		return ChangefeedState{}, ErrSessionClosed
	}
	tree := d.currentStorage.Load()
	if !tree.ChangeLogEnabled() {
		return ChangefeedState{}, nil
	}
	id := d.changeIdentity.Load()
	if id == nil || id.tree != tree {
		header, e := tree.ChangeLogHeader()
		if e != nil {
			return ChangefeedState{}, e
		}
		if header == nil {
			return ChangefeedState{}, nil
		}
		id = &changefeedIdentity{tree: tree, source: header.SourceID, start: header.StartSequence}
		d.changeIdentity.Store(id)
	}
	seq, e := tree.ChangeLogPosition()
	if e != nil {
		return ChangefeedState{}, e
	}
	return ChangefeedState{Enabled: true, SourceID: id.source, StartSequence: id.start, Sequence: seq}, ctx.Err()
}
