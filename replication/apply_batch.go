package replication

import (
	"fmt"

	"github.com/hashicorp/raft"
	"slopdb/storage"
)

const maxApplyBatchBytes = 4 << 20

// ApplyBatch groups already committed WAL commands into native atomic batches.
// Each group publishes its last checkpoint with its mutations, and no caller's
// future completes until the whole group is visible (and synced in sync mode).
// Control/configuration commands are boundaries, keeping their side effects in
// exactly the same order as individual Apply calls. No batching timer is used.
func (n *Node) ApplyBatch(logs []*raft.Log) []interface{} {
	n.fsmMu.Lock()
	defer n.fsmMu.Unlock()
	responses := make([]interface{}, len(logs))
	var pending []storage.Mutation
	var last *raft.Log
	var start, pendingBytes int
	flush := func() error {
		if last == nil {
			return nil
		}
		err := n.applyCheckpoint(last.Index, last.Term, pending)
		pending, last, pendingBytes = nil, nil, 0
		return err
	}
	fail := func(from int, err error) []interface{} {
		n.fail(err)
		for i := from; i < len(responses); i++ {
			responses[i] = err
		}
		return responses
	}
	for i, log := range logs {
		if err := n.failure(); err != nil {
			if last != nil {
				return fail(start, err)
			}
			return fail(i, err)
		}
		if log.Index <= n.applied.Load() {
			continue
		}
		var c command
		var err error
		if log.Type == raft.LogCommand {
			err = unmarshalCommand(log.Data, &c)
		} else if log.Type != raft.LogConfiguration {
			err = fmt.Errorf("unsupported FSM log type %d", log.Type)
		}
		if err != nil {
			if flushErr := flush(); flushErr != nil {
				return fail(start, flushErr)
			}
			return fail(i, err)
		}
		if log.Type == raft.LogConfiguration || c.Kind != "wal" {
			if err = flush(); err != nil {
				return fail(start, err)
			}
			var writes []storage.Mutation
			if log.Type == raft.LogCommand {
				writes, err = n.commandMutations(c)
			}
			if err == nil {
				err = n.applyCheckpointWithBarrier(log.Index, log.Term, writes, c.Kind == "fence")
			}
			if err != nil {
				return fail(i, err)
			}
			if log.Type == raft.LogConfiguration {
				n.configurationIndex.Store(log.Index)
			}
			continue
		}
		writes, err := n.commandMutations(c)
		if err != nil {
			if flushErr := flush(); flushErr != nil {
				return fail(start, flushErr)
			}
			return fail(i, err)
		}
		bytes := 0
		for _, write := range writes {
			bytes += len(write.Key) + len(write.Value) + 64
		}
		if last != nil && pendingBytes+bytes > maxApplyBatchBytes {
			if err = flush(); err != nil {
				return fail(start, err)
			}
		}
		if last == nil {
			start = i
			pending = writes
		} else {
			pending = append(pending, writes...)
		}
		last = log
		pendingBytes += bytes
	}
	if err := flush(); err != nil {
		return fail(start, err)
	}
	return responses
}

var _ raft.BatchingFSM = (*Node)(nil)
