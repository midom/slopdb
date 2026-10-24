package replication

import (
	"bytes"
	"errors"

	"github.com/hashicorp/raft"
	"slopdb/storage"
)

var ErrLogsOnly = errors.New("logs-only replicas cannot serve SQL or become leader")
var ErrNonVoting = errors.New("permanent non-voting replicas cannot be promoted")

func validNodeMetadata(meta nodeMetadata) bool {
	return meta.Version == 1 && !meta.NonVoting && !meta.LogsOnly || meta.Version == 2
}

// Logs-only FSMs validate the same commands and persist only replication
// metadata/checkpoints. Consensus logs and opaque full snapshots retain the
// payload necessary for recovery; there is no second materialized SQL store.
func (n *Node) commandMutations(c command) ([]storage.Mutation, error) {
	writes, err := physicalMutations(n.database.ReplicationStorage(), c)
	if n.config.LogsOnly && c.Kind == "wal" {
		return nil, err
	}
	return writes, err
}

func replicationMetadata(key []byte) bool {
	return bytes.HasPrefix(key, []byte("\x00\xffslopdb:raft:"))
}

// A logs-only voter must never campaign successfully. Hashicorp Raft's
// pre-vote phase leaves the current term unchanged until a majority agrees.
// Refuse all outgoing pre-votes/votes and incoming leadership transfers while
// retaining incoming votes, AppendEntries and snapshots. Supported membership
// changes always retain a full voter: only a full leader can change membership,
// and it cannot remove itself or transfer leadership to a logs-only member.
// Thus a logs-only voter can never win with its self vote alone.
type logsOnlyTransport struct {
	*raft.NetworkTransport
	consumer <-chan raft.RPC
}

func (t *logsOnlyTransport) Consumer() <-chan raft.RPC { return t.consumer }
func (*logsOnlyTransport) RequestVote(raft.ServerID, raft.ServerAddress, *raft.RequestVoteRequest, *raft.RequestVoteResponse) error {
	return ErrLogsOnly
}
func (*logsOnlyTransport) RequestPreVote(raft.ServerID, raft.ServerAddress, *raft.RequestPreVoteRequest, *raft.RequestPreVoteResponse) error {
	return ErrLogsOnly
}

func (n *Node) raftTransport() raft.Transport {
	if !n.config.LogsOnly {
		return n.transport
	}
	consumer := make(chan raft.RPC)
	n.workers.Add(1)
	go func() {
		defer n.workers.Done()
		for {
			select {
			case <-n.ctx.Done():
				return
			case rpc, ok := <-n.transport.Consumer():
				if !ok {
					return
				}
				if _, transfer := rpc.Command.(*raft.TimeoutNowRequest); transfer {
					rpc.Respond(&raft.TimeoutNowResponse{}, ErrLogsOnly)
					continue
				}
				select {
				case consumer <- rpc:
				case <-n.ctx.Done():
					return
				}
			}
		}
	}()
	return &logsOnlyTransport{NetworkTransport: n.transport, consumer: consumer}
}

var _ raft.Transport = (*logsOnlyTransport)(nil)
var _ raft.WithPreVote = (*logsOnlyTransport)(nil)
