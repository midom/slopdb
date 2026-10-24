package replication

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/hashicorp/raft"
	"slopdb/storage"
)

func (n *Node) raftNode() *raft.Raft { return n.raftPointer.Load() }
func (n *Node) startRaft(config *raft.Config) error {
	runtime, err := raft.NewRaft(config, n, n.store, n.store, n.snapshots, n.transport)
	if err != nil {
		return err
	}
	n.workMu.Lock()
	if n.closing.Load() {
		n.workMu.Unlock()
		_ = runtime.Shutdown().Error()
		return ErrClosed
	}
	n.raft = runtime
	n.raftPointer.Store(runtime)
	n.recovering.Store(false)
	n.workers.Add(2)
	if n.async() {
		n.workers.Add(1)
	}
	n.workMu.Unlock()
	go func() { defer n.workers.Done(); <-n.ctx.Done(); _ = runtime.Shutdown().Error() }()
	go n.monitor()
	if n.async() {
		go n.durabilityLoop()
	}
	return nil
}
func (n *Node) takeStableSnapshot(ctx context.Context) error {
	if n.async() {
		return n.flushDurable(ctx, true)
	}
	return n.wait(ctx, n.raftNode().Snapshot())
}

// A restarted async replica does not run Raft at all until a live leader has
// verified a quorum, sealed a prefix and supplied a fresh snapshot. Thus it can
// neither self-elect nor vote from a log that forgot acknowledged entries.
func (n *Node) rejoinLoop(config *raft.Config) {
	defer n.workers.Done()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		default:
		}
		candidates := []string{}
		if n.config.Join != "" {
			candidates = append(candidates, n.config.Join)
		}
		if c := n.currentCertificate(); c != nil {
			for _, p := range c.Body.Peers {
				if p.ID != n.config.ID {
					candidates = append(candidates, p.Control)
				}
			}
		}
		for _, address := range candidates {
			ctx, cancel := context.WithTimeout(n.ctx, n.config.OperationTimeout)
			var status Status
			err := n.getJSON(ctx, address, "/v1/status", &status)
			if err == nil && status.Ready && status.Cluster == n.cluster && status.Durability == DurabilityAsync {
				response, e := n.request(ctx, address, http.MethodGet, "/v1/snapshot", nil)
				if e == nil {
					e = n.installClone(response.Body)
					_ = response.Body.Close()
				}
				if e == nil {
					directory, e := generation(n.config.Directory)
					if e == nil {
						tree, openErr := storage.NewLSMTree(directory, n.storageOptions())
						e = openErr
						if e == nil {
							tree.SetReplicationHooks(n.hooks())
							old, oldTree := n.dataDirectory, n.database.ReplicationStorage()
							e = n.database.ReplaceReplicaStorage(tree, directory, func() error { return nil })
							if n.database.ReplicationStorage() == tree {
								n.dataDirectory = directory
								if e == nil {
									cp, ce := readCheckpoint(tree, n.cluster)
									e = ce
									if e == nil {
										n.applied.Store(cp.Index)
										n.appliedTerm.Store(cp.Term)
									}
								}
								n.retireTree(oldTree, old)
							} else {
								_ = tree.Close()
							}
						}
					}
					if e == nil {
						e = n.startRaft(config)
					}
					if e == nil {
						cancel()
						return
					}
					n.fail(fmt.Errorf("rejoining replica: %w", e))
					cancel()
					return
				}
			}
			cancel()
		}
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
