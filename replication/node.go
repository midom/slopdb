// Package replication ships atomic native WAL mutations through Raft. Nodes own
// their database, durable consensus log, snapshots and authenticated listeners.
package replication

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"slopdb/db"
	"slopdb/internal/wire"
	"slopdb/storage"
)

var ErrNotLeader = errors.New("replication node is not the ready Raft leader")
var ErrClosed = errors.New("replication node closed")

// Config uses canonical tcp://host:port or unix:///absolute/path endpoints.
// Directory is a node directory, distinct from a standalone database directory.
type Config struct {
	// NonVoting keeps a replica outside the quorum, including manual promotion.
	NonVoting bool
	// LogsOnly retains consensus logs/snapshots without materializing SQL data.
	// It can vote, but cannot lead or serve SQL.
	LogsOnly           bool
	ReadBarrier        db.ReadBarrier // default policy for SELECTs: previous, next, or committed
	Durability         Durability
	DurabilityInterval time.Duration
	MaxDurabilityLag   time.Duration
	// ReplicaReadMaxStaleness enables bounded-staleness, read-only SQL on
	// followers and learners. Zero retains leader-only SQL admission.
	ReplicaReadMaxStaleness         time.Duration
	Directory, ID                   string
	Listen, Advertise               string
	ControlListen, ControlAdvertise string
	Token                           string // at least 16 bytes; shared by trusted members
	Bootstrap                       bool   // only for the first node of a new cluster
	Join                            string // leader's control endpoint; empty on ordinary restart
	SeedDirectory                   string // optional standalone database, copied on bootstrap
	AutoPromote                     bool   // learner joins asynchronously, then promotes when caught up
	TLSConfig, ClientTLSConfig      *tls.Config
	StorageOptions                  *storage.Options
	RaftConfig                      *raft.Config // optional timing overrides; identity is set here
	OperationTimeout                time.Duration
}
type nodeMetadata struct {
	NonVoting, LogsOnly           bool `json:",omitempty"`
	Durability                    Durability
	Version                       int
	ID, Cluster, Address, Control string
}
type nodeFailure struct{ err error }
type command struct {
	Version  int
	Kind     string
	Writes   []storage.Mutation
	PageSize int64
	Peer     *Peer
}
type Node struct {
	walProposals                    walProposalQueue
	durableReads                    durableReadViews
	position                        atomic.Pointer[string]
	replicaReads                    replicaReadState
	configurationIndex              atomic.Uint64
	checkpointJSONPrefix            []byte // protected by fsmMu; cluster identity is immutable
	applyNotice                     atomic.Pointer[applyNotice]
	durability                      durabilityState
	raftPointer                     atomic.Pointer[raft.Raft]
	recovering                      atomic.Bool
	generationMu                    sync.Mutex
	snapshotRefs                    map[*storage.LSMTree]int
	retired                         map[*storage.LSMTree]string
	retirementErr                   error
	writeTerm                       atomic.Uint64
	config                          Config
	cluster                         string
	address, control                endpoint
	database                        *db.Database
	dataDirectory                   string
	raft                            *raft.Raft
	transport                       *raft.NetworkTransport
	store                           *diskStore
	snapshots                       *raft.FileSnapshotStore
	httpServer                      *http.Server
	rootLock                        *os.File
	ctx                             context.Context
	cancel                          context.CancelFunc
	readyTerm, applied, appliedTerm atomic.Uint64
	failed                          atomic.Pointer[nodeFailure]
	closing                         atomic.Bool
	fsmMu                           sync.Mutex
	proposalMu                      sync.RWMutex
	largeProposalMu                 sync.Mutex
	membershipMu                    sync.Mutex
	workers                         sync.WaitGroup
	workMu                          sync.Mutex
	promoting                       map[string]bool
	closeOnce                       sync.Once
	closeErr                        error
}

func (n *Node) storageOptions() *storage.Options {
	options := storage.DefaultOptions()
	if n.config.StorageOptions != nil {
		*options = *n.config.StorageOptions
	}
	options.Replica = true
	options.PageSize = 0
	options.CreateIfNotExist = true
	options.ErrorIfExists = false
	return options
}

func Open(ctx context.Context, config Config) (node *Node, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if config.ID == "" || len(config.ID) > 128 || config.Directory == "" {
		return nil, fmt.Errorf("Raft ID and node directory are required")
	}
	if len(config.Token) < 16 {
		return nil, fmt.Errorf("Raft token must contain at least 16 bytes")
	}
	if config.Bootstrap && config.Join != "" {
		return nil, fmt.Errorf("bootstrap and join are mutually exclusive")
	}
	if config.Bootstrap && (config.NonVoting || config.LogsOnly) {
		return nil, fmt.Errorf("bootstrap requires a full voting replica")
	}
	if config.NonVoting {
		config.AutoPromote = false
	}
	if config.LogsOnly && (config.ReplicaReadMaxStaleness != 0 || config.ReadBarrier != db.ReadBarrierDefault) {
		return nil, fmt.Errorf("logs-only replicas cannot enable SQL reads")
	}
	if config.SeedDirectory != "" && !config.Bootstrap {
		return nil, fmt.Errorf("seed database requires bootstrap")
	}
	if config.Durability != "" && config.Durability != DurabilitySync && config.Durability != DurabilityAsync {
		return nil, fmt.Errorf("durability must be sync or async")
	}
	if config.DurabilityInterval == 0 {
		config.DurabilityInterval = 100 * time.Millisecond
	}
	if config.DurabilityInterval < 0 {
		return nil, fmt.Errorf("durability interval must be positive")
	}
	if config.MaxDurabilityLag == 0 {
		config.MaxDurabilityLag = 10 * config.DurabilityInterval
		if config.MaxDurabilityLag < time.Second {
			config.MaxDurabilityLag = time.Second
		}
	}
	if config.MaxDurabilityLag < config.DurabilityInterval {
		return nil, fmt.Errorf("maximum durability lag must cover the flush interval")
	}
	if config.OperationTimeout == 0 {
		config.OperationTimeout = 10 * time.Second
	}
	if config.OperationTimeout < 0 {
		return nil, fmt.Errorf("operation timeout must be positive")
	}
	if config.ReplicaReadMaxStaleness < 0 {
		return nil, fmt.Errorf("replica read maximum staleness must not be negative")
	}
	if !db.ValidReadBarrier(config.ReadBarrier) {
		return nil, fmt.Errorf("invalid read barrier")
	}
	if config.StorageOptions != nil {
		copy := *config.StorageOptions
		config.StorageOptions = &copy
	}
	if config.RaftConfig != nil {
		copy := *config.RaftConfig
		config.RaftConfig = &copy
	}
	if config.StorageOptions != nil && (config.StorageOptions.DisableWAL || config.StorageOptions.DisableWALSync || config.StorageOptions.DisableAutoIncrementPersistence || config.StorageOptions.VerifyIndexesOnOpen || config.StorageOptions.PageSize != 0) {
		return nil, fmt.Errorf("replication requires durable WAL/counters, no catalog repair and page size selected through replicated PRAGMA")
	}
	config.TLSConfig, _, err = wire.TLSConfig(config.TLSConfig, false)
	if err != nil {
		return nil, err
	}
	if config.TLSConfig != nil {
		if config.ClientTLSConfig == nil {
			return nil, fmt.Errorf("replication TLS requires an outbound client TLS configuration")
		}
		config.ClientTLSConfig = config.ClientTLSConfig.Clone()
		if config.ClientTLSConfig.RootCAs != nil {
			config.ClientTLSConfig.RootCAs = config.ClientTLSConfig.RootCAs.Clone()
		}
		if config.ClientTLSConfig.MinVersion < tls.VersionTLS12 {
			config.ClientTLSConfig.MinVersion = tls.VersionTLS12
		}
		if config.ClientTLSConfig.InsecureSkipVerify {
			return nil, fmt.Errorf("replication TLS must verify peer certificates")
		}
	}
	if config.TLSConfig == nil && config.ClientTLSConfig != nil {
		return nil, fmt.Errorf("outbound TLS requires inbound TLS")
	}
	config.Directory, err = filepath.Abs(config.Directory)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(config.Directory, 0700); err != nil {
		return nil, err
	}
	if _, e := os.Stat(filepath.Join(config.Directory, "MANIFEST")); e == nil {
		return nil, fmt.Errorf("use a new node directory; standalone databases must be supplied with SeedDirectory")
	}
	lock, e := os.OpenFile(filepath.Join(config.Directory, "LOCK"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		lock.Close()
		return nil, fmt.Errorf("node directory is locked: %w", e)
	}
	lifetime, cancel := context.WithCancel(context.Background())
	n := &Node{config: config, ctx: lifetime, cancel: cancel, rootLock: lock, promoting: make(map[string]bool), snapshotRefs: make(map[*storage.LSMTree]int), retired: make(map[*storage.LSMTree]string)}
	defer func() {
		if err != nil {
			_ = n.Close()
		}
	}()
	var meta nodeMetadata
	metadataPath := filepath.Join(config.Directory, "node.json")
	if e := retiredNode(config.Directory); e != nil {
		return nil, e
	}
	metadata, e := os.ReadFile(metadataPath)
	fresh := os.IsNotExist(e)
	if e != nil && !fresh {
		return nil, e
	}
	if fresh {
		entries, e := os.ReadDir(config.Directory)
		if e != nil {
			return nil, e
		}
		for _, entry := range entries {
			if entry.Name() != "LOCK" {
				return nil, fmt.Errorf("new node directory must be empty: found %s", entry.Name())
			}
		}
	}
	if !fresh {
		if e = json.Unmarshal(metadata, &meta); e != nil {
			return nil, e
		}
		if !validNodeMetadata(meta) || meta.ID != config.ID {
			return nil, fmt.Errorf("node directory identity/version mismatch")
		}
		if config.NonVoting != meta.NonVoting || config.LogsOnly != meta.LogsOnly {
			return nil, fmt.Errorf("replica role differs from persisted node identity")
		}
		n.cluster = meta.Cluster
		mode := meta.Durability
		if mode == "" {
			mode = DurabilitySync
		}
		if config.Durability != "" && config.Durability != mode {
			return nil, fmt.Errorf("durability mode differs from persisted node identity")
		}
		config.Durability = mode
	} else {
		if !config.Bootstrap && config.Join == "" {
			return nil, fmt.Errorf("new node requires bootstrap or join")
		}
		if config.Join != "" {
			var source Status
			if e = n.getJSON(ctx, config.Join, "/v1/status", &source); e != nil {
				return nil, e
			}
			n.cluster = source.Cluster
			mode := source.Durability
			if mode == "" {
				mode = DurabilitySync
			}
			if config.Durability != "" && config.Durability != mode {
				return nil, fmt.Errorf("durability mode differs from source cluster")
			}
			config.Durability = mode
			if source.ID == config.ID {
				return nil, fmt.Errorf("clone requires a distinct node ID")
			}
		} else {
			n.cluster = randomID()
		}
	}
	if config.Durability == "" {
		config.Durability = DurabilitySync
	}
	n.config.Durability = config.Durability
	raftListener, address, e := listenEndpoint(config.Listen, config.Advertise)
	if e != nil {
		return nil, e
	}
	n.address = address
	controlListener, control, e := listenEndpoint(config.ControlListen, config.ControlAdvertise)
	if e != nil {
		raftListener.Close()
		return nil, e
	}
	n.control = control
	defer func() {
		if err != nil {
			raftListener.Close()
			controlListener.Close()
		}
	}()
	if !fresh && (meta.Address != address.String() || meta.Control != control.String()) {
		return nil, fmt.Errorf("restart must retain the persisted Raft and control endpoints")
	}
	if fresh {
		meta = nodeMetadata{Version: 1, ID: config.ID, Cluster: n.cluster, Address: address.String(), Control: control.String(), Durability: config.Durability, NonVoting: config.NonVoting, LogsOnly: config.LogsOnly}
		if config.NonVoting || config.LogsOnly {
			// Older binaries must reject identities whose voting/application
			// restrictions they do not understand.
			meta.Version = 2
		}
		b, _ := json.Marshal(meta)
		if err = atomicFile(metadataPath, b); err != nil {
			return nil, err
		}
	}
	logOptions := storage.DefaultOptions()
	logOptions.BlockCacheSize = 4 << 20
	logOptions.MaxOpenFiles = 32
	logTree, e := storage.NewLSMTree(filepath.Join(config.Directory, "raft-log"), logOptions)
	if e != nil {
		return nil, e
	}
	n.store = &diskStore{tree: logTree, async: n.async()}
	if e = n.loadCertificate(); e != nil {
		return nil, e
	}
	logger := hclog.NewNullLogger()
	n.snapshots, e = raft.NewFileSnapshotStoreWithLogger(config.Directory, 2, logger)
	if e != nil {
		return nil, e
	}
	stream := newStream(raftListener, address, config.Token, n.cluster, config.TLSConfig, config.ClientTLSConfig)
	n.transport = raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{Stream: stream, MaxPool: 4, Timeout: config.OperationTimeout, Logger: logger})
	if fresh && config.Join != "" {
		response, e := n.request(ctx, config.Join, http.MethodGet, "/v1/snapshot", nil)
		if e != nil {
			return nil, e
		}
		e = n.installClone(response.Body)
		response.Body.Close()
		if e != nil {
			return nil, e
		}
	}
	data, e := generation(config.Directory)
	if os.IsNotExist(e) {
		data = filepath.Join(config.Directory, "data-"+randomID())
		if config.SeedDirectory != "" {
			seed, e := db.Open(config.SeedDirectory, nil)
			if e != nil {
				return nil, e
			}
			e = seed.ReplicationStorage().Backup(ctx, data)
			ce := seed.Close()
			if e != nil {
				return nil, e
			}
			if ce != nil {
				return nil, ce
			}
			if e = stripReplicationState(data); e != nil {
				return nil, e
			}
		} else {
			initial, e := db.Open(data, n.storageOptions())
			if e != nil {
				return nil, e
			}
			if e = initial.Close(); e != nil {
				return nil, e
			}
		}
		if e = publishGeneration(config.Directory, data); e != nil {
			return nil, e
		}
	} else if e != nil {
		return nil, e
	}
	n.dataDirectory = data
	n.database, e = db.Open(data, n.storageOptions())
	if e != nil {
		return nil, e
	}
	cp, e := readCheckpoint(n.database.ReplicationStorage(), n.cluster)
	if e != nil {
		return nil, e
	}
	n.applied.Store(cp.Index)
	n.appliedTerm.Store(cp.Term)
	n.publishPosition(cp.Index, cp.Term)
	if cp.Index > 0 && (!n.async() || cp.Index == n.durableIndex()) {
		n.captureDurableView(cp.Index, cp.Term, true)
	}
	guard := &db.ReplicationGuard{Check: n.checkSQL, CheckControl: n.checkSQLControl, Durable: n.FlushDurable}
	guard.Position, guard.AckedPosition, guard.WaitPosition = n.currentPosition, n.AckedPosition, n.WaitPosition
	guard.CheckRead, guard.CheckSession, guard.ReadSnapshot = n.checkReadSQL, n.checkReadSQL, n.captureReadSnapshot
	n.database.SetReplicationGuard(guard)
	n.database.ReplicationStorage().SetReplicationHooks(n.hooks())
	raftConfig := raft.DefaultConfig()
	if config.RaftConfig != nil {
		*raftConfig = *config.RaftConfig
	}
	raftConfig.ProtocolVersion = raft.ProtocolVersionMax
	raftConfig.LocalID = raft.ServerID(config.ID)
	raftConfig.Logger = logger
	raftConfig.TrailingLogs = 0
	raftConfig.NoSnapshotRestoreOnStart = false
	if config.LogsOnly {
		// Pre-vote keeps failed local campaigns from increasing the term.
		// The logs-only transport refuses both election RPCs and TimeoutNow.
		raftConfig.PreVoteDisabled = false
	}
	existing, e := raft.HasExistingState(n.store, n.store, n.snapshots)
	if e != nil {
		return nil, e
	}
	if !existing && config.Bootstrap {
		if e = raft.BootstrapCluster(raftConfig, n.store, n.store, n.snapshots, n.transport, raft.Configuration{Servers: []raft.Server{{ID: raft.ServerID(config.ID), Address: raft.ServerAddress(address.String()), Suffrage: raft.Voter}}}); e != nil {
			return nil, e
		}
	}
	if e = n.removeOrphanGenerations(); e != nil {
		return nil, e
	}
	if !fresh && n.async() {
		n.recovering.Store(true)
	} else {
		if e = n.startRaft(raftConfig); e != nil {
			return nil, e
		}
	}
	n.httpServer = &http.Server{Handler: n.handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	if config.TLSConfig != nil {
		controlListener = tls.NewListener(controlListener, config.TLSConfig)
	}
	n.workers.Add(1)
	go func() {
		defer n.workers.Done()
		if e := n.httpServer.Serve(controlListener); e != nil && !errors.Is(e, http.ErrServerClosed) {
			n.fail(e)
		}
	}()
	if n.recovering.Load() {
		n.workers.Add(1)
		go n.rejoinLoop(raftConfig)
	}
	if config.Join != "" && !n.recovering.Load() {
		peer := Peer{ID: config.ID, Address: address.String(), Control: control.String(), Cluster: n.cluster, AutoPromote: config.AutoPromote, NonVoting: config.NonVoting, LogsOnly: config.LogsOnly}
		if e = n.postJSON(ctx, config.Join, "/v1/join", peer, nil); e != nil {
			return nil, e
		}
	}
	return n, nil
}
func (n *Node) Database() *db.Database { return n.database }
func (n *Node) Address() string        { return n.address.String() }
func (n *Node) ControlAddress() string { return n.control.String() }
func (n *Node) failure() error {
	if f := n.failed.Load(); f != nil {
		return f.err
	}
	return nil
}
func (n *Node) fail(err error) {
	if err == nil || (n.closing.Load() && errors.Is(err, context.Canceled)) {
		return
	}
	if n.failed.CompareAndSwap(nil, &nodeFailure{err}) {
		n.readyTerm.Store(0)
		n.cancel()
	}
}
func (n *Node) hooks() *storage.ReplicationHooks {
	return &storage.ReplicationHooks{Write: func(batch []storage.Mutation, queued func()) error {
		return n.proposeWAL(batch, queued)
	}, PageSize: func(size int64) error {
		if e := storage.ValidatePageSize(size); e != nil {
			return e
		}
		return n.propose(command{Version: 1, Kind: "page-size", PageSize: size}, nil)
	}}
}
func (n *Node) notLeader() error {
	if n.raftNode() == nil {
		return ErrNotLeader
	}
	address, id := n.raftNode().LeaderWithID()
	return fmt.Errorf("%w (leader %s at %s)", ErrNotLeader, id, address)
}
func (n *Node) checkReadyLeader(ctx context.Context) (uint64, error) {
	if n.config.LogsOnly {
		return 0, ErrLogsOnly
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if n.recovering.Load() || n.raftNode() == nil {
		return 0, ErrRecovering
	}
	if e := n.failure(); e != nil {
		return 0, e
	}
	if n.closing.Load() {
		return 0, ErrClosed
	}
	term := n.readyTerm.Load()
	if term == 0 || n.raftNode().State() != raft.Leader || n.raftNode().CurrentTerm() != term {
		return 0, n.notLeader()
	}
	return term, nil
}

// Transaction control observes no database rows. Its commit writes still pass
// through propose, which waits for quorum replication and native application.
func (n *Node) checkSQLControl(ctx context.Context) error {
	if durableBarrier(n.readBarrier(ctx)) || durableBarrier(n.config.ReadBarrier) {
		return n.checkDurableReadHealth(ctx)
	}
	_, err := n.checkReadyLeader(ctx)
	if err != nil && n.config.ReplicaReadMaxStaleness > 0 {
		return n.checkReplicaReadRole(ctx)
	}
	return err
}

func (n *Node) checkSQL(ctx context.Context) error {
	term, err := n.checkReadyLeader(ctx)
	if err != nil {
		return err
	}
	// Read-index: capture the committed prefix before a fresh quorum check.
	// Leader admission already committed a fence in this term, so a successful
	// check proves this prefix includes all previously acknowledged writes.
	index := n.raftNode().CommitIndex()
	if e := n.wait(ctx, n.raftNode().VerifyLeader()); e != nil {
		return e
	}
	if e := n.waitApplied(ctx, index, term); e != nil {
		return e
	}
	if n.readyTerm.Load() != term || n.raftNode().State() != raft.Leader || n.raftNode().CurrentTerm() != term {
		return n.notLeader()
	}
	return nil
}
func (n *Node) wait(ctx context.Context, f raft.Future) error {
	done := make(chan error, 1)
	go func() { done <- f.Error() }()
	select {
	case e := <-done:
		return e
	case <-ctx.Done():
		return ctx.Err()
	case <-n.ctx.Done():
		if e := n.failure(); e != nil {
			return e
		}
		return ErrClosed
	}
}
func (n *Node) propose(c command, queued func()) error {
	return n.proposeWithGate(c, queued, false)
}

func (n *Node) proposeWithGate(c command, queued func(), gateHeld bool) error {
	if (c.Kind == "wal" || c.Kind == "page-size") && n.readyTerm.Load() != 0 {
		if err := n.checkDurabilityLag(); err != nil {
			if queued != nil {
				queued()
			}
			return err
		}
	}
	// Encoding uses caller-owned immutable buffers. Do it before the ordering
	// gate so independent transactions can encode concurrently. Each enqueue
	// and its catalog-release callback hold a shared proposal gate below.
	payload, e := marshalCommand(c)
	if e == nil && len(payload) > maxCommandBytes {
		e = fmt.Errorf("replicated WAL batch exceeds 64 MiB")
	}
	if e != nil {
		if queued != nil {
			queued()
		}
		return e
	}
	// Keep at most one large command unresolved so concurrent enqueues cannot
	// assemble a Raft batch containing many maximum-size transactions. Small
	// commands remain concurrent and can join the same natural commit group.
	largeHeld := !gateHeld && len(payload) > 64<<10
	if largeHeld {
		n.largeProposalMu.Lock()
		defer func() {
			if largeHeld {
				n.largeProposalMu.Unlock()
			}
		}()
	}
	// Raft sequences independent enqueues. SQL row/catalog/counter locks order
	// dependent writes, and each queued callback runs after its own enqueue.
	// Exclusive seals wait for both enqueues and callbacks to finish before
	// capturing their fence. Never enter the local storage writer queue here.
	if !gateHeld {
		n.proposalMu.RLock()
	}
	finish := func() {
		if queued != nil {
			queued()
		}
		if !gateHeld {
			n.proposalMu.RUnlock()
		}
	}
	if n.raftNode() == nil || n.raftNode().State() != raft.Leader {
		finish()
		return n.notLeader()
	}
	if e := n.failure(); e != nil {
		finish()
		return e
	}
	if n.closing.Load() {
		finish()
		return ErrClosed
	}
	if (c.Kind == "wal" || c.Kind == "page-size") && n.readyTerm.Load() != 0 {
		if err := n.checkDurabilityLag(); err != nil {
			finish()
			return err
		}
	}
	if (c.Kind == "wal" || c.Kind == "page-size") && n.writeTerm.Load() != n.raftNode().CurrentTerm() {
		finish()
		return n.notLeader()
	}
	future := n.raftNode().Apply(payload, n.config.OperationTimeout)
	finish()
	// Once enqueued a write may commit despite a cancelled caller or leadership
	// error. Wait for consensus resolution and fence SQL on an uncertain outcome.
	if e = future.Error(); e != nil {
		n.writeTerm.Store(0)
		n.readyTerm.Store(0)
		return e
	}
	if e, ok := future.Response().(error); ok {
		n.writeTerm.Store(0)
		n.readyTerm.Store(0)
		return e
	}
	if largeHeld {
		n.largeProposalMu.Unlock()
		largeHeld = false
	}
	if (c.Kind == "wal" || c.Kind == "page-size") && n.readyTerm.Load() != 0 && n.checkDurabilityLag() != nil {
		ctx, cancel := context.WithTimeout(n.ctx, n.config.OperationTimeout)
		err := n.flushDurable(ctx, false)
		cancel()
		if err != nil {
			n.readyTerm.Store(0)
			n.writeTerm.Store(0)
			return fmt.Errorf("transaction applied; stable confirmation unavailable: %w", err)
		}
	}
	return nil
}
func (n *Node) Apply(log *raft.Log) interface{} {
	n.fsmMu.Lock()
	defer n.fsmMu.Unlock()
	if e := n.failure(); e != nil {
		return e
	}
	if log.Index <= n.applied.Load() {
		return nil
	}
	var c command
	if e := unmarshalCommand(log.Data, &c); e != nil {
		n.fail(e)
		return e
	}
	if c.Version != 1 {
		e := fmt.Errorf("unsupported Raft command version")
		n.fail(e)
		return e
	}
	batch, err := n.commandMutations(c)
	if err != nil {
		n.fail(err)
		return err
	}
	return n.applyCheckpointWithBarrier(log.Index, log.Term, batch, c.Kind == "fence")
}
func (n *Node) applyCheckpoint(index, term uint64, batch []storage.Mutation) error {
	return n.applyCheckpointWithBarrier(index, term, batch, false)
}
func (n *Node) applyCheckpointWithBarrier(index, term uint64, batch []storage.Mutation, barrier bool) error {
	if n.checkpointJSONPrefix == nil {
		n.checkpointJSONPrefix = checkpointPrefix(n.cluster)
	}
	value := marshalCheckpoint(n.checkpointJSONPrefix, index, term)
	batch = append(batch, storage.Mutation{Key: appliedKey, Value: value})
	apply := n.database.ReplicationStorage().ApplyReplicated
	if n.async() {
		apply = n.database.ReplicationStorage().ApplyReplicatedDeferred
	}
	if e := apply(batch); e != nil {
		n.fail(e)
		return e
	}
	// Register exact historical versions before exposing the application
	// checkpoint to certificate receivers and waiters.
	if !n.async() || barrier {
		n.captureDurableView(index, term, !n.async())
	}
	n.appliedTerm.Store(term)
	n.applied.Store(index)
	n.publishPosition(index, term)
	n.notifyApplied()
	n.noteUnstable(index)
	return nil
}
func (n *Node) StoreConfiguration(index uint64, _ raft.Configuration) {
	n.fsmMu.Lock()
	defer n.fsmMu.Unlock()
	if n.failure() != nil || index <= n.applied.Load() {
		return
	}
	var log raft.Log
	if e := n.store.GetLog(index, &log); e != nil {
		n.fail(e)
		return
	}
	if e := n.applyCheckpoint(index, log.Term, nil); e == nil {
		n.configurationIndex.Store(index)
	}
}
func (n *Node) monitor() {
	defer n.workers.Done()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var wasLeader bool
	var lastPromotionScan time.Time
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
		}
		leader := n.raftNode().State() == raft.Leader
		if !leader {
			n.writeTerm.Store(0)
			n.readyTerm.Store(0)
			if wasLeader {
				_ = n.database.WithReplicaStopped(func() error { return nil })
			}
			wasLeader = false
			continue
		}
		wasLeader = true
		term := n.raftNode().CurrentTerm()
		if n.readyTerm.Load() == term {
			if time.Since(lastPromotionScan) >= time.Second {
				n.resumePromotions()
				lastPromotionScan = time.Now()
			}
			continue
		}
		n.readyTerm.Store(0)
		n.writeTerm.Store(0)
		if e := n.database.WithReplicaStopped(func() error { return nil }); e != nil {
			n.fail(e)
			return
		}
		if e := n.propose(command{Version: 1, Kind: "fence"}, nil); e != nil {
			continue
		}
		n.fsmMu.Lock()
		e := n.database.ReloadReplicaCatalog()
		n.fsmMu.Unlock()
		if e != nil {
			n.fail(e)
			return
		}
		n.writeTerm.Store(term)
		if e = n.database.CleanupReplicaIndexes(); e != nil {
			n.writeTerm.Store(0)
			if n.raftNode().State() == raft.Leader && n.raftNode().CurrentTerm() == term {
				n.fail(e)
				return
			}
			continue
		}
		// Publish every leader's control endpoint for discovery after restart.
		self := Peer{ID: n.config.ID, Address: n.Address(), Control: n.ControlAddress(), Cluster: n.cluster}
		if e = n.propose(command{Version: 1, Kind: "peer", Peer: &self}, nil); e != nil {
			n.writeTerm.Store(0)
			continue
		}
		// Snapshot the complete seed and any abandoned index cleanup before admitting SQL.
		if e := n.takeStableSnapshot(n.ctx); e != nil {
			n.durability.publication.Lock()
			n.durability.lastError = e.Error()
			n.durability.publication.Unlock()
			n.writeTerm.Store(0)
			continue
		}
		if n.raftNode().State() == raft.Leader && n.raftNode().CurrentTerm() == term && n.failure() == nil {
			n.readyTerm.Store(term)
			n.resumePromotions()
		}
	}
}
func (n *Node) WaitReady(ctx context.Context) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if n.readyTerm.Load() != 0 {
			if e := n.checkSQL(ctx); e == nil {
				return nil
			}
		}
		if e := n.failure(); e != nil {
			return e
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-n.ctx.Done():
			return ErrClosed
		case <-ticker.C:
		}
	}
}
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		n.workMu.Lock()
		n.closing.Store(true)
		n.readyTerm.Store(0)
		n.cancel()
		n.workMu.Unlock()
		if n.httpServer != nil {
			_ = n.httpServer.Close()
		}
		if n.raftNode() != nil {
			n.closeErr = errors.Join(n.closeErr, n.raftNode().Shutdown().Error())
		}
		if n.transport != nil {
			n.closeErr = errors.Join(n.closeErr, n.transport.Close())
		}
		n.workers.Wait()
		n.clearDurableViews()
		n.generationMu.Lock()
		n.closeErr = errors.Join(n.closeErr, n.retirementErr)
		n.generationMu.Unlock()
		if n.database != nil {
			n.closeErr = errors.Join(n.closeErr, n.database.Close())
		}
		if n.store != nil {
			n.closeErr = errors.Join(n.closeErr, n.store.tree.Close())
		}
		if n.rootLock != nil {
			n.closeErr = errors.Join(n.closeErr, n.rootLock.Close())
		}
	})
	return n.closeErr
}

var _ raft.FSM = (*Node)(nil)
var _ raft.ConfigurationStore = (*Node)(nil)
var _ io.Closer = (*Node)(nil)
var _ net.Addr = endpoint{}

// Done closes on shutdown or fatal application failure.
func (n *Node) Done() <-chan struct{} { return n.ctx.Done() }
func (n *Node) Err() error            { return n.failure() }
