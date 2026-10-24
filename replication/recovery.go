package replication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"slopdb/db"
	"slopdb/storage"
)

func physicalMutations(tree *storage.LSMTree, c command) ([]storage.Mutation, error) {
	if c.Version != 1 {
		return nil, fmt.Errorf("unsupported Raft command version")
	}
	switch c.Kind {
	case "wal":
		for _, m := range c.Writes {
			if len(m.Key) == 0 || bytes.Equal(m.Key, appliedKey) || (m.RangeDelete && (len(m.Value) == 0 || bytes.Compare(m.Key, m.Value) >= 0)) {
				return nil, fmt.Errorf("invalid replicated WAL mutation")
			}
		}
		return c.Writes, nil
	case "fence":
		if len(c.Writes) != 0 {
			return nil, fmt.Errorf("invalid fence command")
		}
		return nil, nil
	case "page-size":
		return nil, tree.SetReplicatedPageSize(c.PageSize)
	case "peer":
		if c.Peer == nil {
			return nil, fmt.Errorf("invalid peer command")
		}
		value, e := json.Marshal(c.Peer)
		return []storage.Mutation{{Key: peerKey(c.Peer.ID), Value: value}}, e
	default:
		return nil, fmt.Errorf("unknown Raft command %q", c.Kind)
	}
}

// RecoveryConfig is an explicit whole-cluster disaster recovery operation.
// Sources must be offline directories (or complete offline copies) from every
// voter in the selected certificate. Fenced asserts that all original processes
// are stopped/excluded; local locks alone cannot fence a remote original copy.
type RecoveryConfig struct {
	Sources            []string
	Destination, Token string
	Fenced             bool
	Minimum            *DurabilityToken
}
type RecoveryResult struct {
	Version                   int
	RecoveryID, SourceCluster string
	Stable                    DurabilityToken
	Voters                    []string
	Destination               string
	StateSHA256               string
}
type recoverySource struct {
	directory   string
	metadata    nodeMetadata
	lock        *os.File
	store       *diskStore
	snapshots   *raft.FileSnapshotStore
	certificate *durableCertificate
	err         error
}

// RecoverStable rebuilds a standalone seed at a certified stable prefix. It
// preserves source artifacts, retires the supplied node roots, and never reuses
// their Raft votes/logs/identity. Bootstrap a new node directory from the seed;
// its new cluster ID fences the old incarnation and invalidates old tokens.
func RecoverStable(ctx context.Context, config RecoveryConfig) (result RecoveryResult, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !config.Fenced {
		return result, fmt.Errorf("stable-point recovery requires all original members to be fenced")
	}
	if len(config.Sources) == 0 || len(config.Token) < 16 || config.Destination == "" {
		return result, fmt.Errorf("recovery sources, destination and cluster token are required")
	}
	destination, e := filepath.Abs(config.Destination)
	if e != nil {
		return result, e
	}
	parent, e := filepath.EvalSymlinks(filepath.Dir(destination))
	if e != nil {
		return result, e
	}
	destination = filepath.Join(parent, filepath.Base(destination))
	var sources []*recoverySource
	defer func() {
		for _, source := range sources {
			if source.store != nil {
				err = errors.Join(err, source.store.tree.Close())
			}
			if source.lock != nil {
				err = errors.Join(err, source.lock.Close())
			}
		}
	}()
	seen := map[string]bool{}
	var cluster string
	var best *durableCertificate
	for _, path := range config.Sources {
		if e = ctx.Err(); e != nil {
			return result, e
		}
		directory, e := filepath.Abs(path)
		if e != nil {
			return result, e
		}
		directory, e = filepath.EvalSymlinks(directory)
		if e != nil {
			return result, e
		}
		relative, e := filepath.Rel(directory, destination)
		if e != nil {
			return result, e
		}
		if relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(os.PathSeparator))) {
			return result, fmt.Errorf("recovery destination must be outside every source")
		}
		source := &recoverySource{directory: directory}
		sources = append(sources, source)
		source.lock, e = os.OpenFile(filepath.Join(directory, "LOCK"), os.O_RDWR, 0600)
		if e != nil {
			return result, e
		}
		if e = syscall.Flock(int(source.lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
			return result, fmt.Errorf("recovery source is in use: %s: %w", directory, e)
		}
		value, e := os.ReadFile(filepath.Join(directory, "node.json"))
		if e != nil {
			return result, e
		}
		if e = json.Unmarshal(value, &source.metadata); e != nil {
			return result, e
		}
		if source.metadata.Version != 1 || source.metadata.ID == "" || source.metadata.Durability != DurabilityAsync {
			return result, fmt.Errorf("source is not an asynchronous node")
		}
		if seen[source.metadata.ID] {
			return result, fmt.Errorf("duplicate recovery node ID")
		}
		seen[source.metadata.ID] = true
		if cluster == "" {
			cluster = source.metadata.Cluster
		}
		if source.metadata.Cluster != cluster {
			return result, ErrHistory
		}
		options := storage.DefaultOptions()
		options.CreateIfNotExist = false
		options.BlockCacheSize = 4 << 20
		options.MaxOpenFiles = 32
		tree, e := storage.NewLSMTree(filepath.Join(directory, "raft-log"), options)
		if e != nil {
			source.err = e
			continue
		}
		source.store = &diskStore{tree: tree, async: true}
		value, found, e := tree.Get(certificateKey)
		if e != nil || !found {
			source.err = fmt.Errorf("missing durability certificate: %v", e)
			continue
		}
		var certificate durableCertificate
		if e = json.Unmarshal(value, &certificate); e != nil {
			source.err = e
			continue
		}
		if e = validateCertificate(&certificate, config.Token, cluster); e != nil {
			return result, e
		}
		source.certificate = &certificate
		if best == nil || certificate.Body.Index > best.Body.Index {
			best = &certificate
		} else if certificate.Body.Index == best.Body.Index && certificate.Body.Hash != best.Body.Hash {
			return result, ErrHistory
		}
		if info, e := os.Stat(filepath.Join(directory, "snapshots")); e != nil || !info.IsDir() {
			source.err = fmt.Errorf("missing consensus snapshots")
			continue
		}
		source.snapshots, e = raft.NewFileSnapshotStoreWithLogger(directory, 2, hclog.NewNullLogger())
		if e != nil {
			source.err = e
		}
	}
	if best == nil {
		return result, fmt.Errorf("no valid stable-point certificate")
	}
	for _, member := range best.Body.Configuration.Servers {
		if member.Suffrage == raft.Voter {
			if !seen[string(member.ID)] {
				return result, fmt.Errorf("recovery requires offline sources for every voter; missing %s", member.ID)
			}
			result.Voters = append(result.Voters, string(member.ID))
		}
	}
	result.Version = 1
	result.RecoveryID = randomID()
	result.SourceCluster = cluster
	result.Stable = DurabilityToken{cluster, best.Body.Index, best.Body.Term}
	result.Destination = destination
	if config.Minimum != nil && (config.Minimum.Cluster != cluster || config.Minimum.Index > best.Body.Index || (config.Minimum.Index == best.Body.Index && config.Minimum.Term != best.Body.Term)) {
		return result, fmt.Errorf("available stable prefix is behind the required token")
	}
	// A completed export can be finalized again after a crash during retirement.
	if _, e = os.Lstat(destination); e == nil {
		value, e := os.ReadFile(filepath.Join(destination, "RECOVERED_FROM.json"))
		if e != nil {
			return result, fmt.Errorf("recovery destination exists")
		}
		var previous RecoveryResult
		if e = json.Unmarshal(value, &previous); e != nil {
			return result, e
		}
		if previous.Version != 1 || previous.RecoveryID == "" || previous.Destination != destination || previous.SourceCluster != cluster || previous.Stable != result.Stable || !reflect.DeepEqual(previous.Voters, result.Voters) {
			return result, fmt.Errorf("existing recovery has different lineage")
		}
		digest, e := recoveryStateDigest(ctx, destination)
		if e != nil {
			return result, e
		}
		if previous.StateSHA256 == "" || digest != previous.StateSHA256 {
			return result, fmt.Errorf("existing recovery seed has changed")
		}
		if e = syncDir(parent); e != nil {
			return result, e
		}
		result = previous
		if err = retireRecoverySources(sources, result); err != nil {
			return result, err
		}
		return result, nil
	} else if !os.IsNotExist(e) {
		return result, e
	}
	stage, e := os.MkdirTemp(parent, ".slopdb-recovery-")
	if e != nil {
		return result, e
	}
	defer os.RemoveAll(stage)
	var dataDirectory string
	var reconstructionErrors []error
	for _, source := range sources {
		if source.err != nil || source.snapshots == nil {
			reconstructionErrors = append(reconstructionErrors, source.err)
			continue
		}
		dataDirectory, e = reconstructPrefix(ctx, source, best, config.Token, stage)
		if e == nil {
			break
		}
		reconstructionErrors = append(reconstructionErrors, fmt.Errorf("%s: %w", source.metadata.ID, e))
	}
	if dataDirectory == "" {
		return result, fmt.Errorf("cannot reconstruct certified prefix: %w", errors.Join(reconstructionErrors...))
	}
	// Remove replication-private state so the result is a standalone seed, not an
	// accidental copy of an old consensus member or incarnation.
	if e = stripReplicationState(dataDirectory); e != nil {
		return result, e
	}
	restored, e := db.Open(dataDirectory, nil)
	if e != nil {
		return result, e
	}
	_, e = restored.CheckIntegrityContext(ctx)
	closeErr := restored.Close()
	if e != nil {
		return result, e
	}
	if closeErr != nil {
		return result, closeErr
	}
	result.StateSHA256, e = recoveryStateDigest(ctx, dataDirectory)
	if e != nil {
		return result, e
	}
	value, _ := json.Marshal(result)
	if e = atomicFile(filepath.Join(dataDirectory, "RECOVERED_FROM.json"), value); e != nil {
		return result, e
	}
	// Reserve the export name among cooperating recovery invocations. Rename to
	// a new directory publishes only the complete, synchronized standalone seed.
	reservation, e := os.OpenFile(destination+".slopdb-recovery-lock", os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return result, e
	}
	defer reservation.Close()
	// Leave the coordination file in place: unlinking a flock file can let a
	// third invocation acquire a different inode while another still holds it.
	if e = syscall.Flock(int(reservation.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		return result, fmt.Errorf("recovery destination is in use: %w", e)
	}
	if _, e = os.Lstat(destination); e == nil {
		return result, fmt.Errorf("recovery destination exists")
	} else if !os.IsNotExist(e) {
		return result, e
	}
	if e = os.Rename(dataDirectory, destination); e != nil {
		return result, e
	}
	if e = syncDir(parent); e != nil {
		return result, e
	}
	if err = retireRecoverySources(sources, result); err != nil {
		return result, err
	}
	return result, nil
}

// Hash logical state rather than file bytes: reopening or compacting an export
// may change its files without changing the seed. Length framing preserves key
// and value boundaries, and the physical page target is part of the export.
func recoveryStateDigest(ctx context.Context, directory string) (digest string, err error) {
	options := storage.DefaultOptions()
	options.CreateIfNotExist = false
	tree, err := storage.NewLSMTree(directory, options)
	if err != nil {
		return "", err
	}
	defer func() { err = errors.Join(err, tree.Close()) }()
	hash := sha256.New()
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(tree.PageSize()))
	hash.Write(length[:])
	iterator := tree.NewIterator()
	defer iterator.Close()
	for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		if err = ctx.Err(); err != nil {
			return "", err
		}
		for _, value := range [][]byte{iterator.Key().UserKey, iterator.Value()} {
			binary.BigEndian.PutUint64(length[:], uint64(len(value)))
			hash.Write(length[:])
			hash.Write(value)
		}
	}
	if err = storage.IteratorError(iterator); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
func retireRecoverySources(sources []*recoverySource, result RecoveryResult) error {
	value, _ := json.Marshal(result)
	for _, source := range sources {
		if err := atomicFile(filepath.Join(source.directory, "RETIRED"), value); err != nil {
			return err
		}
	}
	return nil
}

func reconstructPrefix(ctx context.Context, source *recoverySource, certificate *durableCertificate, token, stage string) (directory string, err error) {
	list, err := source.snapshots.List()
	if err != nil {
		return "", err
	}
	var meta *raft.SnapshotMeta
	var reader io.ReadCloser
	for _, candidate := range list {
		if candidate.Index <= certificate.Body.Index {
			meta, reader, err = source.snapshots.Open(candidate.ID)
			if err == nil {
				break
			}
		}
	}
	if reader == nil {
		return "", fmt.Errorf("no snapshot at or before the stable prefix")
	}
	defer reader.Close()
	owner := &Node{config: Config{Directory: stage, Token: token, Durability: DurabilityAsync}, cluster: certificate.Body.Cluster, ctx: ctx}
	directory, header, err := owner.decodeSnapshot(reader)
	if err != nil {
		return "", err
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.RemoveAll(directory)
			directory = ""
		}
	}()
	if header.Checkpoint.Index != meta.Index || header.Checkpoint.Term != meta.Term {
		return directory, fmt.Errorf("snapshot checkpoint %d/%d differs from metadata %d/%d: %w", header.Checkpoint.Index, header.Checkpoint.Term, meta.Index, meta.Term, ErrHistory)
	}
	tree, err := storage.NewLSMTree(directory, storage.DefaultOptions())
	if err != nil {
		return directory, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = tree.Close()
		}
	}()
	previous := header.Certificate.Body.Hash
	configuration := meta.Configuration
	configurationIndex := meta.ConfigurationIndex
	for index := meta.Index + 1; index <= certificate.Body.Index; index++ {
		if err = ctx.Err(); err != nil {
			return
		}
		var log raft.Log
		if err = source.store.GetLog(index, &log); err != nil {
			return
		}
		previous = chainedLogHash(previous, &log)
		expected, e := source.store.historyHash(index)
		if e != nil {
			err = e
			return
		}
		if previous != expected {
			err = fmt.Errorf("recovery log hash mismatch at index %d: %w", index, ErrHistory)
			return
		}
		var batch []storage.Mutation
		switch log.Type {
		case raft.LogCommand:
			var command command
			if err = unmarshalCommand(log.Data, &command); err != nil {
				return
			}
			batch, err = physicalMutations(tree, command)
			if err != nil {
				return
			}
		case raft.LogConfiguration:
			configuration, err = decodeRecoveryConfiguration(log.Data)
			if err != nil {
				return
			}
			configurationIndex = index
		case raft.LogNoop, raft.LogBarrier:
		default:
			err = fmt.Errorf("unsupported recovery log type")
			return
		}
		cp, _ := json.Marshal(checkpoint{certificate.Body.Cluster, index, log.Term})
		batch = append(batch, storage.Mutation{Key: appliedKey, Value: cp})
		if err = tree.ApplyReplicatedDeferred(batch); err != nil {
			return
		}
	}
	if previous != certificate.Body.Hash || configurationIndex != certificate.Body.ConfigurationIndex || !reflect.DeepEqual(configuration, certificate.Body.Configuration) {
		err = fmt.Errorf("replayed prefix hash %x expected %x; configuration index %d expected %d; config equal=%v: %w", previous, certificate.Body.Hash, configurationIndex, certificate.Body.ConfigurationIndex, reflect.DeepEqual(configuration, certificate.Body.Configuration), ErrHistory)
		return
	}
	if err = tree.Close(); err != nil {
		return
	}
	closed = true
	keep = true
	return
}

// Raft's configuration decoder panics on malformed MsgPack. Recovery must
// return an error and preserve the original evidence instead of crashing.
func decodeRecoveryConfiguration(value []byte) (configuration raft.Configuration, err error) {
	defer func() {
		if failure := recover(); failure != nil {
			err = fmt.Errorf("invalid recovery configuration: %v: %w", failure, ErrHistory)
		}
	}()
	return raft.DecodeConfiguration(value), nil
}

// stripReplicationState operates only on an owned copy, never the source.
func stripReplicationState(directory string) error {
	tree, e := storage.NewLSMTree(directory, storage.DefaultOptions())
	if e != nil {
		return e
	}
	prefix := []byte("\x00\xffslopdb:raft:")
	for {
		iterator := tree.NewIterator()
		iterator.Seek(prefix)
		batch := make([]storage.Mutation, 0, 1024)
		for iterator.Valid() && bytes.HasPrefix(iterator.Key().UserKey, prefix) && len(batch) < 1024 {
			batch = append(batch, storage.Mutation{Key: bytes.Clone(iterator.Key().UserKey), Delete: true})
			iterator.Next()
		}
		e = storage.IteratorError(iterator)
		iterator.Close()
		if e != nil {
			_ = tree.Close()
			return e
		}
		if len(batch) == 0 {
			break
		}
		if e = tree.ApplyReplicatedDeferred(batch); e != nil {
			_ = tree.Close()
			return e
		}
	}
	return tree.Close()
}
