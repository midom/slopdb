package replication

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/hashicorp/raft"
	"slopdb/storage"
)

var appliedKey = []byte("\x00\xffslopdb:raft:applied")

const snapshotMagic = "SLOPRAFT1\n"
const maxRecord = 256 << 20

type checkpoint struct {
	Cluster     string
	Index, Term uint64
}
type snapshotHeader struct {
	Durability  Durability          `json:",omitempty"`
	Certificate *durableCertificate `json:",omitempty"`
	Checkpoint  checkpoint
	PageSize    int64
}
type stateSnapshot struct {
	node     *Node
	tree     *storage.LSMTree
	seq      uint64
	iterator storage.Iterator
	header   snapshotHeader
	once     sync.Once
}

func (n *Node) Snapshot() (raft.FSMSnapshot, error) {
	n.fsmMu.Lock()
	defer n.fsmMu.Unlock()
	if err := n.failure(); err != nil {
		return nil, err
	}
	if n.recovering.Load() {
		return nil, ErrRecovering
	}
	if n.async() && n.applied.Load() > n.durableIndex() {
		return nil, fmt.Errorf("snapshot is ahead of the durable frontier")
	}
	tree := n.database.ReplicationStorage()
	n.generationMu.Lock()
	n.snapshotRefs[tree]++
	n.generationMu.Unlock()
	seq := tree.Snapshot()
	it := tree.NewIteratorAt(seq)
	return &stateSnapshot{node: n, tree: tree, seq: seq, iterator: it, header: snapshotHeader{Checkpoint: checkpoint{n.cluster, n.applied.Load(), n.appliedTerm.Load()}, PageSize: tree.PageSize(), Durability: n.config.Durability, Certificate: n.currentCertificate()}}, nil
}
func (s *stateSnapshot) Release() {
	s.once.Do(func() { s.iterator.Close(); s.tree.ReleaseSnapshot(s.seq); s.node.releaseSnapshotTree(s.tree) })
}
func (s *stateSnapshot) Persist(sink raft.SnapshotSink) (err error) {
	defer func() {
		if err != nil {
			_ = sink.Cancel()
		}
	}()
	if err = s.write(sink); err != nil {
		return err
	}
	return sink.Close()
}
func (s *stateSnapshot) write(dst io.Writer) error {
	hash := sha256.New()
	w := bufio.NewWriterSize(io.MultiWriter(dst, hash), 64<<10)
	if _, err := io.WriteString(w, snapshotMagic); err != nil {
		return err
	}
	header, err := json.Marshal(s.header)
	if err != nil {
		return err
	}
	if err = writeFrame(w, header, nil); err != nil {
		return err
	}
	for s.iterator.Valid() {
		if err = s.node.ctx.Err(); err != nil {
			return err
		}
		if err = writeFrame(w, s.iterator.Key().UserKey, s.iterator.Value()); err != nil {
			return err
		}
		s.iterator.Next()
	}
	if err = storage.IteratorError(s.iterator); err != nil {
		return err
	}
	// A zero key terminates the sorted stream. SQL/storage keys are never empty.
	if err = writeFrame(w, nil, nil); err != nil {
		return err
	}
	if err = w.Flush(); err != nil {
		return err
	}
	_, err = dst.Write(hash.Sum(nil))
	return err
}
func writeFrame(w io.Writer, k, v []byte) error {
	if len(k)+len(v) > maxRecord {
		return fmt.Errorf("snapshot record exceeds %d bytes", maxRecord)
	}
	var b [12]byte
	binary.BigEndian.PutUint32(b[:4], uint32(len(k)))
	binary.BigEndian.PutUint64(b[4:], uint64(len(v)))
	if _, e := w.Write(b[:]); e != nil {
		return e
	}
	if _, e := w.Write(k); e != nil {
		return e
	}
	_, e := w.Write(v)
	return e
}
func readFrame(r io.Reader) ([]byte, []byte, error) {
	var b [12]byte
	if _, e := io.ReadFull(r, b[:]); e != nil {
		return nil, nil, e
	}
	k, v := uint64(binary.BigEndian.Uint32(b[:4])), binary.BigEndian.Uint64(b[4:])
	if k > maxRecord || v > maxRecord || k+v > maxRecord {
		return nil, nil, fmt.Errorf("oversized snapshot record")
	}
	data := make([]byte, int(k+v))
	if _, e := io.ReadFull(r, data); e != nil {
		return nil, nil, e
	}
	return data[:int(k)], data[int(k):], nil
}

// decodeSnapshot writes only to an unpublished generation, verifies its complete
// hash and checkpoint, and closes it durably before it can become CURRENT.
func (n *Node) decodeSnapshot(src io.Reader) (directory string, h snapshotHeader, err error) {
	hash := sha256.New()
	r := io.TeeReader(src, hash)
	magic := make([]byte, len(snapshotMagic))
	if _, err = io.ReadFull(r, magic); err != nil {
		return
	}
	if string(magic) != snapshotMagic {
		err = fmt.Errorf("unsupported snapshot format")
		return
	}
	k, v, e := readFrame(r)
	if e != nil {
		err = e
		return
	}
	if len(k) > 65536 || len(v) != 0 {
		err = fmt.Errorf("invalid snapshot header")
		return
	}
	if err = json.Unmarshal(k, &h); err != nil {
		return
	}
	if h.Checkpoint.Cluster != n.cluster || h.Checkpoint.Index == 0 {
		err = fmt.Errorf("snapshot belongs to another cluster or has no committed state")
		return
	}
	if n.async() {
		if h.Durability != DurabilityAsync {
			return directory, h, ErrHistory
		}
		if err = validateCertificate(h.Certificate, n.config.Token, n.cluster); err != nil {
			return
		}
		if h.Certificate.Body.Index != h.Checkpoint.Index || h.Certificate.Body.Term != h.Checkpoint.Term {
			err = ErrHistory
			return
		}
	} else if h.Durability == DurabilityAsync {
		err = ErrHistory
		return
	}
	if err = storage.ValidatePageSize(h.PageSize); err != nil {
		return
	}
	directory, err = os.MkdirTemp(n.config.Directory, "data-")
	if err != nil {
		return
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.RemoveAll(directory)
		}
	}()
	options := n.storageOptions()
	options.PageSize = h.PageSize
	tree, e := storage.NewLSMTree(directory, options)
	if e != nil {
		err = e
		return
	}
	closed := false
	defer func() {
		if !closed {
			_ = tree.Close()
		}
	}()
	batch := make([]storage.Mutation, 0, 1024)
	size := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		e := tree.ApplyReplicated(batch)
		batch = nil
		size = 0
		return e
	}
	var previous []byte
	var marker []byte
	for {
		if err = n.ctx.Err(); err != nil {
			return
		}
		k, v, e = readFrame(r)
		if e != nil {
			err = e
			return
		}
		if len(k) == 0 {
			if len(v) != 0 {
				err = fmt.Errorf("invalid snapshot terminator")
				return
			}
			break
		}
		if previous != nil && bytes.Compare(previous, k) >= 0 {
			err = fmt.Errorf("unordered snapshot keys")
			return
		}
		previous = k
		if bytes.Equal(k, appliedKey) {
			marker = bytes.Clone(v)
		}
		batch = append(batch, storage.Mutation{Key: k, Value: v})
		size += len(k) + len(v)
		if len(batch) >= 1024 || size >= 4<<20 {
			if err = flush(); err != nil {
				return
			}
		}
	}
	sum := hash.Sum(nil)
	var expected [sha256.Size]byte
	if _, err = io.ReadFull(src, expected[:]); err != nil {
		return
	}
	if !bytes.Equal(sum, expected[:]) {
		err = fmt.Errorf("snapshot SHA256 mismatch")
		return
	}
	var extra [1]byte
	if count, e := src.Read(extra[:]); count != 0 || e != io.EOF {
		err = fmt.Errorf("trailing snapshot data")
		return
	}
	var cp checkpoint
	if err = json.Unmarshal(marker, &cp); err != nil {
		return
	}
	if cp != h.Checkpoint {
		err = fmt.Errorf("snapshot checkpoint mismatch")
		return
	}
	if err = flush(); err != nil {
		return
	}
	if err = tree.Close(); err != nil {
		return
	}
	closed = true
	if err = syncDir(directory); err != nil {
		return
	}
	keep = true
	return
}
func (n *Node) Restore(reader io.ReadCloser) (err error) {
	defer reader.Close()
	directory, h, err := n.decodeSnapshot(reader)
	if err != nil {
		n.fail(err)
		return err
	}
	// An old InstallSnapshot can finish arriving after an out-of-band re-clone.
	// Reject it without failing the node or rewinding its acknowledged prefix.
	if n.async() && h.Checkpoint.Index < n.applied.Load() {
		_ = os.RemoveAll(directory)
		return fmt.Errorf("snapshot is older than the applied prefix")
	}
	n.readyTerm.Store(0)
	// Quiesce before taking fsmMu: an old SQL commit can be waiting on Apply.
	if err = n.database.WithReplicaStopped(func() error { return nil }); err != nil {
		n.fail(err)
		return err
	}
	options := n.storageOptions()
	tree, err := storage.NewLSMTree(directory, options)
	if err != nil {
		n.fail(err)
		return err
	}
	tree.SetReplicationHooks(n.hooks())
	n.fsmMu.Lock()
	if n.async() {
		if err = n.store.installSnapshotAnchor(h.Checkpoint.Index, h.Certificate.Body.Hash); err == nil {
			err = n.installCertificate(h.Certificate)
		}
		if err != nil {
			n.fsmMu.Unlock()
			_ = tree.Close()
			n.fail(err)
			return err
		}
		n.advanceDurable(h.Certificate)
		n.configurationIndex.Store(h.Certificate.Body.ConfigurationIndex)
	}
	old, oldTree := n.dataDirectory, n.database.ReplicationStorage()
	err = n.database.ReplaceReplicaStorage(tree, directory, func() error { return publishGeneration(n.config.Directory, directory) })
	adopted := n.database.ReplicationStorage() == tree
	if adopted {
		n.dataDirectory = directory
		n.applied.Store(h.Checkpoint.Index)
		n.appliedTerm.Store(h.Checkpoint.Term)
	}
	n.fsmMu.Unlock()
	if adopted {
		n.retireTree(oldTree, old)
	} else {
		_ = tree.Close()
	}
	if err != nil {
		n.fail(err)
		return err
	}

	return nil
}

// Snapshot export includes Raft membership metadata but never local stable state.
func writeSnapshotEnvelope(w io.Writer, meta *raft.SnapshotMeta, src io.Reader) error {
	b, e := json.Marshal(meta)
	if e != nil {
		return e
	}
	if len(b) > 1<<20 {
		return fmt.Errorf("snapshot metadata too large")
	}
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(b)))
	if _, e = w.Write(length[:]); e != nil {
		return e
	}
	if _, e = w.Write(b); e != nil {
		return e
	}
	digest := sha256.Sum256(b)
	if _, e = w.Write(digest[:]); e != nil {
		return e
	}
	_, e = io.Copy(w, src)
	return e
}
func readSnapshotEnvelope(r io.Reader) (*raft.SnapshotMeta, error) {
	var b [4]byte
	if _, e := io.ReadFull(r, b[:]); e != nil {
		return nil, e
	}
	size := binary.BigEndian.Uint32(b[:])
	if size > 1<<20 {
		return nil, fmt.Errorf("snapshot metadata too large")
	}
	data := make([]byte, int(size))
	if _, e := io.ReadFull(r, data); e != nil {
		return nil, e
	}
	var expected [sha256.Size]byte
	if _, e := io.ReadFull(r, expected[:]); e != nil {
		return nil, e
	}
	if digest := sha256.Sum256(data); digest != expected {
		return nil, fmt.Errorf("snapshot metadata SHA256 mismatch")
	}
	var meta raft.SnapshotMeta
	if e := json.Unmarshal(data, &meta); e != nil {
		return nil, e
	}
	if meta.Version != 1 || meta.Index == 0 || meta.Term == 0 || meta.ConfigurationIndex > meta.Index {
		return nil, fmt.Errorf("invalid Raft snapshot metadata")
	}
	return &meta, nil
}
func (n *Node) installClone(r io.Reader) error {
	meta, err := readSnapshotEnvelope(r)
	if err != nil {
		return err
	}
	// Tee to a private file; the native generation is fully validated first.
	f, err := os.CreateTemp(n.config.Directory, ".clone-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	directory, h, err := n.decodeSnapshot(io.TeeReader(r, f))
	if err != nil {
		return err
	}
	defer func() {
		if directory != "" {
			_ = os.RemoveAll(directory)
		}
	}()
	if h.Checkpoint.Index != meta.Index || h.Checkpoint.Term != meta.Term {
		return fmt.Errorf("Raft metadata/checkpoint mismatch")
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	sink, err := n.snapshots.Create(meta.Version, meta.Index, meta.Term, meta.Configuration, meta.ConfigurationIndex, n.transport)
	if err != nil {
		return err
	}
	if _, err = io.Copy(sink, f); err != nil {
		_ = sink.Cancel()
		return err
	}
	if err = sink.Close(); err != nil {
		return err
	}
	if n.async() {
		if err = n.store.resetLogs(h.Checkpoint.Index, h.Certificate.Body.Hash); err != nil {
			return err
		}
		if err = n.installCertificate(h.Certificate); err != nil {
			return err
		}
		n.advanceDurable(h.Certificate)
		n.configurationIndex.Store(h.Certificate.Body.ConfigurationIndex)
	}
	if err = publishGeneration(n.config.Directory, directory); err != nil {
		return err
	}
	directory = ""
	return nil
}
func readCheckpoint(tree *storage.LSMTree, cluster string) (checkpoint, error) {
	b, ok, e := tree.Get(appliedKey)
	if e != nil || !ok {
		return checkpoint{Cluster: cluster}, e
	}
	var cp checkpoint
	if e = json.Unmarshal(b, &cp); e != nil {
		return cp, e
	}
	if cp.Cluster != cluster {
		return cp, fmt.Errorf("database belongs to another Raft cluster")
	}
	return cp, nil
}
func (n *Node) removeOrphanGenerations() error {
	entries, e := os.ReadDir(n.config.Directory)
	if e != nil {
		return e
	}
	for _, entry := range entries {
		if entry.IsDir() && entry.Name() != filepath.Base(n.dataDirectory) && len(entry.Name()) > 5 && entry.Name()[:5] == "data-" {
			if e = os.RemoveAll(filepath.Join(n.config.Directory, entry.Name())); e != nil {
				return e
			}
		}
	}
	return nil
}

// Snapshot readers retain their generation without holding a lock across Raft's
// configuration request. Restore must never wait for Snapshot.Release: the Raft
// main loop can be waiting for Restore while a snapshot awaits that same loop.
func (n *Node) releaseSnapshotTree(tree *storage.LSMTree) {
	n.generationMu.Lock()
	n.snapshotRefs[tree]--
	if n.snapshotRefs[tree] > 0 {
		n.generationMu.Unlock()
		return
	}
	delete(n.snapshotRefs, tree)
	directory, retired := n.retired[tree]
	delete(n.retired, tree)
	n.generationMu.Unlock()
	if retired {
		n.closeRetiredTree(tree, directory)
	}
}
func (n *Node) retireTree(tree *storage.LSMTree, directory string) {
	n.generationMu.Lock()
	if n.snapshotRefs[tree] > 0 {
		n.retired[tree] = directory
		n.generationMu.Unlock()
		return
	}
	n.generationMu.Unlock()
	n.closeRetiredTree(tree, directory)
}
func (n *Node) closeRetiredTree(tree *storage.LSMTree, directory string) {
	err := tree.Close()
	if err == nil {
		err = os.RemoveAll(directory)
	}
	if err != nil {
		n.generationMu.Lock()
		n.retirementErr = errors.Join(n.retirementErr, err)
		n.generationMu.Unlock()
		n.fail(err)
	}
}
