package replication

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sync"

	"github.com/hashicorp/raft"
	"slopdb/storage"
)

// The Raft log and votes live in a separate, synchronously written native LSM.
// Application snapshots never copy this node's term, vote or identity.
type diskStore struct {
	tree  *storage.LSMTree
	async bool
	mu    sync.Mutex
}

func logKey(i uint64) []byte { return binary.BigEndian.AppendUint64([]byte{'z'}, i) }
func (s *diskStore) FirstIndex() (uint64, error) {
	it := s.tree.NewIterator()
	defer it.Close()
	it.Seek([]byte{'z'})
	if err := storage.IteratorError(it); err != nil {
		return 0, err
	}
	if !it.Valid() || len(it.Key().UserKey) != 9 || it.Key().UserKey[0] != 'z' {
		return 0, nil
	}
	return binary.BigEndian.Uint64(it.Key().UserKey[1:]), nil
}
func (s *diskStore) LastIndex() (uint64, error) {
	it := s.tree.NewIterator()
	defer it.Close()
	if last, ok := it.(interface{ SeekToLast() }); ok {
		last.SeekToLast()
	} else {
		return 0, storage.IteratorError(it)
	}
	if err := storage.IteratorError(it); err != nil {
		return 0, err
	}
	if !it.Valid() || len(it.Key().UserKey) != 9 || it.Key().UserKey[0] != 'z' {
		return 0, nil
	}
	return binary.BigEndian.Uint64(it.Key().UserKey[1:]), nil
}
func (s *diskStore) GetLog(i uint64, l *raft.Log) error {
	b, ok, e := s.tree.Get(logKey(i))
	if e != nil {
		return e
	}
	if !ok {
		return raft.ErrLogNotFound
	}
	if e = decodeLog(b, l); e != nil {
		return e
	}
	if l.Index != i {
		return fmt.Errorf("corrupt Raft log index")
	}
	return nil
}
func (s *diskStore) StoreLog(l *raft.Log) error { return s.StoreLogs([]*raft.Log{l}) }
func (s *diskStore) StoreLogs(logs []*raft.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	batch := make([]storage.Mutation, 0, 2*len(logs))
	syncNow := !s.async
	var previous [32]byte
	for i, log := range logs {
		value, err := encodeLog(log)
		if err != nil {
			return err
		}
		batch = append(batch, storage.Mutation{Key: logKey(log.Index), Value: value})
		if s.async {
			if i == 0 || logs[i-1].Index+1 != log.Index {
				previous, err = s.hashAt(log.Index - 1)
				if err != nil {
					return err
				}
			}
			previous = chainedLogHash(previous, log)
			batch = append(batch, storage.Mutation{Key: hashKey(log.Index), Value: bytes.Clone(previous[:])})
			if log.Type == raft.LogConfiguration {
				syncNow = true
			}
		}
	}
	if syncNow {
		return s.tree.ApplyReplicated(batch)
	}
	return s.tree.ApplyReplicatedDeferred(batch)
}
func hashKey(index uint64) []byte { return binary.BigEndian.AppendUint64([]byte{'h'}, index) }

var anchorKey = []byte("sDurabilityAnchor")

type hashAnchor struct {
	Index uint64
	Hash  [32]byte
}

func (s *diskStore) hashAt(index uint64) ([32]byte, error) {
	if index == 0 {
		return [32]byte{}, nil
	}
	value, found, err := s.tree.Get(hashKey(index))
	if err != nil {
		return [32]byte{}, err
	}
	if found {
		if len(value) != 32 {
			return [32]byte{}, fmt.Errorf("invalid log hash")
		}
		var hash [32]byte
		copy(hash[:], value)
		return hash, nil
	}
	value, found, err = s.tree.Get(anchorKey)
	if err != nil {
		return [32]byte{}, err
	}
	var anchor hashAnchor
	if found {
		if err = json.Unmarshal(value, &anchor); err != nil {
			return [32]byte{}, err
		}
		if anchor.Index == index {
			return anchor.Hash, nil
		}
	}
	return [32]byte{}, fmt.Errorf("missing Raft history at index %d", index)
}
func (s *diskStore) historyHash(index uint64) ([32]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hashAt(index)
}
func (s *diskStore) resetLogs(index uint64, hash [32]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, _ := json.Marshal(hashAnchor{index, hash})
	return s.tree.ApplyReplicated([]storage.Mutation{{Key: []byte{'z'}, Value: []byte{'{'}, RangeDelete: true}, {Key: []byte{'h'}, Value: []byte{'i'}, RangeDelete: true}, {Key: anchorKey, Value: value}})
}
func (s *diskStore) DeleteRange(min, max uint64) error {
	if min > max {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	end := []byte{'{'}
	hashEnd := []byte{'i'}
	if max != math.MaxUint64 {
		end = logKey(max + 1)
		hashEnd = hashKey(max + 1)
	}
	batch := []storage.Mutation{{Key: logKey(min), Value: end, RangeDelete: true}}
	if s.async {
		// Only prefix compaction advances the anchor; suffix truncation preserves it.
		first, err := s.FirstIndex()
		if err != nil {
			return err
		}
		if first != 0 && min <= first {
			hash, err := s.hashAt(max)
			if err != nil {
				return err
			}
			value, _ := json.Marshal(hashAnchor{max, hash})
			batch = append(batch, storage.Mutation{Key: anchorKey, Value: value})
		}
		batch = append(batch, storage.Mutation{Key: hashKey(min), Value: hashEnd, RangeDelete: true})
	}
	return s.tree.ApplyReplicated(batch)
}
func (s *diskStore) Set(k, v []byte) error {
	return s.tree.ApplyReplicated([]storage.Mutation{{Key: append([]byte{'s'}, k...), Value: bytes.Clone(v)}})
}
func (s *diskStore) Get(k []byte) ([]byte, error) {
	v, _, e := s.tree.Get(append([]byte{'s'}, k...))
	return v, e
}
func (s *diskStore) SetUint64(k []byte, v uint64) error {
	return s.Set(k, binary.BigEndian.AppendUint64(nil, v))
}
func (s *diskStore) GetUint64(k []byte) (uint64, error) {
	v, e := s.Get(k)
	if e != nil || len(v) == 0 {
		return 0, e
	}
	if len(v) != 8 {
		return 0, fmt.Errorf("corrupt Raft stable value")
	}
	return binary.BigEndian.Uint64(v), nil
}

func (s *diskStore) installSnapshotAnchor(index uint64, hash [32]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, _ := json.Marshal(hashAnchor{index, hash})
	return s.tree.ApplyReplicated([]storage.Mutation{{Key: anchorKey, Value: value}, {Key: hashKey(index), Value: hash[:]}})
}

// Consensus history excludes observation-only timestamps and JSON formatting.
// Fixed-width framing preserves the hash across transport and codec versions.
func chainedLogHash(previous [32]byte, log *raft.Log) [32]byte {
	hash := sha256.New()
	hash.Write(previous[:])
	var fields [25]byte
	binary.BigEndian.PutUint64(fields[0:8], log.Index)
	binary.BigEndian.PutUint64(fields[8:16], log.Term)
	fields[16] = byte(log.Type)
	binary.BigEndian.PutUint64(fields[17:25], uint64(len(log.Data)))
	hash.Write(fields[:])
	hash.Write(log.Data)
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(log.Extensions)))
	hash.Write(size[:])
	hash.Write(log.Extensions)
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}
