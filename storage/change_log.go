package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"
)

var changeLogHeaderKey = []byte("\x00__change_log_v1__")
var changeLogPrefix = []byte{4}
var changeLogSequenceKey = []byte("\x00__change_sequence_v1__")
var changeLogClockKey = []byte("\x00__change_clock_v1__")
var changeCatalogPrefix = []byte{5}

// ChangeLogHeader is a persistent baseline for interpreting subsequent catalog
// and row changes. The source ID and baseline survive checkpoint, backup and crash.
type ChangeLogHeader struct {
	Version       int               `json:"version"`
	SourceID      string            `json:"source_id"`
	StartSequence uint64            `json:"start_sequence"`
	Catalog       map[string][]byte `json:"catalog"`
}

type ChangeMutation struct {
	Key          []byte
	Before       []byte
	After        []byte
	BeforeExists bool
	Delete       bool
}

// One record corresponds to one submitted transaction, including when the
// storage queue shares WAL I/O and publication across several transactions.
type ChangeRecord struct {
	Sequence  uint64
	Time      int64
	Mutations []ChangeMutation
}

func changeLogKey(sequence uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{4}, sequence)
}

func changeCatalogKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte("\x00__schema")) || bytes.Equal(key, []byte("\x00__namespaces_v1__"))
}

func changeRowKey(key []byte) bool { return len(key) >= 10 && key[0] == 1 && key[1] == 0xff }

func (l *LSMTree) ChangeLogHeader() (*ChangeLogHeader, error) {
	data, found, err := l.Get(changeLogHeaderKey)
	if err != nil || !found {
		return nil, err
	}
	var header ChangeLogHeader
	if err = json.Unmarshal(data, &header); err != nil {
		return nil, fmt.Errorf("change log header: %w", err)
	}
	if header.Version != 1 || len(header.SourceID) != 32 {
		return nil, fmt.Errorf("invalid change log header")
	}
	return &header, nil
}

// EnableChangeLog atomically captures the catalog baseline before subsequent
// durable row writes. Replication owners order this control write normally.
func (l *LSMTree) EnableChangeLog(ctx context.Context) (*ChangeLogHeader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if header, err := l.ChangeLogHeader(); header != nil || err != nil {
		return header, err
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	request, err := json.Marshal(ChangeLogHeader{Version: 1, SourceID: hex.EncodeToString(id[:])})
	if err != nil {
		return nil, err
	}
	if err = l.WriteBatch([]Mutation{{Key: changeLogHeaderKey, Value: request}}); err != nil {
		return nil, err
	}
	return l.ChangeLogHeader()
}

func (l *LSMTree) initChangeLog() error {
	header, err := l.ChangeLogHeader()
	if err != nil {
		return err
	}
	l.changeLogEnabled.Store(header != nil)
	return nil
}

// appendChangeLog is called with writerMu held and mu released. Every before
// image is taken before publication; a local overlay incorporates earlier
// writes in this group. Journal bytes share the same WAL record and watermark.
func (l *LSMTree) appendChangeLog(batch []Mutation, base uint64, boundaries []int, rangeDelete bool) ([]Mutation, bool, error) {
	enabled := l.changeLogEnabled.Load()
	activation := -1
	for i, w := range batch {
		if bytes.Equal(w.Key, changeLogHeaderKey) {
			activation = i
			break
		}
	}
	if !enabled && activation < 0 {
		return batch, false, nil
	}
	changed := activation >= 0
	if activation >= 0 {
		var header *ChangeLogHeader
		if enabled {
			var err error
			header, err = l.ChangeLogHeader()
			if err != nil {
				return nil, false, err
			}
			if header == nil {
				return nil, false, fmt.Errorf("enabled change log has no header")
			}
		} else {
			var initial ChangeLogHeader
			if err := json.Unmarshal(batch[activation].Value, &initial); err != nil || initial.Version != 1 || len(initial.SourceID) != 32 {
				return nil, false, fmt.Errorf("invalid change log activation")
			}
			initial.StartSequence = 0
			initial.Catalog = make(map[string][]byte)
			it := l.NewIteratorRange([]byte{0}, []byte{1}, false)
			for ; it.Valid(); it.Next() {
				if changeCatalogKey(it.Key().UserKey) {
					initial.Catalog[string(it.Key().UserKey)] = bytes.Clone(it.Value())
				}
			}
			err := IteratorError(it)
			it.Close()
			if err != nil {
				return nil, false, err
			}
			header = &initial
		}
		data, err := json.Marshal(header)
		if err != nil {
			return nil, false, err
		}
		batch = append([]Mutation(nil), batch...)
		for i := range batch {
			if bytes.Equal(batch[i].Key, changeLogHeaderKey) {
				batch[i].Value = data
			}
		}
	}
	if len(boundaries) == 0 {
		boundaries = []int{len(batch)}
	}
	type image struct {
		value []byte
		found bool
	}
	staged := make(map[string]image)
	read := func(key []byte) (image, error) {
		if value, ok := staged[string(key)]; ok {
			return value, nil
		}
		value, found, err := l.Get(key)
		if err != nil {
			return image{}, err
		}
		return image{bytes.Clone(value), found}, nil
	}
	var records []ChangeRecord
	start := 0
	for _, end := range boundaries {
		if end < start || end > len(batch) {
			return nil, false, fmt.Errorf("invalid change log transaction boundary")
		}
		record := ChangeRecord{Time: time.Now().UTC().UnixNano()}
		for _, w := range batch[start:end] {
			if bytes.Equal(w.Key, changeLogClockKey) && len(w.Value) == 8 {
				record.Time = int64(binary.BigEndian.Uint64(w.Value))
			}
		}
		apply := func(key, value []byte, deleted bool) error {
			before, err := read(key)
			if err != nil {
				return err
			}
			staged[string(key)] = image{bytes.Clone(value), !deleted}
			if deleted && !before.found || !deleted && before.found && bytes.Equal(before.value, value) {
				return nil
			}
			record.Mutations = append(record.Mutations, ChangeMutation{Key: bytes.Clone(key), Before: bytes.Clone(before.value), After: bytes.Clone(value), BeforeExists: before.found, Delete: deleted})
			return nil
		}
		for _, w := range batch[start:end] {
			if w.NoWAL {
				continue
			}
			if rangeDelete || w.RangeDelete {
				if len(w.Key) == 0 || w.Key[0] > 1 || len(w.Value) > 0 && w.Value[0] < 1 {
					continue
				}
				keys := make(map[string]bool)
				it := l.NewIteratorRange(w.Key, w.Value, false)
				for ; it.Valid(); it.Next() {
					key := it.Key().UserKey
					if changeRowKey(key) || changeCatalogKey(key) {
						keys[string(key)] = true
					}
				}
				err := IteratorError(it)
				it.Close()
				if err != nil {
					return nil, false, err
				}
				for key := range staged {
					data := []byte(key)
					if bytes.Compare(data, w.Key) >= 0 && bytes.Compare(data, w.Value) < 0 && (changeRowKey(data) || changeCatalogKey(data)) {
						keys[key] = true
					}
				}
				ordered := make([]string, 0, len(keys))
				for key := range keys {
					ordered = append(ordered, key)
				}
				sort.Strings(ordered)
				for _, key := range ordered {
					if err = apply([]byte(key), nil, true); err != nil {
						return nil, false, err
					}
				}
				continue
			}
			if !changeRowKey(w.Key) && !changeCatalogKey(w.Key) {
				continue
			}
			if err := apply(w.Key, w.Value, w.Delete); err != nil {
				return nil, false, err
			}
		}
		if len(record.Mutations) > 0 {
			records = append(records, record)
		}
		start = end
	}
	if start != len(batch) {
		return nil, false, fmt.Errorf("incomplete change log boundaries")
	}
	if len(records) == 0 {
		return batch, changed, nil
	}
	seq := uint64(0)
	if data, found, err := l.Get(changeLogSequenceKey); err != nil {
		return nil, false, err
	} else if found {
		if len(data) != 8 {
			return nil, false, fmt.Errorf("invalid change log sequence")
		}
		seq = binary.BigEndian.Uint64(data)
	}
	if uint64(len(records)) > math.MaxUint64-seq {
		return nil, false, fmt.Errorf("change log sequence exhausted")
	}
	result := make([]Mutation, 0, len(batch)+len(records)+1)
	result = append(result, batch...)
	var catalog map[string][]byte
	for i := range records {
		records[i].Sequence = seq + uint64(i+1)
		data := encodeChangeRecord(records[i])
		result = append(result, Mutation{Key: changeLogKey(records[i].Sequence), Value: data})
		changedCatalog := false
		for _, mutation := range records[i].Mutations {
			if !changeCatalogKey(mutation.Key) {
				continue
			}
			if catalog == nil {
				catalog = make(map[string][]byte)
				it := l.NewIteratorRange([]byte{0}, []byte{1}, false)
				for ; it.Valid(); it.Next() {
					if changeCatalogKey(it.Key().UserKey) {
						catalog[string(it.Key().UserKey)] = bytes.Clone(it.Value())
					}
				}
				err := IteratorError(it)
				it.Close()
				if err != nil {
					return nil, false, err
				}
			}
			changedCatalog = true
			if mutation.Delete {
				delete(catalog, string(mutation.Key))
			} else {
				catalog[string(mutation.Key)] = bytes.Clone(mutation.After)
			}
		}
		if changedCatalog {
			data, err := json.Marshal(catalog)
			if err != nil {
				return nil, false, err
			}
			key := binary.BigEndian.AppendUint64([]byte{5}, records[i].Sequence)
			result = append(result, Mutation{Key: key, Value: data})
		}
	}
	result = append(result, Mutation{Key: changeLogSequenceKey, Value: binary.BigEndian.AppendUint64(nil, seq+uint64(len(records)))})
	return result, true, nil
}

func encodeChangeRecord(record ChangeRecord) []byte {
	data := []byte{1}
	data = binary.BigEndian.AppendUint64(data, record.Sequence)
	data = binary.BigEndian.AppendUint64(data, uint64(record.Time))
	data = binary.AppendUvarint(data, uint64(len(record.Mutations)))
	for _, m := range record.Mutations {
		flags := byte(0)
		if m.BeforeExists {
			flags |= 1
		}
		if m.Delete {
			flags |= 2
		}
		data = append(data, flags)
		for _, value := range [][]byte{m.Key, m.Before, m.After} {
			data = binary.AppendUvarint(data, uint64(len(value)))
			data = append(data, value...)
		}
	}
	return data
}

func decodeChangeRecord(data []byte) (ChangeRecord, error) {
	var record ChangeRecord
	if len(data) < 18 || data[0] != 1 {
		return record, fmt.Errorf("invalid change record header")
	}
	record.Sequence = binary.BigEndian.Uint64(data[1:9])
	record.Time = int64(binary.BigEndian.Uint64(data[9:17]))
	data = data[17:]
	count, n := binary.Uvarint(data)
	if n <= 0 || count > uint64(len(data)) {
		return record, fmt.Errorf("invalid change record count")
	}
	data = data[n:]
	for i := uint64(0); i < count; i++ {
		if len(data) == 0 || data[0]&^byte(3) != 0 {
			return record, fmt.Errorf("invalid change record flags")
		}
		m := ChangeMutation{BeforeExists: data[0]&1 != 0, Delete: data[0]&2 != 0}
		data = data[1:]
		for _, field := range []*[]byte{&m.Key, &m.Before, &m.After} {
			length, n := binary.Uvarint(data)
			if n <= 0 || length > uint64(len(data)-n) {
				return record, fmt.Errorf("invalid change record field")
			}
			data = data[n:]
			*field = bytes.Clone(data[:int(length)])
			data = data[int(length):]
		}
		record.Mutations = append(record.Mutations, m)
	}
	if len(data) != 0 {
		return record, fmt.Errorf("trailing change record data")
	}
	return record, nil
}

func (l *LSMTree) ReadChanges(ctx context.Context, after uint64, limit int) ([]ChangeRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if after == math.MaxUint64 {
		return nil, nil
	}
	if limit <= 0 || limit > 4096 {
		limit = 256
	}
	it := l.NewIteratorRange(changeLogKey(after+1), []byte{5}, false)
	defer it.Close()
	var records []ChangeRecord
	for ; it.Valid() && len(records) < limit; it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		record, err := decodeChangeRecord(it.Value())
		if err != nil {
			return nil, err
		}
		if len(it.Key().UserKey) != 9 || binary.BigEndian.Uint64(it.Key().UserKey[1:]) != record.Sequence {
			return nil, fmt.Errorf("change record key mismatch")
		}
		records = append(records, record)
	}
	return records, IteratorError(it)
}

// ChangeCatalogAt seeks the nearest catalog checkpoint. Resuming a stream does
// not replay its entire row history merely to learn the table definitions.
func (l *LSMTree) ChangeCatalogAt(ctx context.Context, sequence uint64) (map[string][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	upper := []byte{6}
	if sequence < math.MaxUint64 {
		upper = binary.BigEndian.AppendUint64([]byte{5}, sequence+1)
	}
	it := l.NewIteratorRange(changeCatalogPrefix, upper, true)
	if it.Valid() {
		var catalog map[string][]byte
		err := json.Unmarshal(it.Value(), &catalog)
		it.Close()
		if err != nil {
			return nil, fmt.Errorf("change catalog checkpoint: %w", err)
		}
		return catalog, nil
	}
	err := IteratorError(it)
	it.Close()
	if err != nil {
		return nil, err
	}
	header, err := l.ChangeLogHeader()
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, fmt.Errorf("change log is not enabled")
	}
	return header.Catalog, nil
}

func (l *LSMTree) ChangeLogPosition() (uint64, error) {
	data, found, err := l.Get(changeLogSequenceKey)
	if err != nil || !found {
		return 0, err
	}
	if len(data) != 8 {
		return 0, fmt.Errorf("invalid change log sequence")
	}
	return binary.BigEndian.Uint64(data), nil
}

// ValidateChangeEntry is used by logical integrity inspection in addition to
// the physical SSTable checksum checks. The caller scans the committed view.
func ValidateChangeEntry(key, data []byte) (bool, error) {
	if bytes.Equal(key, changeLogSequenceKey) || bytes.Equal(key, changeLogClockKey) {
		if len(data) != 8 {
			return true, fmt.Errorf("invalid change log numeric metadata")
		}
		return true, nil
	}
	if bytes.Equal(key, changeLogHeaderKey) {
		var header ChangeLogHeader
		if err := json.Unmarshal(data, &header); err != nil {
			return true, err
		}
		if header.Version != 1 || len(header.SourceID) != 32 {
			return true, fmt.Errorf("invalid change log header")
		}
		_, err := hex.DecodeString(header.SourceID)
		return true, err
	}
	if len(key) > 0 && key[0] == 4 {
		record, err := decodeChangeRecord(data)
		if err != nil {
			return true, err
		}
		if len(key) != 9 || binary.BigEndian.Uint64(key[1:]) != record.Sequence {
			return true, fmt.Errorf("invalid change record key")
		}
		return true, nil
	}
	if len(key) > 0 && key[0] == 5 {
		if len(key) != 9 {
			return true, fmt.Errorf("invalid change catalog key")
		}
		var catalog map[string][]byte
		err := json.Unmarshal(data, &catalog)
		return true, err
	}
	return false, nil
}

// Read the notification channel BEFORE reading records to avoid missing a
// publication between the empty read and waiting. Channels block; no polling.
func (l *LSMTree) ChangeNotification() <-chan struct{} {
	l.changeMu.Lock()
	defer l.changeMu.Unlock()
	if l.changeChanged == nil {
		l.changeChanged = make(chan struct{})
	}
	return l.changeChanged
}

func (l *LSMTree) notifyChanges() {
	l.changeMu.Lock()
	defer l.changeMu.Unlock()
	if l.changeChanged != nil {
		close(l.changeChanged)
	}
	l.changeChanged = make(chan struct{})
}

// A replicated proposal carries its commit clock as ordinary internal metadata,
// making the journal deterministic on every FSM. Standalone grouped callers
// carry one clock per original batch. This key never enters user CDC records.
func (l *LSMTree) stampChangeBatch(batch []Mutation) []Mutation {
	if !l.changeLogEnabled.Load() && l.replication.Load() == nil {
		return batch
	}
	eligible := false
	for _, w := range batch {
		if bytes.Equal(w.Key, changeLogClockKey) {
			return batch
		}
		eligible = eligible || !w.NoWAL && (changeRowKey(w.Key) || changeCatalogKey(w.Key))
	}
	if !eligible {
		return batch
	}
	owned := make([]Mutation, len(batch), len(batch)+1)
	copy(owned, batch)
	return append(owned, Mutation{Key: changeLogClockKey, Value: binary.BigEndian.AppendUint64(nil, uint64(time.Now().UTC().UnixNano()))})
}
