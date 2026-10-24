package replication

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/hashicorp/raft"
)

var logMagic = []byte{'S', 'R', 'L', 1}

// Consensus log values have their own versioned encoding. Commands and Raft
// transport bytes are unchanged, and readers retain the legacy JSON format.
// Native WAL/SSTable checksums protect both encodings. The history hash uses
// decoded Raft fields, so rewriting a value does not alter consensus history.
func encodeLog(log *raft.Log) ([]byte, error) {
	var timestamp []byte
	var err error
	if !log.AppendedAt.IsZero() {
		timestamp, err = log.AppendedAt.MarshalBinary()
		if err != nil {
			return nil, err
		}
	}
	value := make([]byte, 23, 64+len(log.Data)+len(log.Extensions))
	copy(value, logMagic)
	binary.LittleEndian.PutUint64(value[4:12], log.Index)
	binary.LittleEndian.PutUint64(value[12:20], log.Term)
	value[20] = byte(log.Type)
	if log.Data == nil {
		value[21] |= 1
	}
	if log.Extensions == nil {
		value[21] |= 2
	}
	value[22] = byte(len(timestamp))
	value = append(value, timestamp...)
	for _, field := range [][]byte{log.Data, log.Extensions} {
		value = binary.AppendUvarint(value, uint64(len(field)))
		value = append(value, field...)
	}
	return value, nil
}

// decodeLog takes ownership of value. Storage.Get returns an owned buffer, so
// data/extensions can share it without another copy. Their capacities are capped
// to prevent callers appending to one field from overwriting another field.
func decodeLog(value []byte, log *raft.Log) error {
	if !bytes.HasPrefix(value, logMagic[:3]) {
		var decoded raft.Log
		if err := json.Unmarshal(value, &decoded); err != nil {
			return err
		}
		*log = decoded
		return nil
	}
	if len(value) < 23 || value[3] != logMagic[3] || value[21]&^byte(3) != 0 {
		return fmt.Errorf("invalid Raft log encoding header")
	}
	decoded := raft.Log{Index: binary.LittleEndian.Uint64(value[4:12]), Term: binary.LittleEndian.Uint64(value[12:20]), Type: raft.LogType(value[20])}
	flags, timestampSize := value[21], int(value[22])
	value = value[23:]
	if timestampSize > len(value) {
		return fmt.Errorf("truncated Raft log timestamp")
	}
	if timestampSize != 0 {
		if err := decoded.AppendedAt.UnmarshalBinary(value[:timestampSize]); err != nil {
			return err
		}
	}
	value = value[timestampSize:]
	for i, target := range []*[]byte{&decoded.Data, &decoded.Extensions} {
		size, n := binary.Uvarint(value)
		if n <= 0 {
			return fmt.Errorf("invalid Raft log field length")
		}
		value = value[n:]
		if size > uint64(len(value)) {
			return fmt.Errorf("truncated Raft log field")
		}
		if flags&(1<<i) != 0 {
			if size != 0 {
				return fmt.Errorf("invalid nil Raft log field")
			}
		} else {
			*target = value[:int(size):int(size)]
		}
		value = value[int(size):]
	}
	if len(value) != 0 {
		return fmt.Errorf("trailing Raft log bytes")
	}
	*log = decoded
	return nil
}
