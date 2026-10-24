package replication

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"

	"slopdb/storage"
)

const (
	walJSONPrefix   = `{"Version":1,"Kind":"wal","Writes":`
	walJSONSuffix   = `,"PageSize":0,"Peer":null}`
	walKey          = `{"Key":`
	walValue        = `,"Value":`
	walDelete       = `,"Delete":`
	walRange        = `,"RangeDelete":`
	maxCommandBytes = 64 << 20
)

// The common WAL command has a fixed JSON shape and binary key/value fields.
// Emit exactly encoding/json's bytes, preserving the transport format and log
// history hashes. Other command shapes retain the standard encoder.
func marshalCommand(c command) ([]byte, error) {
	if c.Version != 1 || c.Kind != "wal" || c.PageSize != 0 || c.Peer != nil {
		return json.Marshal(c)
	}
	size := len(walJSONPrefix) + len(walJSONSuffix)
	if c.Writes == nil {
		size += 4
	} else {
		size += 2
		for i, write := range c.Writes {
			if len(write.Key) > maxCommandBytes || len(write.Value) > maxCommandBytes {
				return nil, fmt.Errorf("replicated WAL batch exceeds 64 MiB")
			}
			size += len(walKey) + jsonBinarySize(write.Key) + len(walValue) + jsonBinarySize(write.Value) + len(walDelete) + len(walRange) + 1
			size += 10
			if write.Delete {
				size--
			}
			if write.RangeDelete {
				size--
			}
			if i != 0 {
				size++
			}
			if size > maxCommandBytes {
				return nil, fmt.Errorf("replicated WAL batch exceeds 64 MiB")
			}
		}
	}
	value := make([]byte, 0, size)
	value = append(value, walJSONPrefix...)
	if c.Writes == nil {
		value = append(value, "null"...)
	} else {
		value = append(value, '[')
		for i, write := range c.Writes {
			if i != 0 {
				value = append(value, ',')
			}
			value = append(value, walKey...)
			value = appendJSONBinary(value, write.Key)
			value = append(value, walValue...)
			value = appendJSONBinary(value, write.Value)
			value = append(value, walDelete...)
			value = appendJSONBool(value, write.Delete)
			value = append(value, walRange...)
			value = appendJSONBool(value, write.RangeDelete)
			value = append(value, '}')
		}
		value = append(value, ']')
	}
	return append(value, walJSONSuffix...), nil
}

func jsonBinarySize(value []byte) int {
	if value == nil {
		return 4
	}
	return 2 + base64.StdEncoding.EncodedLen(len(value))
}
func appendJSONBinary(dst, value []byte) []byte {
	if value == nil {
		return append(dst, "null"...)
	}
	dst = append(dst, '"')
	start := len(dst)
	dst = append(dst, make([]byte, base64.StdEncoding.EncodedLen(len(value)))...)
	base64.StdEncoding.Encode(dst[start:], value)
	return append(dst, '"')
}
func appendJSONBool(dst []byte, value bool) []byte {
	if value {
		return append(dst, "true"...)
	}
	return append(dst, "false"...)
}

func unmarshalCommand(value []byte, destination *command) error {
	if destination != nil {
		if decoded, ok := canonicalWALCommand(value); ok {
			*destination = decoded
			return nil
		}
	}
	return json.Unmarshal(value, destination)
}

type commandJSONReader struct{ value []byte }

func (r *commandJSONReader) take(token string) bool {
	if len(r.value) < len(token) || string(r.value[:len(token)]) != token {
		return false
	}
	r.value = r.value[len(token):]
	return true
}
func (r *commandJSONReader) binary() ([]byte, bool) {
	if r.take("null") {
		return nil, true
	}
	if !r.take(`"`) {
		return nil, false
	}
	end := bytes.IndexByte(r.value, '"')
	if end < 0 {
		return nil, false
	}
	encoded := r.value[:end]
	// The base64 decoder ignores raw newlines, while JSON rejects them inside
	// strings. Escapes and other noncanonical forms use the standard decoder.
	if bytes.IndexByte(encoded, '\n') >= 0 || bytes.IndexByte(encoded, '\r') >= 0 {
		return nil, false
	}
	decoded := make([]byte, base64.StdEncoding.DecodedLen(len(encoded)))
	n, err := base64.StdEncoding.Decode(decoded, encoded)
	if err != nil {
		return nil, false
	}
	r.value = r.value[end+1:]
	return decoded[:n:n], true
}
func (r *commandJSONReader) boolean() (bool, bool) {
	if r.take("true") {
		return true, true
	}
	if r.take("false") {
		return false, true
	}
	return false, false
}

// Recognize only the exact canonical shape we emit. Falling back on any
// mismatch preserves legacy JSON rules for whitespace, escaped strings,
// differently ordered/omitted fields and malformed-input errors.
func canonicalWALCommand(value []byte) (command, bool) {
	r := commandJSONReader{value: value}
	c := command{Version: 1, Kind: "wal"}
	if !r.take(walJSONPrefix) {
		return c, false
	}
	if !r.take("null") {
		if !r.take("[") {
			return c, false
		}
		c.Writes = make([]storage.Mutation, 0)
		if !r.take("]") {
			// The common single-row command leaves space for the application
			// checkpoint appended by the native writer.
			c.Writes = make([]storage.Mutation, 0, 2)
			for {
				var write storage.Mutation
				var ok bool
				if !r.take(walKey) {
					return c, false
				}
				if write.Key, ok = r.binary(); !ok || !r.take(walValue) {
					return c, false
				}
				if write.Value, ok = r.binary(); !ok || !r.take(walDelete) {
					return c, false
				}
				if write.Delete, ok = r.boolean(); !ok || !r.take(walRange) {
					return c, false
				}
				if write.RangeDelete, ok = r.boolean(); !ok || !r.take("}") {
					return c, false
				}
				c.Writes = append(c.Writes, write)
				if r.take("]") {
					break
				}
				if !r.take(",") {
					return c, false
				}
			}
		}
	}
	return c, r.take(walJSONSuffix) && len(r.value) == 0
}

func checkpointPrefix(cluster string) []byte {
	// Use the standard string encoder once per incarnation, including its
	// escaping rules, then append changing numeric fields without reflection.
	quoted, _ := json.Marshal(cluster)
	prefix := append([]byte(`{"Cluster":`), quoted...)
	return append(prefix, `,"Index":`...)
}

func marshalCheckpoint(prefix []byte, index, term uint64) []byte {
	value := make([]byte, 0, len(prefix)+48)
	value = append(value, prefix...)
	value = strconv.AppendUint(value, index, 10)
	value = append(value, `,"Term":`...)
	value = strconv.AppendUint(value, term, 10)
	return append(value, '}')
}
