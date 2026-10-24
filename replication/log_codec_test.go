package replication

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"slopdb/storage"
)

func TestConsensusLogEncodingAndLegacyCompatibility(t *testing.T) {
	for _, log := range []raft.Log{
		{Index: 1, Term: 1, Type: raft.LogNoop},
		{Index: 2, Term: 3, Type: raft.LogCommand, Data: []byte{}, Extensions: []byte{}},
		{Index: 3, Term: 4, Type: raft.LogConfiguration, Data: []byte{0, 0xff, 0x80}, Extensions: []byte("opaque"), AppendedAt: time.Date(2026, 10, 24, 8, 1, 2, 123456789, time.UTC)},
		{Index: 4, Term: 5, Type: raft.LogCommand, Data: bytes.Repeat([]byte("non-JSON binary data"), 8192), AppendedAt: time.Date(2000, 1, 1, 0, 0, 0, 1, time.FixedZone("odd-offset", 3601))},
	} {
		value, err := encodeLog(&log)
		if err != nil {
			t.Fatal(err)
		}
		var decoded raft.Log
		if err = decodeLog(value, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.Index != log.Index || decoded.Term != log.Term || decoded.Type != log.Type || !reflect.DeepEqual(decoded.Data, log.Data) || !reflect.DeepEqual(decoded.Extensions, log.Extensions) || !decoded.AppendedAt.Equal(log.AppendedAt) {
			t.Fatalf("log round trip differs: %+v %+v", decoded, log)
		}
		if chainedLogHash([32]byte{}, &decoded) != chainedLogHash([32]byte{}, &log) {
			t.Fatal("log encoding changed consensus history")
		}
		if cap(decoded.Data) != len(decoded.Data) || cap(decoded.Extensions) != len(decoded.Extensions) {
			t.Fatal("decoded field append could overwrite another field")
		}
		// Legacy JSON preserves its existing timestamp semantics; JSON itself
		// rounds zone offsets to minutes, unlike the binary representation.
		legacy, err := json.Marshal(log)
		if err != nil {
			t.Fatal(err)
		}
		var expected raft.Log
		if err = json.Unmarshal(legacy, &expected); err != nil {
			t.Fatal(err)
		}
		if err = decodeLog(legacy, &decoded); err != nil || !reflect.DeepEqual(expected, decoded) {
			t.Fatalf("legacy log differs: %+v %+v %v", decoded, expected, err)
		}
	}
}

func TestConsensusLogEncodingRejectsMalformedValues(t *testing.T) {
	value, err := encodeLog(&raft.Log{Index: 1, Term: 1, Data: []byte("payload"), Extensions: []byte("extension"), AppendedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	for end := 0; end < len(value); end++ {
		var decoded raft.Log
		if err = decodeLog(value[:end], &decoded); err == nil {
			t.Fatalf("accepted truncated log at %d/%d", end, len(value))
		}
	}
	for _, corrupt := range []func([]byte) []byte{
		func(v []byte) []byte { v[3]++; return v },
		func(v []byte) []byte { v[21] |= 0x80; return v },
		func(v []byte) []byte { v[21] |= 1; return v },
		func(v []byte) []byte { v[22] = 0xff; return v },
		func(v []byte) []byte { v[23] = 0xff; return v },
		func(v []byte) []byte { return append(v, 0) },
	} {
		var decoded raft.Log
		if err = decodeLog(corrupt(bytes.Clone(value)), &decoded); err == nil {
			t.Fatal("accepted malformed log encoding")
		}
	}
}

func TestConsensusLogMixedFormatsAndCallerOwnership(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "raft-log")
	tree, err := storage.NewLSMTree(directory, storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tree.Close() })
	old := raft.Log{Index: 1, Term: 1, Type: raft.LogCommand, Data: []byte("legacy"), Extensions: []byte("old extension")}
	legacy, _ := json.Marshal(old)
	if err = tree.ApplyReplicated([]storage.Mutation{{Key: logKey(1), Value: legacy}}); err != nil {
		t.Fatal(err)
	}
	store := &diskStore{tree: tree}
	newer := raft.Log{Index: 2, Term: 1, Type: raft.LogCommand, Data: []byte("binary"), Extensions: []byte("new extension")}
	if err = store.StoreLog(&newer); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []*raft.Log{&old, &newer} {
		var actual raft.Log
		if err = store.GetLog(expected.Index, &actual); err != nil || !reflect.DeepEqual(actual, *expected) {
			t.Fatalf("mixed log read: %+v %v", actual, err)
		}
		actual.Data[0] ^= 0xff
		actual.Extensions[0] ^= 0xff
		if err = store.GetLog(expected.Index, &actual); err != nil || !reflect.DeepEqual(actual, *expected) {
			t.Fatal("caller changed stored consensus bytes")
		}
	}
	if err = tree.Close(); err != nil {
		t.Fatal(err)
	}
	tree, err = storage.NewLSMTree(directory, storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	store.tree = tree
	for _, expected := range []*raft.Log{&old, &newer} {
		var actual raft.Log
		if err = store.GetLog(expected.Index, &actual); err != nil || !reflect.DeepEqual(actual, *expected) {
			t.Fatalf("reopened mixed log differs: %+v %v", actual, err)
		}
	}
	if err = tree.ApplyReplicated([]storage.Mutation{{Key: logKey(3), Value: legacy}}); err != nil {
		t.Fatal(err)
	}
	var actual raft.Log
	if err = store.GetLog(3, &actual); err == nil {
		t.Fatal("log accepted mismatched index")
	}
	if err = tree.Close(); err != nil {
		t.Fatal(err)
	}
}
