package replication

import (
	"bytes"
	"encoding/json"
	"math"
	"math/rand"
	"reflect"
	"testing"

	"slopdb/storage"
)

func TestWALCommandJSONMatchesStandardLibrary(t *testing.T) {
	random := rand.New(rand.NewSource(314159))
	binary := func() []byte {
		if random.Intn(4) == 0 {
			return nil
		}
		value := make([]byte, random.Intn(128))
		_, _ = random.Read(value)
		return value
	}
	for iteration := 0; iteration < 3000; iteration++ {
		c := command{Version: 1, Kind: "wal"}
		if random.Intn(4) != 0 {
			c.Writes = make([]storage.Mutation, random.Intn(8))
			for i := range c.Writes {
				c.Writes[i] = storage.Mutation{Key: binary(), Value: binary(), Delete: random.Intn(2) == 0, RangeDelete: random.Intn(2) == 0}
			}
		}
		if iteration%11 == 0 {
			c.Version = 2
		}
		if iteration%13 == 0 {
			c.Kind = "peer"
			c.Peer = &Peer{ID: "quoted\"peer", Control: "<endpoint>"}
		}
		if iteration%17 == 0 {
			c.PageSize = 8192
		}
		expected, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := marshalCommand(c)
		if err != nil || !bytes.Equal(actual, expected) {
			t.Fatalf("iteration %d JSON bytes differ:\n%s\n%s\n%v", iteration, actual, expected, err)
		}
		var decoded command
		if err = unmarshalCommand(actual, &decoded); err != nil || !reflect.DeepEqual(decoded, c) {
			t.Fatalf("iteration %d command differs: %+v %+v %v", iteration, decoded, c, err)
		}
		if c.Version == 1 && c.Kind == "wal" && c.PageSize == 0 && c.Peer == nil {
			if _, recognized := canonicalWALCommand(actual); !recognized {
				t.Fatal("canonical WAL command did not use the fast path")
			}
			if cap(actual) != len(actual) {
				t.Fatal("WAL command size calculation differs from its encoding")
			}
		}
	}
}

func checkCommandJSONReference(t *testing.T, value []byte) {
	t.Helper()
	var expected, actual command
	expectedErr := json.Unmarshal(value, &expected)
	actualErr := unmarshalCommand(value, &actual)
	if (expectedErr == nil) != (actualErr == nil) || (actualErr == nil && !reflect.DeepEqual(actual, expected)) {
		t.Fatalf("JSON decode differs for %q: got %+v (%v), expected %+v (%v)", value, actual, actualErr, expected, expectedErr)
	}
}

func TestWALCommandJSONLegacyAndMalformedInputs(t *testing.T) {
	value, err := marshalCommand(command{Version: 1, Kind: "wal", Writes: []storage.Mutation{{Key: []byte("key"), Value: []byte("value")}}})
	if err != nil {
		t.Fatal(err)
	}
	checkCommandJSONReference(t, value)
	for end := 0; end < len(value); end++ {
		checkCommandJSONReference(t, value[:end])
	}
	for index := range value {
		for _, replacement := range []byte{0, ' ', '"', '\\', ',', '[', ']', '0', '\n', '\r', 0xff} {
			changed := bytes.Clone(value)
			changed[index] = replacement
			checkCommandJSONReference(t, changed)
		}
	}
	for _, legacy := range []string{
		`null`, `{}`, `{"Kind":"wal","Version":1,"Writes":[{"Key":[1,2],"Value":null}]}`,
		` { "Version":1, "Kind":"wal", "Writes":[], "PageSize":0, "Peer":null } `,
		`{"Version":1,"Kind":"wal","Writes":[{"Key":"a2V5","Value":"dmFsdWU=","Delete":false,"RangeDelete":false}],"PageSize":0,"Peer":null,"Extra":true}`,
		`{"Version":1,"Kind":"wal","Writes":[{"Key":"a2V5","Value":"dmFsdWU=","Delete":null,"RangeDelete":false}],"PageSize":0,"Peer":null}`,
		`{"Version":1,"Kind":"wal","Writes":[{"Key":"\u00612V5","Value":"dmFsdWU=","Delete":false,"RangeDelete":false}],"PageSize":0,"Peer":null}`,
	} {
		checkCommandJSONReference(t, []byte(legacy))
	}
	var decoded command
	before := bytes.Clone(value)
	if err = unmarshalCommand(value, &decoded); err != nil {
		t.Fatal(err)
	}
	decoded.Writes[0].Key[0] ^= 0xff
	decoded.Writes[0].Value[0] ^= 0xff
	if !bytes.Equal(value, before) {
		t.Fatal("decoded mutation aliases the consensus payload")
	}
}

func FuzzCommandJSONCompatibility(f *testing.F) {
	for _, seed := range []string{`null`, `{}`, `{"Version":1,"Kind":"wal","Writes":null,"PageSize":0,"Peer":null}`, `{"Version":1,"Kind":"wal","Writes":[{"Key":"aw==","Value":"dg==","Delete":false,"RangeDelete":false}],"PageSize":0,"Peer":null}`} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, value []byte) {
		if len(value) > 1<<20 {
			t.Skip()
		}
		checkCommandJSONReference(t, value)
	})
}

func TestCheckpointJSONMatchesStandardLibrary(t *testing.T) {
	for _, cluster := range []string{"cluster-id", "", "quotes\"\\and\ncontrols", "<html>&\u2028\u2029", "\xffinvalid UTF-8"} {
		prefix := checkpointPrefix(cluster)
		for _, index := range []uint64{0, 1, 1 << 63, math.MaxUint64} {
			for _, term := range []uint64{0, 1, math.MaxUint64} {
				expected, err := json.Marshal(checkpoint{Cluster: cluster, Index: index, Term: term})
				if err != nil {
					t.Fatal(err)
				}
				actual := marshalCheckpoint(prefix, index, term)
				if !bytes.Equal(actual, expected) {
					t.Fatalf("checkpoint bytes differ: %s %s", actual, expected)
				}
			}
		}
	}
}
