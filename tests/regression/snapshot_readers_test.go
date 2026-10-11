// Adapted from /tmp/godb/snapshot_reader_review.go; see TESTING.md.
package regression

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"

	"slopdb/storage"
)

func snapshotReadersMust(testState *regressionEnv, e error) {
	testState.t.Helper()
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
}
func snapshotReadersCheck(testState *regressionEnv, b bool, s string) {
	testState.t.Helper()
	if !b {
		testState.t.Fatalf("%v", s)
	}
}
func TestSnapshotReaders(testT *testing.T) {
	testState := newRegressionEnv(testT)

	for mode := 0; mode < 3; mode++ {
		dir := testState.path(fmt.Sprintf("godb_snapshot_readers_%d", mode))
		snapshotReadersMust(testState, os.RemoveAll(dir))
		o := storage.DefaultOptions()
		o.BlockSize = 128
		o.WriteBufferSize = 2048
		o.MaxOpenFiles = 1
		o.DisableWAL = mode == 1
		o.DisableWALSync = mode == 2
		l, e := testState.openStorage(dir, o)
		snapshotReadersMust(testState, e)
		write := func(round int) {
			value := []byte(fmt.Sprintf("round%04d", round))
			mut := make([]storage.Mutation, 100)
			for i := range mut {
				mut[i] = storage.Mutation{Key: []byte(fmt.Sprintf("k%03d", i)), Value: value}
			}
			snapshotReadersMust(testState, l.WriteBatch(mut))
		}
		write(0)
		legacy := l.Snapshot()
		seqs := make([]uint64, 128)
		for i := range seqs {
			seqs[i] = l.SnapshotForReader(uint64(i))
			write(i + 1)
		}
		snapshotReadersMust(testState, l.CompactRange())
		for i, seq := range seqs {
			v, e := l.GetMultiAt([][]byte{[]byte("k000"), []byte("k099")}, seq)
			snapshotReadersMust(testState, e)
			snapshotReadersCheck(testState, v[0].Found && v[1].Found && string(v[0].Value) == fmt.Sprintf("round%04d", i) && bytes.Equal(v[0].Value, v[1].Value), "colliding reader bucket retention")
			l.ReleaseSnapshotForReader(uint64(i), seq)
		}
		v, ok, e := l.GetAt([]byte("k050"), legacy)
		snapshotReadersMust(testState, e)
		snapshotReadersCheck(testState, ok && string(v) == "round0000", "legacy pin retained beside reader buckets")
		l.ReleaseSnapshot(legacy)
		var wg sync.WaitGroup
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < 200; i++ {
					write(200 + w*200 + i)
				}
			}(w)
		}
		for w := 0; w < 32; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				id := uint64(w * 64)
				for i := 0; i < 200; i++ {
					seq := l.SnapshotForReader(id)
					values, e := l.GetMultiAt([][]byte{[]byte("k000"), []byte("k050"), []byte("k099")}, seq)
					snapshotReadersMust(testState, e)
					snapshotReadersCheck(testState, values[0].Found && values[1].Found && values[2].Found && bytes.Equal(values[0].Value, values[1].Value) && bytes.Equal(values[0].Value, values[2].Value), "snapshot/compaction concurrency")
					l.ReleaseSnapshotForReader(id, seq)
				}
			}(w)
		}
		wg.Wait()
		snapshotReadersMust(testState, l.CompactRange())
		snapshotReadersMust(testState, l.Close())
		testState.t.Logf("PASS mode%d: 128 colliding reader pins, legacy pins, compaction and 6400 concurrent snapshot reads\n", mode)
	}
}
