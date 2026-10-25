package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"os/exec"
	"sync"
	"testing"
)

func changeRow(id uint64) []byte { return binary.BigEndian.AppendUint64([]byte{1, 255}, id) }
func TestChangeLogAtomicBeforeImagesTransactionsAndReopen(t *testing.T) {
	path := t.TempDir()
	tree, e := NewLSMTree(path, DefaultOptions())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = tree.Close() })
	ctx := context.Background()
	catalog := []byte("\x00__schema__")
	if e = tree.Put(catalog, []byte(`{"before":1}`)); e != nil {
		t.Fatal(e)
	}
	key := append(changeRow(1), ':', 'k')
	if e = tree.Put(key, []byte("initial")); e != nil {
		t.Fatal(e)
	}
	header, e := tree.EnableChangeLog(ctx)
	if e != nil || header == nil || !bytes.Equal(header.Catalog[string(catalog)], []byte(`{"before":1}`)) {
		t.Fatal(header, e)
	}
	ready := tree.ChangeNotification()
	private := []byte{3, 1, 'k'}
	batch := []Mutation{{Key: key, Value: []byte("second")}, {Key: key, Value: []byte("third")}, {Key: private, Value: []byte("secret"), NoWAL: true}, {Key: []byte{2, 255, 'i'}, Value: []byte("index")}}
	if e = tree.WriteBatch(batch); e != nil {
		t.Fatal(e)
	}
	select {
	case <-ready:
	default:
		t.Fatal("missing change notification")
	}
	records, e := tree.ReadChanges(ctx, 0, 100)
	if e != nil || len(records) != 1 {
		t.Fatal(records, e)
	}
	if len(records[0].Mutations) != 2 || string(records[0].Mutations[0].Before) != "initial" || string(records[0].Mutations[1].Before) != "second" || string(records[0].Mutations[1].After) != "third" {
		t.Fatal(records)
	}
	if e = tree.DeleteRange(key, append(bytes.Clone(key), 255)); e != nil {
		t.Fatal(e)
	}
	records, e = tree.ReadChanges(ctx, records[0].Sequence, 100)
	if e != nil || len(records) != 1 || len(records[0].Mutations) != 1 || !records[0].Mutations[0].Delete || string(records[0].Mutations[0].Before) != "third" {
		t.Fatal(records, e)
	}
	if e = tree.Close(); e != nil {
		t.Fatal(e)
	}
	tree, e = NewLSMTree(path, DefaultOptions())
	if e != nil {
		t.Fatal(e)
	}
	persisted, e := tree.ChangeLogHeader()
	if e != nil || persisted.SourceID != header.SourceID {
		t.Fatal(persisted, e)
	}
	if e = tree.Put(key, []byte("afterreopen")); e != nil {
		t.Fatal(e)
	}
	all, e := tree.ReadChanges(ctx, 0, 100)
	if e != nil || len(all) != 3 || all[2].Sequence <= all[1].Sequence || all[2].Mutations[0].BeforeExists {
		t.Fatal(all, e)
	}
	if e = tree.Close(); e != nil {
		t.Fatal(e)
	}
}
func TestChangeLogQueuePreservesOriginalTransactionBoundaries(t *testing.T) {
	tree := openReaderViewTest(t)
	ctx := context.Background()
	if _, e := tree.EnableChangeLog(ctx); e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	start := make(chan struct{})
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			key := changeRow(uint64(i))
			errs <- tree.WriteBatch([]Mutation{{Key: key, Value: []byte("first")}, {Key: key, Value: []byte("second")}})
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	records, e := tree.ReadChanges(ctx, 0, 100)
	if e != nil || len(records) != 32 {
		t.Fatal(len(records), e)
	}
	for _, r := range records {
		if len(r.Mutations) != 2 || !bytes.Equal(r.Mutations[0].Key, r.Mutations[1].Key) || r.Mutations[0].BeforeExists || string(r.Mutations[1].Before) != "first" {
			t.Fatal(r)
		}
	}
}
func TestChangeLogReplicasShareIdentitySequencesAndBytes(t *testing.T) {
	leader := openReaderViewTest(t)
	follower := openReaderViewTest(t)
	leader.SetReplicationHooks(&ReplicationHooks{Write: func(batch []Mutation, queued func()) error {
		if queued != nil {
			queued()
		}
		if e := leader.ApplyReplicated(batch); e != nil {
			return e
		}
		return follower.ApplyReplicated(batch)
	}})
	ctx := context.Background()
	if _, e := leader.EnableChangeLog(ctx); e != nil {
		t.Fatal(e)
	}
	// Different node-local sequence watermarks must not change CDC positions.
	if e := follower.WriteBatch([]Mutation{{Key: []byte{3, 1}, Value: []byte("temp"), NoWAL: true}}); e != nil {
		t.Fatal(e)
	}
	if e := leader.WriteBatch([]Mutation{{Key: changeRow(7), Value: []byte("value")}}); e != nil {
		t.Fatal(e)
	}
	a, e := leader.ReadChanges(ctx, 0, 100)
	if e != nil {
		t.Fatal(e)
	}
	b, e := follower.ReadChanges(ctx, 0, 100)
	if e != nil {
		t.Fatal(e)
	}
	if len(a) != 1 || len(b) != 1 || !bytes.Equal(encodeChangeRecord(a[0]), encodeChangeRecord(b[0])) {
		t.Fatal(a, b)
	}
	ah, _ := leader.ChangeLogHeader()
	bh, _ := follower.ChangeLogHeader()
	if ah.SourceID != bh.SourceID {
		t.Fatal(ah, bh)
	}
}
func TestChangeLogCrashRecovery(t *testing.T) {
	if path := os.Getenv("SLOPDB_CDC_CRASH_CHILD"); path != "" {
		tree, e := NewLSMTree(path, DefaultOptions())
		if e != nil {
			os.Exit(31)
		}
		if _, e = tree.EnableChangeLog(context.Background()); e != nil {
			os.Exit(32)
		}
		if e = tree.WriteBatch([]Mutation{{Key: changeRow(3), Value: []byte("durable")}}); e != nil {
			os.Exit(33)
		}
		os.Exit(0)
	}
	path := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestChangeLogCrashRecovery$")
	cmd.Env = append(os.Environ(), "SLOPDB_CDC_CRASH_CHILD="+path)
	if out, e := cmd.CombinedOutput(); e != nil {
		t.Fatalf("child %v: %s", e, out)
	}
	tree, e := NewLSMTree(path, DefaultOptions())
	if e != nil {
		t.Fatal(e)
	}
	defer tree.Close()
	rows, e := tree.ReadChanges(context.Background(), 0, 10)
	if e != nil || len(rows) != 1 || string(rows[0].Mutations[0].After) != "durable" {
		t.Fatal(rows, e)
	}
	if _, found, e := tree.Get(changeRow(3)); e != nil || !found {
		t.Fatal(found, e)
	}
}

func TestChangeLogReplicatedBroadAndCatalogRangesShareClock(t *testing.T) {
	leader, follower := openReaderViewTest(t), openReaderViewTest(t)
	ctx := context.Background()
	leader.SetReplicationHooks(&ReplicationHooks{Write: func(batch []Mutation, queued func()) error {
		if queued != nil {
			queued()
		}
		for _, w := range batch {
			if w.RangeDelete && changeRangeMayContainData(w.Key, w.Value) {
				stamped := false
				for _, clock := range batch {
					stamped = stamped || bytes.Equal(clock.Key, changeLogClockKey)
				}
				if !stamped {
					t.Fatal("replicated range has no deterministic clock")
				}
			}
		}
		if err := leader.ApplyReplicated(batch); err != nil {
			return err
		}
		return follower.ApplyReplicated(batch)
	}})
	if err := leader.Put([]byte("\x00__schema__"), []byte(`{"baseline":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.EnableChangeLog(ctx); err != nil {
		t.Fatal(err)
	}
	if err := leader.Put(changeRow(3), []byte("row")); err != nil {
		t.Fatal(err)
	}
	if err := leader.DeleteRange([]byte{1}, []byte{2}); err != nil {
		t.Fatal(err)
	}
	if err := leader.DeleteRange([]byte("\x00__schema"), []byte("\x00__schemb")); err != nil {
		t.Fatal(err)
	}
	a, err := leader.ReadChanges(ctx, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	b, err := follower.ReadChanges(ctx, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != 3 || len(b) != 3 {
		t.Fatalf("range changes missing: %+v %+v", a, b)
	}
	for i := range a {
		if !bytes.Equal(encodeChangeRecord(a[i]), encodeChangeRecord(b[i])) {
			t.Fatal("replica journal differs", a[i], b[i])
		}
	}
	if !a[1].Mutations[0].Delete || !a[2].Mutations[0].Delete {
		t.Fatal(a)
	}
	catalog, err := leader.ChangeCatalogAt(ctx, a[2].Sequence)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := catalog["\x00__schema__"]; ok {
		t.Fatal("range-deleted catalog remains in checkpoint")
	}
}

func TestChangeLogPhysicalSnapshotRestorePreservesHistory(t *testing.T) {
	source := openReaderViewTest(t)
	ctx := context.Background()
	if err := source.Put([]byte("\x00__schema__"), []byte(`{"baseline":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := source.EnableChangeLog(ctx); err != nil {
		t.Fatal(err)
	}
	if err := source.Put(changeRow(4), []byte("before")); err != nil {
		t.Fatal(err)
	}
	if err := source.Put([]byte("\x00__schema__"), []byte(`{"current":2}`)); err != nil {
		t.Fatal(err)
	}
	if err := source.Put(changeRow(4), []byte("after")); err != nil {
		t.Fatal(err)
	}
	path := t.TempDir()
	target, err := NewLSMTree(path, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	expected := map[string][]byte{}
	it := source.NewIterator()
	for ; it.Valid(); it.Next() {
		key, value := bytes.Clone(it.Key().UserKey), bytes.Clone(it.Value())
		expected[string(key)] = value
		if err = target.RestoreSnapshotBatch([]Mutation{{Key: key, Value: value}}); err != nil {
			it.Close()
			t.Fatal(err)
		}
	}
	if err = IteratorError(it); err != nil {
		it.Close()
		t.Fatal(err)
	}
	it.Close()
	if err = target.Close(); err != nil {
		t.Fatal(err)
	}
	target, err = NewLSMTree(path, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	it = target.NewIterator()
	for ; it.Valid(); it.Next() {
		key := string(it.Key().UserKey)
		value, ok := expected[key]
		if !ok || !bytes.Equal(value, it.Value()) {
			it.Close()
			t.Fatalf("restored state differs at %q", key)
		}
		delete(expected, key)
	}
	if err = IteratorError(it); err != nil {
		it.Close()
		t.Fatal(err)
	}
	it.Close()
	if len(expected) != 0 {
		t.Fatal("missing snapshot keys", expected)
	}
	if err = target.Put(changeRow(4), []byte("post-restore")); err != nil {
		t.Fatal(err)
	}
	records, err := target.ReadChanges(ctx, 3, 10)
	if err != nil || len(records) != 1 || string(records[0].Mutations[0].Before) != "after" {
		t.Fatal(records, err)
	}
}

func TestChangeLogReadBytePagesPreserveWholeTransactions(t *testing.T) {
	tree := openReaderViewTest(t)
	ctx := context.Background()
	if _, err := tree.EnableChangeLog(ctx); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte{'x'}, 6<<20)
	for i := uint64(1); i <= 3; i++ {
		if err := tree.Put(changeRow(i), payload); err != nil {
			t.Fatal(err)
		}
	}
	first, err := tree.ReadChanges(ctx, 0, 4096)
	if err != nil || len(first) != 2 {
		t.Fatalf("read ignored byte page: count=%d err=%v", len(first), err)
	}
	second, err := tree.ReadChanges(ctx, first[1].Sequence, 4096)
	if err != nil || len(second) != 1 || second[0].Sequence != 3 {
		t.Fatal(second, err)
	}
	large := bytes.Repeat([]byte{'y'}, changeLogReadPageBytes+1)
	if err = tree.Put(changeRow(4), large); err != nil {
		t.Fatal(err)
	}
	last, err := tree.ReadChanges(ctx, 3, 4096)
	if err != nil || len(last) != 1 || len(last[0].Mutations) != 1 || !bytes.Equal(last[0].Mutations[0].After, large) {
		t.Fatalf("large transaction truncated: count=%d err=%v", len(last), err)
	}
}

func TestChangeLogBudgetFailuresPreserveSourceAndPositions(t *testing.T) {
	tree := openReaderViewTest(t)
	ctx := context.Background()
	if _, err := tree.EnableChangeLog(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tree.Put(changeRow(1), []byte("original")); err != nil {
		t.Fatal(err)
	}
	before, err := tree.ChangeLogPosition()
	if err != nil {
		t.Fatal(err)
	}
	batches := [][]Mutation{
		{{Key: changeRow(1), Value: bytes.Repeat([]byte{'a'}, 1024)}},
		{{Key: []byte{1}, Value: []byte{2}, RangeDelete: true}},
	}
	for _, batch := range batches {
		tree.writerMu.Lock()
		_, _, err = tree.appendChangeLogBounded(batch, tree.published.Load(), nil, false, 32)
		tree.writerMu.Unlock()
		if err == nil {
			t.Fatal("unbounded journal expansion")
		}
		value, ok, e := tree.Get(changeRow(1))
		if e != nil || !ok || string(value) != "original" {
			t.Fatal("failed journal planning changed row", string(value), ok, e)
		}
		after, e := tree.ChangeLogPosition()
		if e != nil || after != before {
			t.Fatal("failed journal planning advanced position", after, e)
		}
	}
}
