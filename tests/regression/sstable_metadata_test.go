// Adapted from current_metadata_review.go; see TESTING.md.
package regression

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"slopdb/storage"
)

func sSTableMetadataMust(testState *regressionEnv, e error) {
	testState.t.Helper()
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
}
func sSTableMetadataCheck(testState *regressionEnv, b bool, s string) {
	testState.t.Helper()
	if !b {
		testState.t.Fatalf("%v", s)
	}
}
func sSTableMetadataCorrupt(testState *regressionEnv, src, dst string, offset int) {
	testState.t.Helper()
	data, e := os.ReadFile(src)
	sSTableMetadataMust(testState, e)
	data[offset] ^= 1
	sSTableMetadataMust(testState, os.WriteFile(dst, data, 0600))
}
func TestSSTableMetadata(testT *testing.T) {
	testState := newRegressionEnv(testT)

	dir := testState.path("godb_startup_metadata_review")
	sSTableMetadataMust(testState, os.RemoveAll(dir))
	sSTableMetadataMust(testState, os.MkdirAll(dir, 0700))
	o := storage.DefaultOptions()
	o.BlockSize = 128
	o.CompressionType = storage.NoCompression
	p := dir + "/table.sst"
	w, e := storage.NewSSTableWriter(p, o)
	sSTableMetadataMust(testState, e)
	sSTableMetadataMust(testState, w.Add(storage.InternalKey{UserKey: []byte("a"), SeqNum: 2, OpType: 1}, []byte(strings.Repeat("payload", 40))))
	sSTableMetadataMust(testState, w.Add(storage.InternalKey{UserKey: []byte("b"), SeqNum: 8, OpType: 2}, []byte("z")))
	sSTableMetadataMust(testState, w.Add(storage.InternalKey{UserKey: []byte("b"), SeqNum: 1, OpType: 1}, []byte("old")))
	sSTableMetadataMust(testState, w.Add(storage.InternalKey{UserKey: []byte("m"), SeqNum: 5, OpType: 1}, []byte("middle")))
	sSTableMetadataMust(testState, w.Add(storage.InternalKey{UserKey: []byte("z"), SeqNum: 7, OpType: 1}, []byte("end")))
	_, e = w.Finish()
	sSTableMetadataMust(testState, e)
	sSTableMetadataMust(testState, w.Close())
	data, e := os.ReadFile(p)
	sSTableMetadataMust(testState, e)
	footer := data[len(data)-48:]
	off, n := binary.Uvarint(footer)
	size, _ := binary.Uvarint(footer[n:])
	filterOff, _ := binary.Uvarint(footer[20:])
	metaOff := int(off + size)
	sSTableMetadataCheck(testState, bytes.Equal(data[metaOff:metaOff+8], []byte("GDBMETA1")), "metadata magic")
	r, e := storage.OpenSSTable(p)
	sSTableMetadataMust(testState, e)
	_, ok, e := r.Get([]byte("m"))
	sSTableMetadataMust(testState, e)
	sSTableMetadataCheck(testState, !ok, "range summary")
	v, ok, e := r.GetAt([]byte("m"), 6)
	sSTableMetadataMust(testState, e)
	sSTableMetadataCheck(testState, ok && string(v) == "middle", "range snapshot")
	r.Close()
	pBad := dir + "/bad-data.sst"
	sSTableMetadataCorrupt(testState, p, pBad, 0)
	r, e = storage.OpenSSTable(pBad)
	sSTableMetadataMust(testState, e)
	_, _, e = r.Get([]byte("a"))
	sSTableMetadataCheck(testState, e != nil, "lazy data CRC missed")
	r.Close()
	sSTableMetadataCorrupt(testState, p, dir+"/bad-filter.sst", int(filterOff))
	r, e = storage.OpenSSTable(dir + "/bad-filter.sst")
	sSTableMetadataCheck(testState, e != nil, "filter CRC ignored")
	sSTableMetadataCorrupt(testState, p, dir+"/bad-meta.sst", metaOff+8)
	r, e = storage.OpenSSTable(dir + "/bad-meta.sst")
	sSTableMetadataCheck(testState, e != nil, "metadata CRC ignored")
	missing := dir + "/missing-metadata.sst"
	sSTableMetadataMust(testState, os.WriteFile(missing, append(append([]byte(nil), data[:metaOff]...), footer...), 0600))
	r, e = storage.OpenSSTable(missing)
	sSTableMetadataCheck(testState, e != nil, "missing metadata accepted")
	sidecars, e := filepath.Glob(dir + "/*.meta")
	sSTableMetadataMust(testState, e)
	sSTableMetadataCheck(testState, len(sidecars) == 0, "created metadata sidecar")
	for mode := 0; mode < 3; mode++ {
		d := testState.path(fmt.Sprintf("godb_startup_metadata_lsm_%d", mode))
		sSTableMetadataMust(testState, os.RemoveAll(d))
		opt := storage.DefaultOptions()
		opt.DisableWALSync = mode == 1
		opt.DisableWAL = mode == 2
		opt.CompactionL0Trigger = 1000
		opt.BlockSize = 128
		l, e := testState.openStorage(d, opt)
		sSTableMetadataMust(testState, e)
		sSTableMetadataMust(testState, l.Put([]byte("a"), []byte("a")))
		sSTableMetadataMust(testState, l.Put([]byte("m"), []byte("old")))
		sSTableMetadataMust(testState, l.Put([]byte("z"), []byte("z")))
		sSTableMetadataMust(testState, l.Flush())
		sSTableMetadataMust(testState, l.DeleteRange([]byte("b"), []byte("y")))
		sSTableMetadataMust(testState, l.DeleteRange([]byte("x"), []byte("x")))
		sSTableMetadataMust(testState, l.DeleteRange([]byte("z"), []byte("b")))
		seq := l.Stats().SeqNum
		sSTableMetadataMust(testState, l.Close())
		sSTableMetadataMust(testState, os.Remove(d+"/MANIFEST"))
		l, e = testState.openStorage(d, opt)
		sSTableMetadataMust(testState, e)
		sSTableMetadataCheck(testState, l.Stats().SeqNum == seq, "missing manifest sequence")
		_, ok, e = l.Get([]byte("m"))
		sSTableMetadataMust(testState, e)
		sSTableMetadataCheck(testState, !ok, "missing manifest lost tombstone")
		it := l.NewIteratorRange([]byte("m"), []byte("n"), true)
		sSTableMetadataCheck(testState, !it.Valid(), "bounded tombstone summary lost")
		sSTableMetadataMust(testState, storage.IteratorError(it))
		it.Close()
		sSTableMetadataMust(testState, l.Put([]byte("m"), []byte("new")))
		sSTableMetadataCheck(testState, l.Stats().SeqNum == seq+1, "sequence regression")
		v, ok, e = l.Get([]byte("m"))
		sSTableMetadataMust(testState, e)
		sSTableMetadataCheck(testState, ok && string(v) == "new", "reinsertion shadowed")
		sSTableMetadataMust(testState, l.Close())
		files, e := filepath.Glob(d + "/*.sst")
		sSTableMetadataMust(testState, e)
		bad, e := os.ReadFile(files[0])
		sSTableMetadataMust(testState, e)
		bad[len(bad)-49] ^= 1
		sSTableMetadataMust(testState, os.WriteFile(files[0], bad, 0600))
		sSTableMetadataMust(testState, os.Remove(d+"/MANIFEST"))
		l, e = testState.openStorage(d, opt)
		sSTableMetadataCheck(testState, e != nil, "corrupt metadata directory recovery ignored")
	}
	testState.t.Log(fmt.Sprint("PASS metadata/filter CRC, lazy data CRC, range summaries, empty ranges, max sequence and missing-MANIFEST recovery in all durability modes"))
}
