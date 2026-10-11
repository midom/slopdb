// Adapted from /tmp/godb/current_codecs_review.go; see TESTING.md.
package regression

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"

	"slopdb/storage"
)

func compressionCodecsMust(testState *regressionEnv, err error) {
	testState.t.Helper()
	if err != nil {
		testState.t.Fatalf("%v", err)
	}
}
func compressionCodecsCheck(testState *regressionEnv, ok bool, message string) {
	testState.t.Helper()
	if !ok {
		testState.t.Fatalf("%v", message)
	}
}
func compressionCodecsHandles(testState *regressionEnv, data []byte) []storage.BlockHandle {
	testState.t.Helper()
	f := data[len(data)-48:]
	off, n := binary.Uvarint(f)
	size, _ := binary.Uvarint(f[n:])
	it := storage.NewBlockReader(data[off : off+size-4]).NewIterator()
	var result []storage.BlockHandle
	for it.Valid() {
		x, n := binary.Uvarint(it.Value())
		y, _ := binary.Uvarint(it.Value()[n:])
		result = append(result, storage.BlockHandle{Offset: x, Size: y})
		it.Next()
	}
	compressionCodecsMust(testState, it.Err())
	return result
}
func compressionCodecsTags(testState *regressionEnv, path string) map[byte]int {
	testState.t.Helper()
	data, err := os.ReadFile(path)
	compressionCodecsMust(testState, err)
	result := map[byte]int{}
	for _, h := range compressionCodecsHandles(testState, data) {
		result[data[h.Offset+h.Size-1]]++
	}
	return result
}
func compressionCodecsDirTags(testState *regressionEnv, dir string) map[byte]int {
	testState.t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	compressionCodecsMust(testState, err)
	result := map[byte]int{}
	for _, f := range files {
		for t, n := range compressionCodecsTags(testState, f) {
			result[t] += n
		}
	}
	return result
}

func compressionCodecsTables(testState *regressionEnv) string {
	testState.t.Helper()
	rng := rand.New(rand.NewSource(42))
	random := make([]byte, 8192)
	_, err := rng.Read(random)
	compressionCodecsMust(testState, err)
	var zstdPath string
	for _, tc := range []struct {
		name  string
		codec storage.CompressionType
		tag   byte
	}{
		{"none", storage.NoCompression, 0},
		{"gzip", storage.GzipCompression, 1},
		{"zstd", storage.ZstdCompression, 2},
		{"snappy", storage.SnappyCompression, 3},
		{"lz4", storage.LZ4Compression, 4},
	} {
		opt := storage.DefaultOptions()
		opt.BlockSize = 256

		opt.CompressionType = tc.codec
		path := testState.path("codecs-") + tc.name + ".sst"
		w, err := storage.NewSSTableWriter(path, opt)
		compressionCodecsMust(testState, err)
		values := map[string][]byte{}
		for i := 0; i < 42; i++ {
			key := fmt.Sprintf("k%03d", i)
			v := bytes.Repeat([]byte("compressible value "), 80)
			if i == 0 {
				v = random
			}
			if i == 41 {
				v = bytes.Repeat([]byte("large row "), 250000)
			}
			values[key] = v
			compressionCodecsMust(testState, w.Add(storage.InternalKey{UserKey: []byte(key), SeqNum: uint64(i + 1), OpType: 1}, v))
		}
		_, err = w.Finish()
		compressionCodecsMust(testState, err)
		compressionCodecsMust(testState, w.Close())
		ts := compressionCodecsTags(testState, path)
		compressionCodecsCheck(testState, ts[tc.tag] > 0, "missing requested codec "+tc.name)
		compressionCodecsCheck(testState, ts[0] > 0, "incompressible block must stay raw")
		r, err := storage.OpenSSTable(path)
		compressionCodecsMust(testState, err)
		for k, v := range values {
			got, ok, err := r.Get([]byte(k))
			compressionCodecsMust(testState, err)
			compressionCodecsCheck(testState, ok && bytes.Equal(got, v), "roundtrip "+tc.name+" "+k)
		}
		it := r.NewIterator()
		n := 0
		for it.Valid() {
			n++
			it.Next()
		}
		compressionCodecsMust(testState, it.Err())
		compressionCodecsCheck(testState, n == 42, "iterator count")
		it.Seek([]byte("z"))
		for i := 41; i >= 0; i-- {
			it.Prev()
			compressionCodecsCheck(testState, it.Valid() && string(it.Key().UserKey) == fmt.Sprintf("k%03d", i), "reverse iterator")
		}
		compressionCodecsMust(testState, it.Err())
		if tc.name == "snappy" || tc.name == "lz4" {
			data, err := os.ReadFile(path)
			compressionCodecsMust(testState, err)
			for _, h := range compressionCodecsHandles(testState, data) {
				b := data[h.Offset : h.Offset+h.Size]
				if b[len(b)-1] == tc.tag {
					var raw []byte
					if tc.tag == 3 {
						raw, err = snappy.Decode(nil, b[:len(b)-5])
					} else {
						compressionCodecsCheck(testState, bytes.Equal(b[:4], []byte{4, 34, 77, 24}), "lz4 frame magic")
						raw, err = io.ReadAll(lz4.NewReader(bytes.NewReader(b[:len(b)-5])))
					}
					compressionCodecsMust(testState, err)
					bi := storage.NewBlockReader(raw).NewIterator()
					for bi.Valid() {
						k := storage.ParseInternalKey(bi.Key())
						compressionCodecsCheck(testState, bytes.Equal(bi.Value(), values[string(k.UserKey)]), "standalone codec decode")
						bi.Next()
					}
					compressionCodecsMust(testState, bi.Err())
				}
			}
			var wg sync.WaitGroup
			for g := 0; g < 8; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 1; i < 41; i++ {
						k := fmt.Sprintf("k%03d", i)
						got, ok, err := r.Get([]byte(k))
						compressionCodecsMust(testState, err)
						compressionCodecsCheck(testState, ok && bytes.Equal(got, values[k]), "concurrent codec read")
					}
				}()
			}
			wg.Wait()
		}
		if tc.name == "zstd" {
			zstdPath = path
			data, err := os.ReadFile(path)
			compressionCodecsMust(testState, err)
			dec, err := zstd.NewReader(nil)
			compressionCodecsMust(testState, err)
			for _, h := range compressionCodecsHandles(testState, data) {
				b := data[h.Offset : h.Offset+h.Size]
				if b[len(b)-1] == 2 {
					compressionCodecsCheck(testState, bytes.Equal(b[:4], []byte{0x28, 0xb5, 0x2f, 0xfd}), "zstd frame magic")
					raw, err := dec.DecodeAll(b[:len(b)-5], nil)
					compressionCodecsMust(testState, err)
					bi := storage.NewBlockReader(raw).NewIterator()
					for bi.Valid() {
						k := storage.ParseInternalKey(bi.Key())
						compressionCodecsCheck(testState, bytes.Equal(bi.Value(), values[string(k.UserKey)]), "standalone zstd decode")
						bi.Next()
					}
					compressionCodecsMust(testState, bi.Err())
				}
			}
			dec.Close()
			var wg sync.WaitGroup
			for g := 0; g < 8; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 1; i < 41; i++ {
						k := fmt.Sprintf("k%03d", i)
						got, ok, err := r.Get([]byte(k))
						compressionCodecsMust(testState, err)
						compressionCodecsCheck(testState, ok && bytes.Equal(got, values[k]), "concurrent read")
					}
				}()
			}
			wg.Wait()
		}
		compressionCodecsMust(testState, r.Close())
	}
	testState.t.Log(fmt.Sprint("PASS none, gzip, zstd, snappy and lz4 SSTables, raw fallback, large rows, iterators, concurrent decode, standalone zstd frames"))
	return zstdPath
}

func compressionCodecsCorrupt(testState *regressionEnv, path string) {
	testState.t.Helper()
	original, err := os.ReadFile(path)
	compressionCodecsMust(testState, err)
	var handle storage.BlockHandle
	for _, h := range compressionCodecsHandles(testState, original) {
		if original[h.Offset+h.Size-1] == 2 {
			handle = h
			break
		}
	}
	for _, tc := range []struct {
		name, contains string
		mutate         func([]byte)
	}{
		{"crc", "block CRC mismatch", func(b []byte) { b[0] ^= 1 }},
		{"zstd", "zstd decompress", func(b []byte) {
			b[0] ^= 1
			binary.LittleEndian.PutUint32(b[len(b)-5:len(b)-1], crc32.ChecksumIEEE(b[:len(b)-5]))
		}},
		{"unknown", "unknown compression type", func(b []byte) { b[len(b)-1] = 255 }},
	} {
		data := append([]byte(nil), original...)
		tc.mutate(data[handle.Offset : handle.Offset+handle.Size])
		bad := testState.path("codecs-corrupt-") + tc.name + ".sst"
		compressionCodecsMust(testState, os.WriteFile(bad, data, 0600))
		r, err := storage.OpenSSTable(bad)
		if r != nil {
			it := r.NewIterator()
			for it.Valid() {
				it.Next()
			}
			err = it.Err()
			it.Close()
			r.Close()
		}
		compressionCodecsCheck(testState, err != nil && strings.Contains(err.Error(), tc.contains), fmt.Sprintf("corruption %s: %v", tc.name, err))
	}
	path = testState.path("codecs-invalid-options.sst")
	compressionCodecsMust(testState, os.WriteFile(path, []byte("preserve"), 0600))
	opt := storage.DefaultOptions()
	opt.CompressionType = "invalid"
	_, err = storage.NewSSTableWriter(path, opt)
	compressionCodecsCheck(testState, err != nil, "invalid SSTable codec accepted")
	data, err := os.ReadFile(path)
	compressionCodecsMust(testState, err)
	compressionCodecsCheck(testState, string(data) == "preserve", "invalid codec truncates file")
	dir := testState.path("godb_codecs_invalid_options")
	compressionCodecsMust(testState, os.RemoveAll(dir))
	_, err = testState.openStorage(dir, opt)
	compressionCodecsCheck(testState, err != nil, "invalid LSM codec accepted")
	_, err = os.Stat(dir)
	compressionCodecsCheck(testState, os.IsNotExist(err), "invalid codec creates database")
	testState.t.Log(fmt.Sprint("PASS corruption errors, checksum verification, codec validation before file creation"))
}

func compressionCodecsMixed(testState *regressionEnv) {
	testState.t.Helper()
	dir := testState.path("godb_codecs_mixed")
	compressionCodecsMust(testState, os.RemoveAll(dir))
	want := map[string][]byte{}
	opt := storage.DefaultOptions()
	opt.WriteBufferSize = 8 << 20
	opt.BlockSize = 256
	opt.CompactionL0Trigger = 100
	opt.DisableWAL = true
	opt.MaxOpenFiles = 1
	for phase, codec := range []storage.CompressionType{storage.NoCompression, storage.GzipCompression, storage.ZstdCompression, storage.SnappyCompression, storage.LZ4Compression} {
		opt.CompressionType = codec
		l, err := testState.openStorage(dir, opt)
		compressionCodecsMust(testState, err)
		for i := 0; i < 50; i++ {
			k := fmt.Sprintf("p%d:k%03d", phase, i)
			v := bytes.Repeat([]byte(k+" value "), 80)
			want[k] = v
			compressionCodecsMust(testState, l.Put([]byte(k), v))
		}
		compressionCodecsMust(testState, l.Close())
	}
	seen := compressionCodecsDirTags(testState, dir)
	for _, tag := range []byte{0, 1, 2, 3, 4} {
		compressionCodecsCheck(testState, seen[tag] > 0, "mixed codec absent")
	}
	opt.CompressionType = storage.ZstdCompression
	l, err := testState.openStorage(dir, opt)
	compressionCodecsMust(testState, err)
	verify := func(old bool, snapshot uint64) {
		for k, v := range want {
			got, ok, err := l.GetAt([]byte(k), snapshot)
			compressionCodecsMust(testState, err)
			if old || !strings.HasPrefix(k, "p1:") {
				compressionCodecsCheck(testState, ok && bytes.Equal(got, v), "mixed get "+k)
			} else {
				compressionCodecsCheck(testState, !ok, "range delete "+k)
			}
		}
	}
	snap := l.Snapshot()
	verify(true, snap)
	compressionCodecsMust(testState, l.DeleteRange([]byte("p1:"), []byte("p2:")))
	compressionCodecsMust(testState, l.CompactRange())
	verify(true, snap)
	verify(false, ^uint64(0))
	l.ReleaseSnapshot(snap)
	compressionCodecsMust(testState, l.Close())
	l, err = testState.openStorage(dir, opt)
	compressionCodecsMust(testState, err)
	compressionCodecsMust(testState, l.CompactRange())
	verify(false, ^uint64(0))
	compressionCodecsMust(testState, l.Close())
	compressionCodecsCheck(testState, compressionCodecsDirTags(testState, dir)[2] > 0, "compaction failed to output zstd")
	opt.CompressionType = storage.NoCompression
	l, err = testState.openStorage(dir, opt)
	compressionCodecsMust(testState, err)
	verify(false, ^uint64(0))
	compressionCodecsMust(testState, l.Close())
	testState.t.Log(fmt.Sprint("PASS mixed codec databases, codec switching, zstd compaction, MVCC snapshots/range deletes, cache eviction and reopen with compression disabled"))
}

func compressionCodecsCli(testState *regressionEnv) {
	testState.t.Helper()
	for _, codec := range []string{"none", "gzip", "zstd", "snappy", "lz4"} {
		dir := testState.path("godb_codecs_cli_") + codec
		other := dir + "_other"
		compressionCodecsMust(testState, os.RemoveAll(dir))
		compressionCodecsMust(testState, os.RemoveAll(other))
		cmd := exec.Command(testState.cli(), "--no-wal", "--compression="+codec, dir)
		value := strings.Repeat("compressible text ", 100)
		cmd.Stdin = strings.NewReader("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES (1, '" + value + "');\n.open " + other + "\nCREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES (1, '" + value + "');\n")
		out, err := cmd.CombinedOutput()
		compressionCodecsMust(testState, err)
		compressionCodecsCheck(testState, !strings.Contains(string(out), "Error"), "CLI write error "+string(out))
		tag := byte(0)
		if codec == "gzip" {
			tag = 1
		}
		if codec == "zstd" {
			tag = 2
		}
		if codec == "snappy" {
			tag = 3
		}
		if codec == "lz4" {
			tag = 4
		}
		for _, path := range []string{dir, other} {
			compressionCodecsCheck(testState, compressionCodecsDirTags(testState, path)[tag] > 0, "CLI option/.open did not apply "+codec)
			cmd = exec.Command(testState.cli(), path)
			cmd.Stdin = strings.NewReader("SELECT length(v) FROM t;\n")
			out, err = cmd.CombinedOutput()
			compressionCodecsMust(testState, err)
			compressionCodecsCheck(testState, strings.Contains(string(out), fmt.Sprint(len(value))), "CLI reopen "+string(out))
		}
	}
	dir := testState.path("godb_codecs_cli_invalid")
	compressionCodecsMust(testState, os.RemoveAll(dir))
	cmd := exec.Command(testState.cli(), "--compression=invalid", dir)
	out, err := cmd.CombinedOutput()
	compressionCodecsCheck(testState, err != nil && strings.Contains(string(out), "invalid compression type"), "CLI accepted invalid codec")
	_, err = os.Stat(dir)
	compressionCodecsCheck(testState, os.IsNotExist(err), "invalid CLI creates database")
	testState.t.Log(fmt.Sprint("PASS CLI compression options, .open preservation, automatic decoding on default reopen, invalid options"))
}

func compressionCodecsCodecCorrupt(testState *regressionEnv) {
	testState.t.Helper()
	for _, codec := range []string{"snappy", "lz4"} {
		path := testState.path("codecs-") + codec + ".sst"
		original, err := os.ReadFile(path)
		compressionCodecsMust(testState, err)
		tag := byte(3)
		if codec == "lz4" {
			tag = 4
		}
		for _, h := range compressionCodecsHandles(testState, original) {
			if original[h.Offset+h.Size-1] == tag {
				for _, fixCRC := range []bool{false, true} {
					data := append([]byte(nil), original...)
					b := data[h.Offset : h.Offset+h.Size]
					for i := 0; i < 10; i++ {
						b[i] = 255
					}
					if fixCRC {
						binary.LittleEndian.PutUint32(b[len(b)-5:len(b)-1], crc32.ChecksumIEEE(b[:len(b)-5]))
					}
					bad := testState.path("codecs-corrupt-") + codec + ".sst"
					compressionCodecsMust(testState, os.WriteFile(bad, data, 0600))
					r, err := storage.OpenSSTable(bad)
					if r != nil {
						it := r.NewIterator()
						for it.Valid() {
							it.Next()
						}
						err = it.Err()
						it.Close()
						r.Close()
					}
					compressionCodecsCheck(testState, err != nil, "malformed codec accepted")
					want := "block CRC mismatch"
					if fixCRC {
						want = codec + " decompress"
					}
					compressionCodecsCheck(testState, strings.Contains(err.Error(), want), err.Error())
				}
				break
			}
		}
	}
	testState.t.Log(fmt.Sprint("PASS malformed Snappy/LZ4 frames and CRC errors"))
}
func TestCompressionCodecs(testT *testing.T) {
	testState := newRegressionEnv(testT)

	path := compressionCodecsTables(testState)
	compressionCodecsCorrupt(testState, path)
	compressionCodecsCodecCorrupt(testState)
	compressionCodecsMixed(testState)
	compressionCodecsCli(testState)
}
