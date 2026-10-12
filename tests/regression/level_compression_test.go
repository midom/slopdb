// Adapted from current_level_compression_review.go; see TESTING.md.
package regression

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"slopdb/storage"
)

func levelCompressionMust(testState *regressionEnv, err error) {
	testState.t.Helper()
	if err != nil {
		testState.t.Fatalf("%v", err)
	}
}
func levelCompressionCheck(testState *regressionEnv, ok bool, message string) {
	testState.t.Helper()
	if !ok {
		testState.t.Fatalf("%v", message)
	}
}
func levelCompressionHandles(testState *regressionEnv, data []byte) []storage.BlockHandle {
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
	levelCompressionMust(testState, it.Err())
	return result
}
func levelCompressionTags(testState *regressionEnv, path string) map[byte]int {
	testState.t.Helper()
	data, err := os.ReadFile(path)
	levelCompressionMust(testState, err)
	result := map[byte]int{}
	for _, h := range levelCompressionHandles(testState, data) {
		result[data[h.Offset+h.Size-1]]++
	}
	return result
}
func levelCompressionDirTags(testState *regressionEnv, dir string) map[byte]int {
	testState.t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	levelCompressionMust(testState, err)
	result := map[byte]int{}
	for _, f := range files {
		for t, n := range levelCompressionTags(testState, f) {
			result[t] += n
		}
	}
	return result
}

func levelCompressionRequireTags(testState *regressionEnv, path string, tag byte) {
	testState.t.Helper()
	seen := levelCompressionTags(testState, path)
	levelCompressionCheck(testState, seen[tag] > 0, fmt.Sprintf("missing codec %d in %s: %v", tag, path, seen))
	for t := range seen {
		levelCompressionCheck(testState, t == tag || t == 0, fmt.Sprintf("unexpected codec %d in %s", t, path))
	}
}
func levelCompressionCheckLevels(testState *regressionEnv, l *storage.LSMTree, dir string, low, high byte) {
	testState.t.Helper()
	for level, ls := range l.Stats().Levels {
		for _, f := range ls.Files {
			tag := high
			if level == 0 {
				tag = low
			}
			levelCompressionRequireTags(testState, filepath.Join(dir, fmt.Sprintf("%d.sst", f.Number)), tag)
		}
	}
}
func levelCompressionPolicy(testState *regressionEnv) {
	testState.t.Helper()
	for _, tc := range []struct {
		name      string
		codec     storage.CompressionType
		low, high byte
	}{
		{"default", "", 0, 2}, {"auto", storage.AutoCompression, 0, 2}, {"none", storage.NoCompression, 0, 0},
		{"gzip", storage.GzipCompression, 1, 1}, {"zstd", storage.ZstdCompression, 2, 2},
	} {
		for _, mode := range []string{"wal", "no-wal", "no-fsync"} {
			dir := testState.path("godb_level_compression_") + tc.name + "_" + mode
			levelCompressionMust(testState, os.RemoveAll(dir))
			opt := storage.DefaultOptions()
			opt.WriteBufferSize = 8 << 20
			opt.BlockSize = 256
			opt.CompactionL0Trigger = 100
			opt.CompressionType = tc.codec

			opt.DisableWAL = mode == "no-wal"
			opt.DisableWALSync = mode == "no-fsync"
			original := *opt
			l, err := testState.openStorage(dir, opt)
			levelCompressionMust(testState, err)
			levelCompressionCheck(testState, *opt == original, "open mutated options")
			value := bytes.Repeat([]byte("level compression test "), 100)
			for i := 0; i < 80; i++ {
				levelCompressionMust(testState, l.Put([]byte(fmt.Sprintf("k%03d", i)), value))
			}
			levelCompressionMust(testState, l.Close())
			l, err = testState.openStorage(dir, opt)
			levelCompressionMust(testState, err)
			levelCompressionCheck(testState, l.Stats().Levels[0].FileCount == 1, "close did not flush to L0")
			levelCompressionCheckLevels(testState, l, dir, tc.low, tc.high)
			levelCompressionMust(testState, l.CompactRange())
			levelCompressionCheck(testState, l.Stats().Levels[6].FileCount > 0, "manual compaction did not reach L6")
			levelCompressionCheckLevels(testState, l, dir, tc.low, tc.high)
			// A later flush must still use L0's policy after earlier compaction.
			levelCompressionMust(testState, l.Put([]byte("new"), value))
			levelCompressionMust(testState, l.Close())
			levelCompressionCheck(testState, *opt == original, "write mutated options")
			l, err = testState.openStorage(dir, opt)
			levelCompressionMust(testState, err)
			levelCompressionCheck(testState, l.Stats().Levels[0].FileCount == 1, "new L0 file absent")
			levelCompressionCheckLevels(testState, l, dir, tc.low, tc.high)
			for i := 0; i < 80; i++ {
				got, ok, err := l.Get([]byte(fmt.Sprintf("k%03d", i)))
				levelCompressionMust(testState, err)
				levelCompressionCheck(testState, ok && bytes.Equal(got, value), "reopen data mismatch")
			}
			levelCompressionMust(testState, l.Close())
		}
	}
	testState.t.Log(fmt.Sprint("PASS default/auto L0 raw, L6 zstd; explicit codec overrides; all WAL modes; reopen; caller options preserved; flush after compaction"))
}
func levelCompressionBackground(testState *regressionEnv) {
	testState.t.Helper()
	dir := testState.path("godb_level_compression_background")
	levelCompressionMust(testState, os.RemoveAll(dir))
	opt := storage.DefaultOptions()
	opt.WriteBufferSize = 500
	opt.BlockSize = 256
	opt.CompactionL0Trigger = 2
	l, err := testState.openStorage(dir, opt)
	levelCompressionMust(testState, err)
	value := bytes.Repeat([]byte("background value "), 100)
	for i := 0; i < 8; i++ {
		levelCompressionMust(testState, l.Put([]byte(fmt.Sprintf("k%03d", i)), value))
	}
	deadline := time.Now().Add(10 * time.Second)
	for l.Stats().Levels[1].FileCount == 0 {
		levelCompressionCheck(testState, time.Now().Before(deadline), "background L1 compaction timed out")
		time.Sleep(time.Millisecond * 10)
	}
	levelCompressionCheckLevels(testState, l, dir, 0, 2)
	levelCompressionMust(testState, l.Close())
	l, err = testState.openStorage(dir, nil)
	levelCompressionMust(testState, err)
	levelCompressionCheckLevels(testState, l, dir, 0, 2)
	for i := 0; i < 8; i++ {
		got, ok, err := l.Get([]byte(fmt.Sprintf("k%03d", i)))
		levelCompressionMust(testState, err)
		levelCompressionCheck(testState, ok && bytes.Equal(got, value), "background persistence")
	}
	levelCompressionMust(testState, l.Close())
	testState.t.Log(fmt.Sprint("PASS automatic background compaction writes zstd in L1; flush writes raw L0"))
}
func levelCompressionCli(testState *regressionEnv) {
	testState.t.Helper()
	for _, auto := range []bool{false, true} {
		dir := testState.path(fmt.Sprintf("godb_level_compression_cli_%t", auto))
		other := dir + "_other"
		levelCompressionMust(testState, os.RemoveAll(dir))
		levelCompressionMust(testState, os.RemoveAll(other))
		args := []string{"--no-wal"}
		if auto {
			args = append(args, "--compression=auto")
		}
		args = append(args, "--allow-dot-commands", dir)
		value := strings.Repeat("CLI level compression ", 100)
		input := "CREATE TABLE t (v TEXT); INSERT INTO t VALUES ('" + value + "');\n.open " + other + "\nCREATE TABLE t (v TEXT); INSERT INTO t VALUES ('" + value + "');\n"
		cmd := exec.Command(testState.cli(), args...)
		cmd.Stdin = strings.NewReader(input)
		out, err := cmd.CombinedOutput()
		levelCompressionMust(testState, err)
		levelCompressionCheck(testState, !strings.Contains(string(out), "Error"), string(out))
		for _, path := range []string{dir, other} {
			levelCompressionCheck(testState, levelCompressionDirTags(testState, path)[0] > 0 && levelCompressionDirTags(testState, path)[2] == 0, "CLI initial L0 must be raw")
			cmd = exec.Command(testState.cli(), path)
			cmd.Stdin = strings.NewReader("VACUUM; SELECT length(v) FROM t;\n")
			out, err = cmd.CombinedOutput()
			levelCompressionMust(testState, err)
			levelCompressionCheck(testState, strings.Contains(string(out), fmt.Sprint(len(value))) && !strings.Contains(string(out), "Error"), string(out))
			levelCompressionCheck(testState, levelCompressionDirTags(testState, path)[2] > 0, "CLI compaction must use zstd")
		}
	}
	testState.t.Log(fmt.Sprint("PASS CLI default and --compression=auto, .open policy preservation, VACUUM to zstd"))
}
func TestLevelCompression(testT *testing.T) {
	testState := newRegressionEnv(testT)

	levelCompressionPolicy(testState)
	levelCompressionBackground(testState)
	levelCompressionCli(testState)
}
