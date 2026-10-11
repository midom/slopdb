// Adapted from /tmp/godb/bloom_quality_review.go; see TESTING.md.
package regression

import (
	"encoding/binary"
	"testing"

	"slopdb/storage"
)

func bloomQualityKey(testState *regressionEnv, id int) []byte {
	testState.t.Helper()
	return binary.LittleEndian.AppendUint64([]byte("\x01t0000:\x01"), uint64(id))
}
func TestBloomQuality(testT *testing.T) {
	testState := newRegressionEnv(testT)

	keys := make([][]byte, 20000)
	for i := range keys {
		keys[i] = bloomQualityKey(testState, i*37+1000000)
	}
	bf := storage.NewBloomFilter(10)
	data := bf.Generate(keys)
	for _, k := range keys {
		if !bf.MayContain(data, k) {
			testState.t.Fatalf("%v", "bloom false negative")
		}
	}
	falsePos := 0
	total := 200000
	for i := 0; i < total; i++ {
		if bf.MayContain(data, bloomQualityKey(testState, i+3000000)) {
			falsePos++
		}
	}
	testState.t.Logf("false positives %d/%d = %.3f%% (no timing measurement)\n", falsePos, total, float64(falsePos)*100/float64(total))
}
