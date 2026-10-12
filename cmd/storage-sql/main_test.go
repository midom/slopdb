package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"slopdb/sql"
)

func fixture(t *testing.T) (string, string, manifest) {
	t.Helper()
	dir := t.TempDir()
	source := `-- canonical fixture; comment punctuation stays intact
CREATE TABLE payload (id BIGINT PRIMARY KEY NOT NULL, n BIGINT, f DOUBLE, txt TEXT);
BEGIN;
INSERT INTO payload VALUES
 (1,-9223372036854775808,-0.0,'a''b;` + "\n" + `世界'),
 (2,9223372036854775807,1.25,NULL),
 (3,NULL,NULL,'same'), (4,NULL,NULL,'same');
COMMIT;
/* no benchmark queries; this is schema metadata */
CREATE INDEX payload_n ON payload(n);
CREATE VIEW payload_view AS SELECT * FROM payload;
ANALYZE;
-- trailing comment
`
	sourceFile := filepath.Join(dir, "canonical.sql")
	if err := os.WriteFile(sourceFile, []byte(source), 0644); err != nil {
		t.Fatal(err)
	}
	// Build expected bytes without calling production encodeRow, including rows
	// with duplicate payloads, exact int64 extremes and a signed zero real.
	var encoded []byte
	var logical int64
	addRow := func(id int64, n *int64, f *float64, text *string) {
		encoded = append(encoded, 'R', 0, 0, 0, 4)
		integer := func(v int64) {
			encoded = append(encoded, 1)
			encoded = binary.BigEndian.AppendUint64(encoded, uint64(v))
			logical += 8
		}
		integer(id)
		if n == nil {
			encoded = append(encoded, 0)
		} else {
			integer(*n)
		}
		if f == nil {
			encoded = append(encoded, 0)
		} else {
			encoded = append(encoded, 2)
			encoded = binary.BigEndian.AppendUint64(encoded, math.Float64bits(*f))
			logical += 8
		}
		if text == nil {
			encoded = append(encoded, 0)
		} else {
			encoded = append(encoded, 3)
			encoded = binary.BigEndian.AppendUint64(encoded, uint64(len(*text)))
			encoded = append(encoded, []byte(*text)...)
			logical += int64(len(*text))
		}
	}
	low, high := int64(math.MinInt64), int64(math.MaxInt64)
	zero, real := 0.0, 1.25
	text, same := "a'b;\n世界", "same"
	addRow(1, &low, &zero, &text)
	addRow(2, &high, &real, nil)
	addRow(3, nil, nil, &same)
	addRow(4, nil, nil, &same)
	dataHash := sha256.Sum256(encoded)
	sqlHash := sha256.Sum256([]byte(source))
	m := manifest{Version: 1, Format: "slopdb-canonical-sql-v1", Dataset: "test", SQLSHA256: hex.EncodeToString(sqlHash[:]), SQLBytes: int64(len(source)), Tables: []tableManifest{{Name: "payload", Columns: []string{"id", "n", "f", "txt"}, LogicalTypes: []string{"int64", "int64", "float64", "text"}, OrderBy: []string{"id"}, Rows: 4, LogicalBytes: logical, EncodedBytes: int64(len(encoded)), SHA256: hex.EncodeToString(dataHash[:])}}, TotalRows: 4, TotalLogicalBytes: logical}
	manifestFile := filepath.Join(dir, "manifest.json")
	writeManifest(t, manifestFile, m)
	return sourceFile, manifestFile, m
}
func writeManifest(t *testing.T, path string, m manifest) {
	t.Helper()
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, raw, 0644); err != nil {
		t.Fatal(err)
	}
}

func TestCanonicalSQLRoundtripFullCompaction(t *testing.T) {
	for _, size := range []int64{4096, 16384, 65536, 1 << 20} {
		t.Run(stringSize(size), func(t *testing.T) {
			source, manifestPath, _ := fixture(t)
			dir := t.TempDir()
			database := filepath.Join(dir, "database")
			report := filepath.Join(dir, "report.json")
			if err := run([]string{"-sql", source, "-manifest", manifestPath, "-dir", database, "-page-size", stringSize(size), "-json", report}); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(report)
			if err != nil {
				t.Fatal(err)
			}
			var got map[string]any
			if err = json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			if got["zstd_window_bytes"] != float64(size) || got["query_benchmarked"] != false || got["wal_fsync"] != false || got["wal_enabled"] != true {
				t.Fatalf("wrong configuration: %s", raw)
			}
			for _, phase := range []string{"closed", "after_maintenance"} {
				p := got[phase].(map[string]any)
				if p["total_rows"] != float64(4) || p["logical_fingerprints_verified"] != true || len(p["wal_files"].([]any)) != 0 {
					t.Fatalf("wrong verification: %v", p)
				}
			}
			maintained := got["after_maintenance"].(map[string]any)
			if maintained["all_sstables_at_bottom"] != true || maintained["physical_sstables_match_bottom_level"] != true {
				t.Fatalf("incomplete compaction: %v", maintained)
			}
			levels := maintained["levels"].([]any)
			for _, level := range levels[:6] {
				if level.(map[string]any)["FileCount"] != float64(0) {
					t.Fatal("upper level contains SSTables")
				}
			}
		})
	}
}
func stringSize(n int64) string { return strconv.FormatInt(n, 10) }

func TestCanonicalSQLRejectsWrongHashBeforeCreatingDatabase(t *testing.T) {
	source, manifestPath, m := fixture(t)
	m.SQLSHA256 = strings.Repeat("0", 64)
	writeManifest(t, manifestPath, m)
	dir := t.TempDir()
	database := filepath.Join(dir, "database")
	report := filepath.Join(dir, "report.json")
	if err := run([]string{"-sql", source, "-manifest", manifestPath, "-dir", database, "-json", report}); err == nil {
		t.Fatal("wrong SQL hash accepted")
	}
	for _, path := range []string{database, report} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("artifact unexpectedly exists: %s", path)
		}
	}
}
func TestCanonicalSQLRejectsWrongLogicalValuesAndExistingDirectory(t *testing.T) {
	source, manifestPath, m := fixture(t)
	m.Tables[0].SHA256 = strings.Repeat("0", 64)
	writeManifest(t, manifestPath, m)
	dir := t.TempDir()
	report := filepath.Join(dir, "report.json")
	if err := run([]string{"-sql", source, "-manifest", manifestPath, "-dir", filepath.Join(dir, "db"), "-json", report}); err == nil || !strings.Contains(err.Error(), "logical fingerprint mismatch") {
		t.Fatalf("wrong values accepted: %v", err)
	}
	if _, err := os.Stat(report); !os.IsNotExist(err) {
		t.Fatal("failed run wrote success report")
	}
	source, manifestPath, _ = fixture(t)
	existing := t.TempDir()
	sentinel := filepath.Join(existing, "keep")
	if err := os.WriteFile(sentinel, []byte("keep"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-sql", source, "-manifest", manifestPath, "-dir", existing, "-json", report}); err == nil {
		t.Fatal("existing directory accepted")
	}
	if raw, err := os.ReadFile(sentinel); err != nil || string(raw) != "keep" {
		t.Fatal("existing directory altered")
	}
}

func TestCanonicalSQLRejectsPartialAndUncommittedDumps(t *testing.T) {
	for _, extra := range []string{"BEGIN;", "INSERT INTO missing_table VALUES (1);"} {
		t.Run(extra, func(t *testing.T) {
			source, manifestPath, m := fixture(t)
			raw, err := os.ReadFile(source)
			if err != nil {
				t.Fatal(err)
			}
			raw = append(raw, []byte("\n"+extra+"\n")...)
			if err = os.WriteFile(source, raw, 0644); err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(raw)
			m.SQLSHA256 = hex.EncodeToString(sum[:])
			m.SQLBytes = int64(len(raw))
			writeManifest(t, manifestPath, m)
			dir := t.TempDir()
			report := filepath.Join(dir, "report.json")
			if err = run([]string{"-sql", source, "-manifest", manifestPath, "-dir", filepath.Join(dir, "database"), "-json", report}); err == nil {
				t.Fatal("incomplete dump accepted")
			}
			if _, err = os.Stat(report); !os.IsNotExist(err) {
				t.Fatal("incomplete dump wrote success report")
			}
		})
	}
}
func TestStatementReaderHandlesQuotesCommentsAndBoundaries(t *testing.T) {
	input := `-- leading ;
INSERT INTO "semi;colon" VALUES ('quote'';text', 'line
break'); /* ; */ SELECT 'next'; -- trailing ;`
	r := newStatementReader(strings.NewReader(input))
	for i := 0; i < 2; i++ {
		q, e := r.next()
		if e != nil || !strings.HasSuffix(q, ";") {
			t.Fatalf("statement %d: %q %v", i, q, e)
		}
	}
	if _, e := r.next(); e != io.EOF {
		t.Fatal(e)
	}
	for _, input := range []string{"SELECT 'unfinished;", "SELECT 1", "/* unfinished;"} {
		if _, e := newStatementReader(strings.NewReader(input)).next(); e == nil {
			t.Fatalf("accepted %q", input)
		}
	}
	r = newStatementReader(strings.NewReader("SELECT '" + strings.Repeat("x", 64<<10) + "'';x'; SELECT 2;"))
	if _, e := r.next(); e != nil {
		t.Fatal(e)
	}
	if q, e := r.next(); e != nil || strings.TrimSpace(q) != "SELECT 2;" {
		t.Fatalf("boundary split: %q %v", q, e)
	}
	r = newStatementReader(strings.NewReader("SELECT 'large';"))
	r.limit = 8
	if _, e := r.next(); e == nil {
		t.Fatal("unbounded statement accepted")
	}
}
func TestHashRejectsNonFiniteAndNormalizesSignedZero(t *testing.T) {
	for _, f := range []float64{math.Inf(1), math.NaN()} {
		if _, _, err := encodeRow(sha256.New(), []sql.Value{sql.NewRealValue(f)}, []string{"float64"}); err == nil {
			t.Fatal("nonfinite value accepted")
		}
	}
	first, second := sha256.New(), sha256.New()
	if _, _, err := encodeRow(first, []sql.Value{sql.NewRealValue(0)}, []string{"float64"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := encodeRow(second, []sql.Value{sql.NewRealValue(math.Copysign(0, -1))}, []string{"float64"}); err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(first.Sum(nil)) != hex.EncodeToString(second.Sum(nil)) {
		t.Fatal("signed zero differed")
	}
}
