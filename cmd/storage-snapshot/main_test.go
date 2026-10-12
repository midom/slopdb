package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotReplicationAndMaintenance(t *testing.T) {
	for _, size := range []int64{16384, 65536, 1048576} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, "source.json")
			directory := filepath.Join(root, "database")
			fixture := `{"ddl":["CREATE TABLE warehouse(w_id INTEGER PRIMARY KEY,value INTEGER)","CREATE TABLE history(h_id INTEGER PRIMARY KEY,h_w_id INTEGER,value INTEGER)","CREATE TABLE item(i_id INTEGER PRIMARY KEY,value INTEGER)"],"tables":[{"name":"warehouse","columns":["w_id","value"],"rows":[[1,11],[2,12]]},{"name":"history","columns":["h_id","h_w_id","value"],"rows":[[2,1,40],[7,2,41]]},{"name":"item","columns":["i_id","value"],"rows":[[1,99]]}]}`
			if err := os.WriteFile(source, []byte(fixture), 0644); err != nil {
				t.Fatal(err)
			}
			for _, mode := range []string{"load", "maintenance", "compact"} {
				report := filepath.Join(root, mode+".json")
				executeForTest(t, "-mode", mode, "-dir", directory, "-snapshot", source, "-replicas", "3", "-page-size", fmt.Sprint(size), "-load-batch-rows", "3", "-json", report)
				raw, err := os.ReadFile(report)
				if err != nil {
					t.Fatal(err)
				}
				var result struct {
					PageSize         int64            `json:"page_size_bytes"`
					Rows             map[string]int64 `json:"rows"`
					Verified         int64            `json:"verified_rows"`
					LoadMode         string           `json:"load_mode"`
					LoadBatchRows    int64            `json:"load_batch_rows"`
					LoadTransactions int64            `json:"load_transactions"`
				}
				if err = json.Unmarshal(raw, &result); err != nil {
					t.Fatal(err)
				}
				if result.PageSize != size || result.Rows["warehouse"] != 6 || result.Rows["history"] != 6 || result.Rows["item"] != 1 {
					t.Fatalf("%s incorrect report: %s", mode, raw)
				}
				if mode == "load" && (result.LoadMode != "bulk_insert" || result.LoadBatchRows != 3 || result.LoadTransactions != 5) {
					t.Fatalf("incorrect bulk load report: %s", raw)
				}
				if mode != "compact" && result.Verified != 13 {
					t.Fatalf("%s unverified rows: %s", mode, raw)
				}
			}
		})
	}
}

func executeForTest(t *testing.T, args ...string) {
	t.Helper()
	oldArgs, oldFlags := os.Args, flag.CommandLine
	defer func() { os.Args, flag.CommandLine = oldArgs, oldFlags }()
	os.Args = append([]string{"storage-snapshot"}, args...)
	flag.CommandLine = flag.NewFlagSet("storage-snapshot", flag.ContinueOnError)
	if err := run(); err != nil {
		t.Fatal(err)
	}
}
