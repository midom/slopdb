package main

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"slopdb/db"
	"slopdb/storage"
)

func TestCLIPageSizeFlagReopenAndOpen(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "slopdb")
	if output, err := exec.Command("go", "build", "-buildvcs=false", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatal(err, string(output))
	}
	first := filepath.Join(t.TempDir(), "first")
	second := filepath.Join(t.TempDir(), "second")
	run := func(args []string, input string) string {
		t.Helper()
		command := exec.Command(binary, args...)
		command.Stdin = strings.NewReader(input)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatal(err, string(output))
		}
		return string(output)
	}
	if output := run([]string{"--page-size=1048576", first}, "PRAGMA page_size;\nCREATE TABLE t(id INTEGER PRIMARY KEY);\nINSERT INTO t VALUES(1);\nVACUUM;\n"); !strings.Contains(output, "1048576") {
		t.Fatal(output)
	}
	if output := run([]string{first}, "PRAGMA page_size;\nSELECT id FROM t;\n"); !strings.Contains(output, "1048576") {
		t.Fatal("CLI reopen lost target", output)
	}
	options := storage.DefaultOptions()
	options.PageSize = 64 << 10
	database, err := db.Open(second, options)
	if err != nil {
		t.Fatal(err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	if output := run([]string{"--allow-dot-commands", first}, ".open "+second+"\nPRAGMA page_size;\n"); !strings.Contains(output, "65536") {
		t.Fatal(".open ignored destination target", output)
	}
	if output := run([]string{"--allow-dot-commands", "--page-size=16384", first}, ".open "+second+"\nPRAGMA page_size;\n"); !strings.Contains(output, "16384") {
		t.Fatal(".open dropped explicit selection", output)
	}
	command := exec.Command(binary, "--page-size=4294967296", filepath.Join(t.TempDir(), "invalid"))
	if output, err := command.CombinedOutput(); err == nil || !strings.Contains(string(output), "page size") {
		t.Fatal("CLI oversized page accepted", err, string(output))
	}
}
