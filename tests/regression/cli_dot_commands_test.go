package regression

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCLIDotCommandsRequireOptIn(t *testing.T) {
	env := newRegressionEnv(t)
	for _, inputMode := range []string{"pipe", "file"} {
		for _, command := range []string{".system touch MARKER", ".shell touch MARKER", ".read |touch MARKER", ".output MARKER", ".quit", ".allow-dot-commands on"} {
			t.Run(inputMode+"/"+command, func(t *testing.T) {
				dir := t.TempDir()
				marker := filepath.Join(dir, "must-not-exist")
				input := "CREATE TABLE t(id INTEGER); INSERT INTO t VALUES(1);\n \t" + strings.ReplaceAll(command, "MARKER", marker) + "\nINSERT INTO t VALUES(2);\n"
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				args := []string{filepath.Join(dir, "db")}
				if command == ".shell touch MARKER" {
					args = append([]string{"--allow-dot-commands=false"}, args...)
				}
				cmd := exec.CommandContext(ctx, env.cli(), args...)
				if inputMode == "pipe" {
					cmd.Stdin = strings.NewReader(input)
				} else {
					file := filepath.Join(dir, "input.sql")
					if err := os.WriteFile(file, []byte(input), 0600); err != nil {
						t.Fatal(err)
					}
					f, err := os.Open(file)
					if err != nil {
						t.Fatal(err)
					}
					defer f.Close()
					cmd.Stdin = f
				}
				output, err := cmd.CombinedOutput()
				if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 1 {
					t.Fatalf("expected rejection, got %v: %s", err, output)
				}
				if !strings.Contains(string(output), "dot commands are disabled") || !strings.Contains(string(output), "--allow-dot-commands") {
					t.Fatalf("missing opt-in diagnostic: %s", output)
				}
				if _, err = os.Stat(marker); !os.IsNotExist(err) {
					t.Fatal("disabled command created a file", err)
				}
				d, err := env.openDatabase(filepath.Join(dir, "db"), nil)
				if err != nil {
					t.Fatal(err)
				}
				defer d.Close()
				r, err := d.Exec("SELECT id FROM t")
				if err != nil {
					t.Fatal(err)
				}
				if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 1 {
					t.Fatal("rejection did not stop and persist earlier SQL", r)
				}
			})
		}
	}
}

func TestCLIDotCommandOptInAndSQLLiterals(t *testing.T) {
	env := newRegressionEnv(t)
	for _, optIn := range []bool{false, true} {
		t.Run(map[bool]string{false: "sql-only", true: "opt-in"}[optIn], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			args := []string{env.path(map[bool]string{false: "plain", true: "enabled"}[optIn])}
			input := "CREATE TABLE t(v TEXT); INSERT INTO t VALUES('.system literal only\n.shell literal only'); SELECT v FROM t;\n"
			if optIn {
				args = append([]string{"--allow-dot-commands"}, args...)
				input = ".print OPT_IN_OK\n" + input + ".quit\n"
			}
			cmd := exec.CommandContext(ctx, env.cli(), args...)
			cmd.Stdin = strings.NewReader(input)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatal(err, string(output))
			}
			if !strings.Contains(string(output), ".system literal only") || strings.Contains(string(output), "dot commands are disabled") {
				t.Fatal(string(output))
			}
			if optIn && !strings.Contains(string(output), "OPT_IN_OK") {
				t.Fatal("explicit opt-in ignored", string(output))
			}
		})
	}
	cmd := exec.Command(env.cli(), "--help")
	output, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "allow-dot-commands") {
		t.Fatal(err, string(output))
	}
}
