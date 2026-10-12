//go:build linux

package regression

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestCLIDotCommandsInteractiveAndShellOptIn(t *testing.T) {
	env := newRegressionEnv(t)
	for _, terminal := range []bool{false, true} {
		t.Run(fmt.Sprintf("terminal=%v", terminal), func(t *testing.T) {
			dir := t.TempDir()
			marker := filepath.Join(dir, "executed")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			args := []string{filepath.Join(dir, "db")}
			if !terminal {
				args = append([]string{"--allow-dot-commands"}, args...)
			}
			cmd := exec.CommandContext(ctx, env.cli(), args...)
			input := ".system touch " + marker + "\n.quit\n"
			if !terminal {
				cmd.Stdin = strings.NewReader(input)
				if out, err := cmd.CombinedOutput(); err != nil {
					t.Fatal(err, string(out))
				}
			} else {
				master, err := os.OpenFile("/dev/ptmx", os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer master.Close()
				if err = unix.IoctlSetPointerInt(int(master.Fd()), unix.TIOCSPTLCK, 0); err != nil {
					t.Fatal(err)
				}
				number, err := unix.IoctlGetInt(int(master.Fd()), unix.TIOCGPTN)
				if err != nil {
					t.Fatal(err)
				}
				slave, err := os.OpenFile(fmt.Sprintf("/dev/pts/%d", number), os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer slave.Close()
				cmd.Stdin, cmd.Stdout, cmd.Stderr = slave, slave, slave
				cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true, Ctty: 0}
				if err = cmd.Start(); err != nil {
					t.Fatal(err)
				}
				slave.Close()
				// Drain prompts and terminal echo so the child cannot block on output.
				go func() { _, _ = io.Copy(io.Discard, master) }()
				if _, err = io.WriteString(master, input); err != nil {
					t.Fatal(err)
				}
				if err = cmd.Wait(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := os.Stat(marker); err != nil {
				t.Fatal("authorized shell command did not execute", err)
			}
		})
	}
}

func TestCLINonTerminalCharacterDevice(t *testing.T) {
	env := newRegressionEnv(t)
	null, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer null.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, env.cli(), env.path("null-input"))
	cmd.Stdin = null
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatal(err, string(out))
	}
	if strings.Contains(string(out), "slopdb v0.1.0") {
		t.Fatal("nonterminal device incorrectly entered interactive mode", string(out))
	}
}
