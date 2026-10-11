//go:build linux

// Adapted from /tmp/godb/no_wal_cli_review.go; see TESTING.md.
package regression

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func noWALCLIMust(testState *regressionEnv, err error) {
	testState.t.Helper()
	if err != nil {
		testState.t.Fatalf("%v", err)
	}
}
func noWALCLIAssert(testState *regressionEnv, ok bool, label string) {
	testState.t.Helper()
	if !ok {
		testState.t.Fatalf("%v", label)
	}
}
func noWALCLIRun(testState *regressionEnv, dir, content string, wantStatus int, args ...string) string {
	testState.t.Helper()
	cmd := exec.Command(testState.cli(), append(args, dir)...)
	cmd.Stdin = strings.NewReader(content)
	out, err := cmd.CombinedOutput()
	status := 0
	if err != nil {
		if e, ok := err.(*exec.ExitError); ok {
			status = e.ExitCode()
		} else {
			testState.t.Fatalf("%v", err)
		}
	}
	noWALCLIAssert(testState, status == wantStatus, fmt.Sprintf("CLI status %d, want %d: %s", status, wantStatus, out))
	noWALCLIAssert(testState, !strings.Contains(string(out), "DATA RACE"), "race: "+string(out))
	return string(out)
}
func noWALCLIVerify(testState *regressionEnv, dir string, want int64) {
	testState.t.Helper()
	files, err := os.ReadDir(dir)
	noWALCLIMust(testState, err)
	for _, f := range files {
		noWALCLIAssert(testState, !strings.HasPrefix(f.Name(), "WAL-"), "unexpected WAL file")
	}
	d, err := testState.openDatabase(dir, nil)
	noWALCLIMust(testState, err)
	r, err := d.Exec("SELECT COUNT(*) AS n FROM t")
	noWALCLIMust(testState, err)
	noWALCLIAssert(testState, r.Rows[0]["n"].Int == want, "CLI persistence: "+dir)
	noWALCLIMust(testState, d.Close())
}
func noWALCLISignalTest(testState *regressionEnv, dir string) {
	testState.t.Helper()
	master, err := os.OpenFile("/dev/ptmx", os.O_RDWR, 0)
	noWALCLIMust(testState, err)
	defer master.Close()
	noWALCLIMust(testState, unix.IoctlSetPointerInt(int(master.Fd()), unix.TIOCSPTLCK, 0))
	number, err := unix.IoctlGetInt(int(master.Fd()), unix.TIOCGPTN)
	noWALCLIMust(testState, err)
	slave, err := os.OpenFile(fmt.Sprintf("/dev/pts/%d", number), os.O_RDWR, 0)
	noWALCLIMust(testState, err)
	cmd := exec.Command(testState.cli(), "--no-wal", dir)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = slave, slave, slave
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true, Ctty: 0}
	noWALCLIMust(testState, cmd.Start())
	testState.t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	slave.Close()
	ready := make(chan struct{}, 1)
	var mu sync.Mutex
	output := ""
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := master.Read(buf)
			mu.Lock()
			output += string(buf[:n])
			done := strings.Contains(output, "last inserted rowid: 1")
			mu.Unlock()
			if done {
				select {
				case ready <- struct{}{}:
				default:
				}
			}
			if err != nil {
				return
			}
		}
	}()
	mustWait := func(ch <-chan struct{}) {
		select {
		case <-ch:
		case <-time.After(15 * time.Second):
			cmd.Process.Kill()
			cmd.Wait()
			mu.Lock()
			defer mu.Unlock()
			testState.t.Fatalf("%v", "PTY timeout: "+output)
		}
	}
	_, err = master.Write([]byte("CREATE TABLE t(id INT); INSERT INTO t VALUES(1);\n"))
	noWALCLIMust(testState, err)
	mustWait(ready)
	noWALCLIMust(testState, cmd.Process.Signal(syscall.SIGTERM))
	finished := make(chan struct{})
	var waitErr error
	go func() { waitErr = cmd.Wait(); close(finished) }()
	mustWait(finished)
	noWALCLIMust(testState, waitErr)
	mu.Lock()
	noWALCLIAssert(testState, !strings.Contains(output, "DATA RACE"), "signal race: "+output)
	mu.Unlock()
	noWALCLIVerify(testState, dir, 1)
}
func TestNoWALCLI(testT *testing.T) {
	testState := newRegressionEnv(testT)

	for _, mode := range []string{"eof", "quit", "exit", "bail"} {
		dir := testState.path("godb_no_wal_cli_") + mode
		os.RemoveAll(dir)
		q := "CREATE TABLE t(id INT); INSERT INTO t VALUES(1);\n"
		status := 0
		if mode == "quit" || mode == "exit" {
			q += "." + mode + "\n"
		}
		if mode == "bail" {
			q += ".bail on\nSELECT * FROM absent;\n"
			status = 1
		}
		noWALCLIRun(testState, dir, q, status, "--no-wal")
		noWALCLIVerify(testState, dir, 1)
	}
	testState.t.Log(fmt.Sprint("PASS WAL-free CLI EOF, .quit, .exit and .bail persistence"))
	a, b := testState.path("godb_no_wal_cli_open_a"), testState.path("godb_no_wal_cli_open_b")
	os.RemoveAll(a)
	os.RemoveAll(b)
	out := noWALCLIRun(testState, a, "CREATE TABLE t(id INT); INSERT INTO t VALUES(1);\n.open "+b+"\nCREATE TABLE t(id INT); INSERT INTO t VALUES(2);\n.manifest\n", 0, "--no-wal")
	noWALCLIAssert(testState, strings.Contains(out, "WAL:       inactive"), "no-WAL option preserved by .open")
	noWALCLIVerify(testState, a, 1)
	noWALCLIVerify(testState, b, 1)
	testState.t.Log(fmt.Sprint("PASS .open preserves WAL-free mode and flushes previous database"))
	dir := testState.path("godb_no_wal_cli_sigterm")
	os.RemoveAll(dir)
	noWALCLISignalTest(testState, dir)
	testState.t.Log(fmt.Sprint("PASS SIGTERM flushes WAL-free CLI writes"))
	dir = testState.path("godb_no_wal_cli_flush_error")
	os.RemoveAll(dir)
	out = noWALCLIRun(testState, dir, "CREATE TABLE t(id INT); INSERT INTO t VALUES(1);\n.shell mkdir "+filepath.Join(dir, "1.sst")+"\n", 1, "--no-wal")
	noWALCLIAssert(testState, strings.Contains(out, "Error closing database"), "CLI reports flush failure")
	testState.t.Log(fmt.Sprint("PASS failed exit flush reports error and exits nonzero"))
	cmd := exec.Command(testState.cli(), "--help")
	outBytes, err := cmd.CombinedOutput()
	noWALCLIMust(testState, err)
	noWALCLIAssert(testState, strings.Contains(string(outBytes), "no-wal"), "CLI flag documented")
	testState.t.Log(fmt.Sprint("PASS --help documents WAL-free mode"))
}
