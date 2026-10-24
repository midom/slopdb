package wire

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestListenUnixPreservesLiveAndRegularPaths(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "live.sock")
	first, e := Listen("unix", path)
	if e != nil {
		t.Fatal(e)
	}
	defer first.Close()
	if other, e := Listen("unix", path); e == nil {
		other.Close()
		t.Fatal("replaced live listener")
	}
	if _, e = os.Stat(path); e != nil {
		t.Fatal(e)
	}
	regular := filepath.Join(root, "regular")
	if e = os.WriteFile(regular, []byte("preserve"), 0600); e != nil {
		t.Fatal(e)
	}
	if other, e := Listen("unix", regular); e == nil {
		other.Close()
		t.Fatal("replaced regular file")
	}
	if b, e := os.ReadFile(regular); e != nil || string(b) != "preserve" {
		t.Fatal("lost regular file")
	}
}
func TestListenUnixReclaimsCrashSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stale.sock")
	old, e := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if e != nil {
		t.Fatal(e)
	}
	old.SetUnlinkOnClose(false)
	if e = old.Close(); e != nil {
		t.Fatal(e)
	}
	next, e := Listen("unix", path)
	if e != nil {
		t.Fatal(e)
	}
	if e = next.Close(); e != nil {
		t.Fatal(e)
	}
}
