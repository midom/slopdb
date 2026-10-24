package wire

import (
	"errors"
	"net"
	"os"
	"syscall"
	"time"
)

// Listen reclaims a Unix socket left by an unclean process exit. It preserves
// live listeners, regular files and symlinks. TCP uses net.Listen unchanged.
func Listen(network, address string) (net.Listener, error) {
	listener, err := net.Listen(network, address)
	if err == nil || network != "unix" {
		return listener, err
	}
	info, statErr := os.Lstat(address)
	if statErr != nil || info.Mode()&os.ModeSocket == 0 {
		return nil, err
	}
	connection, dialErr := net.DialTimeout("unix", address, 200*time.Millisecond)
	if dialErr == nil {
		connection.Close()
		return nil, err
	}
	if !errors.Is(dialErr, syscall.ECONNREFUSED) {
		return nil, err
	}
	current, statErr := os.Lstat(address)
	if statErr != nil || !os.SameFile(info, current) {
		return nil, err
	}
	if removeErr := os.Remove(address); removeErr != nil {
		return nil, removeErr
	}
	return net.Listen(network, address)
}
