//go:build unix

package storage

import "golang.org/x/sys/unix"

func mapBlockArena(size int) ([]byte, error) {
	return unix.Mmap(-1, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANON)
}
func unmapBlockArena(data []byte) error { return unix.Munmap(data) }
