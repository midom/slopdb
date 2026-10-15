//go:build !unix

package storage

import "fmt"

// A platform without anonymous mapping support bypasses decoded cache
// admission rather than silently retaining cache payloads on the Go heap.
func mapBlockArena(size int) ([]byte, error) {
	return nil, fmt.Errorf("block arenas require Unix anonymous mappings")
}
func unmapBlockArena(data []byte) error { return nil }
