package cli

import (
	"fmt"
	"strings"

	"slopdb/storage"
)

// formatDatabaseStatus is a cheap snapshot. It does not scan rows or verify
// checksums; .integrity performs the separate, explicit verification.
func formatDatabaseStatus(stats storage.LSMStats, options *storage.Options) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Database: %s\n", stats.Dir)
	switch {
	case !stats.WALActive:
		b.WriteString("Durability: WAL disabled; unflushed writes require a clean close\n")
	case stats.WALSyncDisabled:
		b.WriteString("Durability: WAL enabled without fsync; power loss may lose recent writes\n")
	default:
		b.WriteString("Durability: WAL enabled with fsync\n")
	}
	fmt.Fprintf(&b, "SSTable page target: %d bytes\n", stats.PageSize)
	if options != nil {
		quoting := "default (double-quoted identifiers)"
		if options.MySQLQuoting {
			quoting = "MySQL (double-quoted strings, backslash escapes)"
		}
		fmt.Fprintf(&b, "SQL quoting: %s\n", quoting)
		queryTimeout := options.QueryTimeout.String()
		if options.QueryTimeout == 0 {
			queryTimeout = "unlimited"
		}
		lockTimeout := options.LockTimeout.String()
		if options.LockTimeout == 0 {
			lockTimeout = "fail immediately on contention"
		}
		fmt.Fprintf(&b, "Timeouts: query %s, lock wait %s\n", queryTimeout, lockTimeout)
		fmt.Fprintf(&b, "Deadlock detection: %t\n", options.DeadlockDetection)
		l0, lower := effectiveCompression(options)
		fmt.Fprintf(&b, "Compression for new blocks: L0 %s, L1-L6 %s\n", l0, lower)
	}
	fmt.Fprintf(&b, "Memtables: %d bytes active, %d bytes immutable\n", stats.MemSize, stats.ImmSize)
	var files int
	var bytes int64
	for _, level := range stats.Levels {
		files += level.FileCount
		bytes += level.TotalSize
	}
	fmt.Fprintf(&b, "SSTables: %d files, %d bytes\n", files, bytes)
	for i, level := range stats.Levels {
		if level.FileCount != 0 {
			fmt.Fprintf(&b, "  L%d: %d files, %d bytes\n", i, level.FileCount, level.TotalSize)
		}
	}
	return b.String()
}

func effectiveCompression(options *storage.Options) (l0, lower storage.CompressionType) {
	l0, lower = options.CompressionType, options.CompressionType
	if lower == "" || lower == storage.AutoCompression {
		l0, lower = storage.NoCompression, storage.ZstdCompression
	}
	if options.L0CompressionType != "" && options.L0CompressionType != storage.AutoCompression {
		l0 = options.L0CompressionType
	}
	return l0, lower
}
