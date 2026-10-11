package storage

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// Backup copies the latest committed key/value state at one pinned MVCC
// snapshot. It does not block writers, copy live WAL files or include private
// transaction writes. The destination must not exist. A fully synced database
// is staged beside it, then published with an atomic rename. The caller must
// keep the source open until Backup returns.
func (l *LSMTree) Backup(ctx context.Context, destination string) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	destination, err = filepath.Abs(destination)
	if err != nil {
		return err
	}
	if _, statErr := os.Lstat(destination); statErr == nil {
		return fmt.Errorf("backup destination already exists: %s", destination)
	} else if !os.IsNotExist(statErr) {
		return statErr
	}
	// Do not place a backup inside the source directory: startup treats files in
	// that namespace as database artifacts, and accidental removal becomes risky.
	source, err := filepath.Abs(l.dir)
	if err != nil {
		return err
	}
	source, err = filepath.EvalSymlinks(source)
	if err != nil {
		return err
	}
	resolvedParent, err := filepath.EvalSymlinks(filepath.Dir(destination))
	if err != nil {
		return err
	}
	resolvedDestination := filepath.Join(resolvedParent, filepath.Base(destination))
	rel, err := filepath.Rel(source, resolvedDestination)
	if err != nil {
		return err
	}
	if rel == "." || (rel != ".." && !bytes.HasPrefix([]byte(rel), []byte(".."+string(os.PathSeparator)))) {
		return fmt.Errorf("backup destination must be outside the source database")
	}
	parent := filepath.Dir(destination)
	stage, err := os.MkdirTemp(parent, ".slopdb-backup-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(stage) }()
	options := *l.opt
	options.PageSize = 0
	options.BlockSize = int(l.PageSize())
	options.Dir = stage
	options.DisableWAL = false
	options.DisableWALSync = false
	options.ErrorIfExists = false
	options.CreateIfNotExist = true
	options.SyncOnClose = true
	copyDB, err := NewLSMTree(stage, &options)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = copyDB.Close()
		}
	}()
	snapshot := l.Snapshot()
	defer l.ReleaseSnapshot(snapshot)
	iterator := l.NewIteratorAt(snapshot)
	defer iterator.Close()
	batch := make([]Mutation, 0, 1024)
	batchBytes := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if e := copyDB.WriteBatch(batch); e != nil {
			return e
		}
		batch = batch[:0]
		batchBytes = 0
		return nil
	}
	for iterator.Valid() {
		if err = ctx.Err(); err != nil {
			return err
		}
		key, value := append([]byte{}, iterator.Key().UserKey...), append([]byte{}, iterator.Value()...)
		batch = append(batch, Mutation{Key: key, Value: value})
		batchBytes += len(key) + len(value)
		if len(batch) >= 1024 || batchBytes >= 4<<20 {
			if err = flush(); err != nil {
				return err
			}
		}
		iterator.Next()
	}
	if err = IteratorError(iterator); err != nil {
		return fmt.Errorf("backup source scan: %w", err)
	}
	if err = flush(); err != nil {
		return err
	}
	iterator.Close()
	if err = copyDB.Close(); err != nil {
		return fmt.Errorf("backup flush: %w", err)
	}
	closed = true
	if err = ctx.Err(); err != nil {
		return err
	}
	// Serialize cooperating backup callers without exposing an empty destination.
	// Go's directory rename also rejects an already existing destination.
	lockPath := destination + ".slopdb-backup-lock"
	reservation, reserveErr := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if reserveErr != nil {
		return fmt.Errorf("reserve backup destination: %w", reserveErr)
	}
	_ = reservation.Close()
	defer os.Remove(lockPath)
	if _, statErr := os.Lstat(destination); statErr == nil {
		return fmt.Errorf("backup destination already exists: %s", destination)
	} else if !os.IsNotExist(statErr) {
		return statErr
	}
	if err = os.Rename(stage, destination); err != nil {
		return err
	}
	directory, err := os.Open(parent)
	if err != nil {
		return err
	}
	err = directory.Sync()
	closeErr := directory.Close()
	if err == nil {
		err = closeErr
	}
	return err
}

// IntegrityReport summarizes physical verification. Errors include the affected
// filename and checksum/structure diagnostic. Dead versions are checked too.
type IntegrityReport struct {
	Tables  int
	Entries int64
}

// CheckIntegrity rereads every live SSTable without the decoded-block cache,
// checking metadata, index and data block checksums and internal key ordering.
// It opens immutable files while holding the version mutex, then releases it
// before scanning so foreground writes continue.
func (l *LSMTree) CheckIntegrity(ctx context.Context) (report IntegrityReport, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	type table struct {
		name   string
		reader *SSTableReader
	}
	var tables []table
	defer func() {
		for _, t := range tables {
			_ = t.reader.Close()
		}
	}()
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return report, fmt.Errorf("database closed")
	}
	if _, found, optionsErr := readPageOptions(l.dir); optionsErr != nil || !found {
		l.mu.Unlock()
		if optionsErr != nil {
			return report, optionsErr
		}
		return report, fmt.Errorf("OPTIONS: missing page configuration")
	}
	manifest, readErr := os.ReadFile(filepath.Join(l.dir, "MANIFEST"))
	if readErr == nil {
		_, readErr = decodeManifest(manifest)
	}
	if readErr != nil && !os.IsNotExist(readErr) {
		l.mu.Unlock()
		return report, fmt.Errorf("MANIFEST: %w", readErr)
	}
	fileCount := 0
	for _, level := range l.levels {
		fileCount += len(level)
	}
	if os.IsNotExist(readErr) && fileCount > 0 {
		l.mu.Unlock()
		return report, fmt.Errorf("MANIFEST: missing with %d live SSTables", fileCount)
	}
	if readErr == nil && !bytes.Equal(manifest, encodeManifest(l.levels)) {
		l.mu.Unlock()
		return report, fmt.Errorf("MANIFEST: live file metadata mismatch")
	}
	for _, level := range l.levels {
		for _, file := range level {
			if err = ctx.Err(); err != nil {
				l.mu.Unlock()
				return report, err
			}
			name := fmt.Sprintf("%d.sst", file.Number)
			reader, openErr := OpenSSTable(filepath.Join(l.dir, name))
			if openErr != nil {
				l.mu.Unlock()
				return report, fmt.Errorf("SSTable %s: %w", name, openErr)
			}
			tables = append(tables, table{name, reader})
			if reader.fileSize != file.FileSize {
				l.mu.Unlock()
				return report, fmt.Errorf("SSTable %s: size mismatch", name)
			}
		}
	}
	l.mu.Unlock()
	for _, t := range tables {
		report.Tables++
		it := t.reader.newIterator(true, false, false)
		var previous InternalKey
		havePrevious := false
		for it.Valid() {
			if err = ctx.Err(); err != nil {
				return report, err
			}
			key := it.Key()
			if len(key.UserKey) == 0 {
				return report, fmt.Errorf("SSTable %s: empty internal key", t.name)
			}
			if havePrevious && previous.Compare(key) >= 0 {
				return report, fmt.Errorf("SSTable %s: internal keys out of order", t.name)
			}
			previous = InternalKey{UserKey: append([]byte{}, key.UserKey...), SeqNum: key.SeqNum, OpType: key.OpType}
			havePrevious = true
			if !NewBloomFilter(l.opt.BloomBitsKey).MayContain(t.reader.filterData, key.UserKey) {
				return report, fmt.Errorf("SSTable %s: bloom filter false negative for key %q", t.name, key.UserKey)
			}
			report.Entries++
			it.Next()
		}
		if err = it.Err(); err != nil {
			return report, fmt.Errorf("SSTable %s: %w", t.name, err)
		}
	}
	return report, nil
}
