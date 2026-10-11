package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

const (
	DefaultPageSize  int64 = 4096
	MinPageSize      int64 = 512
	MaxPageSize      int64 = 64 << 20
	pageOptionsName        = "OPTIONS"
	pageOptionsMagic       = "SLOPPAGE"
)

// ValidatePageSize checks an explicit uncompressed SSTable block target before
// converting it to a native allocation size. Legacy BlockSize targets may be
// smaller/non-power-of-two, but explicit PageSize settings use this interface.
func ValidatePageSize(size int64) error {
	if size < MinPageSize || size > MaxPageSize || size&(size-1) != 0 {
		return fmt.Errorf("page size must be a power of two from %d to %d bytes, got %d", MinPageSize, MaxPageSize, size)
	}
	return nil
}
func validateBlockTarget(size int64) error {
	if size < 1 || size > MaxPageSize {
		return fmt.Errorf("block size must be from 1 to %d bytes, got %d", MaxPageSize, size)
	}
	return nil
}
func readPageOptions(dir string) (int64, bool, error) {
	data, err := os.ReadFile(filepath.Join(dir, pageOptionsName))
	if os.IsNotExist(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if len(data) != 24 || string(data[:8]) != pageOptionsMagic || binary.LittleEndian.Uint32(data[8:12]) != 1 {
		return 0, false, fmt.Errorf("OPTIONS: invalid page configuration format")
	}
	if crc32.ChecksumIEEE(data[:20]) != binary.LittleEndian.Uint32(data[20:24]) {
		return 0, false, fmt.Errorf("OPTIONS: page configuration checksum mismatch")
	}
	raw := binary.LittleEndian.Uint64(data[12:20])
	if raw > uint64(MaxPageSize) || raw == 0 {
		return 0, false, fmt.Errorf("OPTIONS: invalid stored page size %d", raw)
	}
	return int64(raw), true, nil
}
func writePageOptions(dir string, size int64) error {
	data := make([]byte, 24)
	copy(data, pageOptionsMagic)
	binary.LittleEndian.PutUint32(data[8:12], 1)
	binary.LittleEndian.PutUint64(data[12:20], uint64(size))
	binary.LittleEndian.PutUint32(data[20:24], crc32.ChecksumIEEE(data[:20]))
	file, err := os.CreateTemp(dir, ".OPTIONS-")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if _, err = file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, filepath.Join(dir, pageOptionsName)); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}
func (l *LSMTree) initPageSize() error {
	stored, found, err := readPageOptions(l.dir)
	if err != nil {
		return err
	}
	size := int64(l.opt.BlockSize)
	if found && l.opt.BlockSize == int(DefaultPageSize) {
		size = stored
	}
	if l.opt.PageSize != 0 {
		size = l.opt.PageSize
	}
	if err = validateBlockTarget(size); err != nil {
		return err
	}
	// Publish before replay: recovery checkpoints and the first flush must use the
	// selected target. Never change immutable l.opt during concurrent execution.
	l.pageSize.Store(size)
	if !found || stored != size {
		return writePageOptions(l.dir, size)
	}
	return nil
}

// PageSize reports the current uncompressed SSTable data-block target. Existing
// files can contain differently sized blocks; readers use their block handles.
func (l *LSMTree) PageSize() int64 { return l.pageSize.Load() }

// SetPageSize persists the target for writers started after this call. Existing
// SSTables and already-started writers remain valid and are not rewritten. Use
// CompactRange (SQL VACUUM) to rewrite the current contents with the new target.
// This is physical database configuration, independent of SQL transactions.
func (l *LSMTree) SetPageSize(size int64) error {
	if err := ValidatePageSize(size); err != nil {
		return err
	}
	l.writerMu.Lock()
	defer l.writerMu.Unlock()
	l.mu.RLock()
	closed := l.closed
	l.mu.RUnlock()
	if closed {
		return fmt.Errorf("database closed")
	}
	if size == l.pageSize.Load() {
		return nil
	}
	if err := writePageOptions(l.dir, size); err != nil {
		return err
	}
	l.pageSize.Store(size)
	return nil
}

// Full manual compaction must rewrite bottom-level files which normal level
// promotion left untouched; otherwise a VACUUM after changing the block target
// would leave an already compacted database entirely in the old layout.
func (l *LSMTree) rewriteBottomLevelLocked() error {
	inputs := append([]*FileMetaData(nil), l.levels[maxLevels-1]...)
	if len(inputs) == 0 {
		return nil
	}
	oldest := l.oldestSnapshotLocked()
	l.mu.Unlock()
	output, err := l.buildCompaction(inputs, oldest, maxLevels-1)
	l.mu.Lock()
	if err != nil {
		return err
	}
	return l.installCompactionLocked(maxLevels-2, inputs, output)
}
