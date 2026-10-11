package storage

import "fmt"

// Flush seals all completed writes into synced SSTables and a durable manifest.
// Readers keep their pinned views; writers wait until the sealed memtable has
// been installed. It also checkpoints no-WAL databases without closing them.
func (l *LSMTree) Flush() error {
	l.writerMu.Lock()
	defer l.writerMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	for l.imm != nil && !l.closed && l.bgErr == nil {
		l.cond.Wait()
	}
	if l.closed {
		return fmt.Errorf("database closed")
	}
	if l.bgErr != nil {
		return l.bgErr
	}
	if l.mem.ApproximateSize() == 0 {
		return nil
	}
	if l.wal != nil {
		if err := l.syncWAL(); err != nil {
			return err
		}
		if err := l.wal.Close(); err != nil {
			return err
		}
		l.wal = nil
	}
	l.imm = l.mem
	l.flushWALNum = l.walFileNum
	l.walFileNum = 0
	l.mem = NewMemTable()
	if err := l.publishViewLocked(); err != nil {
		l.bgErr = err
		l.cond.Broadcast()
		return err
	}
	if err := l.flushMemTableSync(); err != nil {
		l.bgErr = err
		l.cond.Broadcast()
		return err
	}
	l.scheduleCompaction(0)
	return nil
}
