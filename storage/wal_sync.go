package storage

import (
	"errors"
	"fmt"
)

// A completion covers this generation and every earlier sealed generation.
// err is immutable once done closes. Only in-flight goroutines retain earlier
// tasks; the writer keeps a single tail rather than an ever-growing WAL list.
type walSyncTask struct {
	done chan struct{}
	err  error
}

// sealWAL transfers exclusive ownership of the old writer to a background job.
// The caller holds writerMu and mu. AddRecord already flushed buffered bytes
// to the OS, so the new generation can accept deferred writes immediately.
func (l *LSMTree) sealWAL() error {
	wal := l.wal
	if l.opt.DisableWALSync {
		if err := wal.Close(); err != nil {
			return err
		}
		l.wal = nil
		return nil
	}
	previous, number := l.walSyncTail, l.walFileNum
	task := &walSyncTask{done: make(chan struct{})}
	l.walSyncTail = task
	l.wal = nil
	l.walSyncWg.Add(1)
	go func() {
		defer l.walSyncWg.Done()
		// No storage lock is held during fsync/close or predecessor waits.
		err := wal.Sync()
		if e := wal.Close(); err == nil {
			err = e
		}
		if err != nil {
			err = fmt.Errorf("synchronize rotated WAL %d: %w", number, err)
			// Fail future writes promptly, even if an older generation is
			// still syncing. Barrier waiters never hold mu while joining us.
			l.mu.Lock()
			if l.bgErr == nil {
				l.bgErr = err
			}
			l.cond.Broadcast()
			l.mu.Unlock()
		}
		if previous != nil {
			<-previous.done
			if previous.err != nil {
				if err == nil {
					err = previous.err
				} else {
					err = errors.Join(previous.err, err)
				}
			}
		}
		task.err = err
		close(task.done)
	}()
	return nil
}

// syncWAL is a prefix barrier, not just a sync of the current file. The caller
// holds writerMu (or is opening the database before admitting callers). It must
// not hold mu while pending jobs can still publish errors. Close joins all jobs
// before taking mu. Syncing the active WAL first overlaps its I/O with sync jobs.
func (l *LSMTree) syncWAL() error {
	if l.opt.DisableWALSync {
		return nil
	}
	var err error
	if l.wal != nil {
		err = l.wal.Sync()
	}
	if tail := l.walSyncTail; tail != nil {
		<-tail.done
		if tail.err != nil {
			if err == nil {
				err = tail.err
			} else {
				err = errors.Join(tail.err, err)
			}
		}
	}
	return err
}
