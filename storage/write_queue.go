package storage

import "sync"

var batchRequests = sync.Pool{New: func() any { return &queuedBatch{lead: make(chan struct{}, 1), done: make(chan error, 1)} }}

// A queued batch retains its caller's buffers until completion. One caller leads
// each group; the next queued caller takes over after that group completes.
// This amortizes WAL writes without delaying isolated writes or keeping a leader
// waiting for an unbounded stream of later transactions.
type queuedBatch struct {
	mutations []Mutation
	lead      chan struct{}
	done      chan error
}

// WriteBatch commits atomically. Concurrent batches may share one WAL record and
// one memtable publication; readers see either the entire group or none of it.
func (l *LSMTree) WriteBatch(batch []Mutation) error {
	request := batchRequests.Get().(*queuedBatch)
	request.mutations = batch
	defer func() { request.mutations = nil; batchRequests.Put(request) }()
	l.writeMu.Lock()
	l.writeQueue = append(l.writeQueue, request)
	if !l.writeActive {
		l.writeActive = true
		request.lead <- struct{}{}
	}
	l.writeMu.Unlock()
	select {
	case err := <-request.done:
		return err
	case <-request.lead:
	}

	l.writeMu.Lock()
	n := len(l.writeQueue)
	if n > 64 {
		n = 64
	}
	group := l.writeQueue[:n:n]
	// Pending arrivals can use only the tail; the detached prefix is immutable.
	l.writeQueue = l.writeQueue[n:]
	if len(l.writeQueue) == 0 {
		l.writeQueue = nil
	}
	l.writeMu.Unlock()
	mutations := group[0].mutations
	if len(group) > 1 {
		count := 0
		for _, next := range group {
			count += len(next.mutations)
		}
		mutations = make([]Mutation, 0, count)
		for _, next := range group {
			mutations = append(mutations, next.mutations...)
		}
	}
	err := l.writeBatchDirect(mutations)
	if err == nil {
		l.writeGroups.Add(1)
		l.writeBatches.Add(int64(len(group)))
	}
	// Transfer leadership before notifying completed callers. The next leader
	// can collect their subsequent calls while its WAL write is in progress.
	l.writeMu.Lock()
	if len(l.writeQueue) > 0 {
		l.writeQueue[0].lead <- struct{}{}
	} else {
		l.writeActive = false
	}
	l.writeMu.Unlock()
	for _, next := range group {
		next.done <- err
	}
	return <-request.done
}
