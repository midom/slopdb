package storage

import (
	"runtime"
	"sync"
	"sync/atomic"
)

const maxQueuedBatches = 256
const maxQueuedGroupBytes = 1 << 20

var batchRequests = sync.Pool{New: func() any { return new(queuedBatch) }}

type queuedBatch struct {
	mutations   []Mutation
	ready       sync.WaitGroup
	committed   sync.WaitGroup
	install     *memtableGroupInstall
	installBase uint64
	lead        bool // published by ready.Done, like err
	err         error
	bytes       int
}

// The WAL leader assigns a disjoint sequence span to each participant. One
// barrier holds publication and rotation until all rows have atomic links.
type memtableGroupInstall struct {
	mem       *MemTable
	remaining sync.WaitGroup
}

func (r *queuedBatch) installRows() {
	r.install.mem.installSortedMutations(r.mutations, r.installBase)
	r.install.remaining.Done()
}

// Links are not pooled: a producer can retain its predecessor after another
// producer has exchanged tail. Separate queue nodes prevent request reuse/ABA races.
type queuedLink struct {
	request *queuedBatch
	next    atomic.Pointer[queuedLink]
}

type batchQueue struct {
	once     sync.Once
	padding0 [52]byte
	tail     atomic.Pointer[queuedLink]
	padding1 [56]byte
	head     atomic.Pointer[queuedLink] // only the active collector advances head
	padding2 [56]byte
	active   atomic.Bool
	padding3 [63]byte
}

func (q *batchQueue) init() {
	q.once.Do(func() { dummy := new(queuedLink); q.head.Store(dummy); q.tail.Store(dummy) })
}
func (q *batchQueue) next(head *queuedLink) *queuedLink {
	next := head.next.Load()
	// A producer that has exchanged tail owns the next link. Preserve FIFO if
	// it has not linked yet; no later caller may pass that transaction.
	for next == nil && q.tail.Load() != head {
		runtime.Gosched()
		next = head.next.Load()
	}
	return next
}
func (q *batchQueue) wake() {
	// A failed CAS still takes cache-line ownership on common CPUs. Read busy
	// state first; the collector's empty-queue recheck prevents a missed wake.
	if q.active.Load() {
		return
	}
	for q.active.CompareAndSwap(false, true) {
		head := q.head.Load()
		if next := q.next(head); next != nil {
			next.request.lead = true
			next.request.ready.Done()
			return
		}
		q.active.Store(false)
		// Close the empty-queue race with a producer that saw active before the
		// store. A producer linking later will perform its own wake check.
		if head.next.Load() == nil {
			return
		}
	}
}

// WriteBatch commits atomically. Concurrent batches may share one WAL record and
// one memtable publication; readers see either the entire group or none of it.
func (l *LSMTree) WriteBatch(batch []Mutation) error { return l.WriteBatchWithEnqueue(batch, nil) }

// WriteBatchWithEnqueue calls queued after the batch joins the ordered commit
// queue, before waiting for WAL publication. This lets a caller release a
// metadata lock without letting later metadata records overtake this batch.
// queued must return promptly and must not issue another storage write.
// The batch buffers remain owned by this call until it returns.
func (l *LSMTree) WriteBatchWithEnqueue(batch []Mutation, queued func()) error {
	batch = l.stampChangeBatch(batch)
	if h := l.replication.Load(); h != nil {
		return l.writeWithLocalMutations(h, batch, queued)
	}
	request := batchRequests.Get().(*queuedBatch)
	request.mutations = batch
	request.lead = false
	request.ready.Add(1)
	request.install = nil
	request.bytes = 0
	for _, w := range batch {
		request.bytes += len(w.Key) + len(w.Value) + 64
		if request.bytes > maxQueuedGroupBytes {
			break
		}
	}
	defer func() { request.mutations = nil; request.err = nil; request.install = nil; batchRequests.Put(request) }()
	q := &l.writeQueue
	q.init()
	link := &queuedLink{request: request}
	previous := q.tail.Swap(link)
	previous.next.Store(link)
	if queued != nil {
		queued()
	}
	q.wake()
	request.ready.Wait()
	if !request.lead {
		if request.install != nil {
			request.installRows()
			request.committed.Wait()
		}
		return request.err
	}
	head := q.head.Load()
	var group [maxQueuedBatches]*queuedBatch
	n, size := 0, 0
	for next := q.next(head); next != nil && n < maxQueuedBatches; next = q.next(head) {
		candidate := next.request
		if n > 0 && candidate.bytes > maxQueuedGroupBytes-size {
			break
		}
		group[n] = candidate
		n++
		size += candidate.bytes
		head = next
		// This link becomes the dummy head. Its request is no longer consulted;
		// clear it before completion can return that request to the pool.
		next.request = nil
	}
	q.head.Store(head)
	mutations := group[0].mutations
	if n > 1 {
		count := 0
		for _, next := range group[:n] {
			count += len(next.mutations)
		}
		mutations = make([]Mutation, 0, count)
		for _, next := range group[:n] {
			mutations = append(mutations, next.mutations...)
		}
	}
	var install func(*MemTable, uint64)
	if n > 1 && l.opt.ParallelMemTableWrites && !l.opt.HashMemTable {
		// Copy only the active participants; keep the fixed collection array on
		// the leader stack and avoid allocating it for hash/isolated writes.
		participants := append([]*queuedBatch(nil), group[:n]...)
		install = func(mem *MemTable, base uint64) {
			work := &memtableGroupInstall{mem: mem}
			work.remaining.Add(len(participants))
			offset := base
			for _, participant := range participants {
				participant.install = work
				participant.installBase = offset
				offset += uint64(len(participant.mutations))
				if participant != request {
					participant.committed.Add(1)
					participant.ready.Done()
				}
			}
			request.installRows()
			work.remaining.Wait()
		}
	}
	var boundaries []int
	if n > 1 {
		boundaries = make([]int, n)
		end := 0
		for i, next := range group[:n] {
			end += len(next.mutations)
			boundaries[i] = end
		}
	}
	err := l.writeMutationsWithInstaller(mutations, true, false, install, boundaries...)
	if err == nil {
		l.writeGroups.Add(1)
		l.writeBatches.Add(int64(n))
	}
	// Handoff precedes completed-call wakeups. No collector touches head again
	// after handoff; the next caller can safely collect its following group.
	if next := q.next(head); next != nil {
		next.request.lead = true
		next.request.ready.Done()
	} else {
		q.active.Store(false)
		q.wake()
	}
	for _, next := range group[:n] {
		if next == request {
			continue
		}
		next.err = err
		// Do not touch a request after waking its caller: it can return to the pool.
		if next.install != nil {
			next.committed.Done()
		} else {
			next.ready.Done()
		}
	}
	return err
}
