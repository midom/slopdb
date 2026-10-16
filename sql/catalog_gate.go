package sql

import (
	"sync"
	"sync/atomic"
)

// catalogGate protects committed catalog/snapshot publication like an RWMutex,
// but statement readers register on padded counters selected by their stable
// snapshot reader IDs. Writers publish intent before checking every counter.
// Legacy metadata readers use a separate counter and participate in the same
// exclusion. Slow readers yield to queued writers, matching RWMutex preference.
type catalogGate struct {
	mu           sync.Mutex
	readChanged  *sync.Cond
	writeChanged *sync.Cond
	pending      atomic.Bool
	readers      atomic.Int64
	writers      int
	active       bool
	padding      [23]byte
	shards       [128]catalogReaders
}

type catalogReaders struct {
	count   atomic.Int64
	padding [56]byte
}

func (g *catalogGate) init() {
	if g.readChanged == nil {
		g.readChanged = sync.NewCond(&g.mu)
		g.writeChanged = sync.NewCond(&g.mu)
	}
}

func (g *catalogGate) hasReaders() bool {
	if g.readers.Load() != 0 {
		return true
	}
	for i := range g.shards {
		if g.shards[i].count.Load() != 0 {
			return true
		}
	}
	return false
}

func (g *catalogGate) Lock() {
	g.mu.Lock()
	g.init()
	g.writers++
	g.pending.Store(true)
	for g.active || g.hasReaders() {
		g.writeChanged.Wait()
	}
	g.writers--
	g.active = true
	g.mu.Unlock()
}

func (g *catalogGate) Unlock() {
	g.mu.Lock()
	g.active = false
	g.pending.Store(g.writers > 0)
	// A queued writer needs one successor. Readers stay asleep until the
	// writer queue drains, rather than waking for each catalog publication.
	if g.writers > 0 {
		g.writeChanged.Signal()
	} else {
		g.readChanged.Broadcast()
	}
	g.mu.Unlock()
}

func (g *catalogGate) RLock()   { g.rLock(&g.readers) }
func (g *catalogGate) RUnlock() { g.rUnlock(&g.readers) }

func (g *catalogGate) RLockForReader(reader uint64) {
	g.rLock(&g.shards[reader%uint64(len(g.shards))].count)
}

func (g *catalogGate) RUnlockForReader(reader uint64) {
	g.rUnlock(&g.shards[reader%uint64(len(g.shards))].count)
}

func (g *catalogGate) rLock(readers *atomic.Int64) {
	if !g.pending.Load() {
		readers.Add(1)
		if !g.pending.Load() {
			return
		}
		g.rUnlock(readers)
	}
	g.mu.Lock()
	g.init()
	for g.active || g.writers > 0 {
		g.readChanged.Wait()
	}
	readers.Add(1)
	g.mu.Unlock()
}

func (g *catalogGate) rUnlock(readers *atomic.Int64) {
	if readers.Add(-1) == 0 && g.pending.Load() {
		g.mu.Lock()
		if !g.hasReaders() {
			g.writeChanged.Signal()
		}
		g.mu.Unlock()
	}
}
