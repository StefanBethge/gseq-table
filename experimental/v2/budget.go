package gtable

import (
	"fmt"
	"math"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
	"github.com/stefanbethge/gseq-table/experimental/v2/internal/memlimit"
	"github.com/stefanbethge/gseq-table/experimental/v2/internal/spill"
)

// The memory budget (D28) is one budget for the whole process, shared by
// all runs in it (D65, D101). It counts the memory of the engine: the blocks
// collected by steps over all rows, the raw state of the rows read and the
// copies of the raw state of rejected rows. When a run's count reaches its
// cap, or the count of all runs reaches the budget of the process, the run
// spills to disk (D6): sort and group by spill their rows, a join the rows
// of its left side, and the rejected rows their copies of the raw state
// (P1, D49). The right side of a join must fit in the budget (P1, D58).
//
// The count is what the unit tests check the budget against (D104). The
// memory of the process is larger: the Go runtime, readers and writers,
// and what the engine keeps per source row (G67).

// DefaultMemoryShare is the share of the detected memory limit that is the
// default budget of the process: the limit of the cgroup, else the
// physical memory. The measurements of the prototype set it: without
// GOMEMLIMIT the process grew to over four times the budget, so that a
// quarter failed in containers of 1, 2 and 4 GiB (D108, G13).
const DefaultMemoryShare = 0.10

// detectLimit returns the memory limit of the process, 0 if unknown.
var detectLimit = memlimit.Detect

// SetMemoryBudget sets the memory budget of the process in bytes, for all
// runs in it (D65, D101). 0 restores the default, DefaultMemoryShare of the
// detected limit.
func SetMemoryBudget(bytes int64) { procPool.set.Store(max(bytes, 0)) }

// memPool is the memory budget of a process and what its runs count.
type memPool struct {
	set  atomic.Int64 // set budget; 0 for the default
	used atomic.Int64
	peak atomic.Int64
}

var procPool = newMemPool(0)

func newMemPool(budget int64) *memPool {
	p := &memPool{}
	p.set.Store(budget)
	return p
}

// budget returns the budget of the pool; 0 means no limit is known.
func (p *memPool) budget() int64 {
	if b := p.set.Load(); b > 0 {
		return b
	}
	return int64(float64(detectLimit()) * DefaultMemoryShare)
}

func (p *memPool) add(n int64) {
	u := p.used.Add(n)
	p.notePeak(u)
}

func (p *memPool) notePeak(u int64) {
	for {
		peak := p.peak.Load()
		if u <= peak || p.peak.CompareAndSwap(peak, u) {
			return
		}
	}
}

// manageMu serializes the check and the setting of GOMEMLIMIT.
var manageMu sync.Mutex

// manageMemory sets GOMEMLIMIT to 90 % of the detected limit, unless the
// environment or the program has set it; it is not reset after the run
// (D102).
func manageMemory() {
	if os.Getenv("GOMEMLIMIT") != "" {
		return
	}
	manageMu.Lock()
	defer manageMu.Unlock()
	if debug.SetMemoryLimit(-1) != math.MaxInt64 {
		return
	}
	if l := detectLimit(); l > 0 {
		debug.SetMemoryLimit(l / 10 * 9)
	}
}

// MemoryError says that the right side of a join does not fit in the
// memory budget. In the prototype the right side is held in memory and not
// spilled, so the run ends with StatusAborted (P1, D58).
type MemoryError struct {
	Step   string
	Need   int64
	Budget int64
}

func (e *MemoryError) Error() string {
	return fmt.Sprintf("step %s: the right side of the join needs %d bytes, over the memory budget of %d bytes; "+
		"in the prototype the right side of a join must fit in memory", e.Step, e.Need, e.Budget)
}

// runMem counts the memory of one run against its cap and the budget of
// the process, and spills when one is reached. It holds the run's spill
// directory, created at the first spill (D38, D103).
type runMem struct {
	pool   *memPool
	limit  int64 // budget of the pool at the start of the run; 0: none
	cap    int64 // cap of the run; 0: none
	used   int64
	peak   int64
	read   int64 // bytes of raw state read
	spills int
	done   bool

	keptSpilled bool
	spillers    []func() error
	srcs        []*rawSource // owned sources with raw state, for spilling
	files       []*spillFile
	root        string
	dir         *spill.Dir
	reg         map[*rawSource]int // sources of spilled origins
	regList     []*rawSource

	shared     *shared            // units with a state (D110)
	stateRows  map[*rawSource]int // source rows with a state of their own (D110)
	stateBytes int64              // counted bytes of those states
}

func newRunMem(pool *memPool, cap int64, root string) *runMem {
	return &runMem{pool: pool, limit: pool.budget(), cap: cap, root: root}
}

// budget returns the smaller of the cap and the budget of the process; 0
// if neither is set.
func (m *runMem) budget() int64 {
	switch {
	case m.cap > 0 && m.limit > 0:
		return min(m.cap, m.limit)
	case m.cap > 0:
		return m.cap
	}
	return m.limit
}

func (m *runMem) grow(n int64) {
	if m == nil || m.done || n == 0 {
		return
	}
	m.used += n
	m.peak = max(m.peak, m.used)
	m.pool.add(n)
}

func (m *runMem) shrink(n int64) { m.grow(-n) }

// over reports whether the run is at its cap or the process at its budget.
func (m *runMem) over() bool {
	if m == nil || m.done {
		return false
	}
	return (m.cap > 0 && m.used > m.cap) || (m.limit > 0 && m.pool.used.Load() > m.limit)
}

// claim counts between lo and hi bytes for the run at once, as much as
// its cap and the budget of the process leave, and returns what it
// counted. The check and the count are one atomic step on the pool, so
// that two runs cannot take the same room (D65, D101).
func (m *runMem) claim(lo, hi int64) int64 {
	if m.cap > 0 {
		hi = min(hi, m.cap-m.used)
	}
	for {
		u := m.pool.used.Load()
		n := hi
		if m.limit > 0 {
			n = min(n, m.limit-u)
		}
		n = max(n, lo)
		if m.pool.used.CompareAndSwap(u, u+n) {
			m.pool.notePeak(u + n)
			m.used += n
			m.peak = max(m.peak, m.used)
			return n
		}
	}
}

// relieve spills while the run is over its cap or the budget: first the
// rows collected by steps over all rows, then the copies of the raw state
// of rejected rows (D49).
func (m *runMem) relieve() error {
	if !m.over() {
		return nil
	}
	for _, s := range m.spillers {
		if err := s(); err != nil {
			return err
		}
		if !m.over() {
			return nil
		}
	}
	for _, src := range m.srcs {
		if err := src.spillKept(m); err != nil {
			return err
		}
	}
	return nil
}

// spillDir returns the spill directory of the run, created on first use.
func (m *runMem) spillDir() (*spill.Dir, error) {
	if m.dir == nil {
		d, err := spill.Create(m.root)
		if err != nil {
			return nil, err
		}
		m.dir = d
	}
	return m.dir, nil
}

// finish ends the counting of the run. The raw state that rows of the
// result still need is read back into memory (D87); the rest of the spill
// directory is removed, unless spilled rejected rows live on in the result
// until Close (D49). It returns the closer of the result, or nil.
func (m *runMem) finish() (*resultCloser, error) {
	var err error
	for _, src := range m.srcs {
		if e := src.unspill(); e != nil && err == nil {
			err = e
		}
	}
	m.pool.add(-m.used)
	m.used, m.done = 0, true
	if m.dir == nil {
		return nil, err
	}
	// Files of a run that ended early are still open; the copies of
	// rejected rows stay open until Close.
	for _, f := range m.files {
		if !f.closed && !(m.keptSpilled && f.isKept) {
			f.remove()
		}
	}
	if m.keptSpilled {
		return &resultCloser{dir: m.dir, srcs: m.srcs}, err
	}
	if e := m.dir.Remove(); e != nil && err == nil {
		err = e
	}
	return nil, err
}

// resultCloser removes the spill directory of a result with spilled
// rejected rows at Close.
type resultCloser struct {
	once sync.Once
	dir  *spill.Dir
	srcs []*rawSource
	err  error
}

func (c *resultCloser) close() error {
	c.once.Do(func() {
		for _, src := range c.srcs {
			src.closeSpill()
		}
		c.err = c.dir.Remove()
	})
	return c.err
}

// blockBytes counts the columns of b that are not shared, such as with the
// raw state, which is counted on its own.
func blockBytes(b block.Block) int64 {
	var n int64
	for i := range b.Width() {
		if c := b.Column(i); !c.Shared() {
			n += c.Bytes()
		}
	}
	return n
}

// tableBytes counts all columns of t.
func tableBytes(t Table) int64 {
	var n int64
	for _, b := range t.blocks {
		for i := range b.Width() {
			n += b.Column(i).Bytes()
		}
	}
	return n
}
