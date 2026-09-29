package gtable

// Counting (D43, D84, D110). Every working row leaves every step and the
// plan exactly once, and its source rows are counted at that exit. A source
// row that no other working row holds needs nothing of its own for that:
// the counters move it from "not processed" to its category. Only source
// rows that several working rows may hold have a state per counter, so that
// the precedence of D84 joins the categories of their working rows: the
// rows of the right side of a join, left rows with more than one partner,
// and the aggregated rows among them. Those units are registered in a
// shared set before their working rows are counted.

// rowState is the category of a source row (D84), ordered so that a later
// category wins: rejected over passed over dropped.
type rowState uint8

const (
	stNone rowState = iota
	stUnprocessed
	stDropped
	stPassed
	stRejected
)

// statePage is the number of source rows per page of states and of the
// shared set.
const statePage = 1024

// unitKey is a unit with a state: a source row, or an aggregated row.
type unitKey struct {
	src *rawSource
	row int
	agg *aggOrigin
}

// shared is the set of units that several working rows may hold (D110). A
// run keeps it for its counting; a Table method keeps one too, so that an
// aggregated row keeps the identity of such units.
type shared struct {
	rows map[*rawSource]map[int]*[statePage / 64]uint64
	aggs map[*aggOrigin]int // number from 1, for spill files
	byID []*aggOrigin
	mem  *runMem // counts the pages; nil for a Table method
}

func (s *shared) isEmpty() bool { return s == nil || len(s.rows) == 0 && len(s.aggs) == 0 }

// row reports whether source row r is shared.
func (s *shared) row(r srcRef) bool {
	if s == nil || len(s.rows) == 0 {
		return false
	}
	pages := s.rows[r.src]
	if pages == nil {
		return false
	}
	p := pages[r.row/statePage]
	i := r.row % statePage
	return p != nil && p[i/64]&(1<<(i%64)) != 0
}

// addRow registers source row r as shared and reports whether it was new.
func (s *shared) addRow(r srcRef) bool {
	if s.row(r) {
		return false
	}
	if s.rows == nil {
		s.rows = map[*rawSource]map[int]*[statePage / 64]uint64{}
	}
	pages := s.rows[r.src]
	if pages == nil {
		pages = map[int]*[statePage / 64]uint64{}
		s.rows[r.src] = pages
	}
	p := pages[r.row/statePage]
	if p == nil {
		p = new([statePage / 64]uint64)
		pages[r.row/statePage] = p
		s.count(statePage / 8)
	}
	i := r.row % statePage
	p[i/64] |= 1 << (i % 64)
	if s.mem != nil {
		if s.mem.stateRows == nil {
			s.mem.stateRows = map[*rawSource]int{}
		}
		s.mem.stateRows[r.src]++
	}
	return true
}

func (s *shared) aggShared(a *aggOrigin) bool {
	if s == nil || len(s.aggs) == 0 {
		return false
	}
	_, ok := s.aggs[a]
	return ok
}

// addAgg registers aggregated row a as shared and reports whether it was
// new.
func (s *shared) addAgg(a *aggOrigin) bool {
	if s.aggShared(a) {
		return false
	}
	if s.aggs == nil {
		s.aggs = map[*aggOrigin]int{}
	}
	s.byID = append(s.byID, a)
	s.aggs[a] = len(s.byID)
	s.count(64)
	return true
}

// count counts n bytes of states in the budget (D110).
func (s *shared) count(n int64) {
	if s != nil && s.mem != nil {
		s.mem.grow(n)
		s.mem.stateBytes += n
	}
}

// register registers every unit of the given rows as shared: the right side
// of a join, whose rows can have many partners.
func (s *shared) register(orig []origin) {
	for _, o := range orig {
		for _, r := range o.refs {
			s.addRow(r)
		}
		if a := o.agg(); a != nil {
			s.addAgg(a)
		}
	}
}

// scan registers the units that more than one of the given rows holds, for
// the rows of a table that is the source of a run (D84).
func (s *shared) scan(orig []origin) {
	need := false
	for _, o := range orig {
		need = need || len(o.refs) > 1 || o.agg() != nil
	}
	if !need {
		return // one source row per working row
	}
	rows := map[srcRef]int{}
	aggs := map[*aggOrigin]int{}
	var walk func(a *aggOrigin)
	walk = func(a *aggOrigin) {
		if aggs[a]++; aggs[a] > 1 {
			return
		}
		for _, r := range a.multi {
			rows[r]++
		}
		for _, u := range a.units {
			walk(u)
		}
	}
	for _, o := range orig {
		for _, r := range o.refs {
			rows[r]++
		}
		if a := o.agg(); a != nil {
			walk(a)
		}
	}
	for r, n := range rows {
		if n > 1 {
			s.addRow(r)
		}
	}
	for a, n := range aggs {
		if n > 1 {
			s.addAgg(a)
		}
	}
}

// eachUnit calls plain with the source rows of o that are counted without
// a state, per source, and keyed with every unit of o that has a state.
func (s *shared) eachUnit(o origin, plain func(*rawSource, int), keyed func(unitKey)) {
	for _, r := range o.refs {
		if s.row(r) {
			keyed(unitKey{src: r.src, row: r.row})
		} else {
			plain(r.src, 1)
		}
	}
	if a := o.agg(); a != nil {
		s.eachAgg(a, plain, keyed)
	}
}

func (s *shared) eachAgg(a *aggOrigin, plain func(*rawSource, int), keyed func(unitKey)) {
	if s.aggShared(a) {
		keyed(unitKey{agg: a})
		s.keyedParts(a, keyed)
		return
	}
	for _, c := range a.srcs {
		plain(c.src, c.n)
	}
	for _, r := range a.multi {
		keyed(unitKey{src: r.src, row: r.row})
	}
	for _, u := range a.units {
		s.eachAgg(u, plain, keyed)
	}
}

// keyedParts calls keyed with the parts of a shared aggregated row that
// have a state of their own.
func (s *shared) keyedParts(a *aggOrigin, keyed func(unitKey)) {
	for _, r := range a.multi {
		keyed(unitKey{src: r.src, row: r.row})
	}
	for _, u := range a.units {
		if s.aggShared(u) {
			keyed(unitKey{agg: u})
		}
		s.keyedParts(u, keyed)
	}
}

// plainOf calls f with the source rows a shared aggregated row is counted
// for under its own state: its own and those of aggregated rows in it that
// have no state.
func (s *shared) plainOf(a *aggOrigin, f func(*rawSource, int)) {
	for _, c := range a.srcs {
		f(c.src, c.n)
	}
	for _, u := range a.units {
		if !s.aggShared(u) {
			s.plainOf(u, f)
		}
	}
}

// aggBuilder builds what went into an aggregated row from its input rows.
type aggBuilder struct {
	sh    *shared
	a     aggOrigin
	bySrc map[*rawSource]int // index in a.srcs
	seen  map[unitKey]bool
}

func newAggBuilder(sh *shared) *aggBuilder { return &aggBuilder{sh: sh} }

// add adds an input row.
func (b *aggBuilder) add(o origin) {
	b.a.n += o.weight()
	b.sh.eachUnitTop(o, b.plain, b.keyed)
}

func (b *aggBuilder) plain(src *rawSource, n int) {
	if b.bySrc == nil {
		b.bySrc = map[*rawSource]int{}
	}
	i, ok := b.bySrc[src]
	if !ok {
		i = len(b.a.srcs)
		b.bySrc[src] = i
		b.a.srcs = append(b.a.srcs, srcCount{src, 0})
	}
	b.a.srcs[i].n += n
}

func (b *aggBuilder) keyed(k unitKey) {
	if b.seen[k] {
		return
	}
	if b.seen == nil {
		b.seen = map[unitKey]bool{}
	}
	b.seen[k] = true
	if k.agg != nil {
		b.a.units = append(b.a.units, k.agg)
	} else {
		b.a.multi = append(b.a.multi, srcRef{k.src, k.row})
	}
}

// build returns what went into the row.
func (b *aggBuilder) build() *aggOrigin {
	a := b.a
	return &a
}

// eachUnitTop is eachUnit for building an aggregated row: a shared
// aggregated row goes in as a whole, and so does the part of an unshared
// one that has a state.
func (s *shared) eachUnitTop(o origin, plain func(*rawSource, int), keyed func(unitKey)) {
	for _, r := range o.refs {
		if s.row(r) {
			keyed(unitKey{src: r.src, row: r.row})
		} else {
			plain(r.src, 1)
		}
	}
	if a := o.agg(); a != nil {
		s.flatten(a, plain, keyed)
	}
}

func (s *shared) flatten(a *aggOrigin, plain func(*rawSource, int), keyed func(unitKey)) {
	if s.aggShared(a) {
		keyed(unitKey{agg: a})
		return
	}
	for _, c := range a.srcs {
		plain(c.src, c.n)
	}
	for _, r := range a.multi {
		keyed(unitKey{src: r.src, row: r.row})
	}
	for _, u := range a.units {
		s.flatten(u, plain, keyed)
	}
}

// counter counts the source rows of the run or of one step (D84).
type counter struct {
	name     string
	n        [stRejected + 1]int
	perSrc   map[*rawSource]int // source rows seen per source
	codes    map[string]int
	rescued  int
	rejPlain int // rows counted as rejected without a state
	limits   []Limit
	sh       *shared
	st       *unitStates // nil until a unit with a state is counted
}

// unitStates are the states of the units with a state (D110).
type unitStates struct {
	rows  map[*rawSource]map[int]*[statePage]rowState
	aggs  map[*aggOrigin]rowState
	codes map[string]map[unitKey]bool
	saved map[unitKey]bool
}

func newCounter(name string, sh *shared) *counter {
	return &counter{name: name, perSrc: map[*rawSource]int{}, codes: map[string]int{}, sh: sh}
}

func (c *counter) states() *unitStates {
	if c.st == nil {
		c.st = &unitStates{rows: map[*rawSource]map[int]*[statePage]rowState{}, aggs: map[*aggOrigin]rowState{},
			codes: map[string]map[unitKey]bool{}, saved: map[unitKey]bool{}}
	}
	return c.st
}

func (c *counter) state(k unitKey) rowState {
	if c.st == nil {
		return stNone
	}
	if k.agg != nil {
		return c.st.aggs[k.agg]
	}
	p := c.st.rows[k.src][k.row/statePage]
	if p == nil {
		return stNone
	}
	return p[k.row%statePage]
}

func (c *counter) setState(k unitKey, s rowState) {
	st := c.states()
	if k.agg != nil {
		if _, ok := st.aggs[k.agg]; !ok {
			c.sh.count(16)
		}
		st.aggs[k.agg] = s
		return
	}
	pages := st.rows[k.src]
	if pages == nil {
		pages = map[int]*[statePage]rowState{}
		st.rows[k.src] = pages
	}
	p := pages[k.row/statePage]
	if p == nil {
		p = new([statePage]rowState)
		pages[k.row/statePage] = p
		c.sh.count(statePage)
	}
	p[k.row%statePage] = s
}

// weigh calls f with the source rows of unit k, per source.
func (c *counter) weigh(k unitKey, f func(*rawSource, int)) {
	if k.agg != nil {
		c.sh.plainOf(k.agg, f)
		return
	}
	f(k.src, 1)
}

// move puts unit k in category s: a unit not seen yet counts as read.
func (c *counter) move(k unitKey, s rowState) {
	old := c.state(k)
	c.weigh(k, func(src *rawSource, n int) {
		if old == stNone {
			c.perSrc[src] += n
		} else {
			c.n[old] -= n
		}
		c.n[s] += n
	})
	c.setState(k, s)
}

// enter counts the source rows of o as read.
func (c *counter) enter(o origin) {
	if c == nil {
		return
	}
	c.sh.eachUnit(o, func(src *rawSource, n int) {
		c.perSrc[src] += n
		c.n[stUnprocessed] += n
	}, func(k unitKey) {
		if c.state(k) == stNone {
			c.move(k, stUnprocessed)
		}
	})
}

// exit puts the source rows of o in category s as they leave: those
// without a state from not processed, the others unless they are in a
// later category (D84).
func (c *counter) exit(o origin, s rowState) {
	if c == nil {
		return
	}
	c.sh.eachUnit(o, func(_ *rawSource, n int) {
		c.n[stUnprocessed] -= n
		c.n[s] += n
		if s == stRejected {
			c.rejPlain += n
		}
	}, func(k unitKey) {
		if c.state(k) < s {
			c.move(k, s)
		}
	})
}

// adopt gives unit k, which this counter counts as not processed without a
// state, a state of its own (D110).
func (c *counter) adopt(k unitKey) {
	if c != nil && c.state(k) == stNone {
		c.setState(k, stUnprocessed)
	}
}

// code counts the source rows of o for an error code; fresh says that the
// row has no error with this code in this step yet.
func (c *counter) code(o origin, code string, fresh bool) {
	if c == nil {
		return
	}
	c.sh.eachUnit(o, func(_ *rawSource, n int) {
		if fresh {
			c.codes[code] += n
		}
	}, func(k unitKey) {
		st := c.states()
		if st.codes[code] == nil {
			st.codes[code] = map[unitKey]bool{}
		}
		if !st.codes[code][k] {
			st.codes[code][k] = true
			c.weigh(k, func(_ *rawSource, n int) { c.codes[code] += n })
		}
	})
}

// rescue counts the source rows of o as rescued by a fail branch; first
// says that no branch rescued the row before.
func (c *counter) rescue(o origin, first bool) {
	if c == nil {
		return
	}
	c.sh.eachUnit(o, func(_ *rawSource, n int) {
		if first {
			c.rescued += n
		}
	}, func(k unitKey) {
		st := c.states()
		if !st.saved[k] {
			st.saved[k] = true
			c.weigh(k, func(_ *rawSource, n int) { c.rescued += n })
		}
	})
}

// rejectedPlain returns the rows counted as rejected without a state, for
// gone; 0 without a counter.
func (c *counter) rejectedPlain() int {
	if c == nil {
		return 0
	}
	return c.rejPlain
}

func (c *counter) read() int {
	return c.n[stUnprocessed] + c.n[stDropped] + c.n[stPassed] + c.n[stRejected]
}

func (c *counter) counts() Counts {
	out := Counts{
		Read: c.read(), Passed: c.n[stPassed], Rejected: c.n[stRejected],
		Dropped: c.n[stDropped], Unprocessed: c.n[stUnprocessed], Rescued: c.rescued,
	}
	for code, n := range c.codes {
		if n > 0 {
			if out.ByCode == nil {
				out.ByCode = map[string]int{}
			}
			out.ByCode[code] = n
		}
	}
	return out
}

// over returns the first limit of c that is exceeded, with min rows read
// before a share counts.
func (c *counter) over(min int) (Limit, bool) {
	for _, l := range c.limits {
		if l.exceeded(c.n[stRejected], c.read(), min) {
			return l, true
		}
	}
	return Limit{}, false
}

// tally counts a run: the run and each step (D43, D84), and checks the
// thresholds (D20, D45).
type tally struct {
	run        *counter
	steps      []*counter
	byStep     map[*stepRef]*counter
	stepLimits map[string][]Limit
	abort      bool
	min        int
	mem        *runMem // memory of the run (D28); nil for a Table method
	sh         *shared
}

func newTally(sh *shared) *tally {
	return &tally{run: newCounter("", sh), byStep: map[*stepRef]*counter{}, sh: sh}
}

// step returns the counter of a step, created on first use.
func (t *tally) step(ref *stepRef) *counter {
	if t == nil {
		return nil
	}
	if c, ok := t.byStep[ref]; ok {
		return c
	}
	c := newCounter(ref.name, t.sh)
	c.limits = t.stepLimits[ref.name]
	t.byStep[ref] = c
	t.steps = append(t.steps, c)
	return c
}

// reject counts the source rows of a rejected entry and keeps a copy of
// their raw state (D87). first says that the entry is the row's first in
// its step, fresh that its code is (D15, D84).
func (t *tally) reject(e rejectEntry, first, fresh bool) {
	for _, r := range e.orig.refs {
		r.src.keep(r.row)
	}
	if t == nil {
		return
	}
	// A row rejected in a branch is rejected in the steps the branch is
	// in, too.
	cs := []*counter{t.run}
	for ref := e.step; ref != nil; ref = ref.parent {
		cs = append(cs, t.byStep[ref])
	}
	for _, c := range cs {
		if first {
			c.exit(e.orig, stRejected)
		}
		c.code(e.orig, e.Code, fresh)
	}
}

// carried counts the rejects that an input brings into the run: each row
// once as read and rejected, and once per code (D69).
func (t *tally) carried(es []rejectEntry) {
	ids := map[string]bool{}
	codes := map[[2]string]bool{}
	for _, e := range es {
		for _, r := range e.orig.refs {
			r.src.keep(r.row)
		}
		if t == nil {
			continue
		}
		if !ids[e.ID] {
			ids[e.ID] = true
			t.run.enter(e.orig)
			t.run.exit(e.orig, stRejected)
		}
		k := [2]string{e.ID, e.Code}
		t.run.code(e.orig, e.Code, !codes[k])
		codes[k] = true
	}
}

// check returns a *ThresholdError if the run or step sc is over its
// threshold and the run aborts on it (D45).
func (t *tally) check(sc *counter) error {
	if t == nil || !t.abort {
		return nil
	}
	for _, c := range []*counter{t.run, sc} {
		if c == nil {
			continue
		}
		if l, ok := c.over(t.min); ok {
			return c.thresholdError(l)
		}
	}
	return nil
}

// checkRef checks the run, the step with the given reference and the steps
// whose branch it is in.
func (t *tally) checkRef(ref *stepRef) error {
	if t == nil {
		return nil
	}
	for ; ref != nil; ref = ref.parent {
		if err := t.check(t.byStep[ref]); err != nil {
			return err
		}
	}
	return nil
}

// rescued notes the rows a fail branch of step sc returned (D46). The run
// counts a row once, however many branches return it.
func (t *tally) rescued(sc *counter, orig []origin) {
	if t == nil {
		return
	}
	for _, o := range orig {
		h := o.history()
		sc.rescue(o, true)
		t.run.rescue(o, h == nil || !h.rescued)
		if h != nil {
			h.rescued = true
		}
	}
}

// gone counts the rows that a branch of step sc did not return as dropped
// by sc, unless the branch rejected them: those sc counted as rejected
// since rejBefore, its count of rejected rows without a state when the
// branch started. The branch counted them for the run and released their
// raw state.
func (t *tally) gone(sc *counter, orig []origin, rejBefore int) {
	if sc == nil {
		return
	}
	w := 0
	for _, o := range orig {
		sc.sh.eachUnit(o, func(_ *rawSource, n int) { w += n }, func(k unitKey) {
			if sc.state(k) < stDropped {
				sc.move(k, stDropped)
			}
		})
	}
	w -= sc.rejPlain - rejBefore
	sc.n[stUnprocessed] -= w
	sc.n[stDropped] += w
}

// see counts the source rows of the given rows as read by the run.
func (t *tally) see(orig []origin) {
	if t == nil {
		return
	}
	for _, o := range orig {
		t.run.enter(o)
	}
}

// enter counts the source rows of the given rows as read by step sc.
func (t *tally) enter(sc *counter, orig []origin) {
	if sc == nil {
		return
	}
	for _, o := range orig {
		sc.enter(o)
	}
}

// owns reports whether the raw state of r was read in this run, so that
// the run releases it (D87).
func (t *tally) owns(r srcRef) bool {
	return t != nil && r.src.rel != nil && r.src.rel.owner == t
}

// passed counts the rows as passed by step sc and, with acquire, holds
// their raw state for them.
func (t *tally) passed(sc *counter, orig []origin, acquire bool) {
	for _, o := range orig {
		sc.exit(o, stPassed)
		if acquire {
			for _, r := range o.refs {
				if t.owns(r) {
					r.src.acquire(r.row)
				}
			}
		}
	}
}

// left counts rows that step sc dropped as dropped by the step and the
// run, and with release releases their raw state (D43, D87).
func (t *tally) left(sc *counter, orig []origin, release bool) {
	for _, o := range orig {
		sc.exit(o, stDropped)
		if t != nil {
			t.run.exit(o, stDropped)
		}
	}
	if release {
		t.release(orig)
	}
}

// release releases the raw state of the rows without counting them: rows
// that were rejected and counted, or that go into a step that summarizes
// them (D43, D12).
func (t *tally) release(orig []origin) {
	for _, o := range orig {
		for _, r := range o.refs {
			if t.owns(r) {
				r.src.drop(r.row)
			}
		}
	}
}

// spillRaw spills the chunks of the raw state that the rows need, when the
// rows themselves are spilled (D55).
func (t *tally) spillRaw(orig []origin) error {
	for _, o := range orig {
		for _, r := range o.refs {
			if !t.owns(r) {
				continue
			}
			if err := r.src.spillChunk(r.src.chunkOf(r.row), t.mem); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkSteps checks the run and every step.
func (t *tally) checkSteps() error {
	if t == nil || !t.abort {
		return nil
	}
	if err := t.check(nil); err != nil {
		return err
	}
	for _, c := range t.steps {
		if err := t.check(c); err != nil {
			return err
		}
	}
	return nil
}

func (c *counter) thresholdError(l Limit) *ThresholdError {
	return &ThresholdError{Step: c.name, Rejected: c.n[stRejected], Read: c.read(), Limit: l}
}
