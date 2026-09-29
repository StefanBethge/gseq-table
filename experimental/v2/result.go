package gtable

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

// Status is the status of a run (D21). The statuses are ordered by
// precedence: if several apply, the highest counts (D63).
type Status uint8

const (
	StatusOK              Status = iota
	StatusFailedThreshold        // more rows rejected than the threshold allows (D4, D20)
	StatusAborted                // stopped at a data error, or the context ended (D3, D63)
	StatusDeliveryError          // a delivery error, in any mode (D42)
	StatusSinkError              // a target failed (D40)
	StatusPlanError              // an error in the pipeline itself; nothing ran (D19)
)

var statusNames = [...]string{"ok", "failed_threshold", "aborted", "delivery_error", "sink_error", "plan_error"}

func (s Status) String() string {
	if int(s) < len(statusNames) {
		return statusNames[s]
	}
	return "Status(" + strconv.Itoa(int(s)) + ")"
}

// ExitCode returns the exit code of the status for a scheduler: 0 for ok,
// rising with the precedence to 5 for plan_error (D21, D86).
func (s Status) ExitCode() int { return int(s) }

// Cause is one status that applies to a run, with the error behind it
// (D63): a *PlanError, a *DeliveryError, a *DataError, a *ThresholdError,
// a *SinkError (D40) or the error of the context.
type Cause struct {
	Status Status
	Err    error
}

func (c Cause) String() string { return c.Status.String() + ": " + c.Err.Error() }

// Counts counts source rows, not entries (D43, D84). Every source row
// falls in exactly one category, so Read = Passed + Rejected + Dropped +
// Unprocessed. A row is rejected if one of its result rows was rejected,
// else passed if one reached the end of the plan, else dropped. Unprocessed
// rows were read but the run ended before they were done. ByCode counts
// the rejected source rows per error code; a row with two codes counts for
// both.
//
// For a step, Read are the source rows that went into the step, and Passed
// those that came out of it. A row that failed in a step and that its fail
// branch returned passed the step; Rescued counts these rows apart, and
// for the run the rows any fail branch returned (D46). Only rows that stay
// rejected count as Rejected and for the threshold.
type Counts struct {
	Read, Passed, Rejected, Dropped, Unprocessed int
	Rescued                                      int
	ByCode                                       map[string]int
}

// StepCounts are the counts of one step (D21). The rows a Reader rejects
// while reading count for the step "read".
type StepCounts struct {
	Step string
	Counts
}

// Result is the result of a run (D21, D88): the result table, the status
// with every cause that applies (D63), the counts for the run and per step
// (D43), and the change report (D23).
type Result struct {
	// Table holds the rows that passed, the rejected rows and the findings
	// of the header check. After a run that ended early, it holds what was
	// done until then, and the error that ended the run as its sticky
	// error.
	Table  Table
	Status Status
	// Causes are all statuses that apply, highest first.
	Causes []Cause
	Counts Counts
	Steps  []StepCounts
	// Report is the change report: the findings of the header check
	// (D22), unreadable deliveries (D42) and format changes (D23, D85).
	Report []Finding
	// Trace notes where the engine copied data in the mode CopyInPlace:
	// at branches and at the first change of a column the raw state or a
	// table holds (D9, D64, D93).
	Trace []TraceEntry

	mem    *runMem
	closer *resultCloser // spilled rejected rows until Close (D49)
}

// Close releases the rejected rows the result holds (D49, D98). After
// Close, the tables of RejectedRows of the result table, and of the
// tables derived from it, carry the sticky error ErrClosed. Status,
// causes, counts and the change report stay. Close may be called more
// than once.
func (r Result) Close() error {
	if r.Table.rs != nil {
		r.Table.rs.close(r.Table.rejects)
	}
	// The spilled copies go with the run's spill directory (D28, D103).
	if r.closer != nil {
		return r.closer.close()
	}
	return nil
}

// ExitCode returns the exit code of the status (D86).
func (r Result) ExitCode() int { return r.Status.ExitCode() }

// Step returns the counts of the first step with the given name.
func (r Result) Step(name string) (StepCounts, bool) {
	for _, s := range r.Steps {
		if s.Step == name {
			return s, true
		}
	}
	return StepCounts{}, false
}

// ChangeReport returns the change report as a table with a row per finding
// (D23): kind, source, column, detail, count and examples. Examples are
// quoted and separated by ", ". Count is null for a finding without a
// count.
func (r Result) ChangeReport() Table {
	n := len(r.Report)
	b := map[string]*block.Builder{}
	names := []string{"kind", "source", "column", "detail", "examples"}
	for _, name := range names {
		b[name] = block.NewBuilder(block.Text, n)
	}
	count := block.NewBuilder(block.Int, n)
	for _, f := range r.Report {
		b["kind"].AppendText(f.Kind)
		b["source"].AppendText(f.Source)
		b["column"].AppendText(f.Column)
		b["detail"].AppendText(f.Detail)
		ex := make([]string, len(f.Examples))
		for i, e := range f.Examples {
			ex[i] = strconv.Quote(e)
		}
		b["examples"].AppendText(strings.Join(ex, ", "))
		if f.Count > 0 {
			count.AppendInt(int64(f.Count))
		} else {
			count.AppendNull()
		}
	}
	return NewTable(
		newColumn("kind", b["kind"].Build()),
		newColumn("source", b["source"].Build()),
		newColumn("column", b["column"].Build()),
		newColumn("detail", b["detail"].Build()),
		newColumn("count", count.Build()),
		newColumn("examples", b["examples"].Build()),
	)
}

// Limit is one limit of a threshold (D20): a number of rejected source
// rows, or a share of the rows read.
type Limit struct {
	rows  int
	share float64
	isShr bool
}

// MaxRejected is exceeded when more than n source rows are rejected (D4).
func MaxRejected(n int) Limit { return Limit{rows: n} }

// MaxRejectedShare is exceeded when the share of rejected source rows is
// larger than share, between 0 and 1. The share is over the rows read so
// far for the run, and over the rows that went into the step for a step
// threshold (D45).
func MaxRejectedShare(share float64) Limit { return Limit{share: share, isShr: true} }

func (l Limit) String() string {
	if l.isShr {
		return "share " + strconv.FormatFloat(l.share, 'f', -1, 64)
	}
	return strconv.Itoa(l.rows) + " rows"
}

func (l Limit) check() error {
	switch {
	case l.isShr && (math.IsNaN(l.share) || l.share < 0 || l.share > 1):
		return fmt.Errorf("threshold share %v is not between 0 and 1", l.share)
	case !l.isShr && l.rows < 0:
		return fmt.Errorf("threshold of %d rows is negative", l.rows)
	}
	return nil
}

// exceeded reports whether rejected of read rows exceed l. A share only
// counts from min rows read (D45).
func (l Limit) exceeded(rejected, read, min int) bool {
	if !l.isShr {
		return rejected > l.rows
	}
	return read > 0 && read >= min && float64(rejected)/float64(read) > l.share
}

// ThresholdError says that the rejected rows exceed a threshold (D4, D20).
// Step is empty for the threshold of the run.
type ThresholdError struct {
	Step     string
	Rejected int
	Read     int
	Limit    Limit
}

func (e *ThresholdError) Error() string {
	where := "the run"
	if e.Step != "" {
		where = "step " + e.Step
	}
	return fmt.Sprintf("threshold exceeded in %s: %d of %d rows rejected, limit %v", where, e.Rejected, e.Read, e.Limit)
}

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

// counter counts the source rows of the run or of one step (D84).
type counter struct {
	name   string
	states map[*rawSource][]rowState
	n      [stRejected + 1]int
	codes  map[string]map[srcRef]bool
	perSrc map[*rawSource]int // source rows seen per source
	limits []Limit
	saved  map[srcRef]bool // rescued by a fail branch (D46)
}

func newCounter(name string) *counter {
	return &counter{name: name, states: map[*rawSource][]rowState{}, codes: map[string]map[srcRef]bool{}, perSrc: map[*rawSource]int{}}
}

func (c *counter) state(r srcRef) rowState {
	if c == nil {
		return stNone
	}
	st := c.states[r.src]
	if r.row >= len(st) {
		return stNone
	}
	return st[r.row]
}

func (c *counter) set(r srcRef, s rowState) {
	st := c.states[r.src]
	if r.row >= len(st) {
		st = append(st, make([]rowState, r.row+1-len(st))...)
		c.states[r.src] = st
	}
	old := st[r.row]
	if old == stNone {
		c.perSrc[r.src]++
	} else {
		c.n[old]--
	}
	c.n[s]++
	st[r.row] = s
}

// see counts r as read.
func (c *counter) see(r srcRef) {
	if c != nil && c.state(r) == stNone {
		c.set(r, stUnprocessed)
	}
}

// mark puts r in category s unless it is in a later one.
func (c *counter) mark(r srcRef, s rowState) {
	if c != nil && c.state(r) < s {
		c.set(r, s)
	}
}

func (c *counter) code(r srcRef, code string) {
	if c == nil {
		return
	}
	if c.codes[code] == nil {
		c.codes[code] = map[srcRef]bool{}
	}
	c.codes[code][r] = true
}

// rescue notes r as rescued by a fail branch.
func (c *counter) rescue(r srcRef) {
	if c == nil {
		return
	}
	if c.saved == nil {
		c.saved = map[srcRef]bool{}
	}
	c.saved[r] = true
}

func (c *counter) read() int {
	return c.n[stUnprocessed] + c.n[stDropped] + c.n[stPassed] + c.n[stRejected]
}

func (c *counter) counts() Counts {
	out := Counts{
		Read: c.read(), Passed: c.n[stPassed], Rejected: c.n[stRejected],
		Dropped: c.n[stDropped], Unprocessed: c.n[stUnprocessed], Rescued: len(c.saved),
	}
	if len(c.codes) > 0 {
		out.ByCode = make(map[string]int, len(c.codes))
		for code, rows := range c.codes {
			out.ByCode[code] = len(rows)
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
}

func newTally() *tally {
	return &tally{run: newCounter(""), byStep: map[*stepRef]*counter{}}
}

// step returns the counter of a step, created on first use.
func (t *tally) step(ref *stepRef) *counter {
	if t == nil {
		return nil
	}
	if c, ok := t.byStep[ref]; ok {
		return c
	}
	c := newCounter(ref.name)
	c.limits = t.stepLimits[ref.name]
	t.byStep[ref] = c
	t.steps = append(t.steps, c)
	return c
}

// reject counts the source rows of a rejected entry and keeps a copy of
// their raw state (D87).
func (t *tally) reject(e rejectEntry) {
	for _, r := range e.orig.refs {
		r.src.keep(r.row)
	}
	if t == nil {
		return
	}
	for _, r := range e.orig.rows() {
		t.run.mark(r, stRejected)
		t.run.code(r, e.Code)
	}
	// A row rejected in a branch is rejected in the steps the branch is
	// in, too.
	for ref := e.step; ref != nil; ref = ref.parent {
		sc := t.byStep[ref]
		for _, r := range e.orig.rows() {
			sc.mark(r, stRejected)
			sc.code(r, e.Code)
		}
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

// rescued notes the rows a fail branch of step sc returned (D46).
func (t *tally) rescued(sc *counter, orig []origin) {
	if t == nil {
		return
	}
	for _, o := range orig {
		for _, r := range o.rows() {
			sc.rescue(r)
			t.run.rescue(r)
		}
	}
}

// gone counts rows that a branch of step sc rejected or dropped as dropped
// by sc, unless they are rejected. The branch counted them for the run and
// released their raw state.
func (t *tally) gone(sc *counter, orig []origin) {
	for _, o := range orig {
		for _, r := range o.rows() {
			sc.mark(r, stDropped)
		}
	}
}

// see counts the source rows of the given rows as read by the run.
func (t *tally) see(orig []origin) {
	if t == nil {
		return
	}
	for _, o := range orig {
		for _, r := range o.rows() {
			t.run.see(r)
		}
	}
}

// owns reports whether the raw state of r was read in this run, so that
// the run releases it (D87).
func (t *tally) owns(r srcRef) bool {
	return t != nil && r.src.rel != nil && r.src.rel.owner == t
}

// passed counts the rows as passed by step sc and holds their raw state.
func (t *tally) passed(sc *counter, orig []origin, acquire bool) {
	for _, o := range orig {
		for _, r := range o.rows() {
			sc.mark(r, stPassed)
		}
		if acquire {
			for _, r := range o.refs {
				if t.owns(r) {
					r.src.acquire(r.row)
				}
			}
		}
	}
}

// left counts rows that step sc did not pass on and that it did not
// reject as dropped, and releases their raw state (D43, D87).
func (t *tally) left(sc *counter, orig []origin, release bool) {
	for _, o := range orig {
		for _, r := range o.rows() {
			sc.mark(r, stDropped)
			if t != nil {
				t.run.mark(r, stDropped)
			}
		}
		if release {
			for _, r := range o.refs {
				if t.owns(r) {
					r.src.drop(r.row)
				}
			}
		}
	}
}

// release releases the raw state of the rows without counting them, for
// rows that go into a step that summarizes them and are spilled (D43).
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

// causeOf returns the status an error that ended a run stands for (D63).
func causeOf(err error) Cause {
	var pe *PlanError
	var de *DeliveryError
	var te *ThresholdError
	var se *SinkError
	switch {
	case errors.As(err, &se):
		return Cause{StatusSinkError, err}
	case errors.As(err, &pe):
		return Cause{StatusPlanError, err}
	case errors.As(err, &de):
		return Cause{StatusDeliveryError, err}
	case errors.As(err, &te):
		return Cause{StatusFailedThreshold, err}
	}
	return Cause{StatusAborted, err}
}

// status returns the highest status of the causes, and sorts them highest
// first (D63).
func status(causes []Cause) Status {
	for i := 1; i < len(causes); i++ {
		for j := i; j > 0 && causes[j].Status > causes[j-1].Status; j-- {
			causes[j], causes[j-1] = causes[j-1], causes[j]
		}
	}
	if len(causes) == 0 {
		return StatusOK
	}
	return causes[0].Status
}
