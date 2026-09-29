package gtable

import (
	"errors"
	"fmt"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

// Branch is a sequence of steps that a part of the rows runs through: the
// rows a step fails (a fail branch, see Pipeline.OnFail) or the rows of a
// Split (D25). A branch holds only steps that work block by block; a join,
// a group by or a sort in a branch is a plan error (D90).
type Branch struct {
	steps []step
}

// NewBranch returns an empty branch. An empty branch passes its rows on
// unchanged.
func NewBranch() *Branch { return &Branch{} }

// Then appends op as a step, named after the operation.
func (b *Branch) Then(op Op) *Branch {
	b.steps = appendStep(b.steps, "", op)
	return b
}

// Step appends op as a step with the given name.
func (b *Branch) Step(name string, op Op) *Branch {
	b.steps = appendStep(b.steps, name, op)
	return b
}

// OnFail gives the last step of the branch a fail branch; see
// Pipeline.OnFail.
func (b *Branch) OnFail(fail *Branch) *Branch {
	b.steps = withFail(b.steps, fail)
	return b
}

// Split appends a split; see Pipeline.Split.
func (b *Branch) Split(name string, cond Expr, match, rest *Branch) *Branch {
	b.steps = appendSplit(b.steps, name, cond, match, rest)
	return b
}

// splitSpec is a Split: the condition and the two branches.
type splitSpec struct {
	cond        Expr
	match, rest *Branch
}

func appendStep(steps []step, name string, op Op) []step {
	if name == "" {
		name = "?"
		if op.impl != nil {
			name = op.impl.kind()
		}
	}
	return append(steps, step{name: name, op: op})
}

func appendSplit(steps []step, name string, cond Expr, match, rest *Branch) []step {
	if name == "" {
		name = "split"
	}
	return append(steps, step{name: name, split: &splitSpec{cond, orEmpty(match), orEmpty(rest)}})
}

// withFail sets the fail branch of the last step. Without a step, or for a
// step that has one or is a split, it notes a plan error.
func withFail(steps []step, fail *Branch) []step {
	if len(steps) == 0 {
		return append(steps, step{name: "on_fail", bad: errors.New("OnFail follows no step")})
	}
	last := &steps[len(steps)-1]
	switch {
	case last.bad != nil:
	case last.split != nil:
		last.bad = errors.New("a split has no fail branch")
	case last.fail != nil:
		last.bad = errors.New("the step has a fail branch already")
	default:
		last.fail = orEmpty(fail)
	}
	return steps
}

func orEmpty(b *Branch) *Branch {
	if b == nil {
		return &Branch{}
	}
	return b
}

// branchInfo are the info columns of a row in a fail branch, from its first
// error in the step (D91).
var branchInfo = []field{
	{"reject_id", block.Text},
	{"run_id", block.Text},
	{"row_key", block.Text},
	{"error_count", block.Int},
	{"step", block.Text},
	{"column", block.Text},
	{"value", block.Text},
	{"reason", block.Text},
	{"prev_reason", block.Text},
	{"code", block.Text},
}

// planBranch checks the steps of a branch against its input schema.
func planBranch(in schema, b *Branch, prefix string) (*plannedBranch, error) {
	steps, out, err := planSteps(in, b.steps, prefix, true)
	if err != nil {
		return nil, err
	}
	return &plannedBranch{steps: steps, in: in, out: out}, nil
}

// planSplit checks a split: a boolean condition and two branches whose
// columns merge by name (D26).
func planSplit(in schema, sp *splitSpec, prefix string) (*plannedSplit, schema, error) {
	k, err := sp.cond.check(in)
	if err != nil {
		return nil, nil, err
	}
	if k != block.Bool {
		return nil, nil, fmt.Errorf("condition is %s, want bool", k)
	}
	match, err := planBranch(in, sp.match, prefix)
	if err != nil {
		return nil, nil, err
	}
	rest, err := planBranch(in, sp.rest, prefix)
	if err != nil {
		return nil, nil, err
	}
	out, err := mergeSchema(match.out, rest.out, "the first branch", "the second branch")
	if err != nil {
		return nil, nil, err
	}
	return &plannedSplit{cond: sp.cond, match: match, rest: rest}, out, nil
}

// planFail checks the fail branch of a step with input in and output out.
// The branch gets the input columns and the info columns (D25, D91); its
// info columns are dropped on the return (D26).
func planFail(in, out schema, b *Branch, prefix string) (*plannedBranch, schema, error) {
	bin := in.clone()
	for _, f := range branchInfo {
		name := prefix + f.name
		if bin.index(name) >= 0 {
			return nil, nil, fmt.Errorf("column %q clashes with an info column of the fail branch", name)
		}
		bin = append(bin, field{name, f.kind})
	}
	fb, err := planBranch(bin, b, prefix)
	if err != nil {
		return nil, nil, err
	}
	var back schema
	for _, f := range fb.out {
		if !hasPrefix(f.name, prefix) {
			back = append(back, f)
		}
	}
	merged, err := mergeSchema(out, back, "the main path", "the fail branch")
	if err != nil {
		return nil, nil, err
	}
	return fb, merged, nil
}

func hasPrefix(s, prefix string) bool { return len(s) >= len(prefix) && s[:len(prefix)] == prefix }

// mergeSchema returns the columns of a, then those of b that a lacks. A
// column of the same name and another type is an error (D26).
func mergeSchema(a, b schema, aName, bName string) (schema, error) {
	out := a.clone()
	for _, f := range b {
		i := out.index(f.name)
		if i < 0 {
			out = append(out, f)
			continue
		}
		if out[i].kind != f.kind {
			return nil, fmt.Errorf("column %q is %s in %s and %s in %s", f.name, out[i].kind, aName, f.kind, bName)
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Running branches

// stepNode is a step that runs block by block: an operation, possibly with a
// fail branch, or a split (D90). run returns the output rows and, for each,
// the input row it comes from; nil means row for row.
type stepNode struct {
	op    streamOp // nil for a split
	in    schema
	opOut schema // output of the operation, before the fail branch returns
	out   schema
	sc    *stepCtx
	fail  *chain
	split *splitNode
}

type splitNode struct {
	cond        Expr
	match, rest *chain
}

// chain is the steps of a branch.
type chain struct {
	nodes []*stepNode
	out   schema
}

func (c *chain) run(b batch) (batch, []int, error) {
	var pos []int
	for _, n := range c.nodes {
		if b.blk.Len() == 0 {
			break
		}
		out, keep, err := n.run(b)
		if err != nil {
			return batch{}, nil, err
		}
		pos, b = compose(pos, keep), out
	}
	return b, pos, nil
}

// compose returns the positions pos[keep[i]]; nil stands for row for row.
func compose(pos, keep []int) []int {
	switch {
	case keep == nil:
		return pos
	case pos == nil:
		return keep
	}
	out := make([]int, len(keep))
	for i, k := range keep {
		out[i] = pos[k]
	}
	return out
}

func (n *stepNode) run(b batch) (batch, []int, error) {
	sc := n.sc
	sc.begin(b.blk, b.orig)
	for _, o := range b.orig {
		for _, r := range o.rows() {
			sc.cnt.see(r)
		}
	}
	switch {
	case n.split != nil:
		return n.runSplit(b)
	case n.fail != nil:
		return n.runFail(b)
	}
	out, keep, err := n.op.apply(b.blk, n.in, sc)
	if err != nil {
		return batch{}, nil, err
	}
	orig := b.orig
	t := sc.rx.tally
	if keep != nil {
		orig = pick(b.orig, keep)
		t.left(sc.cnt, others(b.orig, keep), true)
	}
	t.passed(sc.cnt, orig, false)
	return batch{out, orig}, keep, nil
}

// others returns the origins of the rows not in keep.
func others(orig []origin, keep []int) []origin {
	kept := make([]bool, len(orig))
	for _, k := range keep {
		kept[k] = true
	}
	var gone []origin
	for i, o := range orig {
		if !kept[i] {
			gone = append(gone, o)
		}
	}
	return gone
}

// history is what a row that went into a fail branch keeps in the
// background: its reject_id, the steps it failed in and the reason of its
// last failure (D27, D46, D92).
type history struct {
	id, path, reason string
}

// pendingReject is an error of a row in a step with a fail branch. It
// becomes a reject only if the branch fails the row too.
type pendingReject struct {
	row int
	e   rejectEntry
}

// runFail runs a step with a fail branch. The failed rows go into the
// branch in the state before the step, with the info columns of their
// first error; what the branch returns joins the main path in the input
// order (D25, D90, D91). The input is held for the branch, so a change in
// place copies it (D9).
func (n *stepNode) runFail(b batch) (batch, []int, error) {
	sc, t := n.sc, n.sc.rx.tally
	hold := b.blk.Share()
	sc.deferring, sc.held, sc.pending = true, true, nil
	out, keep, err := n.op.apply(b.blk, n.in, sc)
	pending := sc.pending
	sc.deferring, sc.held, sc.pending = false, false, nil
	if err != nil {
		hold.Release()
		return batch{}, nil, err
	}

	firsts := map[int]rejectEntry{}
	errs := map[int]int{}
	for _, p := range pending {
		if _, ok := firsts[p.row]; !ok {
			firsts[p.row] = p.e
		}
		errs[p.row]++
	}
	kept := keep
	if kept == nil {
		kept = allIndexes(b.blk.Len())
	}
	state := make([]byte, b.blk.Len()) // 0 dropped, 1 kept, 2 failed
	for _, k := range kept {
		state[k] = 1
	}
	var failed []int
	var gone []origin
	for i := range state {
		switch {
		case errs[i] > 0:
			state[i] = 2
			failed = append(failed, i)
		case state[i] == 0:
			gone = append(gone, b.orig[i])
		}
	}
	mainOrig := pick(b.orig, kept)
	t.passed(sc.cnt, mainOrig, false)
	t.left(sc.cnt, gone, true)
	main := rowsPart{n.opOut, out, mainOrig, kept}
	if len(failed) == 0 {
		hold.Release()
		blk, orig, _ := mergeRows(n.out, main)
		return batch{blk, orig}, keep, nil
	}

	fblk := hold.Take(failed)
	hold.Release()
	sc.traceCopy("", TraceCopyAtBranch)
	ib := newInfoBuilder(branchInfo, len(failed))
	forig := make([]origin, len(failed))
	for i, row := range failed {
		e := firsts[row]
		vals := entryValues(e, errs[row])
		vals["row_key"] = e.orig.rowKey()
		ib.set(vals)
		o := b.orig[row]
		o.hist = &history{id: e.ID, path: e.Step, reason: e.Reason}
		forig[i] = o
	}
	for _, c := range ib.columns(sc.rx.infoPrefix()) {
		if err := fblk.AppendColumn(c.col); err != nil {
			panic("gtable: " + err.Error())
		}
	}
	fb, fpos, err := n.fail.run(batch{fblk, forig})
	if err != nil {
		return batch{}, nil, err
	}
	back := compose(failed, fpos)
	if fpos == nil {
		back = failed[:fb.blk.Len()]
	}
	// The rows the branch returns pass the step as rescued; the others it
	// rejected or dropped (D46).
	t.passed(sc.cnt, fb.orig, false)
	t.rescued(sc.cnt, fb.orig)
	t.gone(sc.cnt, pick(b.orig, notIn(failed, back)))
	blk, orig, pos := mergeRows(n.out, main, rowsPart{n.fail.out, fb.blk, fb.orig, back})
	return batch{blk, orig}, pos, nil
}

// notIn returns the rows of all that are not in some; both are ascending.
func notIn(all, some []int) []int {
	var out []int
	j := 0
	for _, r := range all {
		if j < len(some) && some[j] == r {
			j++
			continue
		}
		out = append(out, r)
	}
	return out
}

// runSplit sends every row to the first branch if cond is true, else to
// the second, like CASE WHEN in SQL (D30), and merges what the branches
// return in the input order (D26, D90). Every branch gets its own copy of
// its rows (D9). A row for which cond fails is rejected with code "expr"
// (D54).
func (n *stepNode) runSplit(b batch) (batch, []int, error) {
	sc, t := n.sc, n.sc.rx.tally
	c := newEvalCtx(b.blk, n.in)
	v := n.split.cond.n.eval(c)
	var match, rest, bad []int
	for i := range c.n {
		switch {
		case c.reasons[i] != "":
			if err := sc.reject(i, "", "", false, c.reasons[i], CodeExpr); err != nil {
				return batch{}, nil, err
			}
			bad = append(bad, i)
		case !v.null[i] && v.bools[i]:
			match = append(match, i)
		default:
			rest = append(rest, i)
		}
	}
	t.left(sc.cnt, pick(b.orig, bad), true)
	if b.blk.Len() > 0 {
		sc.traceCopy("", TraceCopyAtBranch)
	}
	var parts []rowsPart
	for _, br := range []struct {
		rows []int
		c    *chain
	}{{match, n.split.match}, {rest, n.split.rest}} {
		if len(br.rows) == 0 {
			continue
		}
		ob, opos, err := br.c.run(batch{b.blk.Take(br.rows), pick(b.orig, br.rows)})
		if err != nil {
			return batch{}, nil, err
		}
		back := compose(br.rows, opos)
		if opos == nil {
			back = br.rows[:ob.blk.Len()]
		}
		t.passed(sc.cnt, ob.orig, false)
		t.gone(sc.cnt, pick(b.orig, notIn(br.rows, back)))
		parts = append(parts, rowsPart{br.c.out, ob.blk, ob.orig, back})
	}
	blk, orig, pos := mergeRows(n.out, parts...)
	return batch{blk, orig}, pos, nil
}

// rowsPart is the output of one path of a branching step: its rows, their
// origins and the input row each comes from, ascending.
type rowsPart struct {
	s    schema
	blk  block.Block
	orig []origin
	pos  []int
}

// mergeRows merges the parts into a block of schema out, by column name
// and in the order of the input rows. A column a part lacks is null there
// (D26). A single part keeps its columns.
func mergeRows(out schema, parts ...rowsPart) (block.Block, []origin, []int) {
	var live []rowsPart
	for _, p := range parts {
		if p.blk.Len() > 0 {
			live = append(live, p)
		}
	}
	if len(live) == 0 {
		return emptyBlock(out), nil, nil
	}
	if len(live) == 1 {
		p := live[0]
		cols := make([]block.Column, len(out))
		for i, f := range out {
			if j := p.s.index(f.name); j >= 0 {
				cols[i] = p.blk.Column(j)
			} else {
				cols[i] = nullColumn(f.kind, p.blk.Len())
			}
		}
		return newBlock(cols, p.blk.Len()), p.orig, p.pos
	}
	// Merge the rows of the parts by their input position.
	type ref struct{ part, row int }
	var order []ref
	next := make([]int, len(live))
	for {
		best := -1
		for k, p := range live {
			if next[k] < len(p.pos) && (best < 0 || p.pos[next[k]] < live[best].pos[next[best]]) {
				best = k
			}
		}
		if best < 0 {
			break
		}
		order = append(order, ref{best, next[best]})
		next[best]++
	}
	orig := make([]origin, len(order))
	pos := make([]int, len(order))
	for i, r := range order {
		orig[i], pos[i] = live[r.part].orig[r.row], live[r.part].pos[r.row]
	}
	cols := make([]block.Column, len(out))
	for i, f := range out {
		vs := make([]*vec, len(live))
		for k, p := range live {
			if j := p.s.index(f.name); j >= 0 {
				vs[k] = vecOf(p.blk.Column(j))
			}
		}
		res := newVec(f.kind, len(order))
		for j, r := range order {
			if src := vs[r.part]; src != nil {
				res.set(j, src, r.row)
			} else {
				res.null[j] = true
			}
		}
		cols[i] = res.column()
	}
	return newBlock(cols, len(order)), orig, pos
}

func nullColumn(k block.Kind, n int) block.Column {
	b := block.NewBuilder(k, n)
	for range n {
		b.AppendNull()
	}
	return b.Build()
}

// ---------------------------------------------------------------------------
// Copy modes and the trace

// CopyMode says whether the engine copies data or changes it in place
// (D7, D8). Tables stay immutable in every mode: the engine copies a
// column that the raw state or a table holds before it changes it, and it
// copies the rows at a branch (D9, D64, D93).
type CopyMode uint8

const (
	// CopyAuto changes a column in place where nothing else holds it, and
	// copies it otherwise (D7). It is the default.
	CopyAuto CopyMode = iota
	// CopyAlways never changes data in place, for debugging.
	CopyAlways
	// CopyInPlace changes data in place wherever tables, the raw state and
	// branches allow it, and notes in the trace where they do not (D9,
	// D64).
	CopyInPlace
)

func (m CopyMode) String() string {
	switch m {
	case CopyAuto:
		return "auto"
	case CopyAlways:
		return "always copy"
	case CopyInPlace:
		return "in place"
	}
	return fmt.Sprintf("CopyMode(%d)", uint8(m))
}

// TraceEvent is the kind of a trace entry.
type TraceEvent uint8

const (
	// TraceCopyAtBranch: the engine copied data at a branch (D9).
	TraceCopyAtBranch TraceEvent = iota + 1
	// TraceCopyShared: the engine copied a column that the raw state or a
	// table holds at its first change (D64, D93).
	TraceCopyShared
)

func (e TraceEvent) String() string {
	switch e {
	case TraceCopyAtBranch:
		return "copy_at_branch"
	case TraceCopyShared:
		return "copy_shared"
	}
	return fmt.Sprintf("TraceEvent(%d)", uint8(e))
}

// TraceEntry notes where the engine copied data in the mode CopyInPlace,
// per step, column and event: Column is empty for the rows of a branch,
// and Blocks counts the blocks copied (D9, D64).
type TraceEntry struct {
	Step   string
	Column string
	Event  TraceEvent
	Blocks int
}

func (e TraceEntry) String() string {
	return fmt.Sprintf("step=%s column=%s event=%s blocks=%d", e.Step, e.Column, e.Event, e.Blocks)
}

// tracer collects the trace of a run.
type tracer struct {
	entries []TraceEntry
	at      map[traceKey]int
}

type traceKey struct {
	step  *stepRef
	col   string
	event TraceEvent
}

func (tr *tracer) add(ref *stepRef, col string, ev TraceEvent) {
	k := traceKey{ref, col, ev}
	if i, ok := tr.at[k]; ok {
		tr.entries[i].Blocks++
		return
	}
	if tr.at == nil {
		tr.at = map[traceKey]int{}
	}
	tr.at[k] = len(tr.entries)
	tr.entries = append(tr.entries, TraceEntry{Step: ref.name, Column: col, Event: ev, Blocks: 1})
}

// traceCopy notes a copy in the mode CopyInPlace.
func (sc *stepCtx) traceCopy(col string, ev TraceEvent) {
	if sc.rx.trace != nil && sc.rx.mode == CopyInPlace {
		sc.rx.trace.add(sc.ref, col, ev)
	}
}

// setColumn sets column i of out to v, or appends it if i is -1. With a
// column of the same type that nothing else holds, it writes the values in
// place, unless the mode is CopyAlways or inPlace is false. A column the raw state, a table or
// a fail branch holds is replaced by a new one, and the trace notes it (D7,
// D9, D64, D93).
func setColumn(out *block.Block, sc *stepCtx, name string, i int, v *vec, inPlace bool) {
	if i < 0 {
		if err := out.AppendColumn(v.column()); err != nil {
			panic("gtable: " + err.Error())
		}
		return
	}
	old := out.Column(i)
	if sc.rx.mode == CopyAlways || !inPlace || old.Kind() != v.kind {
		replaceColumn(out, i, v.column())
		return
	}
	if old.Shared() {
		ev := TraceCopyShared
		if sc.held {
			ev = TraceCopyAtBranch
		}
		sc.traceCopy(name, ev)
		replaceColumn(out, i, v.column())
		return
	}
	v.writeInto(out.MutableColumn(i).Column)
}

func replaceColumn(out *block.Block, i int, c block.Column) {
	if err := out.SetColumn(i, c); err != nil {
		panic("gtable: " + err.Error())
	}
}
