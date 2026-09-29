package gtable

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

// The engine runs a checked plan block by block (D6). Operations that work
// block by block pass each block on at once; a step over all rows collects
// its blocks and runs when its input ends. Over the memory budget of a run,
// sort and group by spill the rows they collect, and a join the rows of its
// left side (D28, P1). Table methods and pipelines both run through this
// engine (D31); Table methods do not spill.

type step struct {
	name  string
	op    Op
	fail  *Branch    // the fail branch of the step (D25), or nil
	split *splitSpec // set for a Split; op is empty
	bad   error      // an error in building the plan
}

// planned is a step with the schema of its input and output, and its
// planned branches.
type planned struct {
	step
	in, out schema
	opOut   schema // output of the operation, before a fail branch returns
	fail    *plannedBranch
	split   *plannedSplit
}

type plannedBranch struct {
	steps   []planned
	in, out schema
}

type plannedSplit struct {
	cond        Expr
	match, rest *plannedBranch
}

// checkPlan checks every step against the schema before it and returns the
// planned steps and the output schema. The first failure is a PlanError
// (D19). prefix is the info prefix for the columns of a fail branch.
func checkPlan(src schema, steps []step, prefix string) ([]planned, schema, error) {
	return planSteps(src, steps, prefix, false)
}

// planSteps plans the steps of the plan or, with inBranch, of a branch,
// which holds only steps that work block by block (D90).
func planSteps(src schema, steps []step, prefix string, inBranch bool) ([]planned, schema, error) {
	out := make([]planned, len(steps))
	s := src
	for i, st := range steps {
		p, err := planStep(s, st, prefix, inBranch)
		if err != nil {
			var pe *PlanError
			if errors.As(err, &pe) {
				return nil, nil, err
			}
			return nil, nil, &PlanError{Step: st.name, Err: err}
		}
		out[i] = p
		s = p.out
	}
	return out, s, nil
}

func planStep(in schema, st step, prefix string, inBranch bool) (planned, error) {
	p := planned{step: st, in: in}
	switch {
	case st.bad != nil:
		return p, st.bad
	case st.split != nil:
		sp, out, err := planSplit(in, st.split, prefix)
		p.split, p.out = sp, out
		return p, err
	case st.op.impl == nil:
		return p, fmt.Errorf("empty operation")
	}
	if _, ok := st.op.impl.(streamOp); !ok {
		switch {
		case inBranch:
			return p, fmt.Errorf("%s does not work block by block; a branch holds only steps that work block by block", st.op.impl.kind())
		case st.fail != nil:
			return p, fmt.Errorf("%s does not work block by block; only a step that works block by block has a fail branch", st.op.impl.kind())
		}
	}
	out, err := st.op.impl.plan(in)
	if err != nil {
		return p, err
	}
	p.out, p.opOut = out, out
	if st.fail != nil {
		p.fail, p.out, err = planFail(in, out, st.fail, prefix)
	}
	return p, err
}

// batch is a block with the origin of every row (D9, D11).
type batch struct {
	blk  block.Block
	orig []origin
}

// stage is one step of a running plan.
type stage interface {
	push(ctx context.Context, b batch) error
	finish(ctx context.Context) error
}

// nodeStage runs a step that works block by block, with its branches.
type nodeStage struct {
	n    *stepNode
	next stage
}

func (s *nodeStage) push(ctx context.Context, b batch) error {
	out, _, err := s.n.run(b)
	if err != nil {
		return err
	}
	return s.next.push(ctx, out)
}

func (s *nodeStage) finish(ctx context.Context) error { return s.next.finish(ctx) }

type fullStage struct {
	op       fullOp
	in       schema
	sc       *stepCtx
	blockLen int // 0: pass the result on as one block
	next     stage
	x        *sorter      // the input rows, spilled over the budget
	out      *sorter      // group by: the aggregated rows until they are in order
	g        *streamGroup // group by as the rows come (D112), or nil
}

// Hidden columns of the spilled rows of a group by.
const (
	groupKeyCol   = "\x00key"
	groupSeqCol   = "\x00seq"
	groupFailsCol = "\x00fails"
)

func newFullStage(op fullOp, in schema, sc *stepCtx, blockLen int, next stage) *fullStage {
	s := &fullStage{op: op, in: in, sc: sc, blockLen: blockLen, next: next}
	t := sc.rx.tally
	x := &sorter{m: sc.rx.mem(), s: in, frameLen: max(blockLen, 1), pattern: op.kind() + "-*"}
	switch o := op.(type) {
	case sortOp:
		x.keys = o.keys
		x.onSpill = t.spillRaw
	case groupOp:
		x.s = append(in.clone(), field{groupKeyCol, block.Text})
		x.keys = []SortKey{Asc(groupKeyCol)}
		x.prep = o.withKey(in)
		x.onSpill = func(orig []origin) error {
			// The raw state ends at a step that summarizes rows (D12, D43).
			t.release(orig)
			return nil
		}
		if o.streamable() {
			s.g = newStreamGroup(o, in, sc.rx.shared, sc.rx.mem())
		}
	default:
		x.onSpill = t.spillRaw
	}
	s.x = x
	return s
}

func (s *fullStage) push(_ context.Context, b batch) error {
	t := s.sc.rx.tally
	t.enter(s.sc.cnt, b.orig)
	if s.g != nil {
		rest, seq := s.g.add(b)
		// The rows in a group leave their raw state (D12, D43); the rows
		// of new keys over the budget wait for the sorted way (D112).
		if len(rest) == 0 {
			t.release(b.orig)
		} else {
			t.release(pick(b.orig, notIn(allIndexes(b.blk.Len()), rest)))
			s.x.addSeq(batch{b.blk.Take(rest), pick(b.orig, rest)}, seq)
		}
		return s.sc.rx.mem().relieve()
	}
	s.x.add(b)
	return s.sc.rx.mem().relieve()
}

// spill spills the rows the step holds; the runs calls it over the budget.
func (s *fullStage) spill() error {
	if s.out != nil {
		return s.out.spill()
	}
	return s.x.spill()
}

func (s *fullStage) finish(ctx context.Context) error {
	t := s.sc.rx.tally
	var right []origin
	if j, ok := s.op.(joinOp); ok {
		// The rejects of the right side come before the join's own (D69).
		// Its rows go into the join and count as read (D43).
		s.sc.rx.carry(j.rightRejects())
		right = j.right.origins()
		t.see(right)
		t.enter(s.sc.cnt, right)
	}
	if op, ok := s.op.(groupOp); ok && (s.g != nil || s.x.spilled()) {
		err := s.finishGroups(ctx, op)
		s.x.close()
		if err != nil {
			return err
		}
		return s.next.finish(ctx)
	}
	if s.x.spilled() {
		var err error
		switch op := s.op.(type) {
		case sortOp:
			err = s.finishSorted(ctx)
		case joinOp:
			err = s.finishJoin(ctx, op)
		}
		s.x.close()
		if err != nil {
			return err
		}
		t.left(s.sc.cnt, right, false)
		return s.next.finish(ctx)
	}
	blks, orig := s.x.take()
	if len(blks) == 0 {
		blks = []block.Block{emptyBlock(s.in)}
	}
	s.sc.begin(block.Block{}, orig)
	var out block.Block
	var outOrig []origin
	switch op := s.op.(type) {
	case joinOp:
		b := s.joinRows(op, op.prepare(), concatBlocks(blks, s.in), orig)
		out, outOrig = b.blk, b.orig
	default:
		var err error
		if out, outOrig, err = s.op.applyAll(blks, s.in, s.sc); err != nil {
			return err
		}
		// A sort passes its rows on with their raw state; the rows that
		// go into a group leave it (D12, D43).
		t.passed(s.sc.cnt, outOrig, false)
		if _, ok := op.(groupOp); ok {
			t.release(orig)
		}
	}
	t.left(s.sc.cnt, right, false)
	if err := s.emit(ctx, batch{out, outOrig}); err != nil {
		return err
	}
	return s.next.finish(ctx)
}

// emit passes b on in blocks of the block length.
func (s *fullStage) emit(ctx context.Context, b batch) error {
	for _, c := range chunk(b, s.blockLen) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.next.push(ctx, c); err != nil {
			return err
		}
		if err := s.sc.rx.mem().relieve(); err != nil {
			return err
		}
	}
	return nil
}

// mergeInBlocks merges the rows of x and calls flush with every block of
// the block length, and with the rest.
func (s *fullStage) mergeInBlocks(x *sorter, split func(r row, n int) bool, flush func(batch, []int64) error) error {
	rb := newRowBuilder(x.s, x.m)
	err := x.merge(func(r row) error {
		if split(r, rb.len()) {
			if err := flush(rb.flush()); err != nil {
				return err
			}
		}
		rb.add(r)
		return s.sc.rx.mem().relieve()
	})
	if err == nil && rb.len() > 0 {
		err = flush(rb.flush())
	}
	return err
}

func (s *fullStage) full(_ row, n int) bool { return n >= max(s.blockLen, 1) }

// finishSorted passes on the merged runs of a sort. The rows are the input
// rows in another order.
func (s *fullStage) finishSorted(ctx context.Context) error {
	return s.mergeInBlocks(s.x, s.full, func(b batch, _ []int64) error {
		s.sc.rx.tally.passed(s.sc.cnt, b.orig, false)
		return s.emit(ctx, b)
	})
}

// finishJoin probes the spilled left rows block by block against the right
// side; the join rows keep the order of the left rows (D6).
func (s *fullStage) finishJoin(ctx context.Context, op joinOp) error {
	ix := op.prepare()
	return s.mergeInBlocks(s.x, s.full, func(b batch, _ []int64) error {
		return s.emit(ctx, s.joinRows(op, ix, b.blk, b.orig))
	})
}

// joinRows joins the left rows lb with origins lorig. A left row with more
// than one partner gets a state of its own (D110). The join rows hold the
// raw state before the left rows let go of it; a left row without a
// partner in an inner join is dropped (D43, D84).
func (s *fullStage) joinRows(op joinOp, ix *joinIndex, lb block.Block, lorig []origin) batch {
	t := s.sc.rx.tally
	out, outOrig, matches := op.probe(ix, lb, s.in, lorig)
	var dropped, joined []origin
	for i, n := range matches {
		switch {
		case n > 1:
			s.sc.rx.promote(s.sc.cnt, lorig[i])
			joined = append(joined, lorig[i])
		case n == 0 && !op.left:
			dropped = append(dropped, lorig[i])
		default:
			joined = append(joined, lorig[i])
		}
	}
	t.passed(s.sc.cnt, outOrig, true)
	t.left(s.sc.cnt, dropped, true)
	t.release(joined)
	return batch{out, outOrig}
}

// finishGroups computes the groups: those that ran as the rows came
// (D112), and those of the rows sorted by group key, so that each group's
// rows come together in their order. It puts the groups in the order of
// their first rows (D6), and rejects failed ones (D77).
func (s *fullStage) finishGroups(ctx context.Context, op groupOp) error {
	t := s.sc.rx.tally
	outSchema, _ := op.plan(s.in)
	ext := append(outSchema.clone(), field{groupSeqCol, block.Int}, field{groupFailsCol, block.Text})
	s.out = &sorter{m: s.x.m, s: ext, keys: []SortKey{Asc(groupSeqCol)}, frameLen: s.x.frameLen, pattern: "group_out-*"}
	defer func() { s.out.close(); s.out = nil }()
	add := func(out block.Block, orig []origin, fails [][]aggFailure, first []int64) {
		seqs := block.NewBuilder(block.Int, out.Len())
		fcol := block.NewBuilder(block.Text, out.Len())
		for g := range out.Len() {
			seqs.AppendInt(first[g])
			fcol.AppendText(encodeFails(fails[g]))
		}
		cols := make([]block.Column, 0, len(ext))
		for i := range out.Width() {
			cols = append(cols, out.Column(i))
		}
		s.out.addSeq(batch{newBlock(append(cols, seqs.Build(), fcol.Build()), out.Len()), orig}, first)
	}
	if s.g != nil {
		out, orig, fails, first := s.g.result()
		if out.Len() > 0 {
			add(out, orig, fails, first)
		}
	}

	// A spill released the raw state of the rows; rows still in memory
	// release it here.
	spilled := s.x.spilled()
	keyCol := len(s.in)
	var cur string
	split := func(r row, n int) bool {
		key, _ := r.b.blk.Column(keyCol).Text(r.i)
		next := n >= s.x.frameLen && key != cur
		cur = key
		return next
	}
	err := s.mergeInBlocks(s.x, split, func(b batch, seq []int64) error {
		out, orig, fails, firsts := op.aggregate(b.blk, s.in, b.orig, s.sc.rx.shared)
		if !spilled {
			t.release(b.orig)
		}
		first := make([]int64, out.Len())
		for g := range out.Len() {
			first[g] = seq[firsts[g]]
		}
		add(out, orig, fails, first)
		return s.sc.rx.mem().relieve()
	})
	if err != nil {
		return err
	}
	return s.mergeInBlocks(s.out, s.full, func(b batch, _ []int64) error {
		n := b.blk.Len()
		cols := make([]block.Column, len(outSchema))
		for i := range cols {
			cols[i] = b.blk.Column(i)
		}
		keys := make([]int, n)
		fails := make([][]aggFailure, n)
		for i := range n {
			v, _ := b.blk.Column(len(ext) - 2).Int(i)
			keys[i] = int(v)
			f, _ := b.blk.Column(len(ext) - 1).Text(i)
			fails[i] = decodeFails(f)
		}
		s.sc.begin(block.Block{}, b.orig)
		res, orig, err := op.rejectFailed(newBlock(cols, n), b.orig, fails, keys, s.in, s.sc)
		if err != nil {
			return err
		}
		t.passed(s.sc.cnt, orig, true)
		return s.emit(ctx, batch{res, orig})
	})
}

// encodeFails and decodeFails carry the failed aggregations of a group
// through a spill file.
func encodeFails(fs []aggFailure) string {
	var sb strings.Builder
	for _, f := range fs {
		sb.WriteString(strconv.Itoa(len(f.column)))
		sb.WriteByte(':')
		sb.WriteString(f.column)
		sb.WriteString(strconv.Itoa(len(f.reason)))
		sb.WriteByte(':')
		sb.WriteString(f.reason)
	}
	return sb.String()
}

func decodeFails(s string) []aggFailure {
	var out []aggFailure
	next := func() string {
		i := strings.IndexByte(s, ':')
		n, _ := strconv.Atoi(s[:i])
		v := s[i+1 : i+1+n]
		s = s[i+1+n:]
		return v
	}
	for s != "" {
		c := next()
		out = append(out, aggFailure{c, next()})
	}
	return out
}

// collector is the end of a plan: it keeps the output blocks in memory, or
// writes them to the sink for the results. Its rows have passed the run
// (D43). In memory they keep their raw state (D87); written, they leave the
// plan and let go of it, and only then count as passed (D84, D94).
type collector struct {
	t       *tally
	batches []batch
	sink    Sink
	s       schema // output schema, for the blocks of the sink
	written int
}

func (c *collector) push(ctx context.Context, b batch) error {
	if c.sink != nil {
		if b.blk.Len() == 0 {
			return nil
		}
		return c.write(ctx, b)
	}
	c.pass(b, false)
	if b.blk.Len() > 0 || len(c.batches) == 0 {
		c.batches = append(c.batches, b)
	}
	return nil
}

func (c *collector) write(ctx context.Context, b batch) error {
	if err := c.sink.Write(ctx, sinkBlock(c.s, b)); err != nil {
		return &SinkError{Sink: "result", Err: err}
	}
	c.written++
	c.pass(b, true)
	return nil
}

// pass counts the rows of b as passed, and with release lets go of their
// raw state.
func (c *collector) pass(b batch, release bool) {
	if c.t == nil {
		return
	}
	for _, o := range b.orig {
		c.t.run.exit(o, stPassed)
		if release {
			for _, r := range o.refs {
				if c.t.owns(r) {
					r.src.drop(r.row)
				}
			}
		}
	}
}

// finish writes an empty block if the sink got none, so a file gets its
// header (D100).
func (c *collector) finish(ctx context.Context) error {
	if c.sink != nil && c.written == 0 {
		return c.write(ctx, batch{blk: emptyBlock(c.s)})
	}
	return nil
}

// build links the stages of the planned steps, ending in c. The counters
// of the steps are created in plan order, a branch after its step.
func build(steps []planned, rx *rejector, blockLen int, c *collector) stage {
	stages := make([]func(next stage) stage, len(steps))
	for i, st := range steps {
		if op, ok := st.op.impl.(fullOp); ok {
			sc := newStepCtx(st, nil, rx)
			stages[i] = func(next stage) stage {
				fs := newFullStage(op, st.in, sc, blockLen, next)
				if m := rx.mem(); m != nil {
					m.spillers = append(m.spillers, fs.spill)
				}
				return fs
			}
			continue
		}
		n := buildNode(st, nil, rx)
		stages[i] = func(next stage) stage { return &nodeStage{n: n, next: next} }
	}
	var next stage = c
	for i := len(stages) - 1; i >= 0; i-- {
		next = stages[i](next)
	}
	return next
}

func newStepCtx(st planned, parent *stepRef, rx *rejector) *stepCtx {
	ref := &stepRef{name: st.name, parent: parent}
	return &stepCtx{step: st.name, ref: ref, rx: rx, in: st.in, cnt: rx.tally.step(ref)}
}

// buildNode builds a step that works block by block with its branches.
func buildNode(st planned, parent *stepRef, rx *rejector) *stepNode {
	sc := newStepCtx(st, parent, rx)
	n := &stepNode{in: st.in, opOut: st.opOut, out: st.out, sc: sc}
	if st.split != nil {
		n.split = &splitNode{cond: st.split.cond, match: buildChain(st.split.match, sc.ref, rx), rest: buildChain(st.split.rest, sc.ref, rx)}
		return n
	}
	op, ok := st.op.impl.(streamOp)
	if !ok {
		panic(fmt.Sprintf("gtable: operation %T is neither streamed nor over all rows", st.op.impl))
	}
	n.op = op
	if st.fail != nil {
		n.fail = buildChain(st.fail, sc.ref, rx)
	}
	return n
}

func buildChain(b *plannedBranch, parent *stepRef, rx *rejector) *chain {
	c := &chain{out: b.out}
	for _, st := range b.steps {
		c.nodes = append(c.nodes, buildNode(st, parent, rx))
	}
	return c
}

// run pushes the batches from src through the planned steps into c, and
// calls after once a batch went through, if set. If it ends early, it
// returns the batches that passed until then with the error.
func run(ctx context.Context, src func(yield func(batch) error) error, steps []planned, rx *rejector, blockLen int, c *collector, after func() error) ([]batch, error) {
	first := build(steps, rx, blockLen, c)
	err := src(func(b batch) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		rx.tally.see(b.orig)
		if err := first.push(ctx, b); err != nil {
			return err
		}
		if err := rx.tally.checkSteps(); err != nil {
			return err
		}
		if err := rx.mem().relieve(); err != nil {
			return err
		}
		if after != nil {
			return after()
		}
		return nil
	})
	if err == nil {
		err = first.finish(ctx)
	}
	return c.batches, err
}

// chunk splits b into batches of at most n rows; n <= 0 keeps it whole.
func chunk(b batch, n int) []batch {
	if n <= 0 || b.blk.Len() <= n {
		return []batch{b}
	}
	var out []batch
	for start := 0; start < b.blk.Len(); start += n {
		end := min(start+n, b.blk.Len())
		idx := make([]int, 0, end-start)
		for i := start; i < end; i++ {
			idx = append(idx, i)
		}
		out = append(out, batch{b.blk.Take(idx), b.orig[start:end:end]})
	}
	return out
}

// emptyBlock returns a block without rows with the columns of s.
func emptyBlock(s schema) block.Block {
	cols := make([]block.Column, len(s))
	for i, f := range s {
		cols[i] = block.NewBuilder(f.kind, 0).Build()
	}
	return newBlock(cols, 0)
}
