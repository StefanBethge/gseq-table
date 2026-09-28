package gtable

import (
	"context"
	"fmt"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

// The engine runs a checked plan block by block (D6). Operations that work
// block by block pass each block on at once; a step over all rows collects
// its blocks and runs when its input ends. The prototype collects in memory;
// spilling follows in slice 7 (#51). Table methods and pipelines both run
// through this engine (D31).

type step struct {
	name string
	op   Op
}

// planned is a step with the schema of its input.
type planned struct {
	step
	in schema
}

// checkPlan checks every step against the schema before it and returns the
// planned steps and the output schema. The first failure is a PlanError
// (D19).
func checkPlan(src schema, steps []step) ([]planned, schema, error) {
	out := make([]planned, len(steps))
	s := src
	for i, st := range steps {
		if st.op.impl == nil {
			return nil, nil, &PlanError{Step: st.name, Err: fmt.Errorf("empty operation")}
		}
		next, err := st.op.impl.plan(s)
		if err != nil {
			return nil, nil, &PlanError{Step: st.name, Err: err}
		}
		out[i] = planned{st, s}
		s = next
	}
	return out, s, nil
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

type streamStage struct {
	op   streamOp
	in   schema
	sc   *stepCtx
	next stage
}

func (s *streamStage) push(ctx context.Context, b batch) error {
	s.sc.begin(b.blk, b.orig)
	out, keep, err := s.op.apply(b.blk, s.in, s.sc)
	if err != nil {
		return err
	}
	orig := b.orig
	if keep != nil {
		orig = pick(orig, keep)
	}
	return s.next.push(ctx, batch{out, orig})
}

func (s *streamStage) finish(ctx context.Context) error { return s.next.finish(ctx) }

type fullStage struct {
	op       fullOp
	in       schema
	sc       *stepCtx
	blockLen int // 0: pass the result on as one block
	next     stage
	buf      []block.Block
	orig     []origin
}

func (s *fullStage) push(_ context.Context, b batch) error {
	s.buf = append(s.buf, b.blk)
	s.orig = append(s.orig, b.orig...)
	return nil
}

func (s *fullStage) finish(ctx context.Context) error {
	if j, ok := s.op.(joinOp); ok {
		// The rejects of the right side come before the join's own (D69).
		s.sc.rx.entries = append(s.sc.rx.entries, j.rightRejects()...)
	}
	blks, orig := s.buf, s.orig
	s.buf, s.orig = nil, nil
	if len(blks) == 0 {
		blks = []block.Block{emptyBlock(s.in)}
	}
	s.sc.begin(block.Block{}, orig)
	out, outOrig, err := s.op.applyAll(blks, s.in, s.sc)
	if err != nil {
		return err
	}
	for _, b := range chunk(batch{out, outOrig}, s.blockLen) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.next.push(ctx, b); err != nil {
			return err
		}
	}
	return s.next.finish(ctx)
}

// collector is the end of a plan: it keeps the output blocks in memory.
type collector struct {
	batches []batch
}

func (c *collector) push(_ context.Context, b batch) error {
	if b.blk.Len() > 0 || len(c.batches) == 0 {
		c.batches = append(c.batches, b)
	}
	return nil
}

func (c *collector) finish(context.Context) error { return nil }

// build links the stages of the planned steps, ending in c.
func build(steps []planned, rx *rejector, blockLen int, c *collector) stage {
	var next stage = c
	for i := len(steps) - 1; i >= 0; i-- {
		st := steps[i]
		sc := &stepCtx{step: st.name, ref: &stepRef{st.name}, rx: rx, in: st.in}
		switch op := st.op.impl.(type) {
		case streamOp:
			next = &streamStage{op: op, in: st.in, sc: sc, next: next}
		case fullOp:
			next = &fullStage{op: op, in: st.in, sc: sc, blockLen: blockLen, next: next}
		default:
			panic(fmt.Sprintf("gtable: operation %T is neither streamed nor over all rows", op))
		}
	}
	return next
}

// run pushes the batches from src through the planned steps.
func run(ctx context.Context, src func(yield func(batch) error) error, steps []planned, rx *rejector, blockLen int) ([]batch, error) {
	c := &collector{}
	first := build(steps, rx, blockLen, c)
	err := src(func(b batch) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return first.push(ctx, b)
	})
	if err != nil {
		return nil, err
	}
	if err := first.finish(ctx); err != nil {
		return nil, err
	}
	return c.batches, nil
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
