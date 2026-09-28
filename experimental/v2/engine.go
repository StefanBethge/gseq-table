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

// stage is one step of a running plan.
type stage interface {
	push(ctx context.Context, blk block.Block) error
	finish(ctx context.Context) error
}

type streamStage struct {
	op   streamOp
	in   schema
	sc   *stepCtx
	next stage
}

func (s *streamStage) push(ctx context.Context, blk block.Block) error {
	out, err := s.op.apply(blk, s.in, s.sc)
	if err != nil {
		return err
	}
	return s.next.push(ctx, out)
}

func (s *streamStage) finish(ctx context.Context) error { return s.next.finish(ctx) }

type fullStage struct {
	op       fullOp
	in       schema
	sc       *stepCtx
	blockLen int // 0: pass the result on as one block
	next     stage
	buf      []block.Block
}

func (s *fullStage) push(_ context.Context, blk block.Block) error {
	s.buf = append(s.buf, blk)
	return nil
}

func (s *fullStage) finish(ctx context.Context) error {
	if j, ok := s.op.(joinOp); ok {
		// The rejects of the right side come before the join's own (D69).
		s.sc.rx.rejects = append(s.sc.rx.rejects, j.rightRejects()...)
	}
	blks := s.buf
	s.buf = nil
	if len(blks) == 0 {
		blks = []block.Block{emptyBlock(s.in)}
	}
	out, err := s.op.applyAll(blks, s.in, s.sc)
	if err != nil {
		return err
	}
	for _, b := range chunk(out, s.blockLen) {
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
	blocks []block.Block
}

func (c *collector) push(_ context.Context, blk block.Block) error {
	if blk.Len() > 0 || len(c.blocks) == 0 {
		c.blocks = append(c.blocks, blk)
	}
	return nil
}

func (c *collector) finish(context.Context) error { return nil }

// build links the stages of the planned steps, ending in c.
func build(steps []planned, rx *rejector, blockLen int, c *collector) stage {
	var next stage = c
	for i := len(steps) - 1; i >= 0; i-- {
		st := steps[i]
		sc := &stepCtx{step: st.name, rx: rx}
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

// run pushes the blocks from src through the planned steps.
func run(ctx context.Context, src func(yield func(block.Block) error) error, steps []planned, rx *rejector, blockLen int) ([]block.Block, error) {
	c := &collector{}
	first := build(steps, rx, blockLen, c)
	err := src(func(b block.Block) error {
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
	return c.blocks, nil
}

// chunk splits b into blocks of at most n rows; n <= 0 keeps it whole.
func chunk(b block.Block, n int) []block.Block {
	if n <= 0 || b.Len() <= n {
		return []block.Block{b}
	}
	var out []block.Block
	for start := 0; start < b.Len(); start += n {
		end := min(start+n, b.Len())
		idx := make([]int, 0, end-start)
		for i := start; i < end; i++ {
			idx = append(idx, i)
		}
		out = append(out, b.Take(idx))
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
