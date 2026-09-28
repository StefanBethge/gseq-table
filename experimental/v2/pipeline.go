package gtable

import (
	"context"
	"fmt"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

// Pipeline is a plan (D6): a source and named steps that take the same Op
// values as the Table methods (D31). Nothing runs until Run. The pipeline
// only controls the flow; it has no data operations of its own.
//
// Run checks the whole plan first: an unknown column, a type conflict or an
// invalid parameter is a PlanError before any row is read (D19, D32). The
// engine then runs the plan block by block, in memory in this prototype
// slice.
type Pipeline struct {
	src      source
	blockLen int
	steps    []step
}

// source delivers the blocks of a pipeline's input.
type source interface {
	schema() schema
	rejects() []Reject
	err() error
	// blocks yields blocks of at most n rows.
	blocks(n int, yield func(block.Block) error) error
}

// From returns a pipeline over the rows of src, run in blocks of blockLen
// rows. There is no default block length until the benchmarks of the
// prototype set one (G13).
func From(src Table, blockLen int) *Pipeline {
	return &Pipeline{src: tableSource{src}, blockLen: blockLen}
}

// Then appends op as a step, named after the operation (for example "cast").
func (p *Pipeline) Then(op Op) *Pipeline {
	name := "?"
	if op.impl != nil {
		name = op.impl.kind()
	}
	return p.Step(name, op)
}

// Step appends op as a step with the given name. Rejects name the step.
func (p *Pipeline) Step(name string, op Op) *Pipeline {
	p.steps = append(p.steps, step{name, op})
	return p
}

// Check checks the plan without reading a row and returns a *PlanError, or
// nil (D19).
func (p *Pipeline) Check() error {
	_, _, err := p.check()
	return err
}

func (p *Pipeline) check() ([]planned, schema, error) {
	if p.blockLen <= 0 {
		return nil, nil, &PlanError{Step: "source", Err: fmt.Errorf("block length %d is not positive", p.blockLen)}
	}
	if err := p.src.err(); err != nil {
		return nil, nil, &PlanError{Step: "source", Err: fmt.Errorf("source has an error: %w", err)}
	}
	return checkPlan(p.src.schema(), p.steps)
}

// Run checks the plan and runs it. It returns a *PlanError before any row is
// read, or the context's error if ctx ends. The result carries the rejects
// of the source, then those of the steps.
func (p *Pipeline) Run(ctx context.Context) (Table, error) {
	steps, out, err := p.check()
	if err != nil {
		return Table{}, err
	}
	rx := &rejector{mode: ModeReject}
	blks, err := run(ctx, func(yield func(block.Block) error) error {
		n := 0
		err := p.src.blocks(p.blockLen, func(b block.Block) error {
			n++
			return yield(b)
		})
		if err == nil && n == 0 {
			err = yield(emptyBlock(p.src.schema()))
		}
		return err
	}, steps, rx, p.blockLen)
	if err != nil {
		return Table{}, err
	}
	rejects := append(p.src.rejects(), rx.rejects...)
	return Table{s: out, blocks: blks, rejects: rejects}, nil
}

type tableSource struct{ t Table }

func (s tableSource) schema() schema    { return s.t.s }
func (s tableSource) rejects() []Reject { return s.t.Rejects() }
func (s tableSource) err() error        { return s.t.err }

func (s tableSource) blocks(n int, yield func(block.Block) error) error {
	for _, b := range s.t.blocks {
		for _, c := range chunk(b, n) {
			if c.Len() == 0 {
				continue
			}
			if err := yield(c); err != nil {
				return err
			}
		}
	}
	return nil
}
