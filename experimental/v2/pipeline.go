package gtable

import (
	"context"
	"errors"
	"fmt"
)

// Pipeline is a plan (D6): a source and named steps that take the same Op
// values as the Table methods (D31). Nothing runs until Run. The pipeline
// only controls the flow; it has no data operations of its own.
//
// Run checks the whole plan first: an unknown column, a type conflict or an
// invalid parameter is a PlanError before any row is read (D19, D32). The
// engine then runs the plan block by block, in memory in this prototype
// slice.
//
// Delivery and data errors reject the affected rows by default (D5). The
// behavior is set per pipeline, per error kind and per code (D19).
type Pipeline struct {
	src       source
	blockLen  int
	steps     []step
	policy    errorPolicy
	prefix    string
	hasPrefix bool
}

// source is the input of a pipeline: a table, or a delivery read by a
// Reader. A run opens it and closes it at the end.
type source interface {
	open() (opened, error)
}

// opened is an opened source.
type opened interface {
	schema() schema
	rejects() []rejectEntry
	sources() []*rawSource
	findingList() []Finding
	// stopAtOpen returns the delivery error a header check found, if its
	// code is set to ModeStop (D19).
	stopAtOpen(p errorPolicy) error
	// blocks yields batches of at most n rows; rows rejected while reading
	// go to rx.
	blocks(n int, rx *rejector, yield func(batch) error) error
	close() error
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

// OnError sets the error mode of the pipeline, the default for every kind
// and code (D3, D19).
func (p *Pipeline) OnError(m ErrorMode) *Pipeline {
	p.policy.mode = m
	return p
}

// OnErrorKind sets the error mode for errors of kind k (D19).
func (p *Pipeline) OnErrorKind(k ErrorKind, m ErrorMode) *Pipeline {
	p.policy = p.policy.withKind(k, m)
	return p
}

// OnErrorCode sets the error mode for errors with the given code, such as
// CodeParse or "custom:vip". It takes precedence over the kind and the
// pipeline setting (D19).
func (p *Pipeline) OnErrorCode(code string, m ErrorMode) *Pipeline {
	p.policy = p.policy.withCode(code, m)
	return p
}

// InfoPrefix sets the prefix of the info columns of the rejected rows; the
// default is DefaultInfoPrefix (D14). A raw column that starts with the
// prefix is a plan error.
func (p *Pipeline) InfoPrefix(prefix string) *Pipeline {
	p.prefix, p.hasPrefix = prefix, true
	return p
}

// Check checks the plan without reading a row and returns a *PlanError, or
// nil (D19). A source read by a Reader is opened to read its header; if it
// cannot be read, Check returns a *DeliveryError.
func (p *Pipeline) Check() error {
	if err := p.checkParams(); err != nil {
		return err
	}
	o, err := p.src.open()
	if err != nil {
		return err
	}
	defer o.close()
	_, _, err = p.check(o)
	return err
}

func (p *Pipeline) checkParams() error {
	if p.blockLen <= 0 {
		return &PlanError{Step: "source", Err: fmt.Errorf("block length %d is not positive", p.blockLen)}
	}
	if p.hasPrefix && p.prefix == "" {
		return &PlanError{Step: "source", Err: errors.New("empty info prefix")}
	}
	return nil
}

func (p *Pipeline) check(o opened) ([]planned, schema, error) {
	srcs := o.sources()
	for _, st := range p.steps {
		j, ok := st.op.impl.(joinOp)
		if !ok || j.right.err != nil {
			continue // a broken right side is reported by the join's plan
		}
		if err := sourceClash(srcs, j.right.srcs); err != nil {
			return nil, nil, &PlanError{Step: st.name, Err: err}
		}
		srcs = unionSources(srcs, j.right.srcs)
	}
	prefix := infoPrefix(p.prefix)
	for _, src := range srcs {
		if err := prefixClash(src, prefix); err != nil {
			return nil, nil, &PlanError{Step: "source", Err: err}
		}
	}
	return checkPlan(o.schema(), p.steps)
}

// Run checks the plan and runs it. It returns a *PlanError before any row is
// read, a *DeliveryError if the source cannot be read or a delivery error
// is set to ModeStop, or the context's error if ctx ends. The result carries
// the rejects of the source, then those of the reading and the steps, and
// the findings of the header check.
func (p *Pipeline) Run(ctx context.Context) (Table, error) {
	if err := p.checkParams(); err != nil {
		return Table{}, err
	}
	o, err := p.src.open()
	if err != nil {
		return Table{}, err
	}
	defer o.close()
	steps, out, err := p.check(o)
	if err != nil {
		return Table{}, err
	}
	if err := o.stopAtOpen(p.policy); err != nil {
		return Table{}, err
	}
	rx := &rejector{policy: p.policy, run: newRun()}
	bs, err := run(ctx, func(yield func(batch) error) error {
		n := 0
		err := o.blocks(p.blockLen, rx, func(b batch) error {
			n++
			return yield(b)
		})
		if err == nil && n == 0 {
			err = yield(batch{emptyBlock(o.schema()), nil})
		}
		return err
	}, steps, rx, p.blockLen)
	if err != nil {
		return Table{}, err
	}
	srcs, finds := o.sources(), o.findingList()
	for _, st := range steps {
		if j, ok := st.op.impl.(joinOp); ok {
			srcs = unionSources(srcs, j.right.srcs)
			finds = append(finds, j.right.finds...)
		}
	}
	rejects := append(o.rejects(), rx.entries...)
	return Table{s: out, blocks: blocksOf(bs), orig: originsOf(bs), srcs: srcs, rejects: rejects, finds: finds,
		policy: p.policy, prefix: p.prefix, run: rx.run}, nil
}

type tableSource struct{ t Table }

func (s tableSource) open() (opened, error) {
	if s.t.err != nil {
		return nil, &PlanError{Step: "source", Err: fmt.Errorf("source has an error: %w", s.t.err)}
	}
	return s, nil
}

func (s tableSource) schema() schema               { return s.t.s }
func (s tableSource) rejects() []rejectEntry       { return append([]rejectEntry(nil), s.t.rejects...) }
func (s tableSource) sources() []*rawSource        { return s.t.srcs }
func (s tableSource) findingList() []Finding       { return append([]Finding(nil), s.t.finds...) }
func (s tableSource) stopAtOpen(errorPolicy) error { return nil }
func (s tableSource) close() error                 { return nil }

func (s tableSource) blocks(n int, _ *rejector, yield func(batch) error) error {
	for _, b := range s.t.batches() {
		for _, c := range chunk(b, n) {
			if c.blk.Len() == 0 {
				continue
			}
			if err := yield(c); err != nil {
				return err
			}
		}
	}
	return nil
}
