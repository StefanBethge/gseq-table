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
// behavior is set per pipeline, per error kind and per code (D19). A
// threshold fails the run if too many rows are rejected (D4, D20).
type Pipeline struct {
	src       source
	blockLen  int
	steps     []step
	policy    errorPolicy
	prefix    string
	hasPrefix bool

	limits     []Limit
	stepLimits map[string][]Limit
	abort      bool
	abortMin   int
	formats    formatLimits
	copyMode   CopyMode

	sinks sinks
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
	// are rejected in the step "read" of sc.
	blocks(n int, sc *stepCtx, yield func(batch) error) error
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
	p.steps = appendStep(p.steps, "", op)
	return p
}

// Step appends op as a step with the given name. Rejects name the step.
func (p *Pipeline) Step(name string, op Op) *Pipeline {
	p.steps = appendStep(p.steps, name, op)
	return p
}

// OnFail gives the last step a fail branch (D25). The rows the step fails
// go into the branch in the state they had before the step, with info
// columns about their first error (D91). What the branch returns flows
// back into the main path in the input order, merged by column name, and
// without the info columns (D26, D90). What fails in the branch too is
// rejected, with its reject_id, its path in step, such as "cast ›
// cast_alt", and the reason from the main path in prev_reason (D27, D92).
// Rescued rows keep this history, and only rows that stay rejected count
// for the threshold (D46).
//
// Only a step that works block by block has a fail branch (D90). Without
// a step before, or for a step that has one, OnFail is a plan error. A nil
// branch returns the failed rows unchanged.
func (p *Pipeline) OnFail(fail *Branch) *Pipeline {
	p.steps = withFail(p.steps, fail)
	return p
}

// Split appends a step that sends every row for which cond is true into
// match and every other row, also one for which cond is null, into rest,
// and merges what they return by column name (D25, D26). A column that one
// branch lacks is null in the rows of the other; a column of the same name
// and another type is a plan error. The rows keep the input order (D90).
// If cond fails for a row at run time, the row is rejected with code
// "expr" (D54). A nil branch passes its rows on unchanged.
func (p *Pipeline) Split(name string, cond Expr, match, rest *Branch) *Pipeline {
	p.steps = appendSplit(p.steps, name, cond, match, rest)
	return p
}

// CopyMode sets whether the engine copies data or changes it in place, for
// the whole run (D8). The default is CopyAuto.
func (p *Pipeline) CopyMode(m CopyMode) *Pipeline {
	p.copyMode = m
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

// Threshold sets the threshold of the run (D4, D20): the run fails with
// StatusFailedThreshold if any of the limits is exceeded, counted in
// source rows (D84). By default the run runs to the end; see
// AbortOnThreshold.
func (p *Pipeline) Threshold(limits ...Limit) *Pipeline {
	p.limits = append(p.limits, limits...)
	return p
}

// StepThreshold sets a threshold for the steps with the given name, or for
// "read", the rows a Reader rejects while reading. A share is over the rows
// that went into the step (D45). A name that no step has is a plan error.
func (p *Pipeline) StepThreshold(step string, limits ...Limit) *Pipeline {
	if p.stepLimits == nil {
		p.stepLimits = map[string][]Limit{}
	}
	p.stepLimits[step] = append(p.stepLimits[step], limits...)
	return p
}

// AbortOnThreshold ends the run as soon as a threshold is exceeded, with
// StatusFailedThreshold and a *ThresholdError (D20, D45). An absolute limit
// aborts at once; a share only once minRows rows are read, or went into
// the step (D89).
func (p *Pipeline) AbortOnThreshold(minRows int) *Pipeline {
	p.abort, p.abortMin = true, minRows
	return p
}

// FormatChangeLimit sets the share of the rows that went into a step from
// which failed values of a column with the same code are reported as a
// format change (D59, D85). The default is DefaultFormatChangeLimit.
func (p *Pipeline) FormatChangeLimit(share float64) *Pipeline {
	if checkFormatLimit(share) != nil {
		p.formats.bad = append(p.formats.bad, share)
	}
	p.formats.all = share
	return p
}

// FormatChangeLimitFor sets the limit for a format change of one column,
// in place of FormatChangeLimit (D59).
func (p *Pipeline) FormatChangeLimitFor(column string, share float64) *Pipeline {
	if checkFormatLimit(share) != nil {
		p.formats.bad = append(p.formats.bad, share)
	}
	cols := make(map[string]float64, len(p.formats.cols)+1)
	for c, v := range p.formats.cols {
		cols[c] = v
	}
	cols[column] = share
	p.formats.cols = cols
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
	limits := append([]Limit(nil), p.limits...)
	for _, ls := range p.stepLimits {
		limits = append(limits, ls...)
	}
	for _, l := range limits {
		if err := l.check(); err != nil {
			return &PlanError{Step: "threshold", Err: err}
		}
	}
	if p.abort && p.abortMin < 0 {
		return &PlanError{Step: "threshold", Err: fmt.Errorf("minimum of %d rows is negative", p.abortMin)}
	}
	if len(p.formats.bad) > 0 {
		return &PlanError{Step: "format_change", Err: checkFormatLimit(p.formats.bad[0])}
	}
	if p.copyMode > CopyInPlace {
		return &PlanError{Step: "source", Err: fmt.Errorf("unknown copy mode %d", p.copyMode)}
	}
	return nil
}

// hasStep reports whether steps or their branches have a step of the name.
func hasStep(steps []step, name string) bool {
	for _, st := range steps {
		switch {
		case st.name == name,
			st.fail != nil && hasStep(st.fail.steps, name),
			st.split != nil && (hasStep(st.split.match.steps, name) || hasStep(st.split.rest.steps, name)):
			return true
		}
	}
	return false
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
	if err := p.sinks.check(srcs); err != nil {
		return nil, nil, &PlanError{Step: "sink", Err: err}
	}
	prefix := infoPrefix(p.prefix)
	for _, src := range srcs {
		if err := prefixClash(src, prefix); err != nil {
			return nil, nil, &PlanError{Step: "source", Err: err}
		}
	}
	for name := range p.stepLimits {
		_, reads := o.(*openedReader)
		if !(reads && name == "read") && !hasStep(p.steps, name) {
			return nil, nil, &PlanError{Step: "threshold", Err: fmt.Errorf("no step named %q", name)}
		}
	}
	return checkPlan(o.schema(), p.steps, infoPrefix(p.prefix))
}

// Run checks the plan and runs it, and returns its result (D21, D88). The
// result always holds the status and the counts, also after a plan error.
// Run also returns an error if the run did not run to the end: a
// *PlanError before any row is read, a *DeliveryError if the source cannot
// be read or a delivery error is set to ModeStop, a *DataError for a data
// error set to ModeStop, a *ThresholdError with AbortOnThreshold, a
// *SinkError if a sink failed (D40), or the context's error. The result
// table carries the rejects of the source, then those of the reading and
// the steps, and the findings of the header check. With a sink for the
// results it has no rows (D94). At the end Run closes every sink of the
// plan, also after an early end (D100).
func (p *Pipeline) Run(ctx context.Context) (Result, error) {
	st := &rejectState{}
	rw := newRejectWriters(p.sinks, infoPrefix(p.prefix), st)
	res, err := p.run(ctx, rw)
	res.Table.rs = st
	for _, e := range closeSinks(p.sinks, rw) {
		if err == nil {
			err = e
			res.Table.err = e
		}
		res.Causes = append(res.Causes, Cause{StatusSinkError, e})
	}
	res.Status = status(res.Causes)
	return res, err
}

func (p *Pipeline) run(ctx context.Context, rw *rejectWriters) (Result, error) {
	if err := p.checkParams(); err != nil {
		return failed(err, nil)
	}
	o, err := p.src.open()
	if err != nil {
		return failed(err, nil)
	}
	defer o.close()
	steps, out, err := p.check(o)
	if err != nil {
		return failed(err, nil)
	}
	if err := o.stopAtOpen(p.policy); err != nil {
		return failed(err, o.findingList())
	}

	t := newTally()
	t.run.limits, t.stepLimits = p.limits, p.stepLimits
	t.abort, t.min = p.abort, p.abortMin
	for _, src := range o.sources() {
		if src.rel != nil && src.rel.owner == nil {
			src.rel.owner = t
		}
	}
	rx := &rejector{policy: p.policy, run: newRun(), tally: t, prefix: p.prefix, mode: p.copyMode, trace: &tracer{}}
	read := &stepCtx{step: "read", ref: &stepRef{name: "read"}, rx: rx}
	if _, ok := o.(*openedReader); ok {
		read.cnt = t.step(read.ref)
	}
	rx.carry(o.rejects())
	c := &collector{t: t, sink: p.sinks.result, s: out}
	flush := func() error { return rw.flush(ctx, rx.entries) }
	bs, runErr := run(ctx, func(yield func(batch) error) error {
		n := 0
		err := o.blocks(p.blockLen, read, func(b batch) error {
			n++
			return yield(b)
		})
		if err == nil && n == 0 {
			err = yield(batch{emptyBlock(o.schema()), nil})
		}
		return err
	}, steps, rx, p.blockLen, c, flush)
	if runErr == nil {
		runErr = flush()
	}
	srcs, finds := o.sources(), o.findingList()
	for _, st := range steps {
		if j, ok := st.op.impl.(joinOp); ok {
			srcs = unionSources(srcs, j.right.srcs)
			finds = append(finds, j.right.finds...)
		}
	}
	if len(bs) == 0 {
		bs = []batch{{emptyBlock(out), nil}}
	}
	res := Result{
		Table: Table{s: out, blocks: blocksOf(bs), orig: originsOf(bs), srcs: srcs, rejects: rx.entries, finds: finds,
			err: runErr, policy: p.policy, prefix: p.prefix, run: rx.run},
		Counts: t.run.counts(),
		Trace:  rx.trace.entries,
	}
	res.Report = append(append(append(res.Report, finds...), unreadable(runErr)...), formatChanges(rx.entries, t.byStep, p.formats)...)
	for _, c := range t.steps {
		res.Steps = append(res.Steps, StepCounts{Step: c.name, Counts: c.counts()})
	}
	if runErr != nil {
		res.Causes = append(res.Causes, causeOf(runErr))
	}
	res.Causes = append(res.Causes, deliveryCauses(runErr, finds, rx.entries)...)
	var te *ThresholdError
	if !errors.As(runErr, &te) {
		for _, c := range append([]*counter{t.run}, t.steps...) {
			if l, ok := c.over(0); ok {
				res.Causes = append(res.Causes, Cause{StatusFailedThreshold, c.thresholdError(l)})
			}
		}
	}
	res.Status = status(res.Causes)
	return res, runErr
}

// failed is the result of a run that ended before any row was read.
func failed(err error, finds []Finding) (Result, error) {
	c := causeOf(err)
	report := append(append([]Finding(nil), finds...), unreadable(err)...)
	return Result{Table: Table{err: err}, Status: c.Status, Causes: []Cause{c}, Report: report}, err
}

// unreadable returns the finding for a delivery that could not be read
// (D42).
func unreadable(err error) []Finding {
	var de *DeliveryError
	if errors.As(err, &de) && de.Code == CodeUnreadable {
		return []Finding{{Kind: FindingUnreadable, Source: de.Source, Detail: de.Err.Error()}}
	}
	return nil
}

// deliveryCauses returns a delivery error for every source and code of a
// missing column or of a rejected row with a delivery error code, beyond
// the one that ended the run (D42).
func deliveryCauses(runErr error, finds []Finding, entries []rejectEntry) []Cause {
	type key struct{ src, code string }
	seen := map[key]bool{}
	var de *DeliveryError
	if errors.As(runErr, &de) {
		seen[key{de.Source, de.Code}] = true
	}
	var out []Cause
	add := func(src, code string, err error) {
		if k := (key{src, code}); !seen[k] {
			seen[k] = true
			out = append(out, Cause{StatusDeliveryError, &DeliveryError{Source: src, Code: code, Err: err}})
		}
	}
	for _, f := range finds {
		if f.Kind == FindingMissingColumn {
			add(f.Source, CodeMissingColumn, fmt.Errorf("column %q is missing from the delivery", f.Column))
		}
	}
	for _, e := range entries {
		if kindOfCode(e.Code) == KindDelivery {
			src := ""
			if len(e.orig.refs) > 0 {
				src = e.orig.refs[0].src.name
			}
			add(src, e.Code, errors.New(e.Reason))
		}
	}
	return out
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

// blocks yields the blocks of the table. A block passed on whole is shared
// with the table, so that no step changes it (D5, D93).
func (s tableSource) blocks(n int, _ *stepCtx, yield func(batch) error) error {
	for _, b := range s.t.batches() {
		cs := chunk(b, n)
		for _, c := range cs {
			if c.blk.Len() == 0 {
				continue
			}
			if len(cs) == 1 {
				c.blk = c.blk.Share()
			}
			if err := yield(c); err != nil {
				return err
			}
		}
	}
	return nil
}
