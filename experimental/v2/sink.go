package gtable

import (
	"context"
	"errors"
	"fmt"
)

// Sink is a target of a run (D35): results and rejected rows go through the
// same writers (D2). The packages csv and excel provide file writers; a
// database or an HTTP endpoint is a Sink of its own.
//
// A run hands a sink its rows block by block with Write and ends it with
// Close, also after the run ended early, so what was written stays (D51,
// D96). If Write or Close returns an error, the run ends at once with
// StatusSinkError (D40).
type Sink interface {
	// Write writes one block. The blocks of one run have the same columns.
	Write(ctx context.Context, b Block) error
	// Close ends the writing. A run calls it once at its end; a sink may be
	// written again after Close, by the next run.
	Close() error
}

// Block is one block of rows for a sink: the rows, and the row_key of each
// row (D47), which a writer that upserts uses. For a block of rejected
// rows, RowKeys are those of the rows that failed.
type Block struct {
	Rows    Table
	RowKeys []string
}

// SinkError says that a sink rejected a block or failed to close (D40).
// Sink names the sink: "result", "rejects of <source>" or "overview".
type SinkError struct {
	Sink string
	Err  error
}

func (e *SinkError) Error() string { return fmt.Sprintf("sink error in %s: %v", e.Sink, e.Err) }
func (e *SinkError) Unwrap() error { return e.Err }

// ErrClosed is the sticky error of the tables of rejected rows of a result
// after Close (D94).
var ErrClosed = errors.New("gtable: the result is closed")

// To sets the sink for the result rows (D35). The result table then has the
// columns of the plan but no rows (D90).
func (p *Pipeline) To(s Sink) *Pipeline {
	p.sinks.result, p.sinks.hasResult = s, true
	return p
}

// RejectsTo sets the writer for the rejected rows of the named source
// (D49, D91). The rows are written during the run, block by block, and the
// result then holds the overview and the counts, not the rows. A name that
// no source of the plan has is a plan error.
func (p *Pipeline) RejectsTo(source string, s Sink) *Pipeline {
	if p.sinks.named == nil {
		p.sinks.named = map[string]Sink{}
	}
	if _, ok := p.sinks.named[source]; !ok {
		p.sinks.order = append(p.sinks.order, source)
	}
	p.sinks.named[source] = s
	return p
}

// RejectsToEach sets a factory for the writers of the rejected rows of all
// sources (D91). The run calls it with the name of a source when its first
// row is rejected; a nil Sink keeps the rows of that source in the result.
// A writer set with RejectsTo goes first.
func (p *Pipeline) RejectsToEach(f func(source string) Sink) *Pipeline {
	p.sinks.each = f
	return p
}

// OverviewTo sets the writer for the overview of the rejected rows, one
// entry per error (D49). The result holds the overview too (D91).
func (p *Pipeline) OverviewTo(s Sink) *Pipeline {
	p.sinks.overview, p.sinks.hasOverview = s, true
	return p
}

// ReplaceSource returns a copy of the pipeline that reads src in place of
// its source, such as the rejected rows of an earlier run with FromRejects
// (D16). Steps, error behavior, thresholds and sinks stay.
func (p *Pipeline) ReplaceSource(src Source) *Pipeline {
	c := *p
	c.src = readerSource{src}
	c.steps = append([]step(nil), p.steps...)
	c.limits = append([]Limit(nil), p.limits...)
	c.stepLimits = make(map[string][]Limit, len(p.stepLimits))
	for k, v := range p.stepLimits {
		c.stepLimits[k] = append([]Limit(nil), v...)
	}
	c.sinks.order = append([]string(nil), p.sinks.order...)
	c.sinks.named = make(map[string]Sink, len(p.sinks.named))
	for k, v := range p.sinks.named {
		c.sinks.named[k] = v
	}
	return &c
}

// sinks are the sinks of a plan.
type sinks struct {
	result      Sink
	hasResult   bool
	named       map[string]Sink
	order       []string // named sources in the order they were set
	each        func(string) Sink
	overview    Sink
	hasOverview bool
}

func (s sinks) check(srcs []*rawSource) error {
	if s.hasResult && s.result == nil {
		return errors.New("nil sink for the result")
	}
	if s.hasOverview && s.overview == nil {
		return errors.New("nil sink for the overview")
	}
	for _, name := range s.order {
		if s.named[name] == nil {
			return fmt.Errorf("nil sink for the rejects of %q", name)
		}
		found := false
		for _, src := range srcs {
			found = found || src.name == name
		}
		if !found {
			return fmt.Errorf("no source named %q for a writer of rejected rows", name)
		}
	}
	return nil
}

// sinkBlock turns a batch into a block for a sink.
func sinkBlock(s schema, b batch) Block {
	cols := make([]Column, len(s))
	for i, f := range s {
		cols[i] = newColumn(f.name, b.blk.Column(i))
	}
	keys := make([]string, len(b.orig))
	for i, o := range b.orig {
		keys[i] = o.rowKey()
	}
	return Block{Rows: NewTable(cols...), RowKeys: keys}
}

// rejectBlock turns a table of rejected rows into a block, with the
// row_key of its info columns.
func rejectBlock(t Table, prefix string) Block {
	keys := make([]string, t.Len())
	if c, ok := t.Column(prefix + "row_key"); ok {
		for i := range keys {
			keys[i], _ = c.Text(i)
		}
	}
	return Block{Rows: t, RowKeys: keys}
}

// madeSink is a writer the factory returned for a source.
type madeSink struct {
	source string
	s      Sink
}

// rejectWriters write the rejected rows of a run while it runs (D49, D91).
type rejectWriters struct {
	sinks
	prefix string
	st     *rejectState
	got    map[*rawSource]Sink // resolved writer per source; nil for none
	made   []madeSink          // writers from the factory, to close
	done   int                 // entries written so far
}

func newRejectWriters(s sinks, prefix string, st *rejectState) *rejectWriters {
	if len(s.named) == 0 && s.each == nil && !s.hasOverview {
		return nil
	}
	return &rejectWriters{sinks: s, prefix: prefix, st: st, got: map[*rawSource]Sink{}}
}

// sinkFor returns the writer for the rejected rows of src, or nil.
func (w *rejectWriters) sinkFor(src *rawSource) Sink {
	if s, ok := w.got[src]; ok {
		return s
	}
	s := w.named[src.name]
	if s == nil && w.each != nil {
		if s = w.each(src.name); s != nil {
			w.made = append(w.made, madeSink{src.name, s})
		}
	}
	w.got[src] = s
	return s
}

// flush writes the entries not written yet. A source row written stays out
// of the tables of the result; entries not written, after an error, stay
// in them (D92).
func (w *rejectWriters) flush(ctx context.Context, entries []rejectEntry) error {
	if w == nil || w.done == len(entries) {
		return nil
	}
	es := entries[w.done:]
	w.done = len(entries)
	rr := RejectedRows{prefix: w.prefix, entries: es, st: w.st}
	for _, g := range rr.groups() {
		s := w.sinkFor(g.src)
		if s == nil {
			continue
		}
		t := rr.sourceTable(g)
		if err := t.Err(); err != nil {
			return err
		}
		if err := s.Write(ctx, rejectBlock(t, w.prefix)); err != nil {
			return &SinkError{Sink: "rejects of " + g.src.name, Err: err}
		}
		for _, row := range g.rows {
			w.st.sink(srcRef{g.src, row})
		}
	}
	if w.hasOverview {
		ov := RejectedRows{prefix: w.prefix, entries: es}.Overview()
		if ov.Len() > 0 {
			if err := w.overview.Write(ctx, rejectBlock(ov, w.prefix)); err != nil {
				return &SinkError{Sink: "overview", Err: err}
			}
		}
	}
	return nil
}

// closeSinks closes every sink of the plan and returns their errors (D96).
func closeSinks(s sinks, w *rejectWriters) []error {
	var errs []error
	closeOne := func(name string, sk Sink) {
		if err := sk.Close(); err != nil {
			errs = append(errs, &SinkError{Sink: name, Err: err})
		}
	}
	if s.hasResult && s.result != nil {
		closeOne("result", s.result)
	}
	for _, name := range s.order {
		if sk := s.named[name]; sk != nil {
			closeOne("rejects of "+name, sk)
		}
	}
	if w != nil {
		for _, m := range w.made {
			closeOne("rejects of "+m.source, m.s)
		}
	}
	if s.hasOverview && s.overview != nil {
		closeOne("overview", s.overview)
	}
	return errs
}
