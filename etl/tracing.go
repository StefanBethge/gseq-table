package etl

import (
	"time"

	"github.com/stefanbethge/gseq-table/table"
	"github.com/stefanbethge/gseq/result"
)

// StepRecord captures the metadata of one named pipeline step.
type StepRecord struct {
	Name       string
	InputRows  int
	OutputRows int
	Duration   time.Duration
}

// WithTracing enables execution tracing for the pipeline. Every subsequent
// call to Step will record a StepRecord that can be retrieved via Trace.
//
// WithTracing returns a new Pipeline that shares a trace buffer with all
// pipelines derived from it. Call it once at the start of a chain:
//
//	p := etl.From(t).WithTracing()
//	result := p.
//	    Step("filter", func(t table.Table) table.Table { return t.Where(...) }).
//	    Step("enrich", func(t table.Table) table.Table { return t.AddCol(...) }).
//	    Unwrap()
//	fmt.Println(p.Trace()) // [{filter 1000 420 ...} {enrich 420 420 ...}]
func (p Pipeline) WithTracing() Pipeline {
	buf := make([]StepRecord, 0, 8)
	return Pipeline{r: p.r, trace: &buf}
}

// Step applies fn to the Table and records a StepRecord in the trace buffer
// (if tracing is active). If the pipeline is in an error state, fn is skipped
// and no record is appended.
//
//	p.Step("normalize", func(t table.Table) table.Table { return t.Map("city", strings.ToUpper) })
func (p Pipeline) Step(name string, fn func(table.Table) table.Table) Pipeline {
	if p.r.IsErr() || p.trace == nil {
		return p.Then(fn)
	}
	t := p.r.Unwrap()
	inputRows := t.Len()
	start := time.Now()
	out := fn(t)
	duration := time.Since(start)
	*p.trace = append(*p.trace, StepRecord{
		Name:       name,
		InputRows:  inputRows,
		OutputRows: out.Len(),
		Duration:   duration,
	})
	return Pipeline{r: result.Ok[table.Table, error](out), trace: p.trace}
}

// Trace returns all StepRecords collected since WithTracing was called.
// Returns nil if tracing was not enabled.
func (p Pipeline) Trace() []StepRecord {
	if p.trace == nil {
		return nil
	}
	return *p.trace
}
