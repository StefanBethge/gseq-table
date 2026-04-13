package etl

import (
	"time"

	"github.com/stefanbethge/gseq-table/table"
	"github.com/stefanbethge/gseq/result"
)

// MutableFunc is a function that transforms a *MutableTable in place.
// Use it to name and reuse mutable transformation functions:
//
//	var normalizeCity MutableFunc = func(m *table.MutableTable) *table.MutableTable {
//	    return m.Map("city", strings.ToUpper).FillEmpty("region", "unknown")
//	}
//	mp.Then(normalizeCity)
type MutableFunc = func(*table.MutableTable) *table.MutableTable

// MutablePipeline wraps a Result[*MutableTable, error] for in-place
// transformations. Compared to Pipeline, it avoids intermediate immutable
// Table allocations — each Then step mutates the same underlying storage.
//
// Use Frozen() to convert the result to an immutable Pipeline when you need
// schema validation, joins against immutable tables, or other Table-only ops.
//
//	mp := etl.FromMutable(m).
//	    Then(func(m *table.MutableTable) *table.MutableTable {
//	        return m.Map("city", strings.ToUpper).DropEmpty("id")
//	    }).
//	    Frozen().
//	    ApplySchema(s)
type MutablePipeline struct {
	r     result.Result[*table.MutableTable, error]
	trace *[]StepRecord // nil unless WithTracing was called
}

// FromMutable wraps an existing *MutableTable in a MutablePipeline.
func FromMutable(m *table.MutableTable) MutablePipeline {
	return MutablePipeline{r: result.Ok[*table.MutableTable, error](m)}
}

// ─── Core combinators ─────────────────────────────────────────────────────────

// Then applies an in-place MutableFunc transformation.
// If the pipeline is already in an error state, fn is skipped.
func (p MutablePipeline) Then(fn MutableFunc) MutablePipeline {
	return MutablePipeline{
		r:     result.Map(p.r, fn),
		trace: p.trace,
	}
}

// ThenErr applies a fallible transformation. If fn returns an Err result,
// all subsequent operations are skipped.
func (p MutablePipeline) ThenErr(fn func(*table.MutableTable) result.Result[*table.MutableTable, error]) MutablePipeline {
	return MutablePipeline{
		r:     result.FlatMap(p.r, fn),
		trace: p.trace,
	}
}

// ─── Conditional steps ────────────────────────────────────────────────────────

// IfThen applies fn only when cond is true; otherwise passes through unchanged.
func (p MutablePipeline) IfThen(cond bool, fn MutableFunc) MutablePipeline {
	if !cond {
		return p
	}
	return p.Then(fn)
}

// IfThenErr applies a fallible fn only when cond is true.
func (p MutablePipeline) IfThenErr(cond bool, fn func(*table.MutableTable) result.Result[*table.MutableTable, error]) MutablePipeline {
	if !cond {
		return p
	}
	return p.ThenErr(fn)
}

// ─── Error recovery ───────────────────────────────────────────────────────────

// RecoverWith replaces an error state with fallback. If the pipeline is
// already ok, fallback is ignored.
func (p MutablePipeline) RecoverWith(fallback *table.MutableTable) MutablePipeline {
	if p.r.IsOk() {
		return p
	}
	return MutablePipeline{r: result.Ok[*table.MutableTable, error](fallback), trace: p.trace}
}

// OnError calls fn with the current error. fn may return a recovered
// *MutableTable (with nil error) or a new error.
func (p MutablePipeline) OnError(fn func(error) (*table.MutableTable, error)) MutablePipeline {
	if p.r.IsOk() {
		return p
	}
	m, err := fn(p.r.UnwrapErr())
	if err != nil {
		return MutablePipeline{r: result.Err[*table.MutableTable, error](err), trace: p.trace}
	}
	return MutablePipeline{r: result.Ok[*table.MutableTable, error](m), trace: p.trace}
}

// MapErr transforms the error value, leaving ok pipelines unchanged.
func (p MutablePipeline) MapErr(fn func(error) error) MutablePipeline {
	return MutablePipeline{r: result.MapErr(p.r, fn), trace: p.trace}
}

// ─── Inspection ───────────────────────────────────────────────────────────────

// Peek calls fn with the current *MutableTable without modifying the pipeline
// result. Useful for logging or inspecting intermediate state.
func (p MutablePipeline) Peek(fn func(*table.MutableTable)) MutablePipeline {
	return p.Then(func(m *table.MutableTable) *table.MutableTable {
		fn(m)
		return m
	})
}

// ─── Tracing ─────────────────────────────────────────────────────────────────

// WithTracing enables execution tracing. Every subsequent Step call records a
// StepRecord retrievable via Trace.
func (p MutablePipeline) WithTracing() MutablePipeline {
	buf := make([]StepRecord, 0, 8)
	return MutablePipeline{r: p.r, trace: &buf}
}

// Step applies fn and records a StepRecord (name, row counts, duration) when
// tracing is active. Skipped if the pipeline is in an error state.
func (p MutablePipeline) Step(name string, fn MutableFunc) MutablePipeline {
	if p.r.IsErr() || p.trace == nil {
		return p.Then(fn)
	}
	m := p.r.Unwrap()
	inputRows := m.Len()
	start := time.Now()
	out := fn(m)
	duration := time.Since(start)
	*p.trace = append(*p.trace, StepRecord{
		Name:       name,
		InputRows:  inputRows,
		OutputRows: out.Len(),
		Duration:   duration,
	})
	return MutablePipeline{r: result.Ok[*table.MutableTable, error](out), trace: p.trace}
}

// Trace returns all StepRecords collected since WithTracing was called.
// Returns nil if tracing was not enabled.
func (p MutablePipeline) Trace() []StepRecord {
	if p.trace == nil {
		return nil
	}
	return *p.trace
}

// ─── Bridge to immutable Pipeline ────────────────────────────────────────────

// Frozen converts the MutablePipeline to an immutable Pipeline by calling
// Freeze() on the underlying *MutableTable. Use this to access
// ApplySchema, AssertColumns, joins, and other Table-only operations.
//
//	mp.Then(...).Frozen().ApplySchema(s).Unwrap()
func (p MutablePipeline) Frozen() Pipeline {
	r := result.Map(p.r, func(m *table.MutableTable) table.Table {
		return m.Freeze()
	})
	return Pipeline{r: r, trace: p.trace}
}

// ─── Terminal operations ──────────────────────────────────────────────────────

// Result returns the underlying Result[*MutableTable, error].
func (p MutablePipeline) Result() result.Result[*table.MutableTable, error] {
	return p.r
}

// Unwrap returns the *MutableTable or panics if the pipeline is in an error state.
func (p MutablePipeline) Unwrap() *table.MutableTable {
	return p.r.Unwrap()
}

// IsOk reports whether the pipeline holds a valid *MutableTable.
func (p MutablePipeline) IsOk() bool { return p.r.IsOk() }

// IsErr reports whether the pipeline is in an error state.
func (p MutablePipeline) IsErr() bool { return p.r.IsErr() }
