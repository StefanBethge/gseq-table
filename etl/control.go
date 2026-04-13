package etl

import (
	"github.com/stefanbethge/gseq-table/table"
	"github.com/stefanbethge/gseq/result"
)

// ─── Conditional steps ────────────────────────────────────────────────────────

// IfThen applies fn only when cond is true; otherwise the pipeline passes
// through unchanged. Useful for configuration-driven pipelines:
//
//	p.IfThen(dedupEnabled, func(t table.Table) table.Table { return t.Distinct() })
func (p Pipeline) IfThen(cond bool, fn func(table.Table) table.Table) Pipeline {
	if !cond {
		return p
	}
	return p.Then(fn)
}

// IfThenErr applies a fallible fn only when cond is true.
func (p Pipeline) IfThenErr(cond bool, fn func(table.Table) result.Result[table.Table, error]) Pipeline {
	if !cond {
		return p
	}
	return p.ThenErr(fn)
}

// ─── Error recovery ───────────────────────────────────────────────────────────

// RecoverWith replaces an error state with fallback, returning an ok Pipeline.
// If the pipeline is already ok, fallback is ignored.
//
//	p.ThenErr(enrichFromAPI).RecoverWith(baseTable)
func (p Pipeline) RecoverWith(fallback table.Table) Pipeline {
	if p.r.IsOk() {
		return p
	}
	return Pipeline{r: result.Ok[table.Table, error](fallback), trace: p.trace, errLog: p.errLog}
}

// OnError calls fn with the current error. fn may return a recovered Table
// (with nil error) or replace/wrap the error. If the pipeline is already ok,
// fn is not called.
//
//	p.OnError(func(err error) (table.Table, error) {
//	    log.Printf("enrichment failed: %v — using base table", err)
//	    return baseTable, nil
//	})
func (p Pipeline) OnError(fn func(error) (table.Table, error)) Pipeline {
	if p.r.IsOk() {
		return p
	}
	t, err := fn(p.r.UnwrapErr())
	if err != nil {
		return Pipeline{r: result.Err[table.Table, error](err), trace: p.trace, errLog: p.errLog}
	}
	return Pipeline{r: result.Ok[table.Table, error](t), trace: p.trace, errLog: p.errLog}
}

// MapErr transforms the error value, leaving ok pipelines unchanged. Use this
// to wrap errors with context before surfacing them:
//
//	p.MapErr(func(err error) error { return fmt.Errorf("processing sales.csv: %w", err) })
func (p Pipeline) MapErr(fn func(error) error) Pipeline {
	return Pipeline{r: result.MapErr(p.r, fn), trace: p.trace, errLog: p.errLog}
}
