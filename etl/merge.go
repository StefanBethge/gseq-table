package etl

import (
	"sync"

	"github.com/stefanbethge/gseq-table/table"
	"github.com/stefanbethge/gseq/result"
)

// ─── Multi-source merge ───────────────────────────────────────────────────────

// ConcatPipelines merges multiple Pipelines vertically (row-wise). If any
// input pipeline is in an error state, the first such error is returned and
// no concatenation is performed.
//
//	p := etl.ConcatPipelines(
//	    etl.FromResult(csv.New().ReadFile("jan.csv")),
//	    etl.FromResult(csv.New().ReadFile("feb.csv")),
//	    etl.FromResult(csv.New().ReadFile("mar.csv")),
//	)
func ConcatPipelines(pipelines ...Pipeline) Pipeline {
	if len(pipelines) == 0 {
		return From(table.New(nil, nil))
	}
	tables := make([]table.Table, 0, len(pipelines))
	for _, p := range pipelines {
		if p.r.IsErr() {
			return Pipeline{r: p.r, errLog: p.errLog}
		}
		tables = append(tables, p.r.Unwrap())
	}
	return From(table.Concat(tables...))
}

// ConcatWith merges other onto the current pipeline vertically. Equivalent to
// ConcatPipelines(p, other).
func (p Pipeline) ConcatWith(other Pipeline) Pipeline {
	return ConcatPipelines(p, other)
}

// ─── Parallel branch execution ────────────────────────────────────────────────

// FanOut applies each fn to the current Table concurrently and returns the
// results in the same order. If the pipeline is in an error state, every
// returned pipeline carries that error without calling any fn.
//
//	branches := p.FanOut(
//	    func(t table.Table) table.Table { return t.Where(t.Eq("region", "EU")) },
//	    func(t table.Table) table.Table { return t.Where(t.Eq("region", "US")) },
//	)
func (p Pipeline) FanOut(fns ...func(table.Table) table.Table) []Pipeline {
	out := make([]Pipeline, len(fns))
	if p.r.IsErr() {
		for i := range out {
			out[i] = p
		}
		return out
	}
	t := p.r.Unwrap()
	var wg sync.WaitGroup
	for i, fn := range fns {
		i, fn := i, fn
		wg.Add(1)
		go func() {
			defer wg.Done()
			out[i] = Pipeline{r: result.Ok[table.Table, error](fn(t)), trace: p.trace, errLog: p.errLog}
		}()
	}
	wg.Wait()
	return out
}

