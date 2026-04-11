// Package etl provides a chainable Pipeline for transforming table.Table
// values with automatic error propagation.
//
// A Pipeline wraps a result.Result[table.Table, error]. Every call to Then or
// ThenErr returns a new Pipeline; if the wrapped result is already an error,
// the operation is skipped and the error is forwarded — no explicit error
// checking is needed mid-chain.
//
// # Starting a pipeline
//
//	p := etl.From(t)
//	p := etl.FromResult(csv.New().ReadFile("sales.csv"))
//
// # Chaining operations
//
//	result := etl.FromResult(csv.New().ReadFile("sales.csv")).
//	    Then(func(t table.Table) table.Table { return t.DropEmpty("revenue") }).
//	    Then(func(t table.Table) table.Table { return t.FillEmpty("region", "unknown") }).
//	    ThenErr(schema.Infer(refTable).Apply).
//	    Result()
//
// # Terminal operations
//
//	p.Result()           // result.Result[table.Table, error]
//	p.Unwrap()           // table.Table  (panics on error)
//	p.IsOk() / p.IsErr() // bool
//	p.GroupBy("region")  // map[string]Pipeline
package etl

import (
	"github.com/stefanbethge/gseq-table/schema"
	"github.com/stefanbethge/gseq-table/table"
	"github.com/stefanbethge/gseq/result"
)

// TableFunc is a function that transforms an immutable Table.
// Use it to name and reuse transformation functions:
//
//	var dropTest TableFunc = func(t table.Table) table.Table {
//	    return t.Where(func(r table.Row) bool { return r.Get("env").UnwrapOr("") != "test" })
//	}
//	p.Then(dropTest)
type TableFunc = func(table.Table) table.Table

// Freezable is implemented by both table.Table and *table.MutableTable.
// It allows constructors like From to accept either type.
type Freezable interface {
	Freeze() table.Table
}

// Pipeline wraps a Result[Table, error] and lets you chain transformations.
// If any step produces an error, all subsequent steps are skipped and the
// error is propagated to the final Result.
//
// The zero value is not useful; use From or FromResult to construct a Pipeline.
type Pipeline struct {
	r     result.Result[table.Table, error]
	trace *[]StepRecord // nil unless WithTracing was called
}

// From wraps a Table (or any Freezable value, e.g. *table.MutableTable) in a
// Pipeline. If t is already a table.Table, it is used directly; otherwise
// Freeze() is called to obtain an immutable snapshot.
//
//	etl.From(myTable)    // from an immutable Table
//	etl.From(myMutable)  // from a *MutableTable — calls Freeze() implicitly
func From[T Freezable](t T) Pipeline {
	return Pipeline{r: result.Ok[table.Table, error](t.Freeze())}
}

// FromResult wraps a Result (e.g. from a CSV or Excel reader) in a Pipeline.
func FromResult(r result.Result[table.Table, error]) Pipeline {
	return Pipeline{r: r}
}

// ─── Core combinators ─────────────────────────────────────────────────────────

// Then applies an infallible transformation to the Table.
// If the pipeline is already in an error state, fn is skipped.
//
//	p.Then(func(t table.Table) table.Table { return t.Where(t.Eq("active", "true")) })
func (p Pipeline) Then(fn func(table.Table) table.Table) Pipeline {
	return Pipeline{r: result.Map(p.r, fn), trace: p.trace}
}

// ThenErr applies a fallible transformation to the Table.
// If fn returns an Err result, all subsequent operations are skipped.
// If the pipeline is already in an error state, fn is skipped.
//
//	p.ThenErr(func(t table.Table) result.Result[table.Table, error] {
//	    return t.TryMap("price", strconv.ParseFloat)
//	})
func (p Pipeline) ThenErr(fn func(table.Table) result.Result[table.Table, error]) Pipeline {
	return Pipeline{r: result.FlatMap(p.r, fn), trace: p.trace}
}

// ─── Schema integration ───────────────────────────────────────────────────────

// ApplySchema normalises cell values according to s (lenient: empty cells pass
// through). See schema.Schema.Apply.
func (p Pipeline) ApplySchema(s schema.Schema) Pipeline {
	return p.ThenErr(s.Apply)
}

// ApplySchemaStrict normalises cell values according to s and returns an error
// for any empty cell in a non-string column. See schema.Schema.ApplyStrict.
func (p Pipeline) ApplySchemaStrict(s schema.Schema) Pipeline {
	return p.ThenErr(s.ApplyStrict)
}

// ─── Validation ───────────────────────────────────────────────────────────────

// AssertColumns returns an error pipeline if any required column is missing.
// See table.Table.AssertColumns.
func (p Pipeline) AssertColumns(cols ...string) Pipeline {
	return p.ThenErr(func(t table.Table) result.Result[table.Table, error] {
		if err := t.AssertColumns(cols...); err != nil {
			return result.Err[table.Table, error](err)
		}
		return result.Ok[table.Table, error](t)
	})
}

// AssertNoEmpty returns an error pipeline if any cell in the given columns is
// empty. See table.Table.AssertNoEmpty.
func (p Pipeline) AssertNoEmpty(cols ...string) Pipeline {
	return p.ThenErr(func(t table.Table) result.Result[table.Table, error] {
		if err := t.AssertNoEmpty(cols...); err != nil {
			return result.Err[table.Table, error](err)
		}
		return result.Ok[table.Table, error](t)
	})
}

// ─── Inspection ───────────────────────────────────────────────────────────────

// Peek calls fn with the current Table without modifying it. Useful for
// logging or inspecting intermediate state in a chain.
//
//	p.Peek(func(t table.Table) { log.Printf("rows: %d", t.Len()) }).Then(...)
func (p Pipeline) Peek(fn func(table.Table)) Pipeline {
	return p.Then(func(t table.Table) table.Table {
		fn(t)
		return t
	})
}

// ─── Terminal operations ──────────────────────────────────────────────────────

// Result returns the underlying Result[Table, error].
func (p Pipeline) Result() result.Result[table.Table, error] {
	return p.r
}

// Unwrap returns the Table or panics if the pipeline is in an error state.
func (p Pipeline) Unwrap() table.Table {
	return p.r.Unwrap()
}

// IsOk reports whether the pipeline holds a valid Table.
func (p Pipeline) IsOk() bool {
	return p.r.IsOk()
}

// IsErr reports whether the pipeline is in an error state.
func (p Pipeline) IsErr() bool {
	return p.r.IsErr()
}

// GroupBy is a terminal operation that splits the pipeline into one
// sub-pipeline per distinct value of col. If the pipeline is in an error
// state, the error is forwarded under the empty key "".
//
//	for region, p := range pipeline.GroupBy("region") {
//	    fmt.Println(region, p.Unwrap().Len())
//	}
func (p Pipeline) GroupBy(col string) map[string]Pipeline {
	if p.r.IsErr() {
		return map[string]Pipeline{"": p}
	}
	groups := p.r.Unwrap().GroupBy(col)
	out := make(map[string]Pipeline, len(groups))
	for k, t := range groups {
		out[k] = From(t)
	}
	return out
}

// Partition is a terminal that splits the pipeline into two sub-pipelines.
// If the pipeline is in an error state, both sub-pipelines carry the error.
func (p Pipeline) Partition(fn func(table.Row) bool) (matched, rest Pipeline) {
	if p.r.IsErr() {
		return p, p
	}
	m, r := p.r.Unwrap().Partition(fn)
	return From(m), From(r)
}

// Chunk is a terminal that splits the pipeline result into n-row batches.
// If the pipeline is in an error state, a single error pipeline is returned.
func (p Pipeline) Chunk(n int) []Pipeline {
	if p.r.IsErr() {
		return []Pipeline{p}
	}
	chunks := p.r.Unwrap().Chunk(n)
	out := make([]Pipeline, len(chunks))
	for i, c := range chunks {
		out[i] = From(c)
	}
	return out
}
