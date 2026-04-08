package etl

import (
	"github.com/stefanbethge/gseq-table/table"
	"github.com/stefanbethge/gseq/result"
)

// Pipeline wraps a Result[Table, error] and lets you chain transformations.
// If any step produces an error, all subsequent steps are skipped.
type Pipeline struct {
	r result.Result[table.Table, error]
}

// From wraps an existing Table in a Pipeline.
func From(t table.Table) Pipeline {
	return Pipeline{r: result.Ok[table.Table, error](t)}
}

// FromResult wraps a Result (e.g. from a CSV reader) in a Pipeline.
func FromResult(r result.Result[table.Table, error]) Pipeline {
	return Pipeline{r: r}
}

func (p Pipeline) Select(cols ...string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.Select(cols...)
	})}
}

func (p Pipeline) Where(fn func(table.Row) bool) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.Where(fn)
	})}
}

// Map transforms every value in col using fn.
func (p Pipeline) Map(col string, fn func(string) string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.Map(col, fn)
	})}
}

// AddCol appends a computed column.
func (p Pipeline) AddCol(name string, fn func(table.Row) string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.AddCol(name, fn)
	})}
}

func (p Pipeline) Rename(old, new string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.Rename(old, new)
	})}
}

// Sort sorts by col. asc=true for ascending.
func (p Pipeline) Sort(col string, asc bool) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.Sort(col, asc)
	})}
}

func (p Pipeline) Append(other table.Table) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.Append(other)
	})}
}

func (p Pipeline) Join(other table.Table, leftCol, rightCol string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.Join(other, leftCol, rightCol)
	})}
}

func (p Pipeline) LeftJoin(other table.Table, leftCol, rightCol string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.LeftJoin(other, leftCol, rightCol)
	})}
}

func (p Pipeline) Head(n int) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.Head(n) })}
}

func (p Pipeline) Tail(n int) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.Tail(n) })}
}

func (p Pipeline) Drop(cols ...string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.Drop(cols...) })}
}

func (p Pipeline) DropEmpty(cols ...string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.DropEmpty(cols...) })}
}

func (p Pipeline) FillEmpty(col, val string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.FillEmpty(col, val) })}
}

func (p Pipeline) Distinct(cols ...string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.Distinct(cols...) })}
}

func (p Pipeline) Transform(fn func(table.Row) map[string]string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.Transform(fn) })}
}

func (p Pipeline) SortMulti(keys ...table.SortKey) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.SortMulti(keys...) })}
}

func (p Pipeline) ValueCounts(col string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.ValueCounts(col) })}
}

func (p Pipeline) Melt(idCols []string, varName, valName string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.Melt(idCols, varName, valName)
	})}
}

func (p Pipeline) Pivot(index, col, val string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.Pivot(index, col, val)
	})}
}

// GroupBy is a terminal operation that splits the pipeline result into sub-pipelines.
// If the pipeline is in an error state, the error is forwarded to every group.
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