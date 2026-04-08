// Package etl provides a chainable Pipeline for transforming table.Table
// values with automatic error propagation.
//
// A Pipeline wraps a result.Result[table.Table, error]. Every operation
// returns a new Pipeline; if the wrapped result is already an error, the
// operation is skipped and the error is forwarded — no explicit error
// checking is needed mid-chain.
//
// # Starting a pipeline
//
//	// From a table directly
//	p := etl.From(t)
//
//	// From a CSV reader (or any source returning a Result)
//	p := etl.FromResult(csv.New().ReadFile("sales.csv"))
//
// # Chaining operations
//
//	result := etl.FromResult(csv.New().ReadFile("sales.csv")).
//	    DropEmpty("revenue").
//	    FillEmpty("region", "unknown").
//	    Where(func(r table.Row) bool {
//	        return r.Get("region").UnwrapOr("") != "test"
//	    }).
//	    Map("revenue", func(v string) string { return "$" + v }).
//	    SortMulti(table.Desc("revenue"), table.Asc("region")).
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
	"github.com/stefanbethge/gseq-table/table"
	"github.com/stefanbethge/gseq/result"
)

// Pipeline wraps a Result[Table, error] and lets you chain transformations.
// If any step produces an error, all subsequent steps are skipped and the
// error is propagated to the final Result.
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

// Select keeps only the named columns. See table.Table.Select.
func (p Pipeline) Select(cols ...string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.Select(cols...)
	})}
}

// Where keeps only rows for which fn returns true. See table.Table.Where.
func (p Pipeline) Where(fn func(table.Row) bool) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.Where(fn)
	})}
}

// Map transforms every value in col using fn. See table.Table.Map.
func (p Pipeline) Map(col string, fn func(string) string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.Map(col, fn)
	})}
}

// AddCol appends a computed column. See table.Table.AddCol.
func (p Pipeline) AddCol(name string, fn func(table.Row) string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.AddCol(name, fn)
	})}
}

// Rename renames a column. See table.Table.Rename.
func (p Pipeline) Rename(old, new string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.Rename(old, new)
	})}
}

// Sort sorts by a single column. See table.Table.Sort.
func (p Pipeline) Sort(col string, asc bool) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.Sort(col, asc)
	})}
}

// SortMulti sorts by multiple columns in priority order. See table.Table.SortMulti.
func (p Pipeline) SortMulti(keys ...table.SortKey) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.SortMulti(keys...)
	})}
}

// Append concatenates another table. See table.Table.Append.
func (p Pipeline) Append(other table.Table) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.Append(other)
	})}
}

// Join performs an inner join. See table.Table.Join.
func (p Pipeline) Join(other table.Table, leftCol, rightCol string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.Join(other, leftCol, rightCol)
	})}
}

// LeftJoin performs a left join. See table.Table.LeftJoin.
func (p Pipeline) LeftJoin(other table.Table, leftCol, rightCol string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.LeftJoin(other, leftCol, rightCol)
	})}
}

// Head keeps the first n rows. See table.Table.Head.
func (p Pipeline) Head(n int) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.Head(n) })}
}

// Tail keeps the last n rows. See table.Table.Tail.
func (p Pipeline) Tail(n int) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.Tail(n) })}
}

// Drop removes named columns. See table.Table.Drop.
func (p Pipeline) Drop(cols ...string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.Drop(cols...) })}
}

// DropEmpty removes rows with empty values. See table.Table.DropEmpty.
func (p Pipeline) DropEmpty(cols ...string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.DropEmpty(cols...) })}
}

// FillEmpty replaces empty values in col with val. See table.Table.FillEmpty.
func (p Pipeline) FillEmpty(col, val string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.FillEmpty(col, val) })}
}

// Distinct removes duplicate rows. See table.Table.Distinct.
func (p Pipeline) Distinct(cols ...string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.Distinct(cols...) })}
}

// Transform applies fn to every row as a partial update. See table.Table.Transform.
func (p Pipeline) Transform(fn func(table.Row) map[string]string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.Transform(fn) })}
}

// ValueCounts returns a frequency table for col. See table.Table.ValueCounts.
func (p Pipeline) ValueCounts(col string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.ValueCounts(col) })}
}

// Melt converts wide format to long format. See table.Table.Melt.
func (p Pipeline) Melt(idCols []string, varName, valName string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.Melt(idCols, varName, valName)
	})}
}

// Pivot converts long format to wide format. See table.Table.Pivot.
func (p Pipeline) Pivot(index, col, val string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.Pivot(index, col, val)
	})}
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
