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
	"github.com/stefanbethge/gseq-table/schema"
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

// GroupByAgg groups by groupCols and applies aggregations. See table.Table.GroupByAgg.
func (p Pipeline) GroupByAgg(groupCols []string, aggs []table.AggDef) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.GroupByAgg(groupCols, aggs)
	})}
}

// AddColSwitch appends a column via conditional cases. See table.Table.AddColSwitch.
func (p Pipeline) AddColSwitch(name string, cases []table.Case, else_ func(table.Row) string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.AddColSwitch(name, cases, else_)
	})}
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

// ApplySchema normalises cell values according to s (lenient: empty cells pass
// through). See schema.Schema.Apply.
func (p Pipeline) ApplySchema(s schema.Schema) Pipeline {
	return Pipeline{r: result.FlatMap(p.r, func(t table.Table) result.Result[table.Table, error] {
		return s.Apply(t)
	})}
}

// ApplySchemaStrict normalises cell values according to s and returns an error
// for any empty cell in a non-string column. See schema.Schema.ApplyStrict.
func (p Pipeline) ApplySchemaStrict(s schema.Schema) Pipeline {
	return Pipeline{r: result.FlatMap(p.r, func(t table.Table) result.Result[table.Table, error] {
		return s.ApplyStrict(t)
	})}
}

// RenameMany renames multiple columns at once. See table.Table.RenameMany.
func (p Pipeline) RenameMany(renames map[string]string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.RenameMany(renames) })}
}

// AddRowIndex prepends a row-number column. See table.Table.AddRowIndex.
func (p Pipeline) AddRowIndex(name string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.AddRowIndex(name) })}
}

// Explode splits col on sep into multiple rows. See table.Table.Explode.
func (p Pipeline) Explode(col, sep string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.Explode(col, sep) })}
}

// Transpose pivots rows and columns. See table.Table.Transpose.
func (p Pipeline) Transpose() Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.Transpose() })}
}

// RightJoin performs a right join. See table.Table.RightJoin.
func (p Pipeline) RightJoin(other table.Table, leftCol, rightCol string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.RightJoin(other, leftCol, rightCol)
	})}
}

// OuterJoin performs a full outer join. See table.Table.OuterJoin.
func (p Pipeline) OuterJoin(other table.Table, leftCol, rightCol string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.OuterJoin(other, leftCol, rightCol)
	})}
}

// AntiJoin returns rows without a match in other. See table.Table.AntiJoin.
func (p Pipeline) AntiJoin(other table.Table, leftCol, rightCol string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.AntiJoin(other, leftCol, rightCol)
	})}
}

// FillForward fills empty cells with the previous non-empty value. See table.Table.FillForward.
func (p Pipeline) FillForward(col string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.FillForward(col) })}
}

// FillBackward fills empty cells with the next non-empty value. See table.Table.FillBackward.
func (p Pipeline) FillBackward(col string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.FillBackward(col) })}
}

// Sample returns n random rows. See table.Table.Sample.
func (p Pipeline) Sample(n int) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.Sample(n) })}
}

// SampleFrac returns a random fraction of rows. See table.Table.SampleFrac.
func (p Pipeline) SampleFrac(f float64) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.SampleFrac(f) })}
}

// AddColFloat appends a float64-typed computed column. See table.Table.AddColFloat.
func (p Pipeline) AddColFloat(name string, fn func(table.Row) float64) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.AddColFloat(name, fn) })}
}

// AddColInt appends an int64-typed computed column. See table.Table.AddColInt.
func (p Pipeline) AddColInt(name string, fn func(table.Row) int64) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.AddColInt(name, fn) })}
}

// RollingAgg computes a sliding-window aggregation. See table.Table.RollingAgg.
func (p Pipeline) RollingAgg(outCol string, size int, agg table.Agg) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.RollingAgg(outCol, size, agg)
	})}
}

// Concat stacks additional tables onto the pipeline result. See table.Concat.
func (p Pipeline) Concat(others ...table.Table) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return table.Concat(append([]table.Table{t}, others...)...)
	})}
}

// AssertColumns returns an error pipeline if any required column is missing.
// See table.Table.AssertColumns.
func (p Pipeline) AssertColumns(cols ...string) Pipeline {
	return Pipeline{r: result.FlatMap(p.r, func(t table.Table) result.Result[table.Table, error] {
		if err := t.AssertColumns(cols...); err != nil {
			return result.Err[table.Table, error](err)
		}
		return result.Ok[table.Table, error](t)
	})}
}

// AssertNoEmpty returns an error pipeline if any cell in the given columns is
// empty. See table.Table.AssertNoEmpty.
func (p Pipeline) AssertNoEmpty(cols ...string) Pipeline {
	return Pipeline{r: result.FlatMap(p.r, func(t table.Table) result.Result[table.Table, error] {
		if err := t.AssertNoEmpty(cols...); err != nil {
			return result.Err[table.Table, error](err)
		}
		return result.Ok[table.Table, error](t)
	})}
}

// Peek calls fn with the current Table without modifying it. Useful for
// logging or inspecting intermediate state in a chain.
//
//	p.Peek(func(t table.Table) { log.Printf("rows: %d", t.Len()) }).Select("name")
func (p Pipeline) Peek(fn func(table.Table)) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		fn(t)
		return t
	})}
}

// ForEach calls fn for each row (side-effects only). See table.Table.ForEach.
func (p Pipeline) ForEach(fn func(int, table.Row)) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		t.ForEach(fn)
		return t
	})}
}

// Coalesce adds a column with the first non-empty value from cols. See table.Table.Coalesce.
func (p Pipeline) Coalesce(name string, cols ...string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.Coalesce(name, cols...) })}
}

// Lookup adds a column via a lookup table. See table.Table.Lookup.
func (p Pipeline) Lookup(col, outCol string, lookupTable table.Table, keyCol, valCol string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table {
		return t.Lookup(col, outCol, lookupTable, keyCol, valCol)
	})}
}

// FormatCol rounds float values in col to precision decimal places. See table.Table.FormatCol.
func (p Pipeline) FormatCol(col string, precision int) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.FormatCol(col, precision) })}
}

// Lag adds a lagged column. See table.Table.Lag.
func (p Pipeline) Lag(col, outCol string, n int) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.Lag(col, outCol, n) })}
}

// Lead adds a lead column. See table.Table.Lead.
func (p Pipeline) Lead(col, outCol string, n int) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.Lead(col, outCol, n) })}
}

// CumSum adds a cumulative-sum column. See table.Table.CumSum.
func (p Pipeline) CumSum(col, outCol string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.CumSum(col, outCol) })}
}

// Rank adds a dense-rank column. See table.Table.Rank.
func (p Pipeline) Rank(col, outCol string, asc bool) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.Rank(col, outCol, asc) })}
}

// Bin adds a bucketing column. See table.Table.Bin.
func (p Pipeline) Bin(col, name string, bins []table.BinDef) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.Bin(col, name, bins) })}
}

// Intersect keeps rows that also appear in other. See table.Table.Intersect.
func (p Pipeline) Intersect(other table.Table, cols ...string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.Intersect(other, cols...) })}
}

// TryTransform applies a fallible row transformation. See table.Table.TryTransform.
func (p Pipeline) TryTransform(fn func(table.Row) (map[string]string, error)) Pipeline {
	return Pipeline{r: result.FlatMap(p.r, func(t table.Table) result.Result[table.Table, error] {
		return t.TryTransform(fn)
	})}
}

// TryMap applies a fallible single-column transformation. See table.Table.TryMap.
func (p Pipeline) TryMap(col string, fn func(string) (string, error)) Pipeline {
	return Pipeline{r: result.FlatMap(p.r, func(t table.Table) result.Result[table.Table, error] {
		return t.TryMap(col, fn)
	})}
}

// TransformParallel runs fn concurrently over all rows. See table.Table.TransformParallel.
func (p Pipeline) TransformParallel(fn func(table.Row) map[string]string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.TransformParallel(fn) })}
}

// MapParallel runs fn concurrently over col. See table.Table.MapParallel.
func (p Pipeline) MapParallel(col string, fn func(string) string) Pipeline {
	return Pipeline{r: result.Map(p.r, func(t table.Table) table.Table { return t.MapParallel(col, fn) })}
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
