// Package etl - ops.go provides package-level TableFunc factory functions.
//
// Use them instead of anonymous closures to reduce verbosity:
//
//	// verbose:
//	p.Then(func(t table.Table) table.Table { return t.Select("name", "city") })
//
//	// concise:
//	p.Then(etl.Select("name", "city"))
//
// Compose chains multiple TableFunc values into one:
//
//	normalise := etl.Compose(
//	    etl.DropEmpty("id"),
//	    etl.FillEmpty("region", "unknown"),
//	    etl.Map("name", strings.TrimSpace),
//	)
//	p.Then(normalise)
package etl

import "github.com/stefanbethge/gseq-table/table"

// Compose chains multiple TableFunc transformations into one.
// Functions are applied left to right.
func Compose(fns ...TableFunc) TableFunc {
	return func(t table.Table) table.Table {
		for _, fn := range fns {
			t = fn(t)
		}
		return t
	}
}

// Select returns a TableFunc that keeps only the named columns.
func Select(cols ...string) TableFunc {
	return func(t table.Table) table.Table { return t.Select(cols...) }
}

// Where returns a TableFunc that filters rows using fn.
func Where(fn func(table.Row) bool) TableFunc {
	return func(t table.Table) table.Table { return t.Where(fn) }
}

// Map returns a TableFunc that transforms every value in col using fn.
func Map(col string, fn func(string) string) TableFunc {
	return func(t table.Table) table.Table { return t.Map(col, fn) }
}

// AddCol returns a TableFunc that appends a derived column.
func AddCol(name string, fn func(table.Row) string) TableFunc {
	return func(t table.Table) table.Table { return t.AddCol(name, fn) }
}

// AddColFloat returns a TableFunc that appends a float-derived column.
func AddColFloat(name string, fn func(table.Row) float64) TableFunc {
	return func(t table.Table) table.Table { return t.AddColFloat(name, fn) }
}

// AddColInt returns a TableFunc that appends an int-derived column.
func AddColInt(name string, fn func(table.Row) int64) TableFunc {
	return func(t table.Table) table.Table { return t.AddColInt(name, fn) }
}

// Rename returns a TableFunc that renames column old to new.
func Rename(old, new string) TableFunc {
	return func(t table.Table) table.Table { return t.Rename(old, new) }
}

// RenameMany returns a TableFunc that applies multiple column renames.
func RenameMany(renames map[string]string) TableFunc {
	return func(t table.Table) table.Table { return t.RenameMany(renames) }
}

// Drop returns a TableFunc that removes the named columns.
func Drop(cols ...string) TableFunc {
	return func(t table.Table) table.Table { return t.Drop(cols...) }
}

// DropEmpty returns a TableFunc that removes rows where any of cols is empty.
func DropEmpty(cols ...string) TableFunc {
	return func(t table.Table) table.Table { return t.DropEmpty(cols...) }
}

// FillEmpty returns a TableFunc that replaces empty values in col with val.
func FillEmpty(col, val string) TableFunc {
	return func(t table.Table) table.Table { return t.FillEmpty(col, val) }
}

// FillForward returns a TableFunc that propagates the last non-empty value downward.
func FillForward(col string) TableFunc {
	return func(t table.Table) table.Table { return t.FillForward(col) }
}

// FillBackward returns a TableFunc that propagates the next non-empty value upward.
func FillBackward(col string) TableFunc {
	return func(t table.Table) table.Table { return t.FillBackward(col) }
}

// Sort returns a TableFunc that sorts by col (asc=true → ascending).
func Sort(col string, asc bool) TableFunc {
	return func(t table.Table) table.Table { return t.Sort(col, asc) }
}

// SortMulti returns a TableFunc that sorts by multiple keys.
func SortMulti(keys ...table.SortKey) TableFunc {
	return func(t table.Table) table.Table { return t.SortMulti(keys...) }
}

// Distinct returns a TableFunc that deduplicates rows based on cols.
func Distinct(cols ...string) TableFunc {
	return func(t table.Table) table.Table { return t.Distinct(cols...) }
}

// Head returns a TableFunc that keeps only the first n rows.
func Head(n int) TableFunc {
	return func(t table.Table) table.Table { return t.Head(n) }
}

// Tail returns a TableFunc that keeps only the last n rows.
func Tail(n int) TableFunc {
	return func(t table.Table) table.Table { return t.Tail(n) }
}

// Sample returns a TableFunc that keeps a random sample of n rows.
func Sample(n int) TableFunc {
	return func(t table.Table) table.Table { return t.Sample(n) }
}

// SampleFrac returns a TableFunc that keeps a random fraction of rows.
func SampleFrac(f float64) TableFunc {
	return func(t table.Table) table.Table { return t.SampleFrac(f) }
}

// Append returns a TableFunc that concatenates other to the bottom of the table.
func Append(other table.Table) TableFunc {
	return func(t table.Table) table.Table { return t.Append(other) }
}

// Join returns a TableFunc that inner-joins t with other on the given key columns.
func Join(other table.Table, leftCol, rightCol string) TableFunc {
	return func(t table.Table) table.Table { return t.Join(other, leftCol, rightCol) }
}

// LeftJoin returns a TableFunc that left-joins t with other.
func LeftJoin(other table.Table, leftCol, rightCol string) TableFunc {
	return func(t table.Table) table.Table { return t.LeftJoin(other, leftCol, rightCol) }
}

// Intersect returns a TableFunc that keeps rows present in both t and other (by cols).
func Intersect(other table.Table, cols ...string) TableFunc {
	return func(t table.Table) table.Table { return t.Intersect(other, cols...) }
}

// AddRowIndex returns a TableFunc that prepends a zero-based row-index column.
func AddRowIndex(name string) TableFunc {
	return func(t table.Table) table.Table { return t.AddRowIndex(name) }
}

// Explode returns a TableFunc that splits col on sep into multiple rows.
func Explode(col, sep string) TableFunc {
	return func(t table.Table) table.Table { return t.Explode(col, sep) }
}

// Transpose returns a TableFunc that pivots columns and rows.
func Transpose() TableFunc {
	return func(t table.Table) table.Table { return t.Transpose() }
}

// ValueCounts returns a TableFunc that tallies distinct values in col.
func ValueCounts(col string) TableFunc {
	return func(t table.Table) table.Table { return t.ValueCounts(col) }
}

// Melt returns a TableFunc that unpivots the table from wide to long format.
func Melt(idCols []string, varName, valName string) TableFunc {
	return func(t table.Table) table.Table { return t.Melt(idCols, varName, valName) }
}

// Pivot returns a TableFunc that pivots the table from long to wide format.
func Pivot(index, col, val string) TableFunc {
	return func(t table.Table) table.Table { return t.Pivot(index, col, val) }
}

// AddColSwitch returns a TableFunc that appends a column using switch-case logic.
func AddColSwitch(name string, cases []table.Case, else_ func(table.Row) string) TableFunc {
	return func(t table.Table) table.Table { return t.AddColSwitch(name, cases, else_) }
}

// TransformRows returns a TableFunc that applies fn to each row, replacing or adding columns.
func TransformRows(fn func(table.Row) map[string]string) TableFunc {
	return func(t table.Table) table.Table { return t.Transform(fn) }
}

// GroupByAgg returns a TableFunc that groups rows and applies aggregations.
func GroupByAgg(groupCols []string, aggs []table.AggDef) TableFunc {
	return func(t table.Table) table.Table { return t.GroupByAgg(groupCols, aggs) }
}

// RollingAgg returns a TableFunc that applies a rolling window aggregation.
func RollingAgg(outCol string, size int, agg table.Agg) TableFunc {
	return func(t table.Table) table.Table { return t.RollingAgg(outCol, size, agg) }
}

// Coalesce returns a TableFunc that fills name with the first non-empty value from cols.
func Coalesce(name string, cols ...string) TableFunc {
	return func(t table.Table) table.Table { return t.Coalesce(name, cols...) }
}

// Lookup returns a TableFunc that derives outCol by matching col against a lookup table.
func Lookup(col, outCol string, lookupTable table.Table, keyCol, valCol string) TableFunc {
	return func(t table.Table) table.Table { return t.Lookup(col, outCol, lookupTable, keyCol, valCol) }
}

// FormatCol returns a TableFunc that reformats numeric values in col with given precision.
func FormatCol(col string, precision int) TableFunc {
	return func(t table.Table) table.Table { return t.FormatCol(col, precision) }
}

// Bin returns a TableFunc that maps values in col into labelled bins.
func Bin(col, name string, bins []table.BinDef) TableFunc {
	return func(t table.Table) table.Table { return t.Bin(col, name, bins) }
}
