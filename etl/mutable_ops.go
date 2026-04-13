// Package etl - mutable_ops.go provides MutableFunc factory methods under the Mut namespace.
//
// Use Mut.Method(...) instead of anonymous closures to reduce verbosity:
//
//	// verbose:
//	mp.Then(func(m *table.MutableTable) *table.MutableTable { return m.Map("city", strings.ToUpper) })
//
//	// concise:
//	mp.Then(etl.Mut.Map("city", strings.ToUpper))
//
// MutCompose chains multiple MutableFunc values into one:
//
//	normalise := etl.MutCompose(
//	    etl.Mut.DropEmpty("id"),
//	    etl.Mut.FillEmpty("region", "unknown"),
//	    etl.Mut.Map("name", strings.TrimSpace),
//	)
//	mp.Then(normalise)
package etl

import "github.com/stefanbethge/gseq-table/table"

// mutableOps groups MutableFunc factory methods under a single namespace.
type mutableOps struct{}

// Mut is the package-level handle for all MutableFunc constructors.
var Mut mutableOps

// MutCompose chains multiple MutableFunc transformations into one.
// Functions are applied left to right.
func MutCompose(fns ...MutableFunc) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable {
		for _, fn := range fns {
			m = fn(m)
		}
		return m
	}
}

func (mutableOps) Select(cols ...string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.Select(cols...) }
}

func (mutableOps) Where(fn func(table.Row) bool) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.Where(fn) }
}

func (mutableOps) Map(col string, fn func(string) string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.Map(col, fn) }
}

func (mutableOps) FillEmpty(col, val string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.FillEmpty(col, val) }
}

func (mutableOps) AddCol(name string, fn func(table.Row) string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.AddCol(name, fn) }
}

func (mutableOps) AddColFloat(name string, fn func(table.Row) float64) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.AddColFloat(name, fn) }
}

func (mutableOps) AddColInt(name string, fn func(table.Row) int64) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.AddColInt(name, fn) }
}

func (mutableOps) Drop(cols ...string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.Drop(cols...) }
}

func (mutableOps) DropEmpty(cols ...string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.DropEmpty(cols...) }
}

func (mutableOps) Rename(old, new string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.Rename(old, new) }
}

func (mutableOps) RenameMany(renames map[string]string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.RenameMany(renames) }
}

func (mutableOps) Sort(col string, asc bool) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.Sort(col, asc) }
}

func (mutableOps) SortMulti(keys ...table.SortKey) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.SortMulti(keys...) }
}

func (mutableOps) Head(n int) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.Head(n) }
}

func (mutableOps) Tail(n int) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.Tail(n) }
}

func (mutableOps) Distinct(cols ...string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.Distinct(cols...) }
}

func (mutableOps) AddColSwitch(name string, cases []table.Case, else_ func(table.Row) string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.AddColSwitch(name, cases, else_) }
}

func (mutableOps) Transform(fn func(table.Row) map[string]string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.Transform(fn) }
}

func (mutableOps) TransformParallel(fn func(table.Row) map[string]string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.TransformParallel(fn) }
}

func (mutableOps) AddRowIndex(name string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.AddRowIndex(name) }
}

func (mutableOps) Explode(col, sep string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.Explode(col, sep) }
}

func (mutableOps) Transpose() MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.Transpose() }
}

func (mutableOps) FillForward(col string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.FillForward(col) }
}

func (mutableOps) FillBackward(col string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.FillBackward(col) }
}

func (mutableOps) Sample(n int) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.Sample(n) }
}

func (mutableOps) SampleFrac(f float64) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.SampleFrac(f) }
}

func (mutableOps) Coalesce(name string, cols ...string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.Coalesce(name, cols...) }
}

func (mutableOps) Lookup(col, outCol string, lookupTable table.Table, keyCol, valCol string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable {
		return m.Lookup(col, outCol, lookupTable, keyCol, valCol)
	}
}

func (mutableOps) FormatCol(col string, precision int) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.FormatCol(col, precision) }
}

func (mutableOps) Intersect(other table.Table, cols ...string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.Intersect(other, cols...) }
}

func (mutableOps) Bin(col, name string, bins []table.BinDef) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.Bin(col, name, bins) }
}

func (mutableOps) Join(other table.Table, leftCol, rightCol string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.Join(other, leftCol, rightCol) }
}

func (mutableOps) LeftJoin(other table.Table, leftCol, rightCol string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.LeftJoin(other, leftCol, rightCol) }
}

func (mutableOps) RightJoin(other table.Table, leftCol, rightCol string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.RightJoin(other, leftCol, rightCol) }
}

func (mutableOps) OuterJoin(other table.Table, leftCol, rightCol string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.OuterJoin(other, leftCol, rightCol) }
}

func (mutableOps) AntiJoin(other table.Table, leftCol, rightCol string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.AntiJoin(other, leftCol, rightCol) }
}

func (mutableOps) ValueCounts(col string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.ValueCounts(col) }
}

func (mutableOps) Melt(idCols []string, varName, valName string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.Melt(idCols, varName, valName) }
}

func (mutableOps) Pivot(index, col, val string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.Pivot(index, col, val) }
}

func (mutableOps) Append(other table.Table) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.Append(other) }
}

func (mutableOps) AppendMutable(other *table.MutableTable) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return m.AppendMutable(other) }
}
