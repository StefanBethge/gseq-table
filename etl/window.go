// Package etl - window.go provides TableFunc and MutableFunc factories for
// partitioned window functions.
//
//	p.Then(etl.PartitionBy("customer").OrderBy(table.Asc("date")).CumSum("revenue", "cum"))
//	mp.Then(etl.Mut.PartitionBy("customer").Lag("revenue", "prev", 1))
package etl

import "github.com/stefanbethge/gseq-table/table"

// WindowSpec describes a partitioning (and optional within-partition order)
// for window functions in a Pipeline. Obtain one with PartitionBy.
type WindowSpec struct {
	cols  []string
	order []table.SortKey
}

// PartitionBy returns a WindowSpec whose window functions run independently
// per distinct combination of cols. See table.Table.PartitionBy.
func PartitionBy(cols ...string) WindowSpec {
	return WindowSpec{cols: append([]string(nil), cols...)}
}

// OrderBy returns a copy of w that sorts rows within each partition by keys
// before the window function is applied. See table.Window.OrderBy.
func (w WindowSpec) OrderBy(keys ...table.SortKey) WindowSpec {
	w.order = append([]table.SortKey(nil), keys...)
	return w
}

func (w WindowSpec) window(t table.Table) table.Window {
	win := t.PartitionBy(w.cols...)
	if len(w.order) > 0 {
		win = win.OrderBy(w.order...)
	}
	return win
}

// Lag returns a TableFunc that adds a partitioned lag column.
func (w WindowSpec) Lag(col, outCol string, n int) TableFunc {
	return func(t table.Table) table.Table { return w.window(t).Lag(col, outCol, n) }
}

// Lead returns a TableFunc that adds a partitioned lead column.
func (w WindowSpec) Lead(col, outCol string, n int) TableFunc {
	return func(t table.Table) table.Table { return w.window(t).Lead(col, outCol, n) }
}

// CumSum returns a TableFunc that adds a partitioned running sum column.
func (w WindowSpec) CumSum(col, outCol string) TableFunc {
	return func(t table.Table) table.Table { return w.window(t).CumSum(col, outCol) }
}

// Rank returns a TableFunc that adds a partitioned dense rank column.
func (w WindowSpec) Rank(col, outCol string, asc bool) TableFunc {
	return func(t table.Table) table.Table { return w.window(t).Rank(col, outCol, asc) }
}

// RollingAgg returns a TableFunc that adds a partitioned rolling aggregation.
func (w WindowSpec) RollingAgg(outCol string, size int, agg table.Agg) TableFunc {
	return func(t table.Table) table.Table { return w.window(t).RollingAgg(outCol, size, agg) }
}

// MutWindowSpec is the MutablePipeline counterpart of WindowSpec.
// Obtain one with Mut.PartitionBy.
type MutWindowSpec struct {
	cols  []string
	order []table.SortKey
}

func (mutableOps) PartitionBy(cols ...string) MutWindowSpec {
	return MutWindowSpec{cols: append([]string(nil), cols...)}
}

// OrderBy returns a copy of w that sorts rows within each partition by keys.
func (w MutWindowSpec) OrderBy(keys ...table.SortKey) MutWindowSpec {
	w.order = append([]table.SortKey(nil), keys...)
	return w
}

func (w MutWindowSpec) window(m *table.MutableTable) *table.MutableWindow {
	win := m.PartitionBy(w.cols...)
	if len(w.order) > 0 {
		win = win.OrderBy(w.order...)
	}
	return win
}

func (w MutWindowSpec) Lag(col, outCol string, n int) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return w.window(m).Lag(col, outCol, n) }
}

func (w MutWindowSpec) Lead(col, outCol string, n int) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return w.window(m).Lead(col, outCol, n) }
}

func (w MutWindowSpec) CumSum(col, outCol string) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return w.window(m).CumSum(col, outCol) }
}

func (w MutWindowSpec) Rank(col, outCol string, asc bool) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable { return w.window(m).Rank(col, outCol, asc) }
}

func (w MutWindowSpec) RollingAgg(outCol string, size int, agg table.Agg) MutableFunc {
	return func(m *table.MutableTable) *table.MutableTable {
		return w.window(m).RollingAgg(outCol, size, agg)
	}
}
