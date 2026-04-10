package table

import (
	"strconv"
	"strings"

	"github.com/stefanbethge/gseq/slice"
)

// Agg defines how a group of rows is reduced to a single string value.
// Use the provided constructors (Sum, Mean, Count, StringJoin, First, Last)
// rather than implementing this interface directly.
type Agg interface {
	reduce(group aggRows) string
}

type aggRows interface {
	ColIndex(col string) int
	Len() int
	ValueAt(row, col int) string
}

type tableAggRows struct {
	headerIdx map[string]int
	rows      slice.Slice[Row]
}

func (g tableAggRows) ColIndex(col string) int {
	if idx, ok := g.headerIdx[col]; ok {
		return idx
	}
	return -1
}

func (g tableAggRows) Len() int { return len(g.rows) }

func (g tableAggRows) ValueAt(row, col int) string {
	if row < 0 || row >= len(g.rows) || col < 0 {
		return ""
	}
	values := g.rows[row].values
	if col >= len(values) {
		return ""
	}
	return values[col]
}

type mutableAggRows struct {
	headerIdx map[string]int
	rows      [][]string
}

func (g mutableAggRows) ColIndex(col string) int {
	if idx, ok := g.headerIdx[col]; ok {
		return idx
	}
	return -1
}

func (g mutableAggRows) Len() int { return len(g.rows) }

func (g mutableAggRows) ValueAt(row, col int) string {
	if row < 0 || row >= len(g.rows) || col < 0 {
		return ""
	}
	values := g.rows[row]
	if col >= len(values) {
		return ""
	}
	return values[col]
}

// AggDef pairs an output column name with an aggregation function.
// Used with GroupByAgg.
type AggDef struct {
	Col string
	Agg Agg
}

// Sum sums all parseable float values in col.
//
//	table.Sum("revenue")
func Sum(col string) Agg { return sumAgg{col} }

// Mean returns the arithmetic mean of all parseable float values in col.
//
//	table.Mean("age")
func Mean(col string) Agg { return meanAgg{col} }

// Count counts non-empty values in col.
//
//	table.Count("status")
func Count(col string) Agg { return countAgg{col} }

// StringJoin concatenates non-empty values in col with sep.
//
//	table.StringJoin("id", ", ")
func StringJoin(col, sep string) Agg { return stringJoinAgg{col, sep} }

// First returns the value of col from the first row of the group.
//
//	table.First("created_at")
func First(col string) Agg { return firstAgg{col} }

// Last returns the value of col from the last row of the group.
//
//	table.Last("updated_at")
func Last(col string) Agg { return lastAgg{col} }

// GroupByAgg groups the table by groupCols and applies each aggregation to
// every group. The result contains one row per distinct key combination, with
// groupCols first followed by the aggregated output columns in the order given.
//
//	t.GroupByAgg(
//	    []string{"region", "product"},
//	    []table.AggDef{
//	        {Col: "total",  Agg: table.Sum("revenue")},
//	        {Col: "avg",    Agg: table.Mean("revenue")},
//	        {Col: "count",  Agg: table.Count("revenue")},
//	        {Col: "labels", Agg: table.StringJoin("label", ", ")},
//	    },
//	)
func (t Table) GroupByAgg(groupCols []string, aggs []AggDef) Table {
	type groupEntry struct {
		keyVals []string
		rows    slice.Slice[Row]
	}

	// pre-compute group column indices once
	groupIdx := make([]int, len(groupCols))
	for i, col := range groupCols {
		idx, ok := t.headerIdx[col]
		if !ok {
			return Table{}
		}
		groupIdx[i] = idx
	}

	index := make(map[string]*groupEntry)
	keyOrder := make([]string, 0, len(t.Rows))

	for _, row := range t.Rows {
		parts := make([]string, len(groupCols))
		for i, idx := range groupIdx {
			if idx < len(row.values) {
				parts[i] = row.values[idx]
			}
		}
		key := strings.Join(parts, "\x00")
		if e, ok := index[key]; ok {
			e.rows = append(e.rows, row)
		} else {
			// copy keyVals so the slice is stable
			kv := make([]string, len(parts))
			copy(kv, parts)
			index[key] = &groupEntry{keyVals: kv, rows: slice.Slice[Row]{row}}
			keyOrder = append(keyOrder, key)
		}
	}

	newHeaders := make(slice.Slice[string], 0, len(groupCols)+len(aggs))
	newHeaders = append(newHeaders, groupCols...)
	for _, a := range aggs {
		newHeaders = append(newHeaders, a.Col)
	}

	records := make([][]string, len(keyOrder))
	for i, key := range keyOrder {
		e := index[key]
		grp := tableAggRows{headerIdx: t.headerIdx, rows: e.rows}
		rec := make([]string, 0, len(newHeaders))
		rec = append(rec, e.keyVals...)
		for _, a := range aggs {
			rec = append(rec, a.Agg.reduce(grp))
		}
		records[i] = rec
	}

	return New(newHeaders, records)
}

// --- Agg implementations ---

type sumAgg struct{ col string }

func (a sumAgg) reduce(g aggRows) string {
	idx := g.ColIndex(a.col)
	if idx < 0 {
		return "0"
	}
	var sum float64
	for i := 0; i < g.Len(); i++ {
		if f, err := strconv.ParseFloat(strings.TrimSpace(g.ValueAt(i, idx)), 64); err == nil {
			sum += f
		}
	}
	return strconv.FormatFloat(sum, 'f', -1, 64)
}

type meanAgg struct{ col string }

func (a meanAgg) reduce(g aggRows) string {
	idx := g.ColIndex(a.col)
	if idx < 0 {
		return ""
	}
	var sum float64
	var n int
	for i := 0; i < g.Len(); i++ {
		if f, err := strconv.ParseFloat(strings.TrimSpace(g.ValueAt(i, idx)), 64); err == nil {
			sum += f
			n++
		}
	}
	if n == 0 {
		return ""
	}
	return strconv.FormatFloat(sum/float64(n), 'f', -1, 64)
}

type countAgg struct{ col string }

func (a countAgg) reduce(g aggRows) string {
	idx := g.ColIndex(a.col)
	if idx < 0 {
		return "0"
	}
	var n int
	for i := 0; i < g.Len(); i++ {
		if g.ValueAt(i, idx) != "" {
			n++
		}
	}
	return strconv.Itoa(n)
}

type stringJoinAgg struct {
	col string
	sep string
}

func (a stringJoinAgg) reduce(g aggRows) string {
	idx := g.ColIndex(a.col)
	if idx < 0 {
		return ""
	}
	parts := make([]string, 0, g.Len())
	for i := 0; i < g.Len(); i++ {
		if v := g.ValueAt(i, idx); v != "" {
			parts = append(parts, v)
		}
	}
	return strings.Join(parts, a.sep)
}

type firstAgg struct{ col string }

func (a firstAgg) reduce(g aggRows) string {
	if g.Len() == 0 {
		return ""
	}
	idx := g.ColIndex(a.col)
	if idx < 0 {
		return ""
	}
	return g.ValueAt(0, idx)
}

type lastAgg struct{ col string }

func (a lastAgg) reduce(g aggRows) string {
	if g.Len() == 0 {
		return ""
	}
	idx := g.ColIndex(a.col)
	if idx < 0 {
		return ""
	}
	return g.ValueAt(g.Len()-1, idx)
}

// RollingAgg computes a sliding-window aggregation over the rows of t in their
// current order. For each row i, the window covers rows [max(0, i-size+1)…i].
// The aggregated value is stored in a new column outCol.
//
//	// 3-period rolling mean of "revenue"
//	t.RollingAgg("revenue_3d", 3, table.Mean("revenue"))
func (t Table) RollingAgg(outCol string, size int, agg Agg) Table {
	if size < 1 {
		size = 1
	}
	newHeaders := make(slice.Slice[string], 0, len(t.Headers)+1)
	newHeaders = append(newHeaders, t.Headers...)
	newHeaders = append(newHeaders, outCol)

	rows := make(slice.Slice[Row], len(t.Rows))
	for i, row := range t.Rows {
		start := i - size + 1
		if start < 0 {
			start = 0
		}
		window := tableAggRows{headerIdx: t.headerIdx, rows: t.Rows[start : i+1]}
		val := agg.reduce(window)

		vals := make(slice.Slice[string], 0, len(row.values)+1)
		vals = append(vals, row.values...)
		vals = append(vals, val)
		rows[i] = NewRow(newHeaders, vals)
	}
	return newTable(newHeaders, rows)
}
