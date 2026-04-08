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
	reduce(group Table) string
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
		groupIdx[i] = t.headerIdx[col]
	}

	index := make(map[string]*groupEntry)
	var keyOrder []string

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
		grp := newTable(t.Headers, e.rows)
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

func (a sumAgg) reduce(g Table) string {
	idx := g.headerIdx[a.col]
	var sum float64
	for _, row := range g.Rows {
		if idx < len(row.values) {
			if f, err := strconv.ParseFloat(strings.TrimSpace(row.values[idx]), 64); err == nil {
				sum += f
			}
		}
	}
	return strconv.FormatFloat(sum, 'f', -1, 64)
}

type meanAgg struct{ col string }

func (a meanAgg) reduce(g Table) string {
	idx := g.headerIdx[a.col]
	var sum float64
	var n int
	for _, row := range g.Rows {
		if idx < len(row.values) {
			if f, err := strconv.ParseFloat(strings.TrimSpace(row.values[idx]), 64); err == nil {
				sum += f
				n++
			}
		}
	}
	if n == 0 {
		return ""
	}
	return strconv.FormatFloat(sum/float64(n), 'f', -1, 64)
}

type countAgg struct{ col string }

func (a countAgg) reduce(g Table) string {
	idx := g.headerIdx[a.col]
	var n int
	for _, row := range g.Rows {
		if idx < len(row.values) && row.values[idx] != "" {
			n++
		}
	}
	return strconv.Itoa(n)
}

type stringJoinAgg struct {
	col string
	sep string
}

func (a stringJoinAgg) reduce(g Table) string {
	idx := g.headerIdx[a.col]
	var parts []string
	for _, row := range g.Rows {
		if idx < len(row.values) && row.values[idx] != "" {
			parts = append(parts, row.values[idx])
		}
	}
	return strings.Join(parts, a.sep)
}

type firstAgg struct{ col string }

func (a firstAgg) reduce(g Table) string {
	if len(g.Rows) == 0 {
		return ""
	}
	idx := g.headerIdx[a.col]
	if idx < len(g.Rows[0].values) {
		return g.Rows[0].values[idx]
	}
	return ""
}

type lastAgg struct{ col string }

func (a lastAgg) reduce(g Table) string {
	if len(g.Rows) == 0 {
		return ""
	}
	idx := g.headerIdx[a.col]
	last := g.Rows[len(g.Rows)-1]
	if idx < len(last.values) {
		return last.values[idx]
	}
	return ""
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
		// reuse t.headerIdx — no need to rebuild the map for each window
		window := Table{Headers: t.Headers, Rows: t.Rows[start : i+1], headerIdx: t.headerIdx}
		val := agg.reduce(window)

		vals := make(slice.Slice[string], 0, len(row.values)+1)
		vals = append(vals, row.values...)
		vals = append(vals, val)
		rows[i] = NewRow(newHeaders, vals)
	}
	return newTable(newHeaders, rows)
}
