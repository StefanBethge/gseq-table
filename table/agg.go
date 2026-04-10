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
	plan(cols aggColIndex) aggPlan
}

type aggColIndex interface {
	ColIndex(col string) int
}

type aggRows interface {
	aggColIndex
	Len() int
	ValueAt(row, col int) string
}

type aggHeaderIndex map[string]int

func (m aggHeaderIndex) ColIndex(col string) int {
	if idx, ok := m[col]; ok {
		return idx
	}
	return -1
}

type aggPlan struct {
	colIdx int
	state  aggState
}

type aggState interface {
	step(value string)
	result() string
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
	if row < 0 || row >= len(g.rows) {
		return ""
	}
	return valueAtRow(g.rows[row].values, col)
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
	if row < 0 || row >= len(g.rows) {
		return ""
	}
	return valueAt(g.rows[row], col)
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
		plans   []aggPlan
	}

	// pre-compute group column indices once
	groupIdx := make([]int, len(groupCols))
	for i, col := range groupCols {
		idx, ok := t.headerIdx[col]
		if !ok {
			return t.withErrf("GroupByAgg: unknown column %q", col)
		}
		groupIdx[i] = idx
	}

	ordered := make([]*groupEntry, 0, len(t.Rows))
	headerIdx := aggHeaderIndex(t.headerIdx)
	switch len(groupIdx) {
	case 1:
		groupMap := make(map[string]*groupEntry, len(t.Rows))
		idx := groupIdx[0]
		for _, row := range t.Rows {
			key := valueAtRow(row.values, idx)
			e, ok := groupMap[key]
			if !ok {
				plans := make([]aggPlan, len(aggs))
				for i, agg := range aggs {
					plans[i] = agg.Agg.plan(headerIdx)
				}
				e = &groupEntry{keyVals: []string{key}, plans: plans}
				groupMap[key] = e
				ordered = append(ordered, e)
			}
			for i := range e.plans {
				if e.plans[i].colIdx >= 0 {
					e.plans[i].state.step(valueAtRow(row.values, e.plans[i].colIdx))
				}
			}
		}
	case 2:
		groupMap := make(map[pairKey]*groupEntry, len(t.Rows))
		idx0, idx1 := groupIdx[0], groupIdx[1]
		for _, row := range t.Rows {
			key := pairKey{a: valueAtRow(row.values, idx0), b: valueAtRow(row.values, idx1)}
			e, ok := groupMap[key]
			if !ok {
				plans := make([]aggPlan, len(aggs))
				for i, agg := range aggs {
					plans[i] = agg.Agg.plan(headerIdx)
				}
				e = &groupEntry{keyVals: []string{key.a, key.b}, plans: plans}
				groupMap[key] = e
				ordered = append(ordered, e)
			}
			for i := range e.plans {
				if e.plans[i].colIdx >= 0 {
					e.plans[i].state.step(valueAtRow(row.values, e.plans[i].colIdx))
				}
			}
		}
	default:
		index := make(map[string]*groupEntry)
		keyScratch := make([]byte, 0, len(groupCols)*8)
		for _, row := range t.Rows {
			key, nextScratch := keyFromRowValues(row.values, groupIdx, keyScratch)
			keyScratch = nextScratch
			e, ok := index[key]
			if !ok {
				kv := make([]string, len(groupIdx))
				for i, idx := range groupIdx {
					kv[i] = valueAtRow(row.values, idx)
				}
				plans := make([]aggPlan, len(aggs))
				for i, agg := range aggs {
					plans[i] = agg.Agg.plan(headerIdx)
				}
				e = &groupEntry{keyVals: kv, plans: plans}
				index[key] = e
				ordered = append(ordered, e)
			}
			for i := range e.plans {
				if e.plans[i].colIdx >= 0 {
					e.plans[i].state.step(valueAtRow(row.values, e.plans[i].colIdx))
				}
			}
		}
	}

	newHeaders := make(slice.Slice[string], 0, len(groupCols)+len(aggs))
	newHeaders = append(newHeaders, groupCols...)
	for _, a := range aggs {
		newHeaders = append(newHeaders, a.Col)
	}

	records := newPackedRecords(len(ordered), len(newHeaders))
	for i, e := range ordered {
		copy(records[i], e.keyVals)
		dst := len(groupCols)
		for _, plan := range e.plans {
			records[i][dst] = plan.state.result()
			dst++
		}
	}

	result := New(newHeaders, records)
	result.errs = t.errs
	return result
}

// --- Agg implementations ---

type sumAgg struct{ col string }

func (a sumAgg) reduce(g aggRows) string {
	return reduceAgg(g, a.plan(g))
}

func (a sumAgg) plan(cols aggColIndex) aggPlan {
	return aggPlan{colIdx: cols.ColIndex(a.col), state: &sumAggState{}}
}

type meanAgg struct{ col string }

func (a meanAgg) reduce(g aggRows) string {
	return reduceAgg(g, a.plan(g))
}

func (a meanAgg) plan(cols aggColIndex) aggPlan {
	return aggPlan{colIdx: cols.ColIndex(a.col), state: &meanAggState{}}
}

type countAgg struct{ col string }

func (a countAgg) reduce(g aggRows) string {
	return reduceAgg(g, a.plan(g))
}

func (a countAgg) plan(cols aggColIndex) aggPlan {
	return aggPlan{colIdx: cols.ColIndex(a.col), state: &countAggState{}}
}

type stringJoinAgg struct {
	col string
	sep string
}

func (a stringJoinAgg) reduce(g aggRows) string {
	return reduceAgg(g, a.plan(g))
}

func (a stringJoinAgg) plan(cols aggColIndex) aggPlan {
	return aggPlan{colIdx: cols.ColIndex(a.col), state: &stringJoinAggState{sep: a.sep}}
}

type firstAgg struct{ col string }

func (a firstAgg) reduce(g aggRows) string {
	return reduceAgg(g, a.plan(g))
}

func (a firstAgg) plan(cols aggColIndex) aggPlan {
	return aggPlan{colIdx: cols.ColIndex(a.col), state: &firstAggState{}}
}

type lastAgg struct{ col string }

func (a lastAgg) reduce(g aggRows) string {
	return reduceAgg(g, a.plan(g))
}

func (a lastAgg) plan(cols aggColIndex) aggPlan {
	return aggPlan{colIdx: cols.ColIndex(a.col), state: &lastAggState{}}
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
	return newTableFrom(t, newHeaders, rows)
}

func reduceAgg(g aggRows, plan aggPlan) string {
	if plan.colIdx >= 0 {
		for i := 0; i < g.Len(); i++ {
			plan.state.step(g.ValueAt(i, plan.colIdx))
		}
	}
	return plan.state.result()
}

type sumAggState struct {
	sum float64
}

func (s *sumAggState) step(value string) {
	if f, err := strconv.ParseFloat(strings.TrimSpace(value), 64); err == nil {
		s.sum += f
	}
}

func (s *sumAggState) result() string {
	return strconv.FormatFloat(s.sum, 'f', -1, 64)
}

type meanAggState struct {
	sum float64
	n   int
}

func (s *meanAggState) step(value string) {
	if f, err := strconv.ParseFloat(strings.TrimSpace(value), 64); err == nil {
		s.sum += f
		s.n++
	}
}

func (s *meanAggState) result() string {
	if s.n == 0 {
		return ""
	}
	return strconv.FormatFloat(s.sum/float64(s.n), 'f', -1, 64)
}

type countAggState struct {
	n int
}

func (s *countAggState) step(value string) {
	if value != "" {
		s.n++
	}
}

func (s *countAggState) result() string {
	return strconv.Itoa(s.n)
}

type stringJoinAggState struct {
	sep     string
	builder strings.Builder
	started bool
}

func (s *stringJoinAggState) step(value string) {
	if value == "" {
		return
	}
	if s.started {
		s.builder.WriteString(s.sep)
	} else {
		s.started = true
	}
	s.builder.WriteString(value)
}

func (s *stringJoinAggState) result() string {
	return s.builder.String()
}

type firstAggState struct {
	set   bool
	value string
}

func (s *firstAggState) step(value string) {
	if s.set {
		return
	}
	s.set = true
	s.value = value
}

func (s *firstAggState) result() string {
	return s.value
}

type lastAggState struct {
	value string
}

func (s *lastAggState) step(value string) {
	s.value = value
}

func (s *lastAggState) result() string {
	return s.value
}
