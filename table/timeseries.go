package table

import (
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/stefanbethge/gseq/slice"
)

// Lag adds outCol containing the value of col from n rows back.
// The first n rows receive an empty string.
//
//	t.Lag("revenue", "revenue_prev", 1)
func (t Table) Lag(col, outCol string, n int) Table {
	newHeaders := make(slice.Slice[string], 0, len(t.Headers)+1)
	newHeaders = append(newHeaders, t.Headers...)
	newHeaders = append(newHeaders, outCol)

	colIdx := t.headerIdx[col]
	rows := make(slice.Slice[Row], len(t.Rows))
	for i, row := range t.Rows {
		lagVal := ""
		if i-n >= 0 {
			prev := t.Rows[i-n]
			if colIdx < len(prev.values) {
				lagVal = prev.values[colIdx]
			}
		}
		vals := make(slice.Slice[string], 0, len(row.values)+1)
		vals = append(vals, row.values...)
		vals = append(vals, lagVal)
		rows[i] = NewRow(newHeaders, vals)
	}
	return newTable(newHeaders, rows)
}

// Lead adds outCol containing the value of col from n rows ahead.
// The last n rows receive an empty string.
//
//	t.Lead("revenue", "revenue_next", 1)
func (t Table) Lead(col, outCol string, n int) Table {
	newHeaders := make(slice.Slice[string], 0, len(t.Headers)+1)
	newHeaders = append(newHeaders, t.Headers...)
	newHeaders = append(newHeaders, outCol)

	colIdx := t.headerIdx[col]
	rows := make(slice.Slice[Row], len(t.Rows))
	for i, row := range t.Rows {
		leadVal := ""
		if i+n < len(t.Rows) {
			next := t.Rows[i+n]
			if colIdx < len(next.values) {
				leadVal = next.values[colIdx]
			}
		}
		vals := make(slice.Slice[string], 0, len(row.values)+1)
		vals = append(vals, row.values...)
		vals = append(vals, leadVal)
		rows[i] = NewRow(newHeaders, vals)
	}
	return newTable(newHeaders, rows)
}

// CumSum adds outCol containing the running sum of col up to and including
// each row. Unparseable values are treated as zero and do not reset the sum.
//
//	t.CumSum("revenue", "revenue_cum")
func (t Table) CumSum(col, outCol string) Table {
	newHeaders := make(slice.Slice[string], 0, len(t.Headers)+1)
	newHeaders = append(newHeaders, t.Headers...)
	newHeaders = append(newHeaders, outCol)

	colIdx := t.headerIdx[col]
	rows := make(slice.Slice[Row], len(t.Rows))
	var running float64
	for i, row := range t.Rows {
		if colIdx < len(row.values) {
			if f, err := strconv.ParseFloat(strings.TrimSpace(row.values[colIdx]), 64); err == nil {
				running += f
			}
		}
		vals := make(slice.Slice[string], 0, len(row.values)+1)
		vals = append(vals, row.values...)
		vals = append(vals, strconv.FormatFloat(running, 'f', -1, 64))
		rows[i] = NewRow(newHeaders, vals)
	}
	return newTable(newHeaders, rows)
}

// Rank adds outCol containing the dense rank of col's numeric value for each
// row. asc=true assigns rank 1 to the smallest value. Ties share the same
// rank. Rows with unparseable values receive an empty string.
//
//	t.Rank("score", "score_rank", false) // rank 1 = highest score
func (t Table) Rank(col, outCol string, asc bool) Table {
	// collect parseable values with their original indices
	type entry struct {
		idx   int
		val   float64
		valid bool
	}
	colIdx := t.headerIdx[col]
	entries := make([]entry, len(t.Rows))
	var numericVals []float64
	seen := make(map[float64]bool)
	for i, row := range t.Rows {
		var v string
		if colIdx < len(row.values) {
			v = row.values[colIdx]
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		entries[i] = entry{idx: i, val: f, valid: err == nil}
		if err == nil && !seen[f] {
			seen[f] = true
			numericVals = append(numericVals, f)
		}
	}

	// sort unique values to assign dense ranks
	sort.Float64s(numericVals)
	if !asc {
		for i, j := 0, len(numericVals)-1; i < j; i, j = i+1, j-1 {
			numericVals[i], numericVals[j] = numericVals[j], numericVals[i]
		}
	}
	rankMap := make(map[float64]int, len(numericVals))
	for rank, v := range numericVals {
		rankMap[v] = rank + 1
	}

	newHeaders := make(slice.Slice[string], 0, len(t.Headers)+1)
	newHeaders = append(newHeaders, t.Headers...)
	newHeaders = append(newHeaders, outCol)

	rows := make(slice.Slice[Row], len(t.Rows))
	for i, row := range t.Rows {
		rankStr := ""
		if entries[i].valid {
			rankStr = strconv.Itoa(rankMap[entries[i].val])
		}
		vals := make(slice.Slice[string], 0, len(row.values)+1)
		vals = append(vals, row.values...)
		vals = append(vals, rankStr)
		rows[i] = NewRow(newHeaders, vals)
	}
	return newTable(newHeaders, rows)
}

// ensure math and sort are used (they're used above — this line just
// suppresses lint warnings during incremental development).
var _, _ = math.Sqrt, sort.Float64s
