package table

import (
	"sort"
	"strconv"
	"strings"

	"github.com/stefanbethge/gseq/slice"
)

// Lag adds outCol containing the value of col from n rows back.
// The first n rows receive an empty string.
// Negative n is treated as 0.
//
//	t.Lag("revenue", "revenue_prev", 1)
func (t Table) Lag(col, outCol string, n int) Table {
	if n < 0 {
		n = 0
	}
	colI, ok := t.headerIdx[col]
	if !ok {
		return t
	}
	return t.appendDerivedCol(outCol, func(i int) string {
		if i-n < 0 {
			return ""
		}
		prev := t.Rows[i-n]
		if colI < len(prev.values) {
			return prev.values[colI]
		}
		return ""
	})
}

// Lead adds outCol containing the value of col from n rows ahead.
// The last n rows receive an empty string.
// Negative n is treated as 0.
//
//	t.Lead("revenue", "revenue_next", 1)
func (t Table) Lead(col, outCol string, n int) Table {
	if n < 0 {
		n = 0
	}
	colI, ok := t.headerIdx[col]
	if !ok {
		return t
	}
	return t.appendDerivedCol(outCol, func(i int) string {
		if i+n >= len(t.Rows) {
			return ""
		}
		next := t.Rows[i+n]
		if colI < len(next.values) {
			return next.values[colI]
		}
		return ""
	})
}

// CumSum adds outCol containing the running sum of col up to and including
// each row. Unparseable values are treated as zero and do not reset the sum.
//
//	t.CumSum("revenue", "revenue_cum")
func (t Table) CumSum(col, outCol string) Table {
	colI, ok := t.headerIdx[col]
	if !ok {
		return t
	}
	var running float64
	values := make([]string, len(t.Rows))
	for i, row := range t.Rows {
		if colI < len(row.values) {
			if f, err := strconv.ParseFloat(strings.TrimSpace(row.values[colI]), 64); err == nil {
				running += f
			}
		}
		values[i] = strconv.FormatFloat(running, 'f', -1, 64)
	}
	return t.appendDerivedCol(outCol, func(i int) string { return values[i] })
}

// Rank adds outCol containing the dense rank of col's numeric value for each
// row. asc=true assigns rank 1 to the smallest value. Ties share the same
// rank. Rows with unparseable values receive an empty string.
//
//	t.Rank("score", "score_rank", false) // rank 1 = highest score
func (t Table) Rank(col, outCol string, asc bool) Table {
	// collect parseable values with their original indices
	type entry struct {
		val   float64
		valid bool
	}
	colI, ok := t.headerIdx[col]
	if !ok {
		return t
	}
	entries := make([]entry, len(t.Rows))
	var numericVals []float64
	seen := make(map[float64]bool)
	for i, row := range t.Rows {
		var v string
		if colI < len(row.values) {
			v = row.values[colI]
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		entries[i] = entry{val: f, valid: err == nil}
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
	rankMap := make(map[float64]string, len(numericVals))
	for rank, v := range numericVals {
		rankMap[v] = strconv.Itoa(rank + 1)
	}

	ranks := make([]string, len(t.Rows))
	for i := range t.Rows {
		if entries[i].valid {
			ranks[i] = rankMap[entries[i].val]
		}
	}
	return t.appendDerivedCol(outCol, func(i int) string { return ranks[i] })
}

func (t Table) appendDerivedCol(outCol string, valueAt func(i int) string) Table {
	newHeaders := make(slice.Slice[string], len(t.Headers)+1)
	copy(newHeaders, t.Headers)
	newHeaders[len(t.Headers)] = outCol

	rows := make(slice.Slice[Row], len(t.Rows))
	if len(t.Rows) == 0 {
		return newTable(newHeaders, rows)
	}

	width := len(newHeaders)
	data := make(slice.Slice[string], len(t.Rows)*width)
	for i, row := range t.Rows {
		vals := data[i*width : (i+1)*width]
		copyLen := len(row.values)
		if copyLen > len(t.Headers) {
			copyLen = len(t.Headers)
		}
		copy(vals[:copyLen], row.values[:copyLen])
		vals[len(t.Headers)] = valueAt(i)
		rows[i] = NewRow(newHeaders, vals)
	}
	return newTable(newHeaders, rows)
}
