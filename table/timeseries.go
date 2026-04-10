package table

import (
	"strconv"

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
		return t.withErrf("Lag: unknown column %q", col)
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
		return t.withErrf("Lead: unknown column %q", col)
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
		return t.withErrf("CumSum: unknown column %q", col)
	}
	var runningFloat float64
	var runningInt int64
	intOnly := true
	values := make([]string, len(t.Rows))
	for i, row := range t.Rows {
		entry := parseNumericEntry(valueAtRow(row.values, colI))
		if entry.valid {
			if intOnly && entry.intOnly {
				runningInt += entry.intValue
			} else {
				if intOnly {
					runningFloat = float64(runningInt)
					intOnly = false
				}
				runningFloat += entry.floatValue
			}
		}
		if intOnly {
			values[i] = strconv.FormatInt(runningInt, 10)
		} else {
			values[i] = strconv.FormatFloat(runningFloat, 'f', -1, 64)
		}
	}
	return t.appendDerivedCol(outCol, func(i int) string { return values[i] })
}

// Rank adds outCol containing the dense rank of col's numeric value for each
// row. asc=true assigns rank 1 to the smallest value. Ties share the same
// rank. Rows with unparseable values receive an empty string.
//
//	t.Rank("score", "score_rank", false) // rank 1 = highest score
func (t Table) Rank(col, outCol string, asc bool) Table {
	colI, ok := t.headerIdx[col]
	if !ok {
		return t.withErrf("Rank: unknown column %q", col)
	}
	entries := make([]numericEntry, len(t.Rows))
	for i, row := range t.Rows {
		entries[i] = parseNumericEntry(valueAtRow(row.values, colI))
	}
	ranks := denseRankValues(entries, asc)
	return t.appendDerivedCol(outCol, func(i int) string { return ranks[i] })
}

func (t Table) appendDerivedCol(outCol string, valueAt func(i int) string) Table {
	newHeaders := make(slice.Slice[string], len(t.Headers)+1)
	copy(newHeaders, t.Headers)
	newHeaders[len(t.Headers)] = outCol

	rows := make(slice.Slice[Row], len(t.Rows))
	if len(t.Rows) == 0 {
		return newTableFrom(t, newHeaders, rows)
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
	return newTableFrom(t, newHeaders, rows)
}
