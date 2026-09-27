package table

import (
	"sort"
	"strconv"
)

// Window is a partitioned view of a Table used to run window functions
// (CumSum, Rank, Lag, Lead, RollingAgg) independently per partition.
// Obtain one with Table.PartitionBy and optionally order rows within each
// partition with OrderBy.
//
// Window functions never reorder the result: every output row stays at its
// original position, only the derived column is computed per partition in
// partition order.
//
//	t.PartitionBy("customer").
//	    OrderBy(table.Asc("date")).
//	    CumSum("revenue", "revenue_cum")
type Window struct {
	t         Table
	partIdx   []int
	orderKeys []SortKey
	orderIdx  []int
}

// PartitionBy returns a Window that groups rows by the combined values of
// cols. Partitions keep the original row order unless OrderBy is applied.
// With no cols the whole table forms a single partition.
// Unknown columns are recorded as errors and treated as empty values.
//
//	t.PartitionBy("customer").Lag("revenue", "revenue_prev", 1)
func (t Table) PartitionBy(cols ...string) Window {
	idxs := make([]int, len(cols))
	for i, col := range cols {
		if idx, ok := t.headerIdx[col]; ok {
			idxs[i] = idx
		} else {
			idxs[i] = -1
			t = t.withErrf("PartitionBy: unknown column %q", col)
		}
	}
	return Window{t: t, partIdx: idxs}
}

// OrderBy returns a copy of w that stable-sorts the rows within each
// partition by keys before a window function is applied. Sorting is
// lexicographic, like SortMulti. The output keeps the original row order.
// Unknown columns are recorded as errors and treated as empty values.
//
//	t.PartitionBy("customer").OrderBy(table.Asc("date")).Lag("revenue", "prev", 1)
func (w Window) OrderBy(keys ...SortKey) Window {
	idxs := make([]int, len(keys))
	for i, key := range keys {
		if idx, ok := w.t.headerIdx[key.Col]; ok {
			idxs[i] = idx
		} else {
			idxs[i] = -1
			w.t = w.t.withErrf("OrderBy: unknown column %q", key.Col)
		}
	}
	w.orderKeys = append([]SortKey(nil), keys...)
	w.orderIdx = idxs
	return w
}

// Lag is the partitioned variant of Table.Lag: outCol receives the value of
// col from n rows back within the same partition. The first n rows of each
// partition receive an empty string. Negative n is treated as 0.
func (w Window) Lag(col, outCol string, n int) Table {
	colI, ok := w.t.headerIdx[col]
	if !ok {
		return w.t.withErrf("Lag: unknown column %q", col)
	}
	rows := w.records()
	values := windowShiftValues(rows, w.partitions(rows), colI, -max(n, 0))
	return w.t.appendDerivedCol(outCol, func(i int) string { return values[i] })
}

// Lead is the partitioned variant of Table.Lead: outCol receives the value
// of col from n rows ahead within the same partition. The last n rows of
// each partition receive an empty string. Negative n is treated as 0.
func (w Window) Lead(col, outCol string, n int) Table {
	colI, ok := w.t.headerIdx[col]
	if !ok {
		return w.t.withErrf("Lead: unknown column %q", col)
	}
	rows := w.records()
	values := windowShiftValues(rows, w.partitions(rows), colI, max(n, 0))
	return w.t.appendDerivedCol(outCol, func(i int) string { return values[i] })
}

// CumSum is the partitioned variant of Table.CumSum: the running sum of col
// restarts for every partition. Unparseable values are treated as zero.
func (w Window) CumSum(col, outCol string) Table {
	colI, ok := w.t.headerIdx[col]
	if !ok {
		return w.t.withErrf("CumSum: unknown column %q", col)
	}
	rows := w.records()
	values := windowCumSumValues(rows, w.partitions(rows), colI)
	return w.t.appendDerivedCol(outCol, func(i int) string { return values[i] })
}

// Rank is the partitioned variant of Table.Rank: the dense rank of col is
// computed independently within each partition. Rows with unparseable values
// receive an empty string.
func (w Window) Rank(col, outCol string, asc bool) Table {
	colI, ok := w.t.headerIdx[col]
	if !ok {
		return w.t.withErrf("Rank: unknown column %q", col)
	}
	rows := w.records()
	values := windowRankValues(rows, w.partitions(rows), colI, asc)
	return w.t.appendDerivedCol(outCol, func(i int) string { return values[i] })
}

// RollingAgg is the partitioned variant of Table.RollingAgg: the sliding
// window of size rows never crosses a partition boundary.
func (w Window) RollingAgg(outCol string, size int, agg Agg) Table {
	rows := w.records()
	values := windowRollingValues(rows, w.partitions(rows), w.t.headerIdx, size, agg)
	return w.t.appendDerivedCol(outCol, func(i int) string { return values[i] })
}

func (w Window) records() [][]string {
	rows := make([][]string, len(w.t.Rows))
	for i, row := range w.t.Rows {
		rows[i] = row.values
	}
	return rows
}

func (w Window) partitions(rows [][]string) [][]int {
	return windowPartitions(rows, w.partIdx, w.orderKeys, w.orderIdx)
}

// MutableWindow is the MutableTable counterpart of Window. Its window
// functions add the derived column to the underlying MutableTable in place
// and return it for chaining.
//
//	m.PartitionBy("customer").OrderBy(table.Asc("date")).CumSum("revenue", "cum")
type MutableWindow struct {
	m         *MutableTable
	partCols  []string
	orderKeys []SortKey
}

// PartitionBy returns a MutableWindow that groups rows by the combined values
// of cols. See Table.PartitionBy.
func (m *MutableTable) PartitionBy(cols ...string) *MutableWindow {
	for _, col := range cols {
		if _, ok := m.headerIdx[col]; !ok {
			m.addErrf("PartitionBy: unknown column %q", col)
		}
	}
	return &MutableWindow{m: m, partCols: append([]string(nil), cols...)}
}

// OrderBy sets the within-partition ordering of w. See Window.OrderBy.
func (w *MutableWindow) OrderBy(keys ...SortKey) *MutableWindow {
	for _, key := range keys {
		if _, ok := w.m.headerIdx[key.Col]; !ok {
			w.m.addErrf("OrderBy: unknown column %q", key.Col)
		}
	}
	w.orderKeys = append([]SortKey(nil), keys...)
	return w
}

// Lag adds a partitioned lag column in place. See Window.Lag.
func (w *MutableWindow) Lag(col, outCol string, n int) *MutableTable {
	colIdx := w.m.ColIndex(col)
	if colIdx < 0 {
		w.m.addErrf("Lag: unknown column %q", col)
		return w.m
	}
	values := windowShiftValues(w.m.rows, w.partitions(), colIdx, -max(n, 0))
	w.m.appendDerivedCol(outCol, func(i int) string { return values[i] })
	return w.m
}

// Lead adds a partitioned lead column in place. See Window.Lead.
func (w *MutableWindow) Lead(col, outCol string, n int) *MutableTable {
	colIdx := w.m.ColIndex(col)
	if colIdx < 0 {
		w.m.addErrf("Lead: unknown column %q", col)
		return w.m
	}
	values := windowShiftValues(w.m.rows, w.partitions(), colIdx, max(n, 0))
	w.m.appendDerivedCol(outCol, func(i int) string { return values[i] })
	return w.m
}

// CumSum adds a partitioned running sum column in place. See Window.CumSum.
func (w *MutableWindow) CumSum(col, outCol string) *MutableTable {
	colIdx := w.m.ColIndex(col)
	if colIdx < 0 {
		w.m.addErrf("CumSum: unknown column %q", col)
		return w.m
	}
	values := windowCumSumValues(w.m.rows, w.partitions(), colIdx)
	w.m.appendDerivedCol(outCol, func(i int) string { return values[i] })
	return w.m
}

// Rank adds a partitioned dense rank column in place. See Window.Rank.
func (w *MutableWindow) Rank(col, outCol string, asc bool) *MutableTable {
	colIdx := w.m.ColIndex(col)
	if colIdx < 0 {
		w.m.addErrf("Rank: unknown column %q", col)
		return w.m
	}
	values := windowRankValues(w.m.rows, w.partitions(), colIdx, asc)
	w.m.appendDerivedCol(outCol, func(i int) string { return values[i] })
	return w.m
}

// RollingAgg adds a partitioned rolling aggregation column in place.
// See Window.RollingAgg.
func (w *MutableWindow) RollingAgg(outCol string, size int, agg Agg) *MutableTable {
	values := windowRollingValues(w.m.rows, w.partitions(), w.m.headerIdx, size, agg)
	w.m.appendDerivedCol(outCol, func(i int) string { return values[i] })
	return w.m
}

// partitions resolves column names at call time so the window reflects the
// current MutableTable layout.
func (w *MutableWindow) partitions() [][]int {
	partIdx := make([]int, len(w.partCols))
	for i, col := range w.partCols {
		partIdx[i] = w.m.ColIndex(col)
	}
	orderIdx := make([]int, len(w.orderKeys))
	for i, key := range w.orderKeys {
		orderIdx[i] = w.m.ColIndex(key.Col)
	}
	return windowPartitions(w.m.rows, partIdx, w.orderKeys, orderIdx)
}

// windowPartitions groups row indices by the partition key in order of first
// appearance and stable-sorts each group by keys. Negative column indices
// read as empty values.
func windowPartitions(rows [][]string, partIdx []int, keys []SortKey, orderIdx []int) [][]int {
	var parts [][]int
	if len(partIdx) == 0 {
		all := make([]int, len(rows))
		for i := range all {
			all[i] = i
		}
		parts = [][]int{all}
	} else {
		byKey := make(map[string]int)
		var scratch []byte
		var key string
		for i, row := range rows {
			key, scratch = keyFromValues(row, partIdx, scratch)
			p, ok := byKey[key]
			if !ok {
				p = len(parts)
				byKey[key] = p
				parts = append(parts, nil)
			}
			parts[p] = append(parts[p], i)
		}
	}
	if len(keys) == 0 {
		return parts
	}
	for _, part := range parts {
		sort.SliceStable(part, func(i, j int) bool {
			a, b := rows[part[i]], rows[part[j]]
			for k, key := range keys {
				av, bv := valueAt(a, orderIdx[k]), valueAt(b, orderIdx[k])
				if av == bv {
					continue
				}
				if key.Asc {
					return av < bv
				}
				return av > bv
			}
			return false
		})
	}
	return parts
}

// windowShiftValues returns, for every row, the value of colIdx offset rows
// away within its partition (negative offset = lag, positive = lead).
func windowShiftValues(rows [][]string, parts [][]int, colIdx, offset int) []string {
	values := make([]string, len(rows))
	for _, part := range parts {
		for pos, rowIdx := range part {
			src := pos + offset
			if src >= 0 && src < len(part) {
				values[rowIdx] = valueAt(rows[part[src]], colIdx)
			}
		}
	}
	return values
}

func windowCumSumValues(rows [][]string, parts [][]int, colIdx int) []string {
	values := make([]string, len(rows))
	for _, part := range parts {
		var runningFloat float64
		var runningInt int64
		intOnly := true
		for _, rowIdx := range part {
			entry := parseNumericEntry(valueAt(rows[rowIdx], colIdx))
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
				values[rowIdx] = strconv.FormatInt(runningInt, 10)
			} else {
				values[rowIdx] = strconv.FormatFloat(runningFloat, 'f', -1, 64)
			}
		}
	}
	return values
}

func windowRankValues(rows [][]string, parts [][]int, colIdx int, asc bool) []string {
	values := make([]string, len(rows))
	var entries []numericEntry
	for _, part := range parts {
		entries = entries[:0]
		for _, rowIdx := range part {
			entries = append(entries, parseNumericEntry(valueAt(rows[rowIdx], colIdx)))
		}
		for pos, rank := range denseRankValues(entries, asc) {
			values[part[pos]] = rank
		}
	}
	return values
}

func windowRollingValues(rows [][]string, parts [][]int, headerIdx map[string]int, size int, agg Agg) []string {
	if size < 1 {
		size = 1
	}
	values := make([]string, len(rows))
	var partRows [][]string
	for _, part := range parts {
		partRows = partRows[:0]
		for _, rowIdx := range part {
			partRows = append(partRows, rows[rowIdx])
		}
		for pos, rowIdx := range part {
			start := max(pos-size+1, 0)
			window := mutableAggRows{headerIdx: headerIdx, rows: partRows[start : pos+1]}
			values[rowIdx] = agg.reduce(window)
		}
	}
	return values
}
