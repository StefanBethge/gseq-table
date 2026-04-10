package table

import (
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/stefanbethge/gseq/slice"
)

// Table returns an immutable snapshot of m.
// It is equivalent to Freeze.
func (m *MutableTable) Table() Table { return m.Freeze() }

// Col extracts all values of the named column.
func (m *MutableTable) Col(name string) slice.Slice[string] {
	idx, ok := m.headerIdx[name]
	if !ok {
		return slice.Slice[string]{}
	}
	out := make(slice.Slice[string], len(m.rows))
	for i, row := range m.rows {
		if idx < len(row) {
			out[i] = row[idx]
		}
	}
	return out
}

// Select keeps only the named columns in place.
func (m *MutableTable) Select(cols ...string) *MutableTable {
	newHeaders := make(slice.Slice[string], 0, len(cols))
	indices := make([]int, 0, len(cols))
	for _, col := range cols {
		if idx, ok := m.headerIdx[col]; ok {
			newHeaders = append(newHeaders, col)
			indices = append(indices, idx)
		} else {
			m.addErrf("Select: unknown column %q", col)
		}
	}
	rows := newPackedRecords(len(m.rows), len(indices))
	for i, row := range m.rows {
		vals := rows[i]
		for j, idx := range indices {
			vals[j] = valueAt(row, idx)
		}
	}
	m.replaceAll(newHeaders, rows)
	return m
}

// Where keeps only rows matching fn in place.
func (m *MutableTable) Where(fn func(Row) bool) *MutableTable {
	rows := make([][]string, 0, len(m.rows))
	for _, row := range m.rows {
		if fn(NewRow(m.headers, row)) {
			rows = append(rows, row)
		}
	}
	m.rows = rows
	return m
}

// GroupBy splits the current data into immutable sub-tables keyed by col.
func (m *MutableTable) GroupBy(col string) map[string]Table {
	idx, ok := m.headerIdx[col]
	if !ok {
		m.addErrf("GroupBy: unknown column %q", col)
		return map[string]Table{}
	}
	grouped := make(map[string][][]string)
	for _, row := range m.rows {
		key := ""
		if idx < len(row) {
			key = row[idx]
		}
		grouped[key] = append(grouped[key], append([]string(nil), row...))
	}
	out := make(map[string]Table, len(grouped))
	for key, rows := range grouped {
		out[key] = New(copyHeaders(m.headers), rows)
	}
	return out
}

// Sort sorts by a single column in place.
func (m *MutableTable) Sort(col string, asc bool) *MutableTable {
	idx, ok := m.headerIdx[col]
	if !ok {
		m.addErrf("Sort: unknown column %q", col)
		return m
	}
	sort.SliceStable(m.rows, func(i, j int) bool {
		av, bv := "", ""
		if idx < len(m.rows[i]) {
			av = m.rows[i][idx]
		}
		if idx < len(m.rows[j]) {
			bv = m.rows[j][idx]
		}
		if asc {
			return av < bv
		}
		return av > bv
	})
	return m
}

// SortMulti sorts by multiple columns in place.
func (m *MutableTable) SortMulti(keys ...SortKey) *MutableTable {
	indices := make([]int, len(keys))
	for i, key := range keys {
		idx := m.ColIndex(key.Col)
		if idx < 0 {
			m.addErrf("SortMulti: unknown column %q", key.Col)
		}
		indices[i] = idx
	}
	sort.SliceStable(m.rows, func(i, j int) bool {
		a, b := m.rows[i], m.rows[j]
		for k, key := range keys {
			idx := indices[k]
			av, bv := "", ""
			if idx >= 0 {
				if idx < len(a) {
					av = a[idx]
				}
				if idx < len(b) {
					bv = b[idx]
				}
			}
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
	return m
}

// Append appends other to m in place.
func (m *MutableTable) Append(other Table) *MutableTable {
	otherIdx := make([]int, len(m.headers))
	for i, h := range m.headers {
		otherIdx[i] = other.ColIndex(h)
	}
	rows := make([][]string, 0, len(m.rows)+len(other.Rows))
	rows = append(rows, m.rows...)
	for _, row := range other.Rows {
		vals := make([]string, len(m.headers))
		for i, idx := range otherIdx {
			if idx >= 0 && idx < len(row.values) {
				vals[i] = row.values[idx]
			}
		}
		rows = append(rows, vals)
	}
	m.rows = rows
	return m
}

// AppendMutable appends another mutable table to m in place.
func (m *MutableTable) AppendMutable(other *MutableTable) *MutableTable {
	otherIdx := make([]int, len(m.headers))
	for i, h := range m.headers {
		otherIdx[i] = other.ColIndex(h)
	}
	rows := make([][]string, 0, len(m.rows)+len(other.rows))
	rows = append(rows, m.rows...)
	for _, row := range other.rows {
		vals := make([]string, len(m.headers))
		for i, idx := range otherIdx {
			if idx >= 0 && idx < len(row) {
				vals[i] = row[idx]
			}
		}
		rows = append(rows, vals)
	}
	m.rows = rows
	return m
}

// Head keeps the first n rows in place.
func (m *MutableTable) Head(n int) *MutableTable {
	if n <= 0 {
		m.rows = [][]string{}
		return m
	}
	if n >= len(m.rows) {
		return m
	}
	m.rows = append([][]string(nil), m.rows[:n]...)
	return m
}

// Tail keeps the last n rows in place.
func (m *MutableTable) Tail(n int) *MutableTable {
	if n <= 0 {
		m.rows = [][]string{}
		return m
	}
	if n >= len(m.rows) {
		return m
	}
	m.rows = append([][]string(nil), m.rows[len(m.rows)-n:]...)
	return m
}

// DropEmpty removes rows with empty values in place.
func (m *MutableTable) DropEmpty(cols ...string) *MutableTable {
	check := cols
	if len(check) == 0 {
		check = m.headers
	}
	checkIdx := make([]int, 0, len(check))
	for _, col := range check {
		if idx, ok := m.headerIdx[col]; ok {
			checkIdx = append(checkIdx, idx)
		} else {
			m.addErrf("DropEmpty: unknown column %q", col)
		}
	}
	if len(checkIdx) == 0 {
		return m
	}
	rows := make([][]string, 0, len(m.rows))
rowLoop:
	for _, row := range m.rows {
		for _, idx := range checkIdx {
			if idx >= len(row) || row[idx] == "" {
				continue rowLoop
			}
		}
		rows = append(rows, row)
	}
	m.rows = rows
	return m
}

// Distinct removes duplicate rows in place.
func (m *MutableTable) Distinct(cols ...string) *MutableTable {
	check := cols
	if len(check) == 0 {
		check = m.headers
	}
	checkIdx := make([]int, len(check))
	for i, col := range check {
		idx, ok := m.headerIdx[col]
		if !ok {
			m.addErrf("Distinct: unknown column %q", col)
			return m
		}
		checkIdx[i] = idx
	}
	rows := make([][]string, 0, len(m.rows))
	switch len(checkIdx) {
	case 1:
		seen := make(map[string]bool, len(m.rows))
		idx := checkIdx[0]
		for _, row := range m.rows {
			key := valueAt(row, idx)
			if seen[key] {
				continue
			}
			seen[key] = true
			rows = append(rows, row)
		}
	case 2:
		seen := make(map[pairKey]bool, len(m.rows))
		idx0, idx1 := checkIdx[0], checkIdx[1]
		for _, row := range m.rows {
			key := pairKey{a: valueAt(row, idx0), b: valueAt(row, idx1)}
			if seen[key] {
				continue
			}
			seen[key] = true
			rows = append(rows, row)
		}
	default:
		seen := make(map[string]bool, len(m.rows))
		keyScratch := make([]byte, 0, len(checkIdx)*8)
		for _, row := range m.rows {
			key, nextScratch := keyFromValues(row, checkIdx, keyScratch)
			keyScratch = nextScratch
			if seen[key] {
				continue
			}
			seen[key] = true
			rows = append(rows, row)
		}
	}
	m.rows = rows
	return m
}

// AddColSwitch appends a conditional column in place.
func (m *MutableTable) AddColSwitch(name string, cases []Case, else_ func(Row) string) *MutableTable {
	return m.AddCol(name, func(r Row) string {
		for _, c := range cases {
			if c.When(r) {
				return c.Then(r)
			}
		}
		if else_ != nil {
			return else_(r)
		}
		return ""
	})
}

// Transform applies partial row updates in place.
func (m *MutableTable) Transform(fn func(Row) map[string]string) *MutableTable {
	for i, row := range m.rows {
		updates := fn(NewRow(m.headers, row))
		for col, v := range updates {
			if idx, ok := m.headerIdx[col]; ok && idx < len(m.rows[i]) {
				m.rows[i][idx] = v
			}
		}
	}
	return m
}

// TransformParallel applies partial row updates concurrently in place.
// Each worker processes a contiguous chunk of rows directly on m.rows,
// avoiding any per-row allocation.
func (m *MutableTable) TransformParallel(fn func(Row) map[string]string) *MutableTable {
	n := len(m.rows)
	if n == 0 {
		return m
	}
	workers := runtime.GOMAXPROCS(0)
	if workers > n {
		workers = n
	}
	chunkSize := (n + workers - 1) / workers
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		lo := w * chunkSize
		if lo >= n {
			break
		}
		hi := lo + chunkSize
		if hi > n {
			hi = n
		}
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			for i := lo; i < hi; i++ {
				updates := fn(NewRow(m.headers, m.rows[i]))
				for col, v := range updates {
					if idx, ok := m.headerIdx[col]; ok && idx < len(m.rows[i]) {
						m.rows[i][idx] = v
					}
				}
			}
		}(lo, hi)
	}
	wg.Wait()
	return m
}

// TryTransform applies partial row updates and leaves m unchanged on error.
func (m *MutableTable) TryTransform(fn func(Row) (map[string]string, error)) *MutableTable {
	rows := cloneRecords(m.rows)
	for i, row := range m.rows {
		updates, err := fn(NewRow(m.headers, row))
		if err != nil {
			m.addErrf("TryTransform: %v", err)
			return m
		}
		for col, v := range updates {
			if idx, ok := m.headerIdx[col]; ok && idx < len(rows[i]) {
				rows[i][idx] = v
			}
		}
	}
	m.rows = rows
	return m
}

// TryMap maps a column in place and leaves m unchanged on error.
func (m *MutableTable) TryMap(col string, fn func(string) (string, error)) *MutableTable {
	idx, ok := m.headerIdx[col]
	if !ok {
		m.addErrf("TryMap: unknown column %q", col)
		return m
	}
	rows := cloneRecords(m.rows)
	for i := range rows {
		if idx < len(rows[i]) {
			newVal, err := fn(rows[i][idx])
			if err != nil {
				m.addErrf("TryMap: %v", err)
				return m
			}
			rows[i][idx] = newVal
		}
	}
	m.rows = rows
	return m
}

// RenameMany renames multiple columns in place.
func (m *MutableTable) RenameMany(renames map[string]string) *MutableTable {
	for i, h := range m.headers {
		if newName, ok := renames[h]; ok {
			m.headers[i] = newName
		}
	}
	m.headers = normalizeHeaders(m.headers)
	m.headerIdx = buildHeaderIndex(m.headers)
	return m
}

// AddRowIndex prepends a row-number column in place.
func (m *MutableTable) AddRowIndex(name string) *MutableTable {
	newHeaders := make(slice.Slice[string], 0, len(m.headers)+1)
	newHeaders = append(newHeaders, name)
	newHeaders = append(newHeaders, m.headers...)
	rows := newPackedRecords(len(m.rows), len(newHeaders))
	for i, row := range m.rows {
		vals := rows[i]
		vals[0] = strconv.Itoa(i)
		copyLen := len(row)
		if copyLen > len(m.headers) {
			copyLen = len(m.headers)
		}
		copy(vals[1:1+copyLen], row[:copyLen])
	}
	m.replaceAll(newHeaders, rows)
	return m
}

// Explode splits one column into multiple rows in place.
func (m *MutableTable) Explode(col, sep string) *MutableTable {
	idx, ok := m.headerIdx[col]
	if !ok {
		m.addErrf("Explode: unknown column %q", col)
		return m
	}
	rowCount := 0
	splits := make([][]string, len(m.rows))
	for rowI, row := range m.rows {
		val := ""
		if idx < len(row) {
			val = row[idx]
		}
		parts := splitNonEmpty(val, sep)
		if len(parts) == 0 {
			parts = []string{val}
		}
		rowCount += len(parts)
		splits[rowI] = parts
	}
	rows := newPackedRecords(rowCount, len(m.headers))
	out := 0
	for rowI, row := range m.rows {
		for _, part := range splits[rowI] {
			rec := rows[out]
			out++
			copyLen := len(row)
			if copyLen > len(m.headers) {
				copyLen = len(m.headers)
			}
			copy(rec[:copyLen], row[:copyLen])
			rec[idx] = part
		}
	}
	m.rows = rows
	return m
}

// Transpose pivots rows and columns in place.
func (m *MutableTable) Transpose() *MutableTable {
	newHeaders := make(slice.Slice[string], 0, len(m.rows)+1)
	newHeaders = append(newHeaders, "column")
	for i := range m.rows {
		newHeaders = append(newHeaders, strconv.Itoa(i))
	}
	rows := newPackedRecords(len(m.headers), len(newHeaders))
	for ci, col := range m.headers {
		rec := rows[ci]
		rec[0] = col
		dst := 1
		for _, row := range m.rows {
			rec[dst] = valueAt(row, ci)
			dst++
		}
	}
	m.replaceAll(newHeaders, rows)
	return m
}

// FillForward fills empty cells from the previous non-empty value.
func (m *MutableTable) FillForward(col string) *MutableTable {
	idx := m.ColIndex(col)
	if idx < 0 {
		m.addErrf("FillForward: unknown column %q", col)
		return m
	}
	last := ""
	for _, row := range m.rows {
		if idx < len(row) {
			if row[idx] == "" {
				row[idx] = last
			} else {
				last = row[idx]
			}
		}
	}
	return m
}

// FillBackward fills empty cells from the next non-empty value.
func (m *MutableTable) FillBackward(col string) *MutableTable {
	idx := m.ColIndex(col)
	if idx < 0 {
		m.addErrf("FillBackward: unknown column %q", col)
		return m
	}
	next := ""
	for i := len(m.rows) - 1; i >= 0; i-- {
		row := m.rows[i]
		if idx < len(row) {
			if row[idx] == "" {
				row[idx] = next
			} else {
				next = row[idx]
			}
		}
	}
	return m
}

// Sample keeps a random sample in place.
func (m *MutableTable) Sample(n int) *MutableTable {
	if n >= len(m.rows) {
		m.rows = append([][]string(nil), slice.Slice[[]string](m.rows).Samples(len(m.rows))...)
		return m
	}
	sampled := slice.Slice[[]string](m.rows).Samples(n)
	m.rows = append([][]string(nil), sampled...)
	return m
}

// SampleFrac keeps a random sample fraction in place.
func (m *MutableTable) SampleFrac(f float64) *MutableTable {
	n := int(float64(len(m.rows)) * f)
	return m.Sample(n)
}

// AddColFloat appends a float-derived column in place.
func (m *MutableTable) AddColFloat(name string, fn func(Row) float64) *MutableTable {
	return m.AddCol(name, func(r Row) string {
		return strconv.FormatFloat(fn(r), 'f', -1, 64)
	})
}

// AddColInt appends an int-derived column in place.
func (m *MutableTable) AddColInt(name string, fn func(Row) int64) *MutableTable {
	return m.AddCol(name, func(r Row) string {
		return strconv.FormatInt(fn(r), 10)
	})
}

// Coalesce appends the first non-empty value across cols.
func (m *MutableTable) Coalesce(name string, cols ...string) *MutableTable {
	return m.AddCol(name, func(r Row) string {
		for _, col := range cols {
			if v := r.Get(col).UnwrapOr(""); v != "" {
				return v
			}
		}
		return ""
	})
}

// Lookup appends a lookup-derived column in place.
func (m *MutableTable) Lookup(col, outCol string, lookupTable Table, keyCol, valCol string) *MutableTable {
	keyIdx := lookupTable.ColIndex(keyCol)
	valIdx := lookupTable.ColIndex(valCol)
	colIdx := m.ColIndex(col)
	if colIdx < 0 {
		m.addErrf("Lookup: unknown column %q", col)
		return m
	}
	if keyIdx < 0 {
		m.addErrf("Lookup: unknown column %q in lookup table", keyCol)
		return m
	}
	if valIdx < 0 {
		m.addErrf("Lookup: unknown column %q in lookup table", valCol)
		return m
	}
	lkp := make(map[string]string, len(lookupTable.Rows))
	for _, row := range lookupTable.Rows {
		k, v := "", ""
		if keyIdx < len(row.values) {
			k = row.values[keyIdx]
		}
		if valIdx < len(row.values) {
			v = row.values[valIdx]
		}
		lkp[k] = v
	}
	return m.AddCol(outCol, func(r Row) string {
		k := ""
		if colIdx < len(r.values) {
			k = r.values[colIdx]
		}
		return lkp[k]
	})
}

// FormatCol formats parseable floats in place.
func (m *MutableTable) FormatCol(col string, precision int) *MutableTable {
	idx, ok := m.headerIdx[col]
	if !ok {
		m.addErrf("FormatCol: unknown column %q", col)
		return m
	}
	for _, row := range m.rows {
		if idx >= len(row) {
			continue
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(row[idx]), 64)
		if err == nil {
			row[idx] = strconv.FormatFloat(f, 'f', precision, 64)
		}
	}
	return m
}

// Intersect keeps rows that also appear in other.
func (m *MutableTable) Intersect(other Table, cols ...string) *MutableTable {
	check := cols
	if len(check) == 0 {
		check = m.headers
	}
	mIdx := make([]int, len(check))
	oIdx := make([]int, len(check))
	for i, col := range check {
		mi, ok1 := m.headerIdx[col]
		oi := other.ColIndex(col)
		if !ok1 || oi < 0 {
			m.addErrf("Intersect: unknown column %q", col)
			return m
		}
		mIdx[i] = mi
		oIdx[i] = oi
	}
	rows := make([][]string, 0, len(m.rows))
	switch len(mIdx) {
	case 1:
		otherKeys := make(map[string]bool, len(other.Rows))
		oi := oIdx[0]
		mi := mIdx[0]
		for _, row := range other.Rows {
			otherKeys[valueAtRow(row.values, oi)] = true
		}
		for _, row := range m.rows {
			if otherKeys[valueAt(row, mi)] {
				rows = append(rows, row)
			}
		}
	case 2:
		otherKeys := make(map[pairKey]bool, len(other.Rows))
		oi0, oi1 := oIdx[0], oIdx[1]
		mi0, mi1 := mIdx[0], mIdx[1]
		for _, row := range other.Rows {
			otherKeys[pairKey{a: valueAtRow(row.values, oi0), b: valueAtRow(row.values, oi1)}] = true
		}
		for _, row := range m.rows {
			if otherKeys[pairKey{a: valueAt(row, mi0), b: valueAt(row, mi1)}] {
				rows = append(rows, row)
			}
		}
	default:
		otherKeys := make(map[string]bool, len(other.Rows))
		keyScratch := make([]byte, 0, len(check)*8)
		for _, row := range other.Rows {
			key, nextScratch := keyFromRowValues(row.values, oIdx, keyScratch)
			keyScratch = nextScratch
			otherKeys[key] = true
		}
		queryScratch := make([]byte, 0, len(check)*8)
		for _, row := range m.rows {
			key, nextScratch := keyFromValues(row, mIdx, queryScratch)
			queryScratch = nextScratch
			if otherKeys[key] {
				rows = append(rows, row)
			}
		}
	}
	m.rows = rows
	return m
}

// Bin appends a bin label column in place.
func (m *MutableTable) Bin(col, name string, bins []BinDef) *MutableTable {
	colIdx := m.ColIndex(col)
	if colIdx < 0 {
		m.addErrf("Bin: unknown column %q", col)
		return m
	}
	return m.AddCol(name, func(r Row) string {
		v := ""
		if colIdx < len(r.values) {
			v = r.values[colIdx]
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil || len(bins) == 0 {
			return ""
		}
		lo, hi := 0, len(bins)
		for lo < hi {
			mid := (lo + hi) / 2
			if f < bins[mid].Max {
				hi = mid
			} else {
				lo = mid + 1
			}
		}
		if lo < len(bins) {
			return bins[lo].Label
		}
		return bins[len(bins)-1].Label
	})
}

// Join performs an inner join in place.
func (m *MutableTable) Join(other Table, leftCol, rightCol string) *MutableTable {
	rightKeyIdx := other.ColIndex(rightCol)
	leftKeyIdx := m.ColIndex(leftCol)
	if leftKeyIdx < 0 {
		m.addErrf("Join: unknown column %q", leftCol)
		return m
	}
	if rightKeyIdx < 0 {
		m.addErrf("Join: unknown column %q in other table", rightCol)
		return m
	}
	rightIdx := make(map[string]rowBucket, len(other.Rows))
	for _, row := range other.Rows {
		key := valueAtRow(row.values, rightKeyIdx)
		addRowBucket(rightIdx, key, row.values)
	}
	newHeaders := copyHeaders(m.headers)
	rightExtraIdx := make([]int, 0, len(other.Headers))
	for i, h := range other.Headers {
		if h != rightCol {
			newHeaders = append(newHeaders, h)
			rightExtraIdx = append(rightExtraIdx, i)
		}
	}
	rowCount := 0
	for _, lRow := range m.rows {
		rowCount += rightIdx[valueAt(lRow, leftKeyIdx)].len()
	}
	rows := newPackedRecords(rowCount, len(newHeaders))
	out := 0
	for _, lRow := range m.rows {
		key := valueAt(lRow, leftKeyIdx)
		bucket, ok := rightIdx[key]
		if !ok || bucket.len() == 0 {
			continue
		}
		forEachRowBucket(bucket, func(rRow slice.Slice[string]) {
			vals := rows[out]
			out++
			copyLen := len(lRow)
			if copyLen > len(m.headers) {
				copyLen = len(m.headers)
			}
			copy(vals[:copyLen], lRow[:copyLen])
			dst := len(m.headers)
			for _, idx := range rightExtraIdx {
				vals[dst] = valueAtRow(rRow, idx)
				dst++
			}
		})
	}
	m.replaceAll(newHeaders, rows)
	return m
}

// LeftJoin performs a left join in place.
func (m *MutableTable) LeftJoin(other Table, leftCol, rightCol string) *MutableTable {
	rightKeyIdx := other.ColIndex(rightCol)
	leftKeyIdx := m.ColIndex(leftCol)
	if leftKeyIdx < 0 {
		m.addErrf("LeftJoin: unknown column %q", leftCol)
		return m
	}
	if rightKeyIdx < 0 {
		m.addErrf("LeftJoin: unknown column %q in other table", rightCol)
		return m
	}
	rightIdx := make(map[string]rowBucket, len(other.Rows))
	for _, row := range other.Rows {
		key := valueAtRow(row.values, rightKeyIdx)
		addRowBucket(rightIdx, key, row.values)
	}
	newHeaders := copyHeaders(m.headers)
	rightExtraIdx := make([]int, 0, len(other.Headers))
	for i, h := range other.Headers {
		if h != rightCol {
			newHeaders = append(newHeaders, h)
			rightExtraIdx = append(rightExtraIdx, i)
		}
	}
	rowCount := 0
	for _, lRow := range m.rows {
		n := rightIdx[valueAt(lRow, leftKeyIdx)].len()
		if n == 0 {
			n = 1
		}
		rowCount += n
	}
	rows := newPackedRecords(rowCount, len(newHeaders))
	out := 0
	for _, lRow := range m.rows {
		key := valueAt(lRow, leftKeyIdx)
		bucket := rightIdx[key]
		if bucket.len() == 0 {
			vals := rows[out]
			out++
			copyLen := len(lRow)
			if copyLen > len(m.headers) {
				copyLen = len(m.headers)
			}
			copy(vals[:copyLen], lRow[:copyLen])
			continue
		}
		forEachRowBucket(bucket, func(rRow slice.Slice[string]) {
			vals := rows[out]
			out++
			copyLen := len(lRow)
			if copyLen > len(m.headers) {
				copyLen = len(m.headers)
			}
			copy(vals[:copyLen], lRow[:copyLen])
			dst := len(m.headers)
			for _, idx := range rightExtraIdx {
				vals[dst] = valueAtRow(rRow, idx)
				dst++
			}
		})
	}
	m.replaceAll(newHeaders, rows)
	return m
}

// RightJoin performs a right join in place.
func (m *MutableTable) RightJoin(other Table, leftCol, rightCol string) *MutableTable {
	leftKeyIdx := m.ColIndex(leftCol)
	rightKeyIdx := other.ColIndex(rightCol)
	if leftKeyIdx < 0 {
		m.addErrf("RightJoin: unknown column %q", leftCol)
		return m
	}
	if rightKeyIdx < 0 {
		m.addErrf("RightJoin: unknown column %q in other table", rightCol)
		return m
	}
	leftIdx := make(map[string]rowBucket, len(m.rows))
	for _, row := range m.rows {
		key := valueAt(row, leftKeyIdx)
		addRowBucket(leftIdx, key, row)
	}
	newHeaders := copyHeaders(m.headers)
	rightExtraIdx := make([]int, 0, len(other.Headers))
	for i, h := range other.Headers {
		if h != rightCol {
			newHeaders = append(newHeaders, h)
			rightExtraIdx = append(rightExtraIdx, i)
		}
	}
	leftColPos := m.ColIndex(leftCol)
	rowCount := 0
	for _, rRow := range other.Rows {
		n := leftIdx[valueAtRow(rRow.values, rightKeyIdx)].len()
		if n == 0 {
			n = 1
		}
		rowCount += n
	}
	rows := newPackedRecords(rowCount, len(newHeaders))
	out := 0
	for _, rRow := range other.Rows {
		key := valueAtRow(rRow.values, rightKeyIdx)
		bucket := leftIdx[key]
		if bucket.len() == 0 {
			vals := rows[out]
			out++
			if leftColPos >= 0 && leftColPos < len(vals) {
				vals[leftColPos] = key
			}
			dst := len(m.headers)
			for _, idx := range rightExtraIdx {
				vals[dst] = valueAtRow(rRow.values, idx)
				dst++
			}
			continue
		}
		forEachRowBucket(bucket, func(lRow slice.Slice[string]) {
			vals := rows[out]
			out++
			copyLen := len(lRow)
			if copyLen > len(m.headers) {
				copyLen = len(m.headers)
			}
			copy(vals[:copyLen], lRow[:copyLen])
			dst := len(m.headers)
			for _, idx := range rightExtraIdx {
				vals[dst] = valueAtRow(rRow.values, idx)
				dst++
			}
		})
	}
	m.replaceAll(newHeaders, rows)
	return m
}

// OuterJoin performs a full outer join in place.
func (m *MutableTable) OuterJoin(other Table, leftCol, rightCol string) *MutableTable {
	leftKeyIdx := m.ColIndex(leftCol)
	rightKeyIdx := other.ColIndex(rightCol)
	if leftKeyIdx < 0 {
		m.addErrf("OuterJoin: unknown column %q", leftCol)
		return m
	}
	if rightKeyIdx < 0 {
		m.addErrf("OuterJoin: unknown column %q in other table", rightCol)
		return m
	}

	rightIdx := make(map[string]rowBucket, len(other.Rows))
	for _, row := range other.Rows {
		key := valueAtRow(row.values, rightKeyIdx)
		addRowBucket(rightIdx, key, row.values)
	}

	newHeaders := copyHeaders(m.headers)
	rightExtraIdx := make([]int, 0, len(other.Headers))
	for i, h := range other.Headers {
		if h != rightCol {
			newHeaders = append(newHeaders, h)
			rightExtraIdx = append(rightExtraIdx, i)
		}
	}

	rowCount := 0
	leftKeys := make(map[string]bool, len(m.rows))
	for _, lRow := range m.rows {
		key := valueAt(lRow, leftKeyIdx)
		leftKeys[key] = true
		n := rightIdx[key].len()
		if n == 0 {
			n = 1
		}
		rowCount += n
	}
	leftColPos := leftKeyIdx
	for _, rRow := range other.Rows {
		key := valueAtRow(rRow.values, rightKeyIdx)
		if leftKeys[key] {
			continue
		}
		rowCount++
	}
	rows := newPackedRecords(rowCount, len(newHeaders))
	out := 0
	for _, lRow := range m.rows {
		key := valueAt(lRow, leftKeyIdx)
		bucket := rightIdx[key]
		if bucket.len() == 0 {
			vals := rows[out]
			out++
			copyLen := len(lRow)
			if copyLen > len(m.headers) {
				copyLen = len(m.headers)
			}
			copy(vals[:copyLen], lRow[:copyLen])
			continue
		}
		forEachRowBucket(bucket, func(rRow slice.Slice[string]) {
			vals := rows[out]
			out++
			copyLen := len(lRow)
			if copyLen > len(m.headers) {
				copyLen = len(m.headers)
			}
			copy(vals[:copyLen], lRow[:copyLen])
			dst := len(m.headers)
			for _, idx := range rightExtraIdx {
				vals[dst] = valueAtRow(rRow, idx)
				dst++
			}
		})
	}
	for _, rRow := range other.Rows {
		key := valueAtRow(rRow.values, rightKeyIdx)
		if leftKeys[key] {
			continue
		}
		rec := rows[out]
		out++
		if leftColPos >= 0 && leftColPos < len(rec) {
			rec[leftColPos] = key
		}
		dst := len(m.headers)
		for _, idx := range rightExtraIdx {
			rec[dst] = valueAtRow(rRow.values, idx)
			dst++
		}
	}
	m.replaceAll(newHeaders, rows)
	return m
}

// AntiJoin keeps only rows without a match in other.
func (m *MutableTable) AntiJoin(other Table, leftCol, rightCol string) *MutableTable {
	leftKeyIdx := m.ColIndex(leftCol)
	rightKeyIdx := other.ColIndex(rightCol)
	if leftKeyIdx < 0 {
		m.addErrf("AntiJoin: unknown column %q", leftCol)
		return m
	}
	if rightKeyIdx < 0 {
		m.addErrf("AntiJoin: unknown column %q in other table", rightCol)
		return m
	}
	rightKeys := make(map[string]bool, len(other.Rows))
	for _, row := range other.Rows {
		key := ""
		if rightKeyIdx < len(row.values) {
			key = row.values[rightKeyIdx]
		}
		rightKeys[key] = true
	}
	rows := make([][]string, 0, len(m.rows))
	for _, row := range m.rows {
		key := ""
		if leftKeyIdx < len(row) {
			key = row[leftKeyIdx]
		}
		if !rightKeys[key] {
			rows = append(rows, row)
		}
	}
	m.rows = rows
	return m
}

// ValueCounts replaces the table with a frequency table.
func (m *MutableTable) ValueCounts(col string) *MutableTable {
	idx, ok := m.headerIdx[col]
	if !ok {
		m.addErrf("ValueCounts: unknown column %q", col)
		m.replaceAll(slice.Slice[string]{"value", "count"}, [][]string{})
		return m
	}
	counts := make(map[string]int)
	order := make([]string, 0, len(m.rows))
	for _, row := range m.rows {
		v := valueAt(row, idx)
		if counts[v] == 0 {
			order = append(order, v)
		}
		counts[v]++
	}
	sort.SliceStable(order, func(i, j int) bool {
		return counts[order[i]] > counts[order[j]]
	})
	rows := newPackedRecords(len(order), 2)
	for i, v := range order {
		rows[i][0] = v
		rows[i][1] = strconv.Itoa(counts[v])
	}
	m.replaceAll(slice.Slice[string]{"value", "count"}, rows)
	return m
}

// Melt converts wide format to long format in place.
func (m *MutableTable) Melt(idCols []string, varName, valName string) *MutableTable {
	idSet := make(map[string]bool, len(idCols))
	idIdx := make([]int, len(idCols))
	for i, col := range idCols {
		idSet[col] = true
		idIdx[i] = m.ColIndex(col)
	}
	type meltCol struct {
		name string
		idx  int
	}
	meltCols := make([]meltCol, 0, len(m.headers)-len(idCols))
	for i, h := range m.headers {
		if !idSet[h] {
			meltCols = append(meltCols, meltCol{name: h, idx: i})
		}
	}
	newHeaders := make(slice.Slice[string], 0, len(idCols)+2)
	newHeaders = append(newHeaders, idCols...)
	newHeaders = append(newHeaders, varName, valName)
	rows := newPackedRecords(len(m.rows)*len(meltCols), len(newHeaders))
	out := 0
	for _, row := range m.rows {
		for _, mc := range meltCols {
			rec := rows[out]
			out++
			dst := 0
			for _, idx := range idIdx {
				rec[dst] = valueAt(row, idx)
				dst++
			}
			rec[dst] = mc.name
			rec[dst+1] = valueAt(row, mc.idx)
		}
	}
	m.replaceAll(newHeaders, rows)
	return m
}

// Pivot converts long format to wide format in place.
func (m *MutableTable) Pivot(index, col, val string) *MutableTable {
	indexIdx := m.ColIndex(index)
	colIdx := m.ColIndex(col)
	valIdx := m.ColIndex(val)
	if indexIdx < 0 {
		m.addErrf("Pivot: unknown column %q", index)
	}
	if colIdx < 0 {
		m.addErrf("Pivot: unknown column %q", col)
	}
	if valIdx < 0 {
		m.addErrf("Pivot: unknown column %q", val)
	}
	if indexIdx < 0 || colIdx < 0 || valIdx < 0 {
		return m
	}
	colVals := make([]string, 0, len(m.rows))
	colSeen := make(map[string]bool)
	for _, row := range m.rows {
		c := ""
		if colIdx < len(row) {
			c = row[colIdx]
		}
		if !colSeen[c] {
			colSeen[c] = true
			colVals = append(colVals, c)
		}
	}
	newHeaders := make(slice.Slice[string], 0, 1+len(colVals))
	newHeaders = append(newHeaders, index)
	newHeaders = append(newHeaders, colVals...)
	type entry struct {
		vals map[string]string
	}
	rowMap := make(map[string]*entry)
	rowOrder := make([]string, 0, len(m.rows))
	for _, row := range m.rows {
		idxVal, colVal, cellVal := "", "", ""
		if indexIdx < len(row) {
			idxVal = row[indexIdx]
		}
		if colIdx < len(row) {
			colVal = row[colIdx]
		}
		if valIdx < len(row) {
			cellVal = row[valIdx]
		}
		e, ok := rowMap[idxVal]
		if !ok {
			e = &entry{vals: make(map[string]string)}
			rowMap[idxVal] = e
			rowOrder = append(rowOrder, idxVal)
		}
		e.vals[colVal] = cellVal
	}
	rows := newPackedRecords(len(rowOrder), len(newHeaders))
	for i, idxVal := range rowOrder {
		rec := rows[i]
		rec[0] = idxVal
		dst := 1
		for _, cv := range colVals {
			rec[dst] = rowMap[idxVal].vals[cv]
			dst++
		}
	}
	m.replaceAll(newHeaders, rows)
	return m
}

// GroupByAgg groups in place and replaces the table with the aggregate result.
func (m *MutableTable) GroupByAgg(groupCols []string, aggs []AggDef) *MutableTable {
	type groupEntry struct {
		keyVals []string
		plans   []aggPlan
	}
	groupIdx := make([]int, len(groupCols))
	for i, col := range groupCols {
		idx, ok := m.headerIdx[col]
		if !ok {
			m.addErrf("GroupByAgg: unknown column %q", col)
			m.replaceAll(slice.Slice[string]{}, [][]string{})
			return m
		}
		groupIdx[i] = idx
	}
	ordered := make([]*groupEntry, 0, len(m.rows))
	headerIdx := aggHeaderIndex(m.headerIdx)
	switch len(groupIdx) {
	case 1:
		groupMap := make(map[string]*groupEntry, len(m.rows))
		idx := groupIdx[0]
		for _, row := range m.rows {
			key := valueAt(row, idx)
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
					e.plans[i].state.step(valueAt(row, e.plans[i].colIdx))
				}
			}
		}
	case 2:
		groupMap := make(map[pairKey]*groupEntry, len(m.rows))
		idx0, idx1 := groupIdx[0], groupIdx[1]
		for _, row := range m.rows {
			key := pairKey{a: valueAt(row, idx0), b: valueAt(row, idx1)}
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
					e.plans[i].state.step(valueAt(row, e.plans[i].colIdx))
				}
			}
		}
	default:
		index := make(map[string]*groupEntry)
		keyScratch := make([]byte, 0, len(groupCols)*8)
		for _, row := range m.rows {
			key, nextScratch := keyFromValues(row, groupIdx, keyScratch)
			keyScratch = nextScratch
			e, ok := index[key]
			if !ok {
				kv := make([]string, len(groupIdx))
				for i, idx := range groupIdx {
					kv[i] = valueAt(row, idx)
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
					e.plans[i].state.step(valueAt(row, e.plans[i].colIdx))
				}
			}
		}
	}
	newHeaders := make(slice.Slice[string], 0, len(groupCols)+len(aggs))
	newHeaders = append(newHeaders, groupCols...)
	for _, agg := range aggs {
		newHeaders = append(newHeaders, agg.Col)
	}
	rows := newPackedRecords(len(ordered), len(newHeaders))
	for i, entry := range ordered {
		rec := rows[i]
		copy(rec, entry.keyVals)
		dst := len(groupCols)
		for _, plan := range entry.plans {
			rec[dst] = plan.state.result()
			dst++
		}
	}
	m.replaceAll(newHeaders, rows)
	return m
}

// RollingAgg computes a rolling aggregation in place.
func (m *MutableTable) RollingAgg(outCol string, size int, agg Agg) *MutableTable {
	if size < 1 {
		size = 1
	}
	values := make([]string, len(m.rows))
	for i := range m.rows {
		start := i - size + 1
		if start < 0 {
			start = 0
		}
		window := mutableAggRows{headerIdx: m.headerIdx, rows: m.rows[start : i+1]}
		values[i] = agg.reduce(window)
	}
	m.appendDerivedCol(outCol, func(i int) string { return values[i] })
	return m
}

// Lag adds a lagged column in place.
func (m *MutableTable) Lag(col, outCol string, n int) *MutableTable {
	if n < 0 {
		n = 0
	}
	colIdx := m.ColIndex(col)
	if colIdx < 0 {
		m.addErrf("Lag: unknown column %q", col)
		return m
	}
	m.appendDerivedCol(outCol, func(i int) string {
		if i-n < 0 {
			return ""
		}
		prev := m.rows[i-n]
		if colIdx < len(prev) {
			return prev[colIdx]
		}
		return ""
	})
	return m
}

// Lead adds a lead column in place.
func (m *MutableTable) Lead(col, outCol string, n int) *MutableTable {
	if n < 0 {
		n = 0
	}
	colIdx := m.ColIndex(col)
	if colIdx < 0 {
		m.addErrf("Lead: unknown column %q", col)
		return m
	}
	m.appendDerivedCol(outCol, func(i int) string {
		if i+n >= len(m.rows) {
			return ""
		}
		next := m.rows[i+n]
		if colIdx < len(next) {
			return next[colIdx]
		}
		return ""
	})
	return m
}

// CumSum adds a running sum column in place.
func (m *MutableTable) CumSum(col, outCol string) *MutableTable {
	colIdx := m.ColIndex(col)
	if colIdx < 0 {
		m.addErrf("CumSum: unknown column %q", col)
		return m
	}
	values := make([]string, len(m.rows))
	var runningFloat float64
	var runningInt int64
	intOnly := true
	for i, row := range m.rows {
		entry := parseNumericEntry(valueAt(row, colIdx))
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
	m.appendDerivedCol(outCol, func(i int) string { return values[i] })
	return m
}

// Rank adds a dense rank column in place.
func (m *MutableTable) Rank(col, outCol string, asc bool) *MutableTable {
	colIdx := m.ColIndex(col)
	if colIdx < 0 {
		m.addErrf("Rank: unknown column %q", col)
		return m
	}
	entries := make([]numericEntry, len(m.rows))
	for i, row := range m.rows {
		entries[i] = parseNumericEntry(valueAt(row, colIdx))
	}
	values := denseRankValues(entries, asc)
	m.appendDerivedCol(outCol, func(i int) string { return values[i] })
	return m
}

// Partition splits the current table into immutable matching and rest tables.
func (m *MutableTable) Partition(fn func(Row) bool) (matched, rest Table) {
	mRows := make([][]string, 0, len(m.rows))
	rRows := make([][]string, 0, len(m.rows))
	for _, row := range m.rows {
		if fn(NewRow(m.headers, row)) {
			mRows = append(mRows, append([]string(nil), row...))
		} else {
			rRows = append(rRows, append([]string(nil), row...))
		}
	}
	return New(copyHeaders(m.headers), mRows), New(copyHeaders(m.headers), rRows)
}

// Chunk splits the current table into immutable chunks.
func (m *MutableTable) Chunk(n int) []Table {
	if n <= 0 || len(m.rows) == 0 {
		return []Table{New(copyHeaders(m.headers), cloneRecords(m.rows))}
	}
	chunks := make([]Table, 0, (len(m.rows)+n-1)/n)
	for i := 0; i < len(m.rows); i += n {
		end := i + n
		if end > len(m.rows) {
			end = len(m.rows)
		}
		chunks = append(chunks, New(copyHeaders(m.headers), cloneRecords(m.rows[i:end])))
	}
	return chunks
}

// ForEach iterates over current rows.
func (m *MutableTable) ForEach(fn func(i int, r Row)) {
	for i, row := range m.rows {
		fn(i, NewRow(m.headers, row))
	}
}

// AssertColumns validates required columns.
func (m *MutableTable) AssertColumns(cols ...string) *MutableTable {
	for _, col := range cols {
		if _, ok := m.headerIdx[col]; !ok {
			m.addErrf("AssertColumns: missing column %q", col)
		}
	}
	return m
}

// AssertNoEmpty validates that the requested columns are non-empty.
func (m *MutableTable) AssertNoEmpty(cols ...string) *MutableTable {
	check := cols
	if len(check) == 0 {
		check = m.headers
	}
	errsBefore := len(m.errs)
	m.AssertColumns(check...)
	if len(m.errs) > errsBefore {
		return m
	}
	checkIdx := make([]int, len(check))
	for i, col := range check {
		checkIdx[i] = m.headerIdx[col]
	}
	for ri, row := range m.rows {
		for i, idx := range checkIdx {
			v := ""
			if idx < len(row) {
				v = row[idx]
			}
			if v == "" {
				m.addErrf("AssertNoEmpty: row %d: column %q is empty", ri, check[i])
				return m
			}
		}
	}
	return m
}

// MapParallel maps a single column concurrently in place.
// Each worker processes a disjoint chunk of rows, writing directly to m.rows.
func (m *MutableTable) MapParallel(col string, fn func(string) string) *MutableTable {
	idx, ok := m.headerIdx[col]
	if !ok {
		m.addErrf("MapParallel: unknown column %q", col)
		return m
	}
	n := len(m.rows)
	if n == 0 {
		return m
	}
	workers := runtime.GOMAXPROCS(0)
	if workers > n {
		workers = n
	}
	chunkSize := (n + workers - 1) / workers
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		lo := w * chunkSize
		if lo >= n {
			break
		}
		hi := lo + chunkSize
		if hi > n {
			hi = n
		}
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			for i := lo; i < hi; i++ {
				if idx < len(m.rows[i]) {
					m.rows[i][idx] = fn(m.rows[i][idx])
				}
			}
		}(lo, hi)
	}
	wg.Wait()
	return m
}

// Predicate helpers mirror Table with direct mutable indices.
func (m *MutableTable) Eq(col, val string) func(Row) bool {
	idx, ok := m.headerIdx[col]
	if !ok {
		return func(Row) bool { return val == "" }
	}
	return func(r Row) bool {
		v := ""
		if idx < len(r.values) {
			v = r.values[idx]
		}
		return v == val
	}
}

func (m *MutableTable) Ne(col, val string) func(Row) bool {
	idx, ok := m.headerIdx[col]
	if !ok {
		return func(Row) bool { return val != "" }
	}
	return func(r Row) bool {
		v := ""
		if idx < len(r.values) {
			v = r.values[idx]
		}
		return v != val
	}
}

func (m *MutableTable) Contains(col, sub string) func(Row) bool {
	idx, ok := m.headerIdx[col]
	if !ok {
		return func(Row) bool { return strings.Contains("", sub) }
	}
	return func(r Row) bool {
		v := ""
		if idx < len(r.values) {
			v = r.values[idx]
		}
		return strings.Contains(v, sub)
	}
}

func (m *MutableTable) Prefix(col, prefix string) func(Row) bool {
	idx, ok := m.headerIdx[col]
	if !ok {
		return func(Row) bool { return strings.HasPrefix("", prefix) }
	}
	return func(r Row) bool {
		v := ""
		if idx < len(r.values) {
			v = r.values[idx]
		}
		return strings.HasPrefix(v, prefix)
	}
}

func (m *MutableTable) Suffix(col, suffix string) func(Row) bool {
	idx, ok := m.headerIdx[col]
	if !ok {
		return func(Row) bool { return strings.HasSuffix("", suffix) }
	}
	return func(r Row) bool {
		v := ""
		if idx < len(r.values) {
			v = r.values[idx]
		}
		return strings.HasSuffix(v, suffix)
	}
}

func (m *MutableTable) Matches(col, pattern string) func(Row) bool {
	re := regexp.MustCompile(pattern)
	idx, ok := m.headerIdx[col]
	if !ok {
		return func(Row) bool { return re.MatchString("") }
	}
	return func(r Row) bool {
		v := ""
		if idx < len(r.values) {
			v = r.values[idx]
		}
		return re.MatchString(v)
	}
}

func (m *MutableTable) Empty(col string) func(Row) bool {
	idx, ok := m.headerIdx[col]
	if !ok {
		return func(Row) bool { return true }
	}
	return func(r Row) bool {
		if idx < len(r.values) {
			return r.values[idx] == ""
		}
		return true
	}
}

func (m *MutableTable) NotEmpty(col string) func(Row) bool {
	idx, ok := m.headerIdx[col]
	if !ok {
		return func(Row) bool { return false }
	}
	return func(r Row) bool {
		if idx < len(r.values) {
			return r.values[idx] != ""
		}
		return false
	}
}

func (m *MutableTable) replaceAll(headers slice.Slice[string], rows [][]string) {
	headers = normalizeHeaders(headers)
	for i, row := range rows {
		rows[i] = clampRecordValues(row, len(headers))
	}
	m.headers = headers
	m.rows = rows
	m.headerIdx = buildHeaderIndex(headers)
}

func (m *MutableTable) rowViews() slice.Slice[Row] {
	rows := make(slice.Slice[Row], len(m.rows))
	for i, row := range m.rows {
		rows[i] = NewRow(m.headers, row)
	}
	return rows
}

func (m *MutableTable) appendDerivedCol(outCol string, valueAt func(i int) string) {
	newHeaders := make(slice.Slice[string], len(m.headers)+1)
	copy(newHeaders, m.headers)
	newHeaders[len(m.headers)] = outCol
	rows := make([][]string, len(m.rows))
	if len(m.rows) == 0 {
		m.replaceAll(newHeaders, rows)
		return
	}
	width := len(newHeaders)
	data := make([]string, len(m.rows)*width)
	for i, row := range m.rows {
		vals := data[i*width : (i+1)*width]
		copyLen := len(row)
		if copyLen > len(m.headers) {
			copyLen = len(m.headers)
		}
		copy(vals[:copyLen], row[:copyLen])
		vals[len(m.headers)] = valueAt(i)
		rows[i] = vals
	}
	m.replaceAll(newHeaders, rows)
}
