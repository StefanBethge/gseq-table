package table

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

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
func (m *MutableTable) Select(cols ...string) {
	newHeaders := make(slice.Slice[string], 0, len(cols))
	indices := make([]int, 0, len(cols))
	for _, col := range cols {
		if idx, ok := m.headerIdx[col]; ok {
			newHeaders = append(newHeaders, col)
			indices = append(indices, idx)
		}
	}
	rows := make([][]string, len(m.rows))
	for i, row := range m.rows {
		vals := make([]string, len(indices))
		for j, idx := range indices {
			if idx < len(row) {
				vals[j] = row[idx]
			}
		}
		rows[i] = vals
	}
	m.replaceAll(newHeaders, rows)
}

// Where keeps only rows matching fn in place.
func (m *MutableTable) Where(fn func(Row) bool) {
	rows := make([][]string, 0, len(m.rows))
	for _, row := range m.rows {
		if fn(NewRow(m.headers, row)) {
			rows = append(rows, row)
		}
	}
	m.rows = rows
}

// GroupBy splits the current data into immutable sub-tables keyed by col.
func (m *MutableTable) GroupBy(col string) map[string]Table {
	idx, ok := m.headerIdx[col]
	if !ok {
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
func (m *MutableTable) Sort(col string, asc bool) {
	idx, ok := m.headerIdx[col]
	if !ok {
		return
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
}

// SortMulti sorts by multiple columns in place.
func (m *MutableTable) SortMulti(keys ...SortKey) {
	indices := make([]int, len(keys))
	for i, key := range keys {
		indices[i] = m.ColIndex(key.Col)
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
}

// Append appends other to m in place.
func (m *MutableTable) Append(other Table) {
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
}

// AppendMutable appends another mutable table to m in place.
func (m *MutableTable) AppendMutable(other *MutableTable) {
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
}

// Head keeps the first n rows in place.
func (m *MutableTable) Head(n int) {
	if n <= 0 {
		m.rows = [][]string{}
		return
	}
	if n >= len(m.rows) {
		return
	}
	m.rows = append([][]string(nil), m.rows[:n]...)
}

// Tail keeps the last n rows in place.
func (m *MutableTable) Tail(n int) {
	if n <= 0 {
		m.rows = [][]string{}
		return
	}
	if n >= len(m.rows) {
		return
	}
	m.rows = append([][]string(nil), m.rows[len(m.rows)-n:]...)
}

// DropEmpty removes rows with empty values in place.
func (m *MutableTable) DropEmpty(cols ...string) {
	check := cols
	if len(check) == 0 {
		check = m.headers
	}
	checkIdx := make([]int, 0, len(check))
	for _, col := range check {
		if idx, ok := m.headerIdx[col]; ok {
			checkIdx = append(checkIdx, idx)
		}
	}
	if len(checkIdx) == 0 {
		return
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
}

// Distinct removes duplicate rows in place.
func (m *MutableTable) Distinct(cols ...string) {
	check := cols
	if len(check) == 0 {
		check = m.headers
	}
	checkIdx := make([]int, len(check))
	for i, col := range check {
		idx, ok := m.headerIdx[col]
		if !ok {
			return
		}
		checkIdx[i] = idx
	}
	seen := make(map[string]bool)
	rows := make([][]string, 0, len(m.rows))
	for _, row := range m.rows {
		parts := make([]string, len(checkIdx))
		for i, idx := range checkIdx {
			if idx < len(row) {
				parts[i] = row[idx]
			}
		}
		key := strings.Join(parts, "\x00")
		if seen[key] {
			continue
		}
		seen[key] = true
		rows = append(rows, row)
	}
	m.rows = rows
}

// AddColSwitch appends a conditional column in place.
func (m *MutableTable) AddColSwitch(name string, cases []Case, else_ func(Row) string) {
	m.AddCol(name, func(r Row) string {
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
func (m *MutableTable) Transform(fn func(Row) map[string]string) {
	for i, row := range m.rows {
		updates := fn(NewRow(m.headers, row))
		for col, v := range updates {
			if idx, ok := m.headerIdx[col]; ok && idx < len(m.rows[i]) {
				m.rows[i][idx] = v
			}
		}
	}
}

// TransformParallel applies partial row updates concurrently in place.
func (m *MutableTable) TransformParallel(fn func(Row) map[string]string) {
	rowViews := m.rowViews()
	temp := slice.MapParallel(rowViews, func(row Row) []string {
		vals := append([]string(nil), row.values...)
		updates := fn(row)
		for col, v := range updates {
			if idx, ok := m.headerIdx[col]; ok && idx < len(vals) {
				vals[idx] = v
			}
		}
		return vals
	})
	m.rows = make([][]string, len(temp))
	copy(m.rows, temp)
}

// TryTransform applies partial row updates and leaves m unchanged on error.
func (m *MutableTable) TryTransform(fn func(Row) (map[string]string, error)) error {
	rows := cloneRecords(m.rows)
	for i, row := range m.rows {
		updates, err := fn(NewRow(m.headers, row))
		if err != nil {
			return err
		}
		for col, v := range updates {
			if idx, ok := m.headerIdx[col]; ok && idx < len(rows[i]) {
				rows[i][idx] = v
			}
		}
	}
	m.rows = rows
	return nil
}

// TryMap maps a column in place and leaves m unchanged on error.
func (m *MutableTable) TryMap(col string, fn func(string) (string, error)) error {
	idx, ok := m.headerIdx[col]
	if !ok {
		return nil
	}
	rows := cloneRecords(m.rows)
	for i := range rows {
		if idx < len(rows[i]) {
			newVal, err := fn(rows[i][idx])
			if err != nil {
				return err
			}
			rows[i][idx] = newVal
		}
	}
	m.rows = rows
	return nil
}

// RenameMany renames multiple columns in place.
func (m *MutableTable) RenameMany(renames map[string]string) {
	for i, h := range m.headers {
		if newName, ok := renames[h]; ok {
			m.headers[i] = newName
		}
	}
	m.headerIdx = buildHeaderIndex(m.headers)
}

// AddRowIndex prepends a row-number column in place.
func (m *MutableTable) AddRowIndex(name string) {
	newHeaders := make(slice.Slice[string], 0, len(m.headers)+1)
	newHeaders = append(newHeaders, name)
	newHeaders = append(newHeaders, m.headers...)
	rows := make([][]string, len(m.rows))
	for i, row := range m.rows {
		vals := make([]string, 0, len(row)+1)
		vals = append(vals, strconv.Itoa(i))
		vals = append(vals, row...)
		rows[i] = vals
	}
	m.replaceAll(newHeaders, rows)
}

// Explode splits one column into multiple rows in place.
func (m *MutableTable) Explode(col, sep string) {
	idx, ok := m.headerIdx[col]
	if !ok {
		return
	}
	rows := make([][]string, 0, len(m.rows))
	for _, row := range m.rows {
		val := ""
		if idx < len(row) {
			val = row[idx]
		}
		parts := splitNonEmpty(val, sep)
		if len(parts) == 0 {
			parts = []string{val}
		}
		for _, part := range parts {
			rec := make([]string, len(m.headers))
			copy(rec, row)
			rec[idx] = part
			rows = append(rows, rec)
		}
	}
	m.rows = rows
}

// Transpose pivots rows and columns in place.
func (m *MutableTable) Transpose() {
	newHeaders := make(slice.Slice[string], 0, len(m.rows)+1)
	newHeaders = append(newHeaders, "column")
	for i := range m.rows {
		newHeaders = append(newHeaders, strconv.Itoa(i))
	}
	rows := make([][]string, len(m.headers))
	for ci, col := range m.headers {
		rec := make([]string, 0, len(m.rows)+1)
		rec = append(rec, col)
		for _, row := range m.rows {
			v := ""
			if ci < len(row) {
				v = row[ci]
			}
			rec = append(rec, v)
		}
		rows[ci] = rec
	}
	m.replaceAll(newHeaders, rows)
}

// FillForward fills empty cells from the previous non-empty value.
func (m *MutableTable) FillForward(col string) {
	idx := m.ColIndex(col)
	if idx < 0 {
		return
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
}

// FillBackward fills empty cells from the next non-empty value.
func (m *MutableTable) FillBackward(col string) {
	idx := m.ColIndex(col)
	if idx < 0 {
		return
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
}

// Sample keeps a random sample in place.
func (m *MutableTable) Sample(n int) {
	if n >= len(m.rows) {
		return
	}
	sampled := slice.Slice[[]string](m.rows).Samples(n)
	m.rows = append([][]string(nil), sampled...)
}

// SampleFrac keeps a random sample fraction in place.
func (m *MutableTable) SampleFrac(f float64) {
	n := int(float64(len(m.rows)) * f)
	m.Sample(n)
}

// AddColFloat appends a float-derived column in place.
func (m *MutableTable) AddColFloat(name string, fn func(Row) float64) {
	m.AddCol(name, func(r Row) string {
		return strconv.FormatFloat(fn(r), 'f', -1, 64)
	})
}

// AddColInt appends an int-derived column in place.
func (m *MutableTable) AddColInt(name string, fn func(Row) int64) {
	m.AddCol(name, func(r Row) string {
		return strconv.FormatInt(fn(r), 10)
	})
}

// Coalesce appends the first non-empty value across cols.
func (m *MutableTable) Coalesce(name string, cols ...string) {
	m.AddCol(name, func(r Row) string {
		for _, col := range cols {
			if v := r.Get(col).UnwrapOr(""); v != "" {
				return v
			}
		}
		return ""
	})
}

// Lookup appends a lookup-derived column in place.
func (m *MutableTable) Lookup(col, outCol string, lookupTable Table, keyCol, valCol string) {
	keyIdx := lookupTable.ColIndex(keyCol)
	valIdx := lookupTable.ColIndex(valCol)
	colIdx := m.ColIndex(col)
	if keyIdx < 0 || valIdx < 0 || colIdx < 0 {
		return
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
	m.AddCol(outCol, func(r Row) string {
		k := ""
		if colIdx < len(r.values) {
			k = r.values[colIdx]
		}
		return lkp[k]
	})
}

// FormatCol formats parseable floats in place.
func (m *MutableTable) FormatCol(col string, precision int) {
	idx, ok := m.headerIdx[col]
	if !ok {
		return
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
}

// Intersect keeps rows that also appear in other.
func (m *MutableTable) Intersect(other Table, cols ...string) {
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
			return
		}
		mIdx[i] = mi
		oIdx[i] = oi
	}
	otherKeys := make(map[string]bool, len(other.Rows))
	parts := make([]string, len(check))
	for _, row := range other.Rows {
		for i, idx := range oIdx {
			parts[i] = ""
			if idx < len(row.values) {
				parts[i] = row.values[idx]
			}
		}
		otherKeys[strings.Join(parts, "\x00")] = true
	}
	rows := make([][]string, 0, len(m.rows))
	queryParts := make([]string, len(check))
	for _, row := range m.rows {
		for i, idx := range mIdx {
			queryParts[i] = ""
			if idx < len(row) {
				queryParts[i] = row[idx]
			}
		}
		if otherKeys[strings.Join(queryParts, "\x00")] {
			rows = append(rows, row)
		}
	}
	m.rows = rows
}

// Bin appends a bin label column in place.
func (m *MutableTable) Bin(col, name string, bins []BinDef) {
	colIdx := m.ColIndex(col)
	if colIdx < 0 {
		return
	}
	m.AddCol(name, func(r Row) string {
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
func (m *MutableTable) Join(other Table, leftCol, rightCol string) {
	rightKeyIdx := other.ColIndex(rightCol)
	leftKeyIdx := m.ColIndex(leftCol)
	if rightKeyIdx < 0 || leftKeyIdx < 0 {
		return
	}
	rightIdx := make(map[string][][]string, len(other.Rows))
	for _, row := range other.Rows {
		key := ""
		if rightKeyIdx < len(row.values) {
			key = row.values[rightKeyIdx]
		}
		rightIdx[key] = append(rightIdx[key], append([]string(nil), row.values...))
	}
	newHeaders := copyHeaders(m.headers)
	rightExtraIdx := make([]int, 0, len(other.Headers))
	for i, h := range other.Headers {
		if h != rightCol {
			newHeaders = append(newHeaders, h)
			rightExtraIdx = append(rightExtraIdx, i)
		}
	}
	rows := make([][]string, 0, len(m.rows))
	for _, lRow := range m.rows {
		key := ""
		if leftKeyIdx < len(lRow) {
			key = lRow[leftKeyIdx]
		}
		rRows, ok := rightIdx[key]
		if !ok {
			continue
		}
		for _, rRow := range rRows {
			vals := make([]string, 0, len(newHeaders))
			vals = append(vals, lRow...)
			for _, idx := range rightExtraIdx {
				if idx < len(rRow) {
					vals = append(vals, rRow[idx])
				} else {
					vals = append(vals, "")
				}
			}
			rows = append(rows, vals)
		}
	}
	m.replaceAll(newHeaders, rows)
}

// LeftJoin performs a left join in place.
func (m *MutableTable) LeftJoin(other Table, leftCol, rightCol string) {
	rightKeyIdx := other.ColIndex(rightCol)
	leftKeyIdx := m.ColIndex(leftCol)
	if rightKeyIdx < 0 || leftKeyIdx < 0 {
		return
	}
	rightIdx := make(map[string][][]string, len(other.Rows))
	for _, row := range other.Rows {
		key := ""
		if rightKeyIdx < len(row.values) {
			key = row.values[rightKeyIdx]
		}
		rightIdx[key] = append(rightIdx[key], append([]string(nil), row.values...))
	}
	newHeaders := copyHeaders(m.headers)
	rightExtraIdx := make([]int, 0, len(other.Headers))
	for i, h := range other.Headers {
		if h != rightCol {
			newHeaders = append(newHeaders, h)
			rightExtraIdx = append(rightExtraIdx, i)
		}
	}
	rows := make([][]string, 0, len(m.rows))
	for _, lRow := range m.rows {
		key := ""
		if leftKeyIdx < len(lRow) {
			key = lRow[leftKeyIdx]
		}
		rRows := rightIdx[key]
		if len(rRows) == 0 {
			vals := make([]string, 0, len(newHeaders))
			vals = append(vals, lRow...)
			for range rightExtraIdx {
				vals = append(vals, "")
			}
			rows = append(rows, vals)
			continue
		}
		for _, rRow := range rRows {
			vals := make([]string, 0, len(newHeaders))
			vals = append(vals, lRow...)
			for _, idx := range rightExtraIdx {
				if idx < len(rRow) {
					vals = append(vals, rRow[idx])
				} else {
					vals = append(vals, "")
				}
			}
			rows = append(rows, vals)
		}
	}
	m.replaceAll(newHeaders, rows)
}

// RightJoin performs a right join in place.
func (m *MutableTable) RightJoin(other Table, leftCol, rightCol string) {
	leftKeyIdx := m.ColIndex(leftCol)
	rightKeyIdx := other.ColIndex(rightCol)
	if leftKeyIdx < 0 || rightKeyIdx < 0 {
		return
	}
	leftIdx := make(map[string][][]string, len(m.rows))
	for _, row := range m.rows {
		key := ""
		if leftKeyIdx < len(row) {
			key = row[leftKeyIdx]
		}
		leftIdx[key] = append(leftIdx[key], append([]string(nil), row...))
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
	rows := make([][]string, 0, len(other.Rows))
	for _, rRow := range other.Rows {
		key := ""
		if rightKeyIdx < len(rRow.values) {
			key = rRow.values[rightKeyIdx]
		}
		lRows := leftIdx[key]
		if len(lRows) == 0 {
			vals := make([]string, len(m.headers), len(newHeaders))
			if leftColPos >= 0 && leftColPos < len(vals) {
				vals[leftColPos] = key
			}
			for _, idx := range rightExtraIdx {
				v := ""
				if idx < len(rRow.values) {
					v = rRow.values[idx]
				}
				vals = append(vals, v)
			}
			rows = append(rows, vals)
			continue
		}
		for _, lRow := range lRows {
			vals := make([]string, 0, len(newHeaders))
			vals = append(vals, lRow...)
			for _, idx := range rightExtraIdx {
				v := ""
				if idx < len(rRow.values) {
					v = rRow.values[idx]
				}
				vals = append(vals, v)
			}
			rows = append(rows, vals)
		}
	}
	m.replaceAll(newHeaders, rows)
}

// OuterJoin performs a full outer join in place.
func (m *MutableTable) OuterJoin(other Table, leftCol, rightCol string) {
	leftKeyIdx := m.ColIndex(leftCol)
	rightKeyIdx := other.ColIndex(rightCol)
	if leftKeyIdx < 0 || rightKeyIdx < 0 {
		return
	}

	leftKeys := make(map[string]bool, len(m.rows))
	for _, row := range m.rows {
		key := ""
		if leftKeyIdx < len(row) {
			key = row[leftKeyIdx]
		}
		leftKeys[key] = true
	}

	newHeaders := copyHeaders(m.headers)
	rightExtraIdx := make([]int, 0, len(other.Headers))
	for i, h := range other.Headers {
		if h != rightCol {
			newHeaders = append(newHeaders, h)
			rightExtraIdx = append(rightExtraIdx, i)
		}
	}

	rows := make([][]string, 0, len(m.rows)+len(other.Rows))
	for _, lRow := range m.rows {
		key := ""
		if leftKeyIdx < len(lRow) {
			key = lRow[leftKeyIdx]
		}
		rMatches := make([][]string, 0)
		for _, rRow := range other.Rows {
			rKey := ""
			if rightKeyIdx < len(rRow.values) {
				rKey = rRow.values[rightKeyIdx]
			}
			if rKey == key {
				rMatches = append(rMatches, rRow.values)
			}
		}
		if len(rMatches) == 0 {
			vals := make([]string, 0, len(newHeaders))
			vals = append(vals, lRow...)
			for range rightExtraIdx {
				vals = append(vals, "")
			}
			rows = append(rows, vals)
			continue
		}
		for _, rRow := range rMatches {
			vals := make([]string, 0, len(newHeaders))
			vals = append(vals, lRow...)
			for _, idx := range rightExtraIdx {
				if idx < len(rRow) {
					vals = append(vals, rRow[idx])
				} else {
					vals = append(vals, "")
				}
			}
			rows = append(rows, vals)
		}
	}

	leftColPos := leftKeyIdx
	for _, rRow := range other.Rows {
		key := ""
		if rightKeyIdx < len(rRow.values) {
			key = rRow.values[rightKeyIdx]
		}
		if leftKeys[key] {
			continue
		}
		rec := make([]string, len(newHeaders))
		if leftColPos >= 0 && leftColPos < len(rec) {
			rec[leftColPos] = key
		}
		dst := len(m.headers)
		for _, idx := range rightExtraIdx {
			if idx < len(rRow.values) {
				rec[dst] = rRow.values[idx]
			}
			dst++
		}
		rows = append(rows, rec)
	}
	m.replaceAll(newHeaders, rows)
}

// AntiJoin keeps only rows without a match in other.
func (m *MutableTable) AntiJoin(other Table, leftCol, rightCol string) {
	leftKeyIdx := m.ColIndex(leftCol)
	rightKeyIdx := other.ColIndex(rightCol)
	if leftKeyIdx < 0 || rightKeyIdx < 0 {
		return
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
}

// ValueCounts replaces the table with a frequency table.
func (m *MutableTable) ValueCounts(col string) {
	idx, ok := m.headerIdx[col]
	if !ok {
		m.replaceAll(slice.Slice[string]{"value", "count"}, [][]string{})
		return
	}
	counts := make(map[string]int)
	order := make([]string, 0, len(m.rows))
	for _, row := range m.rows {
		v := ""
		if idx < len(row) {
			v = row[idx]
		}
		if counts[v] == 0 {
			order = append(order, v)
		}
		counts[v]++
	}
	rows := make([][]string, len(order))
	for i, v := range order {
		rows[i] = []string{v, strconv.Itoa(counts[v])}
	}
	m.replaceAll(slice.Slice[string]{"value", "count"}, rows)
	m.SortMulti(Desc("count"))
}

// Melt converts wide format to long format in place.
func (m *MutableTable) Melt(idCols []string, varName, valName string) {
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
	rows := make([][]string, 0, len(m.rows)*len(meltCols))
	for _, row := range m.rows {
		for _, mc := range meltCols {
			rec := make([]string, 0, len(newHeaders))
			for _, idx := range idIdx {
				v := ""
				if idx >= 0 && idx < len(row) {
					v = row[idx]
				}
				rec = append(rec, v)
			}
			v := ""
			if mc.idx < len(row) {
				v = row[mc.idx]
			}
			rec = append(rec, mc.name, v)
			rows = append(rows, rec)
		}
	}
	m.replaceAll(newHeaders, rows)
}

// Pivot converts long format to wide format in place.
func (m *MutableTable) Pivot(index, col, val string) {
	indexIdx := m.ColIndex(index)
	colIdx := m.ColIndex(col)
	valIdx := m.ColIndex(val)
	if indexIdx < 0 || colIdx < 0 || valIdx < 0 {
		return
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
	rows := make([][]string, len(rowOrder))
	for i, idxVal := range rowOrder {
		rec := make([]string, 0, len(newHeaders))
		rec = append(rec, idxVal)
		for _, cv := range colVals {
			rec = append(rec, rowMap[idxVal].vals[cv])
		}
		rows[i] = rec
	}
	m.replaceAll(newHeaders, rows)
}

// GroupByAgg groups in place and replaces the table with the aggregate result.
func (m *MutableTable) GroupByAgg(groupCols []string, aggs []AggDef) {
	type groupEntry struct {
		keyVals []string
		rows    [][]string
	}
	groupIdx := make([]int, len(groupCols))
	for i, col := range groupCols {
		idx, ok := m.headerIdx[col]
		if !ok {
			m.replaceAll(slice.Slice[string]{}, [][]string{})
			return
		}
		groupIdx[i] = idx
	}
	index := make(map[string]*groupEntry)
	keyOrder := make([]string, 0, len(m.rows))
	for _, row := range m.rows {
		parts := make([]string, len(groupCols))
		for i, idx := range groupIdx {
			if idx < len(row) {
				parts[i] = row[idx]
			}
		}
		key := strings.Join(parts, "\x00")
		if e, ok := index[key]; ok {
			e.rows = append(e.rows, row)
		} else {
			kv := append([]string(nil), parts...)
			index[key] = &groupEntry{keyVals: kv, rows: [][]string{row}}
			keyOrder = append(keyOrder, key)
		}
	}
	newHeaders := make(slice.Slice[string], 0, len(groupCols)+len(aggs))
	newHeaders = append(newHeaders, groupCols...)
	for _, agg := range aggs {
		newHeaders = append(newHeaders, agg.Col)
	}
	rows := make([][]string, len(keyOrder))
	for i, key := range keyOrder {
		entry := index[key]
		rec := make([]string, 0, len(newHeaders))
		rec = append(rec, entry.keyVals...)
		groupTable := mutableAggRows{headerIdx: m.headerIdx, rows: entry.rows}
		for _, agg := range aggs {
			rec = append(rec, agg.Agg.reduce(groupTable))
		}
		rows[i] = rec
	}
	m.replaceAll(newHeaders, rows)
}

// RollingAgg computes a rolling aggregation in place.
func (m *MutableTable) RollingAgg(outCol string, size int, agg Agg) {
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
}

// Lag adds a lagged column in place.
func (m *MutableTable) Lag(col, outCol string, n int) {
	if n < 0 {
		n = 0
	}
	colIdx := m.ColIndex(col)
	if colIdx < 0 {
		return
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
}

// Lead adds a lead column in place.
func (m *MutableTable) Lead(col, outCol string, n int) {
	if n < 0 {
		n = 0
	}
	colIdx := m.ColIndex(col)
	if colIdx < 0 {
		return
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
}

// CumSum adds a running sum column in place.
func (m *MutableTable) CumSum(col, outCol string) {
	colIdx := m.ColIndex(col)
	if colIdx < 0 {
		return
	}
	values := make([]string, len(m.rows))
	var running float64
	for i, row := range m.rows {
		if colIdx < len(row) {
			if f, err := strconv.ParseFloat(strings.TrimSpace(row[colIdx]), 64); err == nil {
				running += f
			}
		}
		values[i] = strconv.FormatFloat(running, 'f', -1, 64)
	}
	m.appendDerivedCol(outCol, func(i int) string { return values[i] })
}

// Rank adds a dense rank column in place.
func (m *MutableTable) Rank(col, outCol string, asc bool) {
	type entry struct {
		val   float64
		valid bool
	}
	colIdx := m.ColIndex(col)
	if colIdx < 0 {
		return
	}
	entries := make([]entry, len(m.rows))
	numericVals := make([]float64, 0, len(m.rows))
	seen := make(map[float64]bool)
	for i, row := range m.rows {
		v := ""
		if colIdx < len(row) {
			v = row[colIdx]
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		entries[i] = entry{val: f, valid: err == nil}
		if err == nil && !seen[f] {
			seen[f] = true
			numericVals = append(numericVals, f)
		}
	}
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
	values := make([]string, len(m.rows))
	for i := range m.rows {
		if entries[i].valid {
			values[i] = rankMap[entries[i].val]
		}
	}
	m.appendDerivedCol(outCol, func(i int) string { return values[i] })
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
func (m *MutableTable) AssertColumns(cols ...string) error {
	for _, col := range cols {
		if _, ok := m.headerIdx[col]; !ok {
			return fmt.Errorf("missing required column: %q", col)
		}
	}
	return nil
}

// AssertNoEmpty validates that the requested columns are non-empty.
func (m *MutableTable) AssertNoEmpty(cols ...string) error {
	check := cols
	if len(check) == 0 {
		check = m.headers
	}
	if err := m.AssertColumns(check...); err != nil {
		return err
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
				return fmt.Errorf("row %d: column %q is empty", ri, check[i])
			}
		}
	}
	return nil
}

// MapParallel maps a single column concurrently in place.
func (m *MutableTable) MapParallel(col string, fn func(string) string) error {
	idx, ok := m.headerIdx[col]
	if !ok {
		return fmt.Errorf("unknown column %q", col)
	}
	rowViews := m.rowViews()
	temp := slice.MapParallel(rowViews, func(row Row) []string {
		vals := append([]string(nil), row.values...)
		if idx < len(vals) {
			vals[idx] = fn(vals[idx])
		}
		return vals
	})
	m.rows = make([][]string, len(temp))
	copy(m.rows, temp)
	return nil
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
	for i, row := range m.rows {
		vals := make([]string, len(newHeaders))
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
