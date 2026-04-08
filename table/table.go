package table

import (
	"strconv"
	"strings"

	"github.com/stefanbethge/gseq/option"
	"github.com/stefanbethge/gseq/slice"
)

// Map transforms every value in col using fn, leaving other columns unchanged.
func (t Table) Map(col string, fn func(string) string) Table {
	idx := -1
	for i, h := range t.Headers {
		if h == col {
			idx = i
			break
		}
	}
	if idx < 0 {
		return t
	}
	rows := make(slice.Slice[Row], len(t.Rows))
	for i, row := range t.Rows {
		vals := make(slice.Slice[string], len(row.values))
		copy(vals, row.values)
		if idx < len(vals) {
			vals[idx] = fn(vals[idx])
		}
		rows[i] = NewRow(t.Headers, vals)
	}
	return Table{Headers: t.Headers, Rows: rows}
}

// AddCol appends a new column whose value per row is computed by fn.
func (t Table) AddCol(name string, fn func(Row) string) Table {
	newHeaders := make(slice.Slice[string], len(t.Headers)+1)
	copy(newHeaders, t.Headers)
	newHeaders[len(t.Headers)] = name
	rows := make(slice.Slice[Row], len(t.Rows))
	for i, row := range t.Rows {
		vals := make(slice.Slice[string], len(row.values)+1)
		copy(vals, row.values)
		vals[len(row.values)] = fn(row)
		rows[i] = NewRow(newHeaders, vals)
	}
	return Table{Headers: newHeaders, Rows: rows}
}

// GroupBy splits the table into sub-tables keyed by the value of col.
func (t Table) GroupBy(col string) map[string]Table {
	groups := make(map[string]Table)
	for _, row := range t.Rows {
		key := row.Get(col).UnwrapOr("")
		g := groups[key]
		if g.Headers == nil {
			g = Table{Headers: t.Headers, Rows: slice.Slice[Row]{}}
		}
		g.Rows = append(g.Rows, NewRow(t.Headers, row.values))
		groups[key] = g
	}
	return groups
}

// Sort returns a new table sorted by col. asc=true for ascending order.
// Sorting is lexicographic (string comparison).
func (t Table) Sort(col string, asc bool) Table {
	idx := -1
	for i, h := range t.Headers {
		if h == col {
			idx = i
			break
		}
	}
	if idx < 0 {
		return t
	}
	rows := make(slice.Slice[Row], len(t.Rows))
	copy(rows, t.Rows)
	rows = rows.SortBy(func(a, b Row) bool {
		av, bv := "", ""
		if idx < len(a.values) {
			av = a.values[idx]
		}
		if idx < len(b.values) {
			bv = b.values[idx]
		}
		if asc {
			return av < bv
		}
		return av > bv
	})
	return Table{Headers: t.Headers, Rows: rows}
}

// Join performs an inner join on leftCol = rightCol.
// The join key column from other is excluded from the result to avoid duplication.
func (t Table) Join(other Table, leftCol, rightCol string) Table {
	// build lookup index on right table
	rightIdx := make(map[string][]Row)
	for _, row := range other.Rows {
		key := row.Get(rightCol).UnwrapOr("")
		rightIdx[key] = append(rightIdx[key], row)
	}

	// merged headers: all left cols + right cols except rightCol
	newHeaders := make(slice.Slice[string], len(t.Headers))
	copy(newHeaders, t.Headers)
	for _, h := range other.Headers {
		if h != rightCol {
			newHeaders = append(newHeaders, h)
		}
	}

	var rows slice.Slice[Row]
	for _, lRow := range t.Rows {
		key := lRow.Get(leftCol).UnwrapOr("")
		rRows, ok := rightIdx[key]
		if !ok {
			continue
		}
		for _, rRow := range rRows {
			vals := make(slice.Slice[string], 0, len(newHeaders))
			vals = append(vals, lRow.values...)
			for _, h := range other.Headers {
				if h != rightCol {
					vals = append(vals, rRow.Get(h).UnwrapOr(""))
				}
			}
			rows = append(rows, NewRow(newHeaders, vals))
		}
	}
	if rows == nil {
		rows = slice.Slice[Row]{}
	}
	return Table{Headers: newHeaders, Rows: rows}
}

type Row struct {
	headers slice.Slice[string]
	values  slice.Slice[string]
}

func NewRow(headers, values slice.Slice[string]) Row {
	return Row{headers: headers, values: values}
}

func (r Row) Get(col string) option.Option[string] {
	for i, h := range r.headers {
		if h == col {
			if i < len(r.values) {
				return option.Some(r.values[i])
			}
			return option.None[string]()
		}
	}
	return option.None[string]()
}

func (r Row) At(i int) option.Option[string] {
	if i < 0 || i >= len(r.values) {
		return option.None[string]()
	}
	return option.Some(r.values[i])
}

func (r Row) Headers() slice.Slice[string] { return r.headers }
func (r Row) Values() slice.Slice[string]  { return r.values }

func (r Row) ToMap() map[string]string {
	m := make(map[string]string, len(r.headers))
	for i, h := range r.headers {
		if i < len(r.values) {
			m[h] = r.values[i]
		}
	}
	return m
}

type Table struct {
	Headers slice.Slice[string]
	Rows    slice.Slice[Row]
}

func New(headers slice.Slice[string], records [][]string) Table {
	rows := make(slice.Slice[Row], len(records))
	for i, rec := range records {
		rows[i] = NewRow(headers, rec)
	}
	return Table{Headers: headers, Rows: rows}
}

func (t Table) Select(cols ...string) Table {
	newHeaders := make(slice.Slice[string], 0, len(cols))
	indices := make([]int, 0, len(cols))
	for _, col := range cols {
		for i, h := range t.Headers {
			if h == col {
				newHeaders = append(newHeaders, col)
				indices = append(indices, i)
				break
			}
		}
	}
	rows := make(slice.Slice[Row], len(t.Rows))
	for ri, row := range t.Rows {
		vals := make(slice.Slice[string], len(indices))
		for vi, idx := range indices {
			if idx < len(row.values) {
				vals[vi] = row.values[idx]
			}
		}
		rows[ri] = NewRow(newHeaders, vals)
	}
	return Table{Headers: newHeaders, Rows: rows}
}

func (t Table) Where(fn func(Row) bool) Table {
	var rows slice.Slice[Row]
	for _, row := range t.Rows {
		if fn(row) {
			rows = append(rows, NewRow(t.Headers, row.values))
		}
	}
	if rows == nil {
		rows = slice.Slice[Row]{}
	}
	return Table{Headers: t.Headers, Rows: rows}
}

func (t Table) Col(name string) slice.Slice[string] {
	idx := -1
	for i, h := range t.Headers {
		if h == name {
			idx = i
			break
		}
	}
	if idx < 0 {
		return slice.Slice[string]{}
	}
	out := make(slice.Slice[string], len(t.Rows))
	for i, row := range t.Rows {
		if idx < len(row.values) {
			out[i] = row.values[idx]
		}
	}
	return out
}

func (t Table) Rename(old, new string) Table {
	idx := -1
	for i, h := range t.Headers {
		if h == old {
			idx = i
			break
		}
	}
	if idx < 0 {
		return t
	}
	newHeaders := make(slice.Slice[string], len(t.Headers))
	copy(newHeaders, t.Headers)
	newHeaders[idx] = new
	rows := make(slice.Slice[Row], len(t.Rows))
	for i, row := range t.Rows {
		rows[i] = NewRow(newHeaders, row.values)
	}
	return Table{Headers: newHeaders, Rows: rows}
}

func (t Table) Append(other Table) Table {
	rows := make(slice.Slice[Row], 0, len(t.Rows)+len(other.Rows))
	for _, row := range t.Rows {
		rows = append(rows, NewRow(t.Headers, row.values))
	}
	for _, row := range other.Rows {
		vals := make(slice.Slice[string], len(t.Headers))
		for i, h := range t.Headers {
			vals[i] = row.Get(h).UnwrapOr("")
		}
		rows = append(rows, NewRow(t.Headers, vals))
	}
	return Table{Headers: t.Headers, Rows: rows}
}

// --- Shape & inspection ---

// Len returns the number of rows.
func (t Table) Len() int { return len(t.Rows) }

// Shape returns (rows, cols).
func (t Table) Shape() (int, int) { return len(t.Rows), len(t.Headers) }

// Head returns the first n rows. If n >= Len, the full table is returned.
func (t Table) Head(n int) Table {
	if n >= len(t.Rows) {
		return t
	}
	return Table{Headers: t.Headers, Rows: t.Rows[:n]}
}

// Tail returns the last n rows. If n >= Len, the full table is returned.
func (t Table) Tail(n int) Table {
	if n >= len(t.Rows) {
		return t
	}
	return Table{Headers: t.Headers, Rows: t.Rows[len(t.Rows)-n:]}
}

// --- Column operations ---

// Drop removes the named columns and returns a new table.
func (t Table) Drop(cols ...string) Table {
	drop := make(map[string]bool, len(cols))
	for _, c := range cols {
		drop[c] = true
	}
	keep := make([]string, 0, len(t.Headers))
	for _, h := range t.Headers {
		if !drop[h] {
			keep = append(keep, h)
		}
	}
	return t.Select(keep...)
}

// --- Cleaning ---

// DropEmpty removes rows where any of the specified columns is an empty string.
// If no columns are given, rows with any empty value are removed.
func (t Table) DropEmpty(cols ...string) Table {
	check := cols
	if len(check) == 0 {
		check = t.Headers
	}
	return t.Where(func(r Row) bool {
		for _, c := range check {
			if r.Get(c).UnwrapOr("") == "" {
				return false
			}
		}
		return true
	})
}

// FillEmpty replaces empty string values in col with val.
func (t Table) FillEmpty(col, val string) Table {
	return t.Map(col, func(v string) string {
		if v == "" {
			return val
		}
		return v
	})
}

// Distinct removes duplicate rows. If cols are given, only those columns
// are considered when deciding uniqueness. Otherwise all columns are used.
func (t Table) Distinct(cols ...string) Table {
	check := cols
	if len(check) == 0 {
		check = t.Headers
	}
	seen := make(map[string]bool)
	var rows slice.Slice[Row]
	for _, row := range t.Rows {
		parts := make([]string, len(check))
		for i, c := range check {
			parts[i] = row.Get(c).UnwrapOr("")
		}
		key := strings.Join(parts, "\x00")
		if !seen[key] {
			seen[key] = true
			rows = append(rows, NewRow(t.Headers, row.values))
		}
	}
	if rows == nil {
		rows = slice.Slice[Row]{}
	}
	return Table{Headers: t.Headers, Rows: rows}
}

// --- Transformation ---

// Transform applies fn to each row. The returned map is used to update
// column values; columns not present in the map are left unchanged.
func (t Table) Transform(fn func(Row) map[string]string) Table {
	rows := make(slice.Slice[Row], len(t.Rows))
	for i, row := range t.Rows {
		updates := fn(row)
		vals := make(slice.Slice[string], len(row.values))
		copy(vals, row.values)
		for col, v := range updates {
			for j, h := range t.Headers {
				if h == col && j < len(vals) {
					vals[j] = v
					break
				}
			}
		}
		rows[i] = NewRow(t.Headers, vals)
	}
	return Table{Headers: t.Headers, Rows: rows}
}

// --- Multi-column sort ---

// SortKey specifies a column and sort direction for SortMulti.
type SortKey struct {
	Col string
	Asc bool
}

// Asc returns a SortKey for ascending order.
func Asc(col string) SortKey { return SortKey{Col: col, Asc: true} }

// Desc returns a SortKey for descending order.
func Desc(col string) SortKey { return SortKey{Col: col, Asc: false} }

// SortMulti sorts by multiple columns in priority order.
func (t Table) SortMulti(keys ...SortKey) Table {
	indices := make([]int, len(keys))
	for k, sk := range keys {
		indices[k] = -1
		for i, h := range t.Headers {
			if h == sk.Col {
				indices[k] = i
				break
			}
		}
	}
	rows := make(slice.Slice[Row], len(t.Rows))
	copy(rows, t.Rows)
	rows = rows.SortBy(func(a, b Row) bool {
		for k, sk := range keys {
			idx := indices[k]
			av, bv := "", ""
			if idx >= 0 {
				if idx < len(a.values) {
					av = a.values[idx]
				}
				if idx < len(b.values) {
					bv = b.values[idx]
				}
			}
			if av == bv {
				continue
			}
			if sk.Asc {
				return av < bv
			}
			return av > bv
		}
		return false
	})
	return Table{Headers: t.Headers, Rows: rows}
}

// --- Joins ---

// LeftJoin keeps all rows from t and matches rows from other on leftCol = rightCol.
// Unmatched rows get empty strings for the right-side columns.
func (t Table) LeftJoin(other Table, leftCol, rightCol string) Table {
	rightIdx := make(map[string][]Row)
	for _, row := range other.Rows {
		key := row.Get(rightCol).UnwrapOr("")
		rightIdx[key] = append(rightIdx[key], row)
	}
	newHeaders := make(slice.Slice[string], len(t.Headers))
	copy(newHeaders, t.Headers)
	var rightExtra []string
	for _, h := range other.Headers {
		if h != rightCol {
			newHeaders = append(newHeaders, h)
			rightExtra = append(rightExtra, h)
		}
	}
	var rows slice.Slice[Row]
	for _, lRow := range t.Rows {
		key := lRow.Get(leftCol).UnwrapOr("")
		rRows := rightIdx[key]
		if len(rRows) == 0 {
			vals := make(slice.Slice[string], 0, len(newHeaders))
			vals = append(vals, lRow.values...)
			for range rightExtra {
				vals = append(vals, "")
			}
			rows = append(rows, NewRow(newHeaders, vals))
		} else {
			for _, rRow := range rRows {
				vals := make(slice.Slice[string], 0, len(newHeaders))
				vals = append(vals, lRow.values...)
				for _, h := range rightExtra {
					vals = append(vals, rRow.Get(h).UnwrapOr(""))
				}
				rows = append(rows, NewRow(newHeaders, vals))
			}
		}
	}
	if rows == nil {
		rows = slice.Slice[Row]{}
	}
	return Table{Headers: newHeaders, Rows: rows}
}

// --- Aggregation ---

// ValueCounts returns a two-column table ("value", "count") with the
// frequency of each distinct value in col, sorted by count descending.
func (t Table) ValueCounts(col string) Table {
	counts := make(map[string]int)
	var order []string
	for _, row := range t.Rows {
		v := row.Get(col).UnwrapOr("")
		if counts[v] == 0 {
			order = append(order, v)
		}
		counts[v]++
	}
	headers := slice.Slice[string]{"value", "count"}
	records := make([][]string, len(order))
	for i, v := range order {
		records[i] = []string{v, strconv.Itoa(counts[v])}
	}
	return New(headers, records).SortMulti(Desc("count"))
}

// --- Reshape ---

// Melt converts a wide-format table to long format.
// idCols are kept as identifier columns; all other columns are stacked into
// varName (column name) and valName (cell value).
func (t Table) Melt(idCols []string, varName, valName string) Table {
	idSet := make(map[string]bool, len(idCols))
	for _, c := range idCols {
		idSet[c] = true
	}
	var meltCols []string
	for _, h := range t.Headers {
		if !idSet[h] {
			meltCols = append(meltCols, h)
		}
	}
	newHeaders := make(slice.Slice[string], 0, len(idCols)+2)
	for _, c := range idCols {
		newHeaders = append(newHeaders, c)
	}
	newHeaders = append(newHeaders, varName, valName)
	var records [][]string
	for _, row := range t.Rows {
		for _, mc := range meltCols {
			rec := make([]string, 0, len(newHeaders))
			for _, ic := range idCols {
				rec = append(rec, row.Get(ic).UnwrapOr(""))
			}
			rec = append(rec, mc, row.Get(mc).UnwrapOr(""))
			records = append(records, rec)
		}
	}
	return New(newHeaders, records)
}

// Pivot converts a long-format table to wide format.
// index becomes the row identifier, the unique values of col become new
// column headers, and val supplies the cell values.
func (t Table) Pivot(index, col, val string) Table {
	var colVals []string
	colSeen := make(map[string]bool)
	for _, row := range t.Rows {
		c := row.Get(col).UnwrapOr("")
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
	var rowOrder []string
	for _, row := range t.Rows {
		idxVal := row.Get(index).UnwrapOr("")
		colVal := row.Get(col).UnwrapOr("")
		cellVal := row.Get(val).UnwrapOr("")
		e, ok := rowMap[idxVal]
		if !ok {
			e = &entry{vals: make(map[string]string)}
			rowMap[idxVal] = e
			rowOrder = append(rowOrder, idxVal)
		}
		e.vals[colVal] = cellVal
	}
	records := make([][]string, len(rowOrder))
	for i, idxVal := range rowOrder {
		rec := make([]string, 0, len(newHeaders))
		rec = append(rec, idxVal)
		for _, cv := range colVals {
			rec = append(rec, rowMap[idxVal].vals[cv])
		}
		records[i] = rec
	}
	return New(newHeaders, records)
}
