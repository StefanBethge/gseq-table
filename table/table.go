package table

import (
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
