package table

import (
	"github.com/stefanbethge/gseq/option"
	"github.com/stefanbethge/gseq/slice"
)

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
		rows[i] = NewRow(headers, slice.Slice[string](rec))
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
