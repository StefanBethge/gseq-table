package table

import "github.com/stefanbethge/gseq/slice"

// TransformParallel applies fn to every row concurrently using GOMAXPROCS
// workers. Row order is preserved. Use this for CPU-intensive or
// I/O-bound per-row operations (e.g. external API lookups, heavy parsing).
//
// fn must be safe to call from multiple goroutines simultaneously.
//
//	t.TransformParallel(func(r table.Row) map[string]string {
//	    return map[string]string{"hash": sha256sum(r.Get("data").UnwrapOr(""))}
//	})
func (t Table) TransformParallel(fn func(Row) map[string]string) Table {
	rows := slice.MapParallel(t.Rows, func(row Row) Row {
		updates := fn(row)
		vals := make(slice.Slice[string], len(row.values))
		copy(vals, row.values)
		for col, v := range updates {
			if j, ok := t.headerIdx[col]; ok && j < len(vals) {
				vals[j] = v
			}
		}
		return NewRow(t.Headers, vals)
	})
	return newTable(t.Headers, rows)
}

// MapParallel applies fn to every value in col concurrently. Row order is
// preserved. fn must be safe to call from multiple goroutines.
//
//	t.MapParallel("url", func(v string) string { return fetchTitle(v) })
func (t Table) MapParallel(col string, fn func(string) string) Table {
	idx := colIdx(t, col)
	if idx < 0 {
		return t
	}
	rows := slice.MapParallel(t.Rows, func(row Row) Row {
		vals := make(slice.Slice[string], len(row.values))
		copy(vals, row.values)
		if idx < len(vals) {
			vals[idx] = fn(vals[idx])
		}
		return NewRow(t.Headers, vals)
	})
	return newTable(t.Headers, rows)
}
