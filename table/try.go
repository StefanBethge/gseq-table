package table

import (
	"github.com/stefanbethge/gseq/result"
	"github.com/stefanbethge/gseq/slice"
)

// TryTransform is like Transform but fn may return an error. Processing stops
// at the first error and the error is returned in the Result.
//
//	res := t.TryTransform(func(r table.Row) (map[string]string, error) {
//	    price, err := strconv.ParseFloat(r.Get("raw_price").UnwrapOr(""), 64)
//	    if err != nil {
//	        return nil, fmt.Errorf("row %v: invalid price", r.Get("id"))
//	    }
//	    return map[string]string{"price": fmt.Sprintf("%.2f", price * 1.19)}, nil
//	})
func (t Table) TryTransform(fn func(Row) (map[string]string, error)) result.Result[Table, error] {
	rows := make(slice.Slice[Row], len(t.Rows))
	for i, row := range t.Rows {
		updates, err := fn(row)
		if err != nil {
			return result.Err[Table, error](err)
		}
		vals := make(slice.Slice[string], len(row.values))
		copy(vals, row.values)
		for col, v := range updates {
			if j, ok := t.headerIdx[col]; ok && j < len(vals) {
				vals[j] = v
			}
		}
		rows[i] = NewRow(t.Headers, vals)
	}
	return result.Ok[Table, error](newTable(t.Headers, rows))
}

// TryMap is like Map but fn may return an error. Processing stops at the
// first error.
//
//	res := t.TryMap("amount", func(v string) (string, error) {
//	    f, err := strconv.ParseFloat(v, 64)
//	    if err != nil {
//	        return "", fmt.Errorf("invalid amount %q", v)
//	    }
//	    return fmt.Sprintf("%.2f EUR", f), nil
//	})
func (t Table) TryMap(col string, fn func(string) (string, error)) result.Result[Table, error] {
	idx := colIdx(t, col)
	if idx < 0 {
		return result.Ok[Table, error](t)
	}
	rows := make(slice.Slice[Row], len(t.Rows))
	for i, row := range t.Rows {
		vals := make(slice.Slice[string], len(row.values))
		copy(vals, row.values)
		if idx < len(vals) {
			newVal, err := fn(vals[idx])
			if err != nil {
				return result.Err[Table, error](err)
			}
			vals[idx] = newVal
		}
		rows[i] = NewRow(t.Headers, vals)
	}
	return result.Ok[Table, error](newTable(t.Headers, rows))
}
