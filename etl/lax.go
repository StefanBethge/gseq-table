package etl

import (
	"fmt"

	"github.com/stefanbethge/gseq-table/table"
)

// laxTryMap applies fn to col on each row.
// Rows where fn returns an error are filtered out and logged to log.
// If the column is missing, a hard error is returned (short-circuits the pipeline).
func laxTryMap(t table.Table, col, step string, fn func(string) (string, error), log *ErrorLog) (table.Table, error) {
	idx := t.ColIndex(col)
	if idx < 0 {
		return t, fmt.Errorf("%s: unknown column %q", step, col)
	}

	source := t.Source()
	outRows := make([]table.Row, 0, len(t.Rows))
	for i, row := range t.Rows {
		vals := make([]string, len(row.Values()))
		copy(vals, row.Values())
		if idx < len(vals) {
			newVal, err := fn(vals[idx])
			if err != nil {
				log.add(ErrorEntry{
					Source:      source,
					Step:        step,
					Row:         i,
					Err:         err,
					OriginalRow: row.ToMap(),
				})
				continue
			}
			vals[idx] = newVal
		}
		outRows = append(outRows, table.NewRow(t.Headers, vals))
	}
	out := table.NewFromRows(t.Headers, outRows)
	return out.CopyErrsFrom(t), nil
}

// laxTryTransform applies fn to each row.
// Rows where fn returns an error are filtered out and logged to log.
func laxTryTransform(t table.Table, step string, fn func(table.Row) (map[string]string, error), log *ErrorLog) table.Table {
	source := t.Source()
	outRows := make([]table.Row, 0, len(t.Rows))
	for i, row := range t.Rows {
		updates, err := fn(row)
		if err != nil {
			log.add(ErrorEntry{
				Source:      source,
				Step:        step,
				Row:         i,
				Err:         err,
				OriginalRow: row.ToMap(),
			})
			continue
		}
		vals := make([]string, len(row.Values()))
		copy(vals, row.Values())
		for col, v := range updates {
			if j := t.ColIndex(col); j >= 0 && j < len(vals) {
				vals[j] = v
			}
		}
		outRows = append(outRows, table.NewRow(t.Headers, vals))
	}
	result := table.NewFromRows(t.Headers, outRows)
	return result.CopyErrsFrom(t)
}
