package table

import "github.com/stefanbethge/gseq/slice"

// RightJoin keeps every row from other and attaches matching rows from t on
// leftCol = rightCol. Unmatched rows from other get empty strings for the
// left-side columns. The join-key column from other is excluded from the result.
//
//	// Keep all customers, attach orders where available
//	orders.RightJoin(customers, "customer_id", "id")
func (t Table) RightJoin(other Table, leftCol, rightCol string) Table {
	// index left table by leftCol
	leftIdx := make(map[string][]Row)
	for _, row := range t.Rows {
		key := row.Get(leftCol).UnwrapOr("")
		leftIdx[key] = append(leftIdx[key], row)
	}

	// merged headers: left headers + right headers (minus rightCol)
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
	for _, rRow := range other.Rows {
		key := rRow.Get(rightCol).UnwrapOr("")
		lRows := leftIdx[key]
		if len(lRows) == 0 {
			// unmatched right row: empty left columns, set join key
			vals := make(slice.Slice[string], 0, len(newHeaders))
			for _, h := range t.Headers {
				if h == leftCol {
					vals = append(vals, key) // carry the join key
				} else {
					vals = append(vals, "")
				}
			}
			for _, h := range rightExtra {
				vals = append(vals, rRow.Get(h).UnwrapOr(""))
			}
			rows = append(rows, NewRow(newHeaders, vals))
		} else {
			for _, lRow := range lRows {
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
	return newTable(newHeaders, rows)
}

// OuterJoin (full outer join) keeps every row from both tables. Rows that
// match on leftCol = rightCol are merged; unmatched rows from either side
// receive empty strings for the other side's columns.
//
//	t.OuterJoin(other, "id", "id")
func (t Table) OuterJoin(other Table, leftCol, rightCol string) Table {
	// start with a left join (all left rows + matched right rows)
	result := t.LeftJoin(other, leftCol, rightCol)

	// index left keys to find unmatched right rows
	leftKeys := make(map[string]bool, len(t.Rows))
	for _, row := range t.Rows {
		leftKeys[row.Get(leftCol).UnwrapOr("")] = true
	}

	// build the right-only rows aligned to result's headers
	var rightExtra []string
	for _, h := range other.Headers {
		if h != rightCol {
			rightExtra = append(rightExtra, h)
		}
	}

	var unmatched [][]string
	for _, rRow := range other.Rows {
		key := rRow.Get(rightCol).UnwrapOr("")
		if leftKeys[key] {
			continue // already covered by left join
		}
		rec := make([]string, len(result.Headers))
		// set the join key in the left-side position
		for i, h := range result.Headers {
			if h == leftCol {
				rec[i] = key
				break
			}
		}
		// fill right-side columns
		for _, h := range rightExtra {
			for i, lh := range result.Headers {
				if lh == h {
					rec[i] = rRow.Get(h).UnwrapOr("")
					break
				}
			}
		}
		unmatched = append(unmatched, rec)
	}

	if len(unmatched) == 0 {
		return result
	}
	return result.Append(New(result.Headers, unmatched))
}

// AntiJoin returns only the rows from t for which no matching row exists in
// other (leftCol = rightCol). This answers the question "what is in t but not
// in other?"
//
//	// Find orders without a matching customer
//	orders.AntiJoin(customers, "customer_id", "id")
func (t Table) AntiJoin(other Table, leftCol, rightCol string) Table {
	rightKeys := make(map[string]bool, len(other.Rows))
	for _, row := range other.Rows {
		rightKeys[row.Get(rightCol).UnwrapOr("")] = true
	}
	return t.Where(func(r Row) bool {
		return !rightKeys[r.Get(leftCol).UnwrapOr("")]
	})
}
