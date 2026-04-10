package table

import "github.com/stefanbethge/gseq/slice"

// RightJoin keeps every row from other and attaches matching rows from t on
// leftCol = rightCol. Unmatched rows from other get empty strings for the
// left-side columns. The join-key column from other is excluded from the
// result. Remaining name collisions are disambiguated with numeric suffixes.
//
//	// Keep all customers, attach orders where available
//	orders.RightJoin(customers, "customer_id", "id")
func (t Table) RightJoin(other Table, leftCol, rightCol string) Table {
	// index left table by leftCol
	leftKeyIdx, lok := t.headerIdx[leftCol]
	if !lok {
		return t
	}
	leftIdx := make(map[string][]Row, len(t.Rows))
	for _, row := range t.Rows {
		key := ""
		if leftKeyIdx < len(row.values) {
			key = row.values[leftKeyIdx]
		}
		leftIdx[key] = append(leftIdx[key], row)
	}

	// merged headers: left headers + right headers (minus rightCol)
	newHeaders := make(slice.Slice[string], len(t.Headers))
	copy(newHeaders, t.Headers)
	var rightExtraIdx []int
	for i, h := range other.Headers {
		if h != rightCol {
			newHeaders = append(newHeaders, h)
			rightExtraIdx = append(rightExtraIdx, i)
		}
	}

	// position of leftCol in t.Headers (for unmatched rows)
	leftColPos := t.headerIdx[leftCol]

	rightKeyIdx, rok := other.headerIdx[rightCol]
	if !rok {
		return t
	}
	var rows slice.Slice[Row]
	for _, rRow := range other.Rows {
		key := ""
		if rightKeyIdx < len(rRow.values) {
			key = rRow.values[rightKeyIdx]
		}
		lRows := leftIdx[key]
		if len(lRows) == 0 {
			// unmatched right row: empty left columns, carry join key
			vals := make(slice.Slice[string], len(t.Headers), len(newHeaders))
			if leftColPos < len(vals) {
				vals[leftColPos] = key
			}
			for _, idx := range rightExtraIdx {
				v := ""
				if idx < len(rRow.values) {
					v = rRow.values[idx]
				}
				vals = append(vals, v)
			}
			rows = append(rows, NewRow(newHeaders, vals))
		} else {
			for _, lRow := range lRows {
				vals := make(slice.Slice[string], 0, len(newHeaders))
				vals = append(vals, lRow.values...)
				for _, idx := range rightExtraIdx {
					v := ""
					if idx < len(rRow.values) {
						v = rRow.values[idx]
					}
					vals = append(vals, v)
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
// receive empty strings for the other side's columns. Remaining name
// collisions are disambiguated with numeric suffixes.
//
//	t.OuterJoin(other, "id", "id")
func (t Table) OuterJoin(other Table, leftCol, rightCol string) Table {
	if _, ok := t.headerIdx[leftCol]; !ok {
		return t
	}
	if _, ok := other.headerIdx[rightCol]; !ok {
		return t
	}
	// start with a left join (all left rows + matched right rows)
	result := t.LeftJoin(other, leftCol, rightCol)

	// index left keys to find unmatched right rows
	leftKeyIdx := t.headerIdx[leftCol]
	leftKeys := make(map[string]bool, len(t.Rows))
	for _, row := range t.Rows {
		key := ""
		if leftKeyIdx < len(row.values) {
			key = row.values[leftKeyIdx]
		}
		leftKeys[key] = true
	}

	// pre-compute positions in result for the join key and each rightExtra column
	resultLeftColPos := result.headerIdx[leftCol]
	var rightExtraIdx []int // indices in other.Rows
	var rightExtraPos []int // positions in result.Headers
	for i, h := range other.Headers {
		if h != rightCol {
			rightExtraIdx = append(rightExtraIdx, i)
			rightExtraPos = append(rightExtraPos, result.headerIdx[h])
		}
	}

	rightKeyIdx := other.headerIdx[rightCol]
	var unmatched [][]string
	for _, rRow := range other.Rows {
		key := ""
		if rightKeyIdx < len(rRow.values) {
			key = rRow.values[rightKeyIdx]
		}
		if leftKeys[key] {
			continue
		}
		rec := make([]string, len(result.Headers))
		if resultLeftColPos >= 0 && resultLeftColPos < len(rec) {
			rec[resultLeftColPos] = key
		}
		for i, srcIdx := range rightExtraIdx {
			if srcIdx < len(rRow.values) && rightExtraPos[i] < len(rec) {
				rec[rightExtraPos[i]] = rRow.values[srcIdx]
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
	rightKeyIdx, rok := other.headerIdx[rightCol]
	if !rok {
		return t
	}
	rightKeys := make(map[string]bool, len(other.Rows))
	for _, row := range other.Rows {
		key := ""
		if rightKeyIdx < len(row.values) {
			key = row.values[rightKeyIdx]
		}
		rightKeys[key] = true
	}
	leftKeyIdx, lok := t.headerIdx[leftCol]
	if !lok {
		return t
	}
	return t.Where(func(r Row) bool {
		key := ""
		if leftKeyIdx < len(r.values) {
			key = r.values[leftKeyIdx]
		}
		return !rightKeys[key]
	})
}
