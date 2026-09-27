package table

import "iter"

// Iterators for range-over-func loops. None of them copy cells: rows are
// yielded as views onto the table storage, the same as t.Rows[i] and
// MutableTable.Row. Stopping a loop early (break, return) stops the iteration.
// An unknown column yields an empty sequence and records no error, the same
// as Col.

// All returns an iterator over the row index and row of every row.
//
//	for i, r := range t.All() {
//	    fmt.Println(i, r.Get("name").UnwrapOr(""))
//	}
func (t Table) All() iter.Seq2[int, Row] {
	return func(yield func(int, Row) bool) {
		for i, row := range t.Rows {
			if !yield(i, row) {
				return
			}
		}
	}
}

// RowsSeq returns an iterator over every row.
//
//	for r := range t.RowsSeq() { ... }
func (t Table) RowsSeq() iter.Seq[Row] {
	return func(yield func(Row) bool) {
		for _, row := range t.Rows {
			if !yield(row) {
				return
			}
		}
	}
}

// ColSeq returns an iterator over the values of col, one per row. Missing
// cells yield "". Returns an empty sequence if col does not exist.
//
//	for city := range t.ColSeq("city") { ... }
func (t Table) ColSeq(col string) iter.Seq[string] {
	idx, ok := t.headerIdx[col]
	return func(yield func(string) bool) {
		if !ok {
			return
		}
		for _, row := range t.Rows {
			if !yield(valueAtRow(row.values, idx)) {
				return
			}
		}
	}
}

// ColSeqAs returns an iterator over the row index and value of col parsed as
// T. Empty and unparseable cells are skipped, as in ReduceAs; the index
// identifies the row. Returns an empty sequence if col does not exist.
//
//	for i, age := range t.ColSeqAs[int]("age") { ... }
//
// Use ColAs to fail on bad cells instead of skipping them.
func (t Table) ColSeqAs[T Value](col string) iter.Seq2[int, T] {
	idx, ok := t.headerIdx[col]
	return func(yield func(int, T) bool) {
		if !ok {
			return
		}
		for i, row := range t.Rows {
			if v, ok := parseValue[T](valueAtRow(row.values, idx)); ok && !yield(i, v) {
				return
			}
		}
	}
}

// All returns an iterator over the row index and row of every current row.
// Rows are views backed by the mutable storage, as returned by Row.
//
// The MutableTable iterators (All, RowsSeq, ColSeq, ColSeqAs) read the live
// table on every step instead of a snapshot taken when the loop starts:
//
//   - cell updates (Set, SetAs, Map, FillEmpty, ...) to rows not yet visited
//     are observed
//   - rows appended during the loop (AppendRow, AppendMap, Append) are
//     visited too, so appending on every step never terminates
//   - after operations that change the columns or reorder, filter or replace
//     the rows (Select, Drop, AddCol, Rename, Where, Sort, Head, Join, ...)
//     the loop continues at the next row index of the new layout, and column
//     iterators keep the column position resolved when the loop started, so
//     which values are yielded is unspecified. The loop never panics.
//
// Iterate m.Freeze() instead to be independent of changes.
//
//	for i, r := range m.All() {
//	    if r.Get("status").UnwrapOr("") == "" {
//	        m.Set(i, "status", "new")
//	    }
//	}
func (m *MutableTable) All() iter.Seq2[int, Row] {
	return func(yield func(int, Row) bool) {
		for i := 0; i < len(m.rows); i++ {
			if !yield(i, NewRow(m.headers, m.rows[i])) {
				return
			}
		}
	}
}

// RowsSeq returns an iterator over every current row. See MutableTable.All
// for the behavior when m is changed during iteration.
func (m *MutableTable) RowsSeq() iter.Seq[Row] {
	return func(yield func(Row) bool) {
		for i := 0; i < len(m.rows); i++ {
			if !yield(NewRow(m.headers, m.rows[i])) {
				return
			}
		}
	}
}

// ColSeq returns an iterator over the current values of col, one per row.
// Missing cells yield "". Returns an empty sequence if col does not exist.
// See MutableTable.All for the behavior when m is changed during iteration.
func (m *MutableTable) ColSeq(col string) iter.Seq[string] {
	idx, ok := m.headerIdx[col]
	return func(yield func(string) bool) {
		if !ok {
			return
		}
		for i := 0; i < len(m.rows); i++ {
			if !yield(valueAt(m.rows[i], idx)) {
				return
			}
		}
	}
}

// ColSeqAs returns an iterator over the row index and current value of col
// parsed as T, skipping empty and unparseable cells. See Table.ColSeqAs, and
// MutableTable.All for the behavior when m is changed during iteration.
func (m *MutableTable) ColSeqAs[T Value](col string) iter.Seq2[int, T] {
	idx, ok := m.headerIdx[col]
	return func(yield func(int, T) bool) {
		if !ok {
			return
		}
		for i := 0; i < len(m.rows); i++ {
			if v, ok := parseValue[T](valueAt(m.rows[i], idx)); ok && !yield(i, v) {
				return
			}
		}
	}
}
