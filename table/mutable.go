package table

import (
	"fmt"

	"github.com/stefanbethge/gseq/slice"
)

// MutableTable is an opt-in, in-place variant of Table.
//
// Use MutableTable when you want to incrementally build or update rows
// without allocating a new Table on every step. Call Freeze to obtain an
// immutable Table snapshot again.
//
// For ownership-transfer or zero-copy flows, use MutableView / FreezeView.
type MutableTable struct {
	headers   slice.Slice[string]
	rows      [][]string
	headerIdx map[string]int
}

// NewMutable constructs a MutableTable from headers and records.
// Input slices are copied so later caller mutations do not affect the table.
func NewMutable(headers slice.Slice[string], records [][]string) *MutableTable {
	return newMutableOwned(copyHeaders(headers), cloneRecordsPacked(records))
}

// Mutable returns a mutable copy of t.
// Later in-place updates on the returned table do not affect t.
func (t Table) Mutable() *MutableTable {
	return newMutableOwned(copyHeaders(t.Headers), cloneRowValuesPacked(t.Rows))
}

// MutableView returns a mutable view onto t without copying row storage.
// Mutating the returned table also mutates t. The header slice is shared;
// operations that change the column structure (AddCol, Drop, Rename, etc.)
// on the view may invalidate the source Table.
// Use Mutable() for a fully independent copy.
func (t Table) MutableView() *MutableTable {
	rows := make([][]string, len(t.Rows))
	for i, row := range t.Rows {
		rows[i] = row.values
	}
	return newMutableOwned(t.Headers, rows)
}

// Freeze returns an immutable snapshot of m.
// The returned Table is isolated from subsequent changes to m.
func (m *MutableTable) Freeze() Table {
	return New(copyHeaders(m.headers), cloneRecordsPacked(m.rows))
}

// FreezeView returns an immutable view onto m without copying row storage.
// Later in-place updates to m that modify row values are reflected in
// the returned Table. Structural changes to m (Rename, AddCol, Drop)
// are NOT reflected and may make the view inconsistent.
// Use Freeze() for a fully independent snapshot.
func (m *MutableTable) FreezeView() Table {
	return newTable(m.headers, rowsToRowViews(m.headers, m.rows))
}

// Headers returns a copy of the column names.
func (m *MutableTable) Headers() slice.Slice[string] {
	return copyHeaders(m.headers)
}

// Len returns the number of rows.
func (m *MutableTable) Len() int { return len(m.rows) }

// Shape returns (rows, cols).
func (m *MutableTable) Shape() (int, int) { return len(m.rows), len(m.headers) }

// ColIndex returns the zero-based index of col, or -1 if it does not exist.
func (m *MutableTable) ColIndex(col string) int {
	if idx, ok := m.headerIdx[col]; ok {
		return idx
	}
	return -1
}

// Row returns row i as a Row view backed by the mutable storage.
func (m *MutableTable) Row(i int) (Row, bool) {
	if i < 0 || i >= len(m.rows) {
		return Row{}, false
	}
	return NewRow(m.headers, m.rows[i]), true
}

// Set updates a single cell in place.
// Short rows are extended with empty strings as needed.
func (m *MutableTable) Set(row int, col, val string) error {
	if row < 0 || row >= len(m.rows) {
		return fmt.Errorf("row %d out of range", row)
	}
	idx, ok := m.headerIdx[col]
	if !ok {
		return fmt.Errorf("unknown column %q", col)
	}
	m.ensureRowWidth(row, len(m.headers))
	m.rows[row][idx] = val
	return nil
}

// AppendRow appends values as a new row. The input slice is copied.
func (m *MutableTable) AppendRow(values []string) {
	m.rows = append(m.rows, append([]string(nil), values...))
}

// Rename renames a column in place.
func (m *MutableTable) Rename(old, new string) error {
	idx, ok := m.headerIdx[old]
	if !ok {
		return fmt.Errorf("unknown column %q", old)
	}
	m.headers[idx] = new
	m.headerIdx = buildHeaderIndex(m.headers)
	return nil
}

// Map transforms every value in col in place.
func (m *MutableTable) Map(col string, fn func(string) string) error {
	idx, ok := m.headerIdx[col]
	if !ok {
		return fmt.Errorf("unknown column %q", col)
	}
	for i := range m.rows {
		m.ensureRowWidth(i, len(m.headers))
		m.rows[i][idx] = fn(m.rows[i][idx])
	}
	return nil
}

// FillEmpty replaces empty values in col with val in place.
func (m *MutableTable) FillEmpty(col, val string) error {
	return m.Map(col, func(v string) string {
		if v == "" {
			return val
		}
		return v
	})
}

// AddCol appends a derived column in place.
func (m *MutableTable) AddCol(name string, fn func(Row) string) {
	oldHeaders := m.headers
	for i := range m.rows {
		m.ensureRowWidth(i, len(oldHeaders))
		view := NewRow(oldHeaders, m.rows[i])
		m.rows[i] = append(m.rows[i], fn(view))
	}
	m.headers = append(m.headers, name)
	m.headerIdx = buildHeaderIndex(m.headers)
}

// Drop removes the named columns in place. Unknown column names are ignored.
func (m *MutableTable) Drop(cols ...string) {
	if len(cols) == 0 {
		return
	}
	drop := make(map[string]bool, len(cols))
	for _, col := range cols {
		drop[col] = true
	}

	keepHeaders := make(slice.Slice[string], 0, len(m.headers))
	keepIdx := make([]int, 0, len(m.headers))
	for i, h := range m.headers {
		if !drop[h] {
			keepHeaders = append(keepHeaders, h)
			keepIdx = append(keepIdx, i)
		}
	}

	rows := make([][]string, len(m.rows))
	if len(keepIdx) > 0 {
		rows = newPackedRecords(len(m.rows), len(keepIdx))
		for i, row := range m.rows {
			vals := rows[i]
			for j, idx := range keepIdx {
				vals[j] = valueAt(row, idx)
			}
		}
	}

	m.headers = keepHeaders
	m.rows = rows
	m.headerIdx = buildHeaderIndex(m.headers)
}

func (m *MutableTable) ensureRowWidth(row, width int) {
	if len(m.rows[row]) >= width {
		return
	}
	padded := make([]string, width)
	copy(padded, m.rows[row])
	m.rows[row] = padded
}

func buildHeaderIndex(headers slice.Slice[string]) map[string]int {
	idx := make(map[string]int, len(headers))
	for i, h := range headers {
		idx[h] = i
	}
	return idx
}

func copyHeaders(headers slice.Slice[string]) slice.Slice[string] {
	return append(slice.Slice[string](nil), headers...)
}

func cloneRecords(records [][]string) [][]string {
	cloned := make([][]string, len(records))
	for i, row := range records {
		cloned[i] = append([]string(nil), row...)
	}
	return cloned
}

func cloneRecordsPacked(records [][]string) [][]string {
	rows := make([][]string, len(records))
	total := 0
	for _, row := range records {
		total += len(row)
	}
	flat := make([]string, total)
	pos := 0
	for i, row := range records {
		next := pos + len(row)
		rows[i] = flat[pos:next:next]
		copy(rows[i], row)
		pos = next
	}
	return rows
}

func cloneRowValuesPacked(rows slice.Slice[Row]) [][]string {
	records := make([][]string, len(rows))
	total := 0
	for _, row := range rows {
		total += len(row.values)
	}
	flat := make([]string, total)
	pos := 0
	for i, row := range rows {
		next := pos + len(row.values)
		records[i] = flat[pos:next:next]
		copy(records[i], row.values)
		pos = next
	}
	return records
}

func rowsToRowViews(headers slice.Slice[string], records [][]string) slice.Slice[Row] {
	rows := make(slice.Slice[Row], len(records))
	for i, row := range records {
		rows[i] = NewRow(headers, row)
	}
	return rows
}

func newMutableOwned(headers slice.Slice[string], rows [][]string) *MutableTable {
	return &MutableTable{
		headers:   headers,
		rows:      rows,
		headerIdx: buildHeaderIndex(headers),
	}
}

func newPackedRecords(rowCount, width int) [][]string {
	rows := make([][]string, rowCount)
	if rowCount == 0 || width == 0 {
		for i := range rows {
			rows[i] = []string{}
		}
		return rows
	}
	flat := make([]string, rowCount*width)
	for i := range rows {
		start := i * width
		end := start + width
		rows[i] = flat[start:end:end]
	}
	return rows
}
