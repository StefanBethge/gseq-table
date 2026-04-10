// Package table provides in-memory data tables with immutable and mutable APIs.
//
// Table exposes the functional, immutable API: all operations return new
// Tables and leave the original unchanged, making it safe to branch a
// transformation without defensive copies.
//
// MutableTable is the opt-in in-place variant for incremental updates and
// lower-allocation build paths. Call Table.Mutable to obtain a mutable copy
// and MutableTable.Freeze to return to an immutable Table.
//
// Every cell value is a plain string. Type conversions (string → int, etc.)
// are the caller's responsibility; helper packages may be added in the future.
//
// # Row access
//
//	row.Get("price")          // option.Option[string]
//	row.At(2)                 // option.Option[string]  (index-based)
//	row.ToMap()               // map[string]string
//
// # Table construction
//
//	t := table.New(
//	    []string{"name", "city"},
//	    [][]string{
//	        {"Alice", "Berlin"},
//	        {"Bob",   "Munich"},
//	    },
//	)
package table

import (
	"strconv"
	"strings"

	"github.com/stefanbethge/gseq/option"
	"github.com/stefanbethge/gseq/slice"
)

// Row is a single record in a Table.
// It holds a reference to the shared header slice so column lookups by name
// work without an extra map.
type Row struct {
	headers slice.Slice[string]
	values  slice.Slice[string]
}

// NewRow constructs a Row from a header slice and a value slice.
// Both slices are stored by reference; callers that need isolation should
// copy them before passing.
func NewRow(headers, values slice.Slice[string]) Row {
	return Row{headers: headers, values: values}
}

// Get returns the value for the named column, or None if the column does not
// exist or the row has no value at that index.
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

// At returns the value at position i (zero-based), or None if i is out of
// range.
func (r Row) At(i int) option.Option[string] {
	if i < 0 || i >= len(r.values) {
		return option.None[string]()
	}
	return option.Some(r.values[i])
}

// Headers returns the column names for this row.
func (r Row) Headers() slice.Slice[string] { return r.headers }

// Values returns the raw cell values for this row.
func (r Row) Values() slice.Slice[string] { return r.values }

// ToMap converts the row to a map[string]string keyed by column name.
func (r Row) ToMap() map[string]string {
	m := make(map[string]string, len(r.headers))
	for i, h := range r.headers {
		if i < len(r.values) {
			m[h] = r.values[i]
		}
	}
	return m
}

// Table is an ordered, in-memory collection of rows with a shared set of
// column headers.
//
// All methods are value receivers and return new Tables so transformations can
// be chained freely:
//
//	result := t.
//	    Where(func(r table.Row) bool { return r.Get("active").UnwrapOr("") == "true" }).
//	    Select("name", "email").
//	    Sort("name", true)
type Table struct {
	Headers   slice.Slice[string]
	Rows      slice.Slice[Row]
	headerIdx map[string]int
}

// newTable is the internal constructor. It builds the header index once so all
// subsequent column lookups are O(1) instead of O(k).
func newTable(headers slice.Slice[string], rows slice.Slice[Row]) Table {
	idx := make(map[string]int, len(headers))
	for i, h := range headers {
		idx[h] = i
	}
	return Table{Headers: headers, Rows: rows, headerIdx: idx}
}

// New builds a Table from a header slice and a slice of raw string records.
// Each record in records becomes one Row; short records are padded with empty
// strings on access.
func New(headers slice.Slice[string], records [][]string) Table {
	rows := make(slice.Slice[Row], len(records))
	for i, rec := range records {
		rows[i] = NewRow(headers, rec)
	}
	return newTable(headers, rows)
}

// NewFromRows constructs a Table from a header slice and a slice of already-built
// Rows. The header index is built automatically.
func NewFromRows(headers slice.Slice[string], rows slice.Slice[Row]) Table {
	return newTable(headers, rows)
}

// ColIndex returns the zero-based index of col in t.Headers, or -1 if the
// column does not exist. This allows external packages to use the O(1) header
// index without accessing unexported fields.
func (t Table) ColIndex(col string) int {
	if idx, ok := t.headerIdx[col]; ok {
		return idx
	}
	return -1
}

// Select returns a new table containing only the named columns, in the order
// given. Columns not present in the table are silently ignored.
//
//	t.Select("name", "email")
func (t Table) Select(cols ...string) Table {
	newHeaders := make(slice.Slice[string], 0, len(cols))
	indices := make([]int, 0, len(cols))
	for _, col := range cols {
		if i, ok := t.headerIdx[col]; ok {
			newHeaders = append(newHeaders, col)
			indices = append(indices, i)
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
	return newTable(newHeaders, rows)
}

// Where returns a new table containing only the rows for which fn returns
// true.
//
//	t.Where(func(r table.Row) bool {
//	    return r.Get("country").UnwrapOr("") == "DE"
//	})
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
	return newTable(t.Headers, rows)
}

// Col extracts all values of the named column as a flat slice.
// Returns an empty slice if the column does not exist.
//
//	names := t.Col("name") // slice.Slice[string]
func (t Table) Col(name string) slice.Slice[string] {
	idx, ok := t.headerIdx[name]
	if !ok {
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

// Rename returns a new table with column old renamed to new. If old does not
// exist the original table is returned unchanged.
//
//	t.Rename("cust_id", "customer_id")
func (t Table) Rename(old, new string) Table {
	idx, ok := t.headerIdx[old]
	if !ok {
		return t
	}
	newHeaders := make(slice.Slice[string], len(t.Headers))
	copy(newHeaders, t.Headers)
	newHeaders[idx] = new
	rows := make(slice.Slice[Row], len(t.Rows))
	for i, row := range t.Rows {
		rows[i] = NewRow(newHeaders, row.values)
	}
	return newTable(newHeaders, rows)
}

// Append concatenates other onto t, aligning columns by name. Columns present
// in other but not in t are discarded; missing columns are filled with empty
// strings.
//
//	combined := jan.Append(feb).Append(mar)
func (t Table) Append(other Table) Table {
	// pre-compute mapping: for each position in t.Headers, the index in other
	otherIdx := make([]int, len(t.Headers))
	for i, h := range t.Headers {
		if idx, ok := other.headerIdx[h]; ok {
			otherIdx[i] = idx
		} else {
			otherIdx[i] = -1
		}
	}
	rows := make(slice.Slice[Row], 0, len(t.Rows)+len(other.Rows))
	for _, row := range t.Rows {
		rows = append(rows, NewRow(t.Headers, row.values))
	}
	for _, row := range other.Rows {
		vals := make(slice.Slice[string], len(t.Headers))
		for i, idx := range otherIdx {
			if idx >= 0 && idx < len(row.values) {
				vals[i] = row.values[idx]
			}
		}
		rows = append(rows, NewRow(t.Headers, vals))
	}
	return newTable(t.Headers, rows)
}

// Map transforms every value in col using fn, leaving all other columns
// unchanged. Returns the table unchanged if col does not exist.
//
//	t.Map("price", func(v string) string { return "$" + v })
func (t Table) Map(col string, fn func(string) string) Table {
	idx, ok := t.headerIdx[col]
	if !ok {
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
	return newTable(t.Headers, rows)
}

// AddCol appends a new column whose value for each row is computed by fn.
//
//	t.AddCol("full_name", func(r table.Row) string {
//	    return r.Get("first").UnwrapOr("") + " " + r.Get("last").UnwrapOr("")
//	})
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
	return newTable(newHeaders, rows)
}

// GroupBy splits the table into sub-tables keyed by the distinct values of
// col. The original row order is preserved within each group.
//
//	groups := t.GroupBy("country")
//	deRows := groups["DE"].Rows
func (t Table) GroupBy(col string) map[string]Table {
	idx, ok := t.headerIdx[col]
	if !ok {
		return map[string]Table{}
	}
	groups := make(map[string]Table)
	for _, row := range t.Rows {
		key := ""
		if idx < len(row.values) {
			key = row.values[idx]
		}
		g := groups[key]
		if g.Headers == nil {
			g = newTable(t.Headers, slice.Slice[Row]{})
		}
		g.Rows = append(g.Rows, NewRow(t.Headers, row.values))
		groups[key] = g
	}
	return groups
}

// Sort returns a new table sorted by col in ascending (asc=true) or
// descending (asc=false) order. Sorting is lexicographic.
// For numeric or multi-column sorting use SortMulti.
//
//	t.Sort("name", true)   // A → Z
//	t.Sort("date", false)  // newest first
func (t Table) Sort(col string, asc bool) Table {
	idx, ok := t.headerIdx[col]
	if !ok {
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
	return newTable(t.Headers, rows)
}

// Join performs an inner join on leftCol = rightCol.
// Only rows that have a matching key in other are kept.
// The join-key column from other is excluded from the result to avoid
// duplication.
//
//	orders.Join(customers, "customer_id", "id")
func (t Table) Join(other Table, leftCol, rightCol string) Table {
	rightKeyIdx, rok := other.headerIdx[rightCol]
	if !rok {
		return t
	}
	rightIdx := make(map[string][]Row, len(other.Rows))
	for _, row := range other.Rows {
		key := ""
		if rightKeyIdx < len(row.values) {
			key = row.values[rightKeyIdx]
		}
		rightIdx[key] = append(rightIdx[key], row)
	}

	newHeaders := make(slice.Slice[string], len(t.Headers))
	copy(newHeaders, t.Headers)
	var rightExtraIdx []int
	for i, h := range other.Headers {
		if h != rightCol {
			newHeaders = append(newHeaders, h)
			rightExtraIdx = append(rightExtraIdx, i)
		}
	}

	leftKeyIdx, lok := t.headerIdx[leftCol]
	if !lok {
		return t
	}
	var rows slice.Slice[Row]
	for _, lRow := range t.Rows {
		key := ""
		if leftKeyIdx < len(lRow.values) {
			key = lRow.values[leftKeyIdx]
		}
		rRows, ok := rightIdx[key]
		if !ok {
			continue
		}
		for _, rRow := range rRows {
			vals := make(slice.Slice[string], 0, len(newHeaders))
			vals = append(vals, lRow.values...)
			for _, idx := range rightExtraIdx {
				if idx < len(rRow.values) {
					vals = append(vals, rRow.values[idx])
				} else {
					vals = append(vals, "")
				}
			}
			rows = append(rows, NewRow(newHeaders, vals))
		}
	}
	if rows == nil {
		rows = slice.Slice[Row]{}
	}
	return newTable(newHeaders, rows)
}

// --- Shape & inspection ---

// Len returns the number of rows.
func (t Table) Len() int { return len(t.Rows) }

// Shape returns (rows, cols).
//
//	rows, cols := t.Shape()
func (t Table) Shape() (int, int) { return len(t.Rows), len(t.Headers) }

// Head returns the first n rows. If n >= Len the full table is returned.
func (t Table) Head(n int) Table {
	if n <= 0 {
		return newTable(t.Headers, slice.Slice[Row]{})
	}
	if n >= len(t.Rows) {
		return t
	}
	return newTable(t.Headers, t.Rows[:n])
}

// Tail returns the last n rows. If n >= Len the full table is returned.
func (t Table) Tail(n int) Table {
	if n <= 0 {
		return newTable(t.Headers, slice.Slice[Row]{})
	}
	if n >= len(t.Rows) {
		return t
	}
	return newTable(t.Headers, t.Rows[len(t.Rows)-n:])
}

// --- Column operations ---

// Drop removes the named columns and returns a new table. Unknown column names
// are silently ignored.
//
//	t.Drop("internal_id", "debug_flag")
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

// DropEmpty removes rows where any of the specified columns is an empty
// string. If no columns are given, a row is removed when any of its values is
// empty.
//
//	t.DropEmpty()              // remove rows with any empty cell
//	t.DropEmpty("email", "id") // remove rows where email or id is empty
func (t Table) DropEmpty(cols ...string) Table {
	check := cols
	if len(check) == 0 {
		check = t.Headers
	}
	// pre-compute column indices; skip nonexistent columns
	type colCheck struct {
		idx int
	}
	checks := make([]colCheck, 0, len(check))
	for _, c := range check {
		if idx, ok := t.headerIdx[c]; ok {
			checks = append(checks, colCheck{idx})
		}
	}
	if len(checks) == 0 {
		return t
	}
	return t.Where(func(r Row) bool {
		for _, cc := range checks {
			if cc.idx < len(r.values) && r.values[cc.idx] == "" {
				return false
			}
			if cc.idx >= len(r.values) {
				return false
			}
		}
		return true
	})
}

// FillEmpty replaces every empty string in col with val. Other columns are
// not affected.
//
//	t.FillEmpty("region", "unknown")
func (t Table) FillEmpty(col, val string) Table {
	return t.Map(col, func(v string) string {
		if v == "" {
			return val
		}
		return v
	})
}

// Distinct removes duplicate rows. If cols are given, only those columns are
// considered when determining uniqueness; all other columns retain their
// values from the first occurrence. If no cols are given, all columns are
// used.
//
//	t.Distinct()           // fully unique rows
//	t.Distinct("email")    // one row per email address
func (t Table) Distinct(cols ...string) Table {
	check := cols
	if len(check) == 0 {
		check = t.Headers
	}
	checkIdx := make([]int, len(check))
	for i, c := range check {
		idx, ok := t.headerIdx[c]
		if !ok {
			return t
		}
		checkIdx[i] = idx
	}
	seen := make(map[string]bool)
	var rows slice.Slice[Row]
	for _, row := range t.Rows {
		parts := make([]string, len(checkIdx))
		for i, idx := range checkIdx {
			if idx < len(row.values) {
				parts[i] = row.values[idx]
			}
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
	return newTable(t.Headers, rows)
}

// --- Transformation ---

// Case is a condition/value pair for use with AddColSwitch.
type Case struct {
	When func(Row) bool
	Then func(Row) string
}

// AddColSwitch appends a new column whose value is determined by the first
// matching Case. If no case matches, else_ is called (pass nil for empty
// string). This is the Go equivalent of numpy's np.select.
//
//	t.AddColSwitch("category",
//	    []table.Case{
//	        {When: func(r table.Row) bool { return r.Get("score").UnwrapOr("") == "A" },
//	         Then: func(r table.Row) string { return "excellent" }},
//	        {When: func(r table.Row) bool { return r.Get("score").UnwrapOr("") == "B" },
//	         Then: func(r table.Row) string { return "good" }},
//	    },
//	    func(r table.Row) string { return "other" },
//	)
func (t Table) AddColSwitch(name string, cases []Case, else_ func(Row) string) Table {
	return t.AddCol(name, func(r Row) string {
		for _, c := range cases {
			if c.When(r) {
				return c.Then(r)
			}
		}
		if else_ != nil {
			return else_(r)
		}
		return ""
	})
}

// CartesianProduct returns a new table with every combination of rows from a
// and b. The result has len(a.Headers)+len(b.Headers) columns and
// len(a.Rows)*len(b.Rows) rows. Column names from b that conflict with a are
// kept as-is; use Rename to disambiguate if needed.
//
//	regions := table.New([]string{"region"}, [][]string{{"EU"}, {"US"}})
//	products := table.New([]string{"product"}, [][]string{{"Widget"}, {"Gadget"}})
//	table.CartesianProduct(regions, products)
//	// region  product
//	// EU      Widget
//	// EU      Gadget
//	// US      Widget
//	// US      Gadget
func CartesianProduct(a, b Table) Table {
	newHeaders := make(slice.Slice[string], 0, len(a.Headers)+len(b.Headers))
	newHeaders = append(newHeaders, a.Headers...)
	newHeaders = append(newHeaders, b.Headers...)

	records := make([][]string, 0, len(a.Rows)*len(b.Rows))
	for _, ra := range a.Rows {
		for _, rb := range b.Rows {
			rec := make([]string, 0, len(newHeaders))
			rec = append(rec, ra.Values()...)
			rec = append(rec, rb.Values()...)
			records = append(records, rec)
		}
	}
	return New(newHeaders, records)
}

// Transform applies fn to every row. The map returned by fn is used as a
// partial update: only the columns present in the map are changed; all other
// columns keep their current value.
//
//	t.Transform(func(r table.Row) map[string]string {
//	    score := r.Get("score").UnwrapOr("0")
//	    return map[string]string{
//	        "score":  score + "pts",
//	        "source": "import",
//	    }
//	})
func (t Table) Transform(fn func(Row) map[string]string) Table {
	rows := make(slice.Slice[Row], len(t.Rows))
	for i, row := range t.Rows {
		updates := fn(row)
		vals := make(slice.Slice[string], len(row.values))
		copy(vals, row.values)
		for col, v := range updates {
			if j, ok := t.headerIdx[col]; ok && j < len(vals) {
				vals[j] = v
			}
		}
		rows[i] = NewRow(t.Headers, vals)
	}
	return newTable(t.Headers, rows)
}

// --- Multi-column sort ---

// SortKey specifies a column and sort direction for use with SortMulti.
type SortKey struct {
	Col string
	Asc bool
}

// Asc returns a SortKey that sorts col in ascending (A→Z, 0→9) order.
func Asc(col string) SortKey { return SortKey{Col: col, Asc: true} }

// Desc returns a SortKey that sorts col in descending (Z→A, 9→0) order.
func Desc(col string) SortKey { return SortKey{Col: col, Asc: false} }

// SortMulti sorts the table by multiple columns in priority order. The first
// key is the primary sort; subsequent keys break ties.
//
//	t.SortMulti(table.Asc("country"), table.Desc("revenue"))
func (t Table) SortMulti(keys ...SortKey) Table {
	indices := make([]int, len(keys))
	for k, sk := range keys {
		if i, ok := t.headerIdx[sk.Col]; ok {
			indices[k] = i
		} else {
			indices[k] = -1
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
	return newTable(t.Headers, rows)
}

// --- Joins ---

// LeftJoin keeps every row from t and attaches matching rows from other on
// leftCol = rightCol. When no match is found the right-side columns are filled
// with empty strings. The join-key column from other is excluded from the
// result to avoid duplication.
//
//	// Keep all orders, attach customer name where available
//	orders.LeftJoin(customers, "customer_id", "id")
func (t Table) LeftJoin(other Table, leftCol, rightCol string) Table {
	rightKeyIdx, rok := other.headerIdx[rightCol]
	if !rok {
		return t
	}
	rightIdx := make(map[string][]Row, len(other.Rows))
	for _, row := range other.Rows {
		key := ""
		if rightKeyIdx < len(row.values) {
			key = row.values[rightKeyIdx]
		}
		rightIdx[key] = append(rightIdx[key], row)
	}
	newHeaders := make(slice.Slice[string], len(t.Headers))
	copy(newHeaders, t.Headers)
	var rightExtraIdx []int
	for i, h := range other.Headers {
		if h != rightCol {
			newHeaders = append(newHeaders, h)
			rightExtraIdx = append(rightExtraIdx, i)
		}
	}
	leftKeyIdx, lok := t.headerIdx[leftCol]
	if !lok {
		return t
	}
	var rows slice.Slice[Row]
	for _, lRow := range t.Rows {
		key := ""
		if leftKeyIdx < len(lRow.values) {
			key = lRow.values[leftKeyIdx]
		}
		rRows := rightIdx[key]
		if len(rRows) == 0 {
			vals := make(slice.Slice[string], 0, len(newHeaders))
			vals = append(vals, lRow.values...)
			for range rightExtraIdx {
				vals = append(vals, "")
			}
			rows = append(rows, NewRow(newHeaders, vals))
		} else {
			for _, rRow := range rRows {
				vals := make(slice.Slice[string], 0, len(newHeaders))
				vals = append(vals, lRow.values...)
				for _, idx := range rightExtraIdx {
					if idx < len(rRow.values) {
						vals = append(vals, rRow.values[idx])
					} else {
						vals = append(vals, "")
					}
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

// --- Aggregation ---

// ValueCounts returns a two-column table with headers "value" and "count"
// containing the frequency of each distinct value in col, sorted by count
// descending.
//
//	t.ValueCounts("country")
//	// value   count
//	// DE      42
//	// US      37
//	// FR      15
func (t Table) ValueCounts(col string) Table {
	idx, ok := t.headerIdx[col]
	if !ok {
		return newTable(slice.Slice[string]{"value", "count"}, slice.Slice[Row]{})
	}
	counts := make(map[string]int)
	order := make([]string, 0, len(t.Rows))
	for _, row := range t.Rows {
		v := ""
		if idx < len(row.values) {
			v = row.values[idx]
		}
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

// Melt converts a wide-format table to long format (unpivot).
//
// idCols are kept as identifier columns; every other column is stacked into
// two new columns: varName holds the original column name and valName holds
// the cell value.
//
//	// wide:  name  q1   q2
//	//        Alice 100  200
//	//        Bob   150  250
//
//	t.Melt([]string{"name"}, "quarter", "revenue")
//
//	// long:  name  quarter  revenue
//	//        Alice q1       100
//	//        Alice q2       200
//	//        Bob   q1       150
//	//        Bob   q2       250
func (t Table) Melt(idCols []string, varName, valName string) Table {
	idSet := make(map[string]bool, len(idCols))
	for _, c := range idCols {
		idSet[c] = true
	}
	// pre-compute id column indices
	idIdx := make([]int, len(idCols))
	for i, c := range idCols {
		idIdx[i] = t.headerIdx[c]
	}
	// collect melt columns with their indices
	type meltCol struct {
		name string
		idx  int
	}
	meltCols := make([]meltCol, 0, len(t.Headers)-len(idCols))
	for i, h := range t.Headers {
		if !idSet[h] {
			meltCols = append(meltCols, meltCol{name: h, idx: i})
		}
	}
	newHeaders := make(slice.Slice[string], 0, len(idCols)+2)
	for _, c := range idCols {
		newHeaders = append(newHeaders, c)
	}
	newHeaders = append(newHeaders, varName, valName)
	records := make([][]string, 0, len(t.Rows)*len(meltCols))
	for _, row := range t.Rows {
		for _, mc := range meltCols {
			rec := make([]string, 0, len(newHeaders))
			for _, ii := range idIdx {
				v := ""
				if ii < len(row.values) {
					v = row.values[ii]
				}
				rec = append(rec, v)
			}
			v := ""
			if mc.idx < len(row.values) {
				v = row.values[mc.idx]
			}
			rec = append(rec, mc.name, v)
			records = append(records, rec)
		}
	}
	return New(newHeaders, records)
}

// Pivot converts a long-format table to wide format.
//
// index becomes the row identifier; the distinct values of col become new
// column headers; val supplies the cell values. Row order matches the first
// appearance of each index value.
//
//	// long:  name  quarter  revenue
//	//        Alice q1       100
//	//        Alice q2       200
//
//	t.Pivot("name", "quarter", "revenue")
//
//	// wide:  name  q1   q2
//	//        Alice 100  200
func (t Table) Pivot(index, col, val string) Table {
	indexIdx, ok1 := t.headerIdx[index]
	colIdx, ok2 := t.headerIdx[col]
	valIdx, ok3 := t.headerIdx[val]
	if !ok1 || !ok2 || !ok3 {
		return t
	}

	colVals := make([]string, 0, len(t.Rows))
	colSeen := make(map[string]bool)
	for _, row := range t.Rows {
		c := ""
		if colIdx < len(row.values) {
			c = row.values[colIdx]
		}
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
		idxVal := ""
		if indexIdx < len(row.values) {
			idxVal = row.values[indexIdx]
		}
		colVal := ""
		if colIdx < len(row.values) {
			colVal = row.values[colIdx]
		}
		cellVal := ""
		if valIdx < len(row.values) {
			cellVal = row.values[valIdx]
		}
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
