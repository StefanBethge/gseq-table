package table

import (
	"strconv"
	"strings"

	"github.com/stefanbethge/gseq/slice"
)

// --- Rename ---

// RenameMany renames multiple columns at once from the mapping old→new.
// Unknown column names are silently ignored.
//
//	t.RenameMany(map[string]string{
//	    "EquipmentIdent": "NutzungID",
//	    "EpecLocationID": "OrtsID",
//	})
func (t Table) RenameMany(renames map[string]string) Table {
	newHeaders := make(slice.Slice[string], len(t.Headers))
	copy(newHeaders, t.Headers)
	for i, h := range newHeaders {
		if newName, ok := renames[h]; ok {
			newHeaders[i] = newName
		}
	}
	rows := make(slice.Slice[Row], len(t.Rows))
	for i, row := range t.Rows {
		rows[i] = NewRow(newHeaders, row.values)
	}
	return newTable(newHeaders, rows)
}

// --- Concat ---

// Concat stacks tables vertically in the order given, aligning columns by
// name. It is the variadic generalisation of Append.
//
//	table.Concat(jan, feb, mar)
func Concat(tables ...Table) Table {
	if len(tables) == 0 {
		return Table{}
	}
	result := tables[0]
	for _, t := range tables[1:] {
		result = result.Append(t)
	}
	return result
}

// --- Row index ---

// AddRowIndex prepends a column named name containing the zero-based row
// position (0, 1, 2, …). Useful for tracing rows after filtering or sorting.
//
//	t.AddRowIndex("row_id")
func (t Table) AddRowIndex(name string) Table {
	newHeaders := make(slice.Slice[string], 0, len(t.Headers)+1)
	newHeaders = append(newHeaders, name)
	newHeaders = append(newHeaders, t.Headers...)

	rows := make(slice.Slice[Row], len(t.Rows))
	for i, row := range t.Rows {
		vals := make(slice.Slice[string], 0, len(row.values)+1)
		vals = append(vals, strconv.Itoa(i))
		vals = append(vals, row.values...)
		rows[i] = NewRow(newHeaders, vals)
	}
	return newTable(newHeaders, rows)
}

// --- Explode ---

// Explode splits the values of col on sep and returns a new table where each
// split part becomes its own row. All other column values are duplicated.
// Empty parts are skipped.
//
//	// "tags" contains "go,etl,data" → three rows
//	t.Explode("tags", ",")
func (t Table) Explode(col, sep string) Table {
	idx, ok := t.headerIdx[col]
	if !ok {
		return t
	}

	var records [][]string
	for _, row := range t.Rows {
		val := row.values[idx]
		parts := splitNonEmpty(val, sep)
		if len(parts) == 0 {
			parts = []string{val} // keep the row even if empty
		}
		for _, part := range parts {
			rec := make([]string, len(row.values))
			copy(rec, row.values)
			rec[idx] = part
			records = append(records, rec)
		}
	}
	return New(t.Headers, records)
}

// splitNonEmpty splits s on sep and returns non-empty, trimmed parts.
func splitNonEmpty(s, sep string) []string {
	var parts []string
	for _, p := range splitString(s, sep) {
		p = trimSpace(p)
		if p != "" {
			parts = append(parts, p)
		}
	}
	return parts
}

func splitString(s, sep string) []string {
	if sep == "" {
		return []string{s}
	}
	var result []string
	for {
		i := indexOf(s, sep)
		if i < 0 {
			result = append(result, s)
			break
		}
		result = append(result, s[:i])
		s = s[i+len(sep):]
	}
	return result
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n' || s[start] == '\r') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n' || s[end-1] == '\r') {
		end--
	}
	return s[start:end]
}

// --- Transpose ---

// Transpose pivots the table so that columns become rows and rows become
// columns. The first column of the result is named "column" and contains the
// original header names; subsequent columns are named by row index ("0","1",…).
//
//	// t: name  age
//	//    Alice 30
//	//    Bob   25
//	// t.Transpose():
//	//    column  0      1
//	//    name    Alice  Bob
//	//    age     30     25
func (t Table) Transpose() Table {
	newHeaders := make(slice.Slice[string], 0, len(t.Rows)+1)
	newHeaders = append(newHeaders, "column")
	for i := range t.Rows {
		newHeaders = append(newHeaders, strconv.Itoa(i))
	}

	records := make([][]string, len(t.Headers))
	for ci, col := range t.Headers {
		rec := make([]string, 0, len(t.Rows)+1)
		rec = append(rec, col)
		for _, row := range t.Rows {
			v := ""
			if ci < len(row.values) {
				v = row.values[ci]
			}
			rec = append(rec, v)
		}
		records[ci] = rec
	}
	return New(newHeaders, records)
}

// --- Fill forward / backward ---

// FillForward replaces empty string values in col with the most recent
// non-empty value above. Rows before the first non-empty value are unchanged.
//
//	// region: ["EU", "", "", "US", ""]  →  ["EU", "EU", "EU", "US", "US"]
//	t.FillForward("region")
func (t Table) FillForward(col string) Table {
	idx := colIdx(t, col)
	if idx < 0 {
		return t
	}
	rows := make(slice.Slice[Row], len(t.Rows))
	last := ""
	for i, row := range t.Rows {
		vals := make(slice.Slice[string], len(row.values))
		copy(vals, row.values)
		if idx < len(vals) {
			if vals[idx] == "" {
				vals[idx] = last
			} else {
				last = vals[idx]
			}
		}
		rows[i] = NewRow(t.Headers, vals)
	}
	return newTable(t.Headers, rows)
}

// FillBackward replaces empty string values in col with the next non-empty
// value below. Rows after the last non-empty value are unchanged.
//
//	// region: ["", "", "EU", "", "US"]  →  ["EU", "EU", "EU", "US", "US"]
//	t.FillBackward("region")
func (t Table) FillBackward(col string) Table {
	idx := colIdx(t, col)
	if idx < 0 {
		return t
	}
	rows := make(slice.Slice[Row], len(t.Rows))
	for i, row := range t.Rows {
		vals := make(slice.Slice[string], len(row.values))
		copy(vals, row.values)
		rows[i] = NewRow(t.Headers, vals)
	}
	next := ""
	for i := len(rows) - 1; i >= 0; i-- {
		vals := rows[i].values
		if idx < len(vals) {
			if vals[idx] == "" {
				vals[idx] = next
			} else {
				next = vals[idx]
			}
		}
	}
	return newTable(t.Headers, rows)
}

// --- Sampling ---

// Sample returns n randomly selected rows without replacement. If n >= Len,
// the full table is returned (unordered).
//
//	t.Sample(100)
func (t Table) Sample(n int) Table {
	if n >= len(t.Rows) {
		return t
	}
	sampled := t.Rows.Samples(n)
	return newTable(t.Headers, sampled)
}

// SampleFrac returns a random fraction of rows. f=0.2 returns ~20% of rows.
//
//	t.SampleFrac(0.1) // 10% random sample
func (t Table) SampleFrac(f float64) Table {
	n := int(float64(len(t.Rows)) * f)
	return t.Sample(n)
}

// --- Typed column constructors ---

// AddColFloat appends a new column whose value per row is computed by fn
// (returning float64). The float is formatted without trailing zeros.
//
//	t.AddColFloat("ratio", func(r table.Row) float64 {
//	    a := schema.Float(r, "a").UnwrapOr(0)
//	    b := schema.Float(r, "b").UnwrapOr(1)
//	    return a / b
//	})
func (t Table) AddColFloat(name string, fn func(Row) float64) Table {
	return AddColOf(t, name, fn, func(f float64) string {
		return strconv.FormatFloat(f, 'f', -1, 64)
	})
}

// AddColInt appends a new column whose value per row is computed by fn
// (returning int64).
//
//	t.AddColInt("year", func(r table.Row) int64 {
//	    return int64(schema.Int(r, "year_month").UnwrapOr(0) / 100)
//	})
func (t Table) AddColInt(name string, fn func(Row) int64) Table {
	return AddColOf(t, name, fn, func(n int64) string {
		return strconv.FormatInt(n, 10)
	})
}

// --- Partition / Chunk / ForEach ---

// Partition splits the table into two: the first contains rows where fn
// returns true, the second the rest. Both tables share the same headers.
//
//	active, inactive := t.Partition(table.Eq("status", "active"))
func (t Table) Partition(fn func(Row) bool) (matched, rest Table) {
	var mRows, rRows slice.Slice[Row]
	for _, row := range t.Rows {
		if fn(row) {
			mRows = append(mRows, NewRow(t.Headers, row.values))
		} else {
			rRows = append(rRows, NewRow(t.Headers, row.values))
		}
	}
	if mRows == nil {
		mRows = slice.Slice[Row]{}
	}
	if rRows == nil {
		rRows = slice.Slice[Row]{}
	}
	return newTable(t.Headers, mRows), newTable(t.Headers, rRows)
}

// Chunk splits the table into consecutive sub-tables of at most n rows each.
// The last chunk may be smaller than n.
//
//	for _, batch := range t.Chunk(100) { process(batch) }
func (t Table) Chunk(n int) []Table {
	if n <= 0 || len(t.Rows) == 0 {
		return []Table{t}
	}
	var chunks []Table
	for i := 0; i < len(t.Rows); i += n {
		end := i + n
		if end > len(t.Rows) {
			end = len(t.Rows)
		}
		chunks = append(chunks, newTable(t.Headers, t.Rows[i:end]))
	}
	return chunks
}

// ForEach calls fn for each row with its zero-based index. It is intended for
// side-effects (logging, metrics) and does not modify the table.
//
//	t.ForEach(func(i int, r table.Row) { log.Printf("row %d: %v", i, r.ToMap()) })
func (t Table) ForEach(fn func(i int, r Row)) {
	for i, row := range t.Rows {
		fn(i, row)
	}
}

// --- Column enrichment ---

// Coalesce adds a new column name containing the first non-empty value from
// the listed cols, left to right.
//
//	t.Coalesce("display_name", "nickname", "full_name", "email")
func (t Table) Coalesce(name string, cols ...string) Table {
	return t.AddCol(name, func(r Row) string {
		for _, col := range cols {
			if v := r.Get(col).UnwrapOr(""); v != "" {
				return v
			}
		}
		return ""
	})
}

// Lookup adds outCol by looking up each row's col value in lookupTable.keyCol
// and returning the corresponding lookupTable.valCol. Rows without a match
// receive an empty string.
//
//	// enrich orders with customer name
//	orders.Lookup("customer_id", "customer_name", customers, "id", "name")
func (t Table) Lookup(col, outCol string, lookupTable Table, keyCol, valCol string) Table {
	keyIdx := lookupTable.headerIdx[keyCol]
	valIdx := lookupTable.headerIdx[valCol]
	lkp := make(map[string]string, len(lookupTable.Rows))
	for _, row := range lookupTable.Rows {
		k, v := "", ""
		if keyIdx < len(row.values) {
			k = row.values[keyIdx]
		}
		if valIdx < len(row.values) {
			v = row.values[valIdx]
		}
		lkp[k] = v
	}
	colIdx := t.headerIdx[col]
	return t.AddCol(outCol, func(r Row) string {
		k := ""
		if colIdx < len(r.values) {
			k = r.values[colIdx]
		}
		return lkp[k]
	})
}

// FormatCol rounds all parseable float values in col to precision decimal
// places and stores them back as strings. Non-parseable values are left
// unchanged.
//
//	t.FormatCol("price", 2) // "3.14159" → "3.14"
func (t Table) FormatCol(col string, precision int) Table {
	return t.Map(col, func(v string) string {
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return v
		}
		return strconv.FormatFloat(f, 'f', precision, 64)
	})
}

// --- Set operations ---

// Union returns a table with all distinct rows from a and b.
// Equivalent to Concat(a, b).Distinct(cols...).
//
//	table.Union(jan, feb, "id") // unique by "id"
//	table.Union(jan, feb)       // unique across all columns
func Union(a, b Table, cols ...string) Table {
	return Concat(a, b).Distinct(cols...)
}

// Intersect returns rows from t that also appear in other, matched by cols.
// If no cols are given, all columns are used for comparison.
//
//	// rows in t whose "id" also appears in other
//	t.Intersect(other, "id")
func (t Table) Intersect(other Table, cols ...string) Table {
	check := cols
	if len(check) == 0 {
		check = t.Headers
	}
	// pre-compute column indices for both tables
	tIdx := make([]int, len(check))
	oIdx := make([]int, len(check))
	for i, c := range check {
		tIdx[i] = t.headerIdx[c]
		oIdx[i] = other.headerIdx[c]
	}
	otherKeys := make(map[string]bool, len(other.Rows))
	parts := make([]string, len(check))
	for _, row := range other.Rows {
		for i, idx := range oIdx {
			parts[i] = ""
			if idx < len(row.values) {
				parts[i] = row.values[idx]
			}
		}
		otherKeys[strings.Join(parts, "\x00")] = true
	}
	return t.Where(func(r Row) bool {
		for i, idx := range tIdx {
			parts[i] = ""
			if idx < len(r.values) {
				parts[i] = r.values[idx]
			}
		}
		return otherKeys[strings.Join(parts, "\x00")]
	})
}

// --- Binning ---

// BinDef defines a single bin for use with Bin.
// A row falls into the first bin where its value < Max.
// The last bin also captures all values ≥ its Max.
type BinDef struct {
	Max   float64
	Label string
}

// Bin adds a new column name that categorises col's numeric values according
// to bins. Rows with unparseable values get an empty string.
//
//	t.Bin("age", "age_group", []table.BinDef{
//	    {Max: 18,  Label: "minor"},
//	    {Max: 65,  Label: "adult"},
//	    {Max: 999, Label: "senior"},
//	})
func (t Table) Bin(col, name string, bins []BinDef) Table {
	return t.AddCol(name, func(r Row) string {
		f, err := strconv.ParseFloat(strings.TrimSpace(r.Get(col).UnwrapOr("")), 64)
		if err != nil {
			return ""
		}
		for _, bin := range bins {
			if f < bin.Max {
				return bin.Label
			}
		}
		if len(bins) > 0 {
			return bins[len(bins)-1].Label
		}
		return ""
	})
}

// --- helpers ---

// colIdx returns the index of col in t.Headers, or -1.
func colIdx(t Table, col string) int {
	if i, ok := t.headerIdx[col]; ok {
		return i
	}
	return -1
}
