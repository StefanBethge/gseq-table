package table

import "github.com/stefanbethge/gseq/slice"

// Builder accumulates rows mutably and seals them into an immutable Table via
// Build. It avoids the O(n²) cost of repeated Append calls when constructing
// a table row by row.
//
//	b := table.NewBuilder("name", "city", "score")
//	for _, rec := range records {
//	    b.Add(rec...)
//	}
//	t := b.Build()
type Builder struct {
	headers   slice.Slice[string]
	headerIdx map[string]int
	rows      [][]string
}

// NewBuilder creates a Builder with the given column headers.
func NewBuilder(headers ...string) *Builder {
	idx := make(map[string]int, len(headers))
	for i, h := range headers {
		idx[h] = i
	}
	return &Builder{
		headers:   slice.Slice[string](headers),
		headerIdx: idx,
	}
}

// Add appends a row. Extra values are silently truncated; missing values are
// left as empty string on access.
func (b *Builder) Add(vals ...string) *Builder {
	row := make([]string, len(b.headers))
	copy(row, vals)
	b.rows = append(b.rows, row)
	return b
}

// AddMap appends a row from a map[string]string. Columns not present in the
// map receive an empty string.
func (b *Builder) AddMap(m map[string]string) *Builder {
	row := make([]string, len(b.headers))
	for col, val := range m {
		if idx, ok := b.headerIdx[col]; ok {
			row[idx] = val
		}
	}
	b.rows = append(b.rows, row)
	return b
}

// Set overwrites the value at rowIdx / col. Panics if rowIdx is out of range
// or col does not exist.
func (b *Builder) Set(rowIdx int, col, val string) *Builder {
	b.rows[rowIdx][b.headerIdx[col]] = val
	return b
}

// Len returns the number of rows accumulated so far.
func (b *Builder) Len() int { return len(b.rows) }

// Build seals the accumulated rows into an immutable Table. The Builder may
// continue to be used after Build; subsequent mutations do not affect the
// returned Table.
func (b *Builder) Build() Table {
	rows := make(slice.Slice[Row], len(b.rows))
	for i, rec := range b.rows {
		// copy so later Builder mutations don't alias the Table's data
		vals := make(slice.Slice[string], len(rec))
		copy(vals, rec)
		rows[i] = NewRow(b.headers, vals)
	}
	return newTable(b.headers, rows)
}
