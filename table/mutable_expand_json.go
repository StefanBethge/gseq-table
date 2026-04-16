package table

import (
	"sort"

	"github.com/stefanbethge/gseq/slice"
)

// MapJSON transforms JSON values in the named column in-place.
// See Table.MapJSON for full documentation.
func (m *MutableTable) MapJSON(col string, args ...any) *MutableTable {
	idx, ok := m.headerIdx[col]
	if !ok {
		m.addErrf("MapJSON: unknown column %q", col)
		return m
	}

	path, cfg := parseMapJSONArgs(args)
	mapFn := buildMapJSONFunc(path, cfg)

	for i, row := range m.rows {
		if idx < len(row) && row[idx] != "" {
			m.rows[i][idx] = mapFn(row[idx])
		}
	}
	return m
}

// ExpandJSON parses the JSON string in the named column and expands it
// into additional columns in-place. The original column is removed.
//
// See Table.ExpandJSON for full documentation of modes and options.
func (m *MutableTable) ExpandJSON(col string, opts ...ExpandJSONOption) *MutableTable {
	idx, ok := m.headerIdx[col]
	if !ok {
		m.addErrf("ExpandJSON: unknown column %q", col)
		return m
	}

	cfg := resolveExpandJSONConfig(opts)

	// Parse JSON from each row and collect extracted fields.
	parsed := make([]map[string]string, len(m.rows))
	var newHeaders []string
	seen := make(map[string]bool)

	for i, row := range m.rows {
		cellVal := ""
		if idx < len(row) {
			cellVal = row[idx]
		}
		if cellVal == "" {
			parsed[i] = map[string]string{}
			continue
		}

		fields, hdrs := ejExpandCell(cellVal, col, cfg)
		parsed[i] = fields

		for _, h := range hdrs {
			if !seen[h] {
				seen[h] = true
				newHeaders = append(newHeaders, h)
			}
		}
	}

	if cfg.SortedHeaders {
		sort.Strings(newHeaders)
	}

	// Build new headers: original (minus col) + new.
	origHeaders := make([]string, 0, len(m.headers)-1)
	origIdx := make([]int, 0, len(m.headers)-1)
	for i, h := range m.headers {
		if i != idx {
			origHeaders = append(origHeaders, h)
			origIdx = append(origIdx, i)
		}
	}
	allHeaders := append(origHeaders, newHeaders...)
	width := len(allHeaders)

	// Build new rows.
	rows := newPackedRecords(len(m.rows), width)
	for i, oldRow := range m.rows {
		rec := rows[i]
		for j, oi := range origIdx {
			if oi < len(oldRow) {
				rec[j] = oldRow[oi]
			}
		}
		for k, h := range newHeaders {
			rec[len(origHeaders)+k] = parsed[i][h]
		}
	}

	m.headers = slice.Slice[string](allHeaders)
	m.rows = rows
	m.headerIdx = buildHeaderIndex(m.headers)
	return m
}