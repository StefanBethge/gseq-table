package table

import (
	"fmt"
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

	if err := validateJSONPaths(path, cfg); err != nil {
		m.addErrf("MapJSON: %v", err)
		return m
	}

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

	if err := validateJSONFieldMapping(cfg); err != nil {
		m.addErrf("ExpandJSON: %v", err)
		return m
	}

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

// TryMapJSON is like MapJSON but returns an error when a cell contains invalid
// JSON. The table data is left unchanged on error (rows are cloned before
// processing and only committed on success).
//
// Programming errors (unknown column, invalid path) go through addErrf.
// Data errors (unparseable JSON) are appended to m.errs without panicking,
// even in strict builds.
func (m *MutableTable) TryMapJSON(col string, args ...any) *MutableTable {
	idx, ok := m.headerIdx[col]
	if !ok {
		m.addErrf("TryMapJSON: unknown column %q", col)
		return m
	}

	path, cfg := parseMapJSONArgs(args)

	if err := validateJSONPaths(path, cfg); err != nil {
		m.addErrf("TryMapJSON: %v", err)
		return m
	}

	tryFn := buildTryMapJSONFunc(path, cfg)

	rows := cloneRecords(m.rows)
	for i := range rows {
		if idx < len(rows[i]) && rows[i][idx] != "" {
			newVal, err := tryFn(rows[i][idx])
			if err != nil {
				m.errs = append(m.errs, fmt.Errorf("TryMapJSON: %v", err))
				return m
			}
			rows[i][idx] = newVal
		}
	}
	m.rows = rows
	return m
}