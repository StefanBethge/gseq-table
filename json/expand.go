package json

import (
	stdjson "encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/stefanbethge/gseq-table/table"
	"github.com/stefanbethge/gseq/slice"
)

// ExpandCol parses the JSON string in the named column and expands it
// into additional columns. The original column is removed.
//
// Supports the same extraction options as the reader:
//
//   - No option (default): top-level keys become new columns.
//   - WithFlatten(): recursively flatten nested objects with dot-separated
//     names, prefixed by the original column name.
//   - WithFieldMapping(): extract specific paths into named columns.
//   - WithFlattenSeparator(), WithMaxDepth(), WithSortedHeaders() work
//     as expected.
//
// If a cell contains invalid JSON, the expanded columns for that row are
// empty strings.
//
// Example:
//
//	// Column "meta" contains: {"role":"admin","level":3}
//	t = json.ExpandCol(t, "meta", json.WithFieldMapping(map[string]string{
//	    "role":  ".role",
//	    "level": ".level",
//	}))
func ExpandCol(t table.Table, col string, opts ...Option) table.Table {
	colIdx := t.ColIndex(col)
	if colIdx < 0 {
		// Unknown column: return table with accumulated error via Select.
		return t.Select(col)
	}

	cfg := Config{}
	for _, opt := range opts {
		opt(&cfg)
	}

	// Parse JSON from each row's cell and collect extracted fields.
	type rowFields struct {
		fields map[string]string
	}
	parsed := make([]rowFields, len(t.Rows))
	var newHeaders []string
	seen := make(map[string]bool)

	for i, row := range t.Rows {
		cellVal := row.Get(col).UnwrapOr("")
		if cellVal == "" {
			parsed[i] = rowFields{fields: map[string]string{}}
			continue
		}

		fields, hdrs := expandCell(cellVal, col, &cfg)
		parsed[i] = rowFields{fields: fields}

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

	// Build new table: original headers (minus col) + new headers.
	origHeaders := make([]string, 0, len(t.Headers)-1)
	for _, h := range t.Headers {
		if h != col {
			origHeaders = append(origHeaders, h)
		}
	}
	allHeaders := append(origHeaders, newHeaders...)

	records := make([][]string, len(t.Rows))
	for i, row := range t.Rows {
		rec := make([]string, len(allHeaders))
		// Copy original values (minus expanded column).
		j := 0
		for _, h := range t.Headers {
			if h != col {
				rec[j] = row.Get(h).UnwrapOr("")
				j++
			}
		}
		// Fill in expanded values.
		for k, h := range newHeaders {
			rec[len(origHeaders)+k] = parsed[i].fields[h]
		}
		records[i] = rec
	}

	out := table.New(slice.Slice[string](allHeaders), records)
	if t.Source() != "" {
		out = out.WithSource(t.Source())
	}
	return out
}

// expandCell parses a JSON string and extracts fields according to cfg.
// Returns the extracted fields map and the header names in deterministic order.
func expandCell(cellVal, col string, cfg *Config) (map[string]string, []string) {
	// Parse the JSON string.
	var raw any
	dec := stdjson.NewDecoder(strings.NewReader(cellVal))
	dec.UseNumber()
	if err := dec.Decode(&raw); err != nil {
		return map[string]string{}, nil
	}

	switch {
	case len(cfg.FieldMapping) > 0:
		return expandMapping(raw, cfg.FieldMapping)
	case cfg.Flatten:
		sep := cfg.FlattenSeparator
		if sep == "" {
			sep = "."
		}
		return expandFlatten(raw, col, sep, cfg.MaxDepth)
	default:
		return expandDefault(raw, col)
	}
}

// expandDefault extracts top-level keys from a JSON value.
// For objects: each key becomes a column named "col.key".
// For arrays: each index becomes a column named "col.0", "col.1", etc.
// For scalars: a single column named "col" with the stringified value.
func expandDefault(v any, col string) (map[string]string, []string) {
	fields := make(map[string]string)
	var headers []string

	switch val := v.(type) {
	case map[string]any:
		for k, child := range val {
			key := col + "." + k
			fields[key] = stringify(child)
			headers = append(headers, key)
		}
	case []any:
		for i, child := range val {
			key := fmt.Sprintf("%s.%d", col, i)
			fields[key] = stringify(child)
			headers = append(headers, key)
		}
	default:
		fields[col] = stringify(v)
		headers = []string{col}
	}

	return fields, headers
}

// expandFlatten recursively flattens a JSON value, prefixing all keys
// with the original column name.
func expandFlatten(v any, prefix, sep string, maxDepth int) (map[string]string, []string) {
	fields := make(map[string]string)
	keys := flattenValueOrdered(v, prefix, sep, 0, maxDepth, fields)
	return fields, keys
}

// expandMapping extracts fields from a JSON value using path expressions.
func expandMapping(v any, mapping map[string]string) (map[string]string, []string) {
	obj, ok := v.(map[string]any)
	if !ok {
		// Non-object: all mapped fields get empty strings.
		fields := make(map[string]string, len(mapping))
		headers := make([]string, 0, len(mapping))
		for col := range mapping {
			headers = append(headers, col)
			fields[col] = ""
		}
		sort.Strings(headers)
		return fields, headers
	}

	// Sort column names for deterministic order.
	cols := make([]string, 0, len(mapping))
	for col := range mapping {
		cols = append(cols, col)
	}
	sort.Strings(cols)

	fields := make(map[string]string, len(cols))
	for _, col := range cols {
		segs, err := parsePath(mapping[col])
		if err != nil {
			fields[col] = ""
			continue
		}
		val := traversePath(obj, segs)
		fields[col] = stringify(val)
	}

	return fields, cols
}
