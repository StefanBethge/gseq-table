package table

import (
	stdjson "encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/stefanbethge/gseq/slice"
)

// ExpandJSONConfig holds the resolved settings for an ExpandJSON call.
type ExpandJSONConfig struct {
	// Flatten enables recursive flattening of nested objects with
	// dot-separated column names (prefixed by the original column name).
	Flatten bool

	// MaxDepth limits recursion during flattening. Zero means unlimited.
	MaxDepth int

	// FlattenSeparator is the string between key segments. Defaults to ".".
	FlattenSeparator string

	// FieldMapping maps output column names to JSON path expressions
	// (e.g. ".user.name", ".items[0].city").
	FieldMapping map[string]string

	// SortedHeaders orders the new columns alphabetically.
	SortedHeaders bool
}

// ExpandJSONOption is a functional option for configuring ExpandJSON.
type ExpandJSONOption func(*ExpandJSONConfig)

// WithJSONFlatten enables recursive flattening of nested JSON objects.
func WithJSONFlatten() ExpandJSONOption { return func(c *ExpandJSONConfig) { c.Flatten = true } }

// WithJSONMaxDepth limits the recursion depth during flattening.
func WithJSONMaxDepth(n int) ExpandJSONOption { return func(c *ExpandJSONConfig) { c.MaxDepth = n } }

// WithJSONFlattenSeparator sets the separator between key segments.
func WithJSONFlattenSeparator(sep string) ExpandJSONOption {
	return func(c *ExpandJSONConfig) { c.FlattenSeparator = sep }
}

// WithJSONFieldMapping provides explicit JSON path expressions to extract.
func WithJSONFieldMapping(mapping map[string]string) ExpandJSONOption {
	return func(c *ExpandJSONConfig) { c.FieldMapping = mapping }
}

// WithJSONSortedHeaders orders new columns alphabetically.
func WithJSONSortedHeaders() ExpandJSONOption {
	return func(c *ExpandJSONConfig) { c.SortedHeaders = true }
}

// ExpandJSON parses the JSON string in the named column and expands it
// into additional columns. The original column is removed.
//
// Three modes:
//   - Default (no option): top-level keys become new columns prefixed with
//     the original column name (e.g. "meta.role").
//   - WithJSONFlatten(): recursively flatten nested objects with dot-separated
//     column names.
//   - WithJSONFieldMapping(): extract specific paths into named columns.
//
// If a cell contains invalid JSON, the expanded columns for that row are
// empty strings.
//
// Example:
//
//	t = t.ExpandJSON("meta", table.WithJSONFieldMapping(map[string]string{
//	    "role":  ".role",
//	    "level": ".level",
//	}))
func (t Table) ExpandJSON(col string, opts ...ExpandJSONOption) Table {
	idx := t.ColIndex(col)
	if idx < 0 {
		return t.withErrf("ExpandJSON: unknown column %q", col)
	}

	cfg := resolveExpandJSONConfig(opts)

	// Parse JSON from each row and collect extracted fields.
	parsed := make([]map[string]string, len(t.Rows))
	var newHeaders []string
	seen := make(map[string]bool)

	for i, row := range t.Rows {
		cellVal := ""
		if idx < len(row.values) {
			cellVal = row.values[idx]
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
	origHeaders := make([]string, 0, len(t.Headers)-1)
	origIdx := make([]int, 0, len(t.Headers)-1)
	for i, h := range t.Headers {
		if i != idx {
			origHeaders = append(origHeaders, h)
			origIdx = append(origIdx, i)
		}
	}
	allHeaders := append(origHeaders, newHeaders...)

	// Build records.
	records := make([][]string, len(t.Rows))
	for i, row := range t.Rows {
		rec := make([]string, len(allHeaders))
		for j, oi := range origIdx {
			if oi < len(row.values) {
				rec[j] = row.values[oi]
			}
		}
		for k, h := range newHeaders {
			rec[len(origHeaders)+k] = parsed[i][h]
		}
		records[i] = rec
	}

	out := New(slice.Slice[string](allHeaders), records)
	out.errs = t.errs
	out.source = t.source
	return out
}

// MapJSON transforms JSON values in the named column by extracting a path
// or restructuring the JSON. The result stays in the same column.
//
// With a path argument, each cell is replaced with the value at that path.
// Non-leaf values become compact JSON strings. If the path does not resolve,
// the cell becomes an empty string.
//
// With WithJSONFieldMapping, a new JSON object is constructed from the
// mapped paths and written back as a compact JSON string.
//
// Invalid JSON cells are left unchanged.
//
// Example:
//
//	// Extract a nested value:
//	t = t.MapJSON("data", ".user.name")
//
//	// Restructure JSON:
//	t = t.MapJSON("data", table.WithJSONFieldMapping(map[string]string{
//	    "name": ".user.name",
//	    "city": ".addr.city",
//	}))
func (t Table) MapJSON(col string, args ...any) Table {
	idx := t.ColIndex(col)
	if idx < 0 {
		return t.withErrf("MapJSON: unknown column %q", col)
	}

	path, cfg := parseMapJSONArgs(args)

	mapFn := buildMapJSONFunc(path, cfg)

	var records [][]string
	for _, row := range t.Rows {
		rec := make([]string, len(t.Headers))
		copy(rec, row.values)
		if idx < len(rec) && rec[idx] != "" {
			rec[idx] = mapFn(rec[idx])
		}
		records = append(records, rec)
	}

	out := New(t.Headers, records)
	out.errs = t.errs
	out.source = t.source
	return out
}

// parseMapJSONArgs separates the path string and options from the variadic args.
func parseMapJSONArgs(args []any) (string, *ExpandJSONConfig) {
	var path string
	var opts []ExpandJSONOption
	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			path = v
		case ExpandJSONOption:
			opts = append(opts, v)
		}
	}
	cfg := resolveExpandJSONConfig(opts)
	return path, cfg
}

// buildMapJSONFunc creates a function that transforms a JSON cell value.
func buildMapJSONFunc(path string, cfg *ExpandJSONConfig) func(string) string {
	// Path extraction mode.
	if path != "" {
		segs, err := ejParsePath(path)
		if err != nil {
			return func(v string) string { return v }
		}
		return func(v string) string {
			var raw any
			dec := stdjson.NewDecoder(strings.NewReader(v))
			dec.UseNumber()
			if err := dec.Decode(&raw); err != nil {
				return v
			}
			obj, ok := raw.(map[string]any)
			if !ok {
				return v
			}
			result := ejTraversePath(obj, segs)
			return ejStringify(result)
		}
	}

	// Field mapping mode → restructure into new JSON object.
	if len(cfg.FieldMapping) > 0 {
		type parsed struct {
			col  string
			segs []ejPathSegment
		}
		cols := make([]string, 0, len(cfg.FieldMapping))
		for col := range cfg.FieldMapping {
			cols = append(cols, col)
		}
		sort.Strings(cols)

		mappings := make([]parsed, 0, len(cols))
		for _, col := range cols {
			segs, err := ejParsePath(cfg.FieldMapping[col])
			if err != nil {
				continue
			}
			mappings = append(mappings, parsed{col: col, segs: segs})
		}

		return func(v string) string {
			var raw any
			dec := stdjson.NewDecoder(strings.NewReader(v))
			dec.UseNumber()
			if err := dec.Decode(&raw); err != nil {
				return v
			}
			obj, ok := raw.(map[string]any)
			if !ok {
				return v
			}
			result := make(map[string]any, len(mappings))
			for _, m := range mappings {
				val := ejTraversePath(obj, m.segs)
				if val != nil {
					result[m.col] = val
				}
			}
			b, err := stdjson.Marshal(result)
			if err != nil {
				return v
			}
			return string(b)
		}
	}

	// No path, no mapping → identity.
	return func(v string) string { return v }
}

func resolveExpandJSONConfig(opts []ExpandJSONOption) *ExpandJSONConfig {
	cfg := &ExpandJSONConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// --- Internal JSON helpers (not exported) ---

// ejExpandCell parses a JSON string and extracts fields.
func ejExpandCell(cellVal, col string, cfg *ExpandJSONConfig) (map[string]string, []string) {
	var raw any
	dec := stdjson.NewDecoder(strings.NewReader(cellVal))
	dec.UseNumber()
	if err := dec.Decode(&raw); err != nil {
		return map[string]string{}, nil
	}

	switch {
	case len(cfg.FieldMapping) > 0:
		return ejExpandMapping(raw, cfg.FieldMapping)
	case cfg.Flatten:
		sep := cfg.FlattenSeparator
		if sep == "" {
			sep = "."
		}
		return ejExpandFlatten(raw, col, sep, cfg.MaxDepth)
	default:
		return ejExpandDefault(raw, col)
	}
}

// ejExpandDefault extracts top-level keys from a JSON value.
func ejExpandDefault(v any, col string) (map[string]string, []string) {
	fields := make(map[string]string)
	var headers []string

	switch val := v.(type) {
	case map[string]any:
		for k, child := range val {
			key := col + "." + k
			fields[key] = ejStringify(child)
			headers = append(headers, key)
		}
	case []any:
		for i, child := range val {
			key := fmt.Sprintf("%s.%d", col, i)
			fields[key] = ejStringify(child)
			headers = append(headers, key)
		}
	default:
		fields[col] = ejStringify(v)
		headers = []string{col}
	}

	return fields, headers
}

// ejExpandFlatten recursively flattens a JSON value.
func ejExpandFlatten(v any, prefix, sep string, maxDepth int) (map[string]string, []string) {
	fields := make(map[string]string)
	keys := ejFlattenOrdered(v, prefix, sep, 0, maxDepth, fields)
	return fields, keys
}

// ejFlattenOrdered recursively walks v, populates fields, and returns keys
// in order.
func ejFlattenOrdered(v any, prefix, sep string, depth, maxDepth int, fields map[string]string) []string {
	if maxDepth > 0 && depth > maxDepth {
		fields[prefix] = ejStringify(v)
		return []string{prefix}
	}

	switch val := v.(type) {
	case map[string]any:
		if len(val) == 0 {
			if prefix != "" {
				fields[prefix] = "{}"
				return []string{prefix}
			}
			return nil
		}
		var keys []string
		for k, child := range val {
			childKey := k
			if prefix != "" {
				childKey = prefix + sep + k
			}
			keys = append(keys, ejFlattenOrdered(child, childKey, sep, depth+1, maxDepth, fields)...)
		}
		return keys
	case []any:
		if len(val) == 0 {
			if prefix != "" {
				fields[prefix] = "[]"
				return []string{prefix}
			}
			return nil
		}
		var keys []string
		for i, child := range val {
			childKey := strconv.Itoa(i)
			if prefix != "" {
				childKey = prefix + sep + childKey
			}
			keys = append(keys, ejFlattenOrdered(child, childKey, sep, depth+1, maxDepth, fields)...)
		}
		return keys
	default:
		if prefix != "" {
			fields[prefix] = ejStringify(v)
			return []string{prefix}
		}
		return nil
	}
}

// ejExpandMapping extracts fields from a JSON value using path expressions.
func ejExpandMapping(v any, mapping map[string]string) (map[string]string, []string) {
	obj, ok := v.(map[string]any)
	if !ok {
		fields := make(map[string]string, len(mapping))
		headers := make([]string, 0, len(mapping))
		for col := range mapping {
			headers = append(headers, col)
			fields[col] = ""
		}
		sort.Strings(headers)
		return fields, headers
	}

	cols := make([]string, 0, len(mapping))
	for col := range mapping {
		cols = append(cols, col)
	}
	sort.Strings(cols)

	fields := make(map[string]string, len(cols))
	for _, col := range cols {
		segs, err := ejParsePath(mapping[col])
		if err != nil {
			fields[col] = ""
			continue
		}
		val := ejTraversePath(obj, segs)
		fields[col] = ejStringify(val)
	}

	return fields, cols
}

// ejStringify converts a decoded JSON value to its string representation.
func ejStringify(v any) string {
	switch val := v.(type) {
	case nil:
		return ""
	case string:
		return val
	case stdjson.Number:
		return val.String()
	case bool:
		if val {
			return "true"
		}
		return "false"
	case map[string]any, []any:
		b, err := stdjson.Marshal(val)
		if err != nil {
			return fmt.Sprintf("%v", val)
		}
		return string(b)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// --- Path parsing for FieldMapping ---

type ejPathSegment struct {
	key   string
	index int // -1 means no index
}

func ejParsePath(path string) ([]ejPathSegment, error) {
	if path == "" || path[0] != '.' {
		return nil, fmt.Errorf("path %q must start with '.'", path)
	}
	path = path[1:]
	if path == "" {
		return nil, fmt.Errorf("path is empty after leading '.'")
	}

	var segments []ejPathSegment
	for path != "" {
		end := strings.IndexAny(path, ".[")
		if end == 0 && path[0] == '.' {
			return nil, fmt.Errorf("empty key segment in path")
		}
		if end == -1 {
			end = len(path)
		}

		key := path[:end]
		path = path[end:]

		seg := ejPathSegment{key: key, index: -1}

		if strings.HasPrefix(path, "[") {
			closing := strings.Index(path, "]")
			if closing == -1 {
				return nil, fmt.Errorf("unclosed bracket in path")
			}
			idxStr := path[1:closing]
			idx, err := strconv.Atoi(idxStr)
			if err != nil {
				return nil, fmt.Errorf("invalid array index %q: %w", idxStr, err)
			}
			if idx < 0 {
				return nil, fmt.Errorf("negative array index %d", idx)
			}
			seg.index = idx
			path = path[closing+1:]
		}

		segments = append(segments, seg)

		if strings.HasPrefix(path, ".") {
			path = path[1:]
		}
	}

	return segments, nil
}

func ejTraversePath(v any, segments []ejPathSegment) any {
	current := v
	for _, seg := range segments {
		obj, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current, ok = obj[seg.key]
		if !ok {
			return nil
		}
		if seg.index >= 0 {
			arr, ok := current.([]any)
			if !ok || seg.index >= len(arr) {
				return nil
			}
			current = arr[seg.index]
		}
	}
	return current
}
