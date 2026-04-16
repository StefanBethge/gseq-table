package json

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// pathSegment represents one step in a JSON path traversal.
type pathSegment struct {
	key   string
	index int  // -1 means no index
}

// parsePath parses a JSON path like ".user.addr[0].city" into segments.
// The leading dot is required.
func parsePath(path string) ([]pathSegment, error) {
	if path == "" || path[0] != '.' {
		return nil, fmt.Errorf("json: path %q must start with '.'", path)
	}
	path = path[1:] // strip leading dot
	if path == "" {
		return nil, fmt.Errorf("json: path %q is empty after leading '.'", "."+path)
	}

	var segments []pathSegment
	for path != "" {
		// Find the end of the key (next dot or bracket).
		end := strings.IndexAny(path, ".[")
		if end == 0 && path[0] == '.' {
			// double dot or leading dot in remainder
			return nil, fmt.Errorf("json: empty key segment in path")
		}
		if end == -1 {
			end = len(path)
		}

		key := path[:end]
		path = path[end:]

		seg := pathSegment{key: key, index: -1}

		// Check for bracket index.
		if strings.HasPrefix(path, "[") {
			closing := strings.Index(path, "]")
			if closing == -1 {
				return nil, fmt.Errorf("json: unclosed bracket in path")
			}
			idxStr := path[1:closing]
			idx, err := strconv.Atoi(idxStr)
			if err != nil {
				return nil, fmt.Errorf("json: invalid array index %q: %w", idxStr, err)
			}
			if idx < 0 {
				return nil, fmt.Errorf("json: negative array index %d in path", idx)
			}
			seg.index = idx
			path = path[closing+1:]
		}

		segments = append(segments, seg)

		// Skip dot separator between segments.
		if strings.HasPrefix(path, ".") {
			path = path[1:]
		}
	}

	return segments, nil
}

// traversePath walks a decoded JSON value along the given path segments
// and returns the value at the end, or nil if the path doesn't resolve.
func traversePath(v any, segments []pathSegment) any {
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

// extractMapping extracts fields from JSON objects using explicit path
// mappings. Returns headers in deterministic (sorted) order of the output
// column names, the corresponding row maps, and an error if any path is
// invalid.
func extractMapping(records []record, mapping map[string]string) ([]string, []map[string]string, error) {
	// Parse all paths upfront.
	type parsedMapping struct {
		col      string
		segments []pathSegment
	}

	// Sort column names for deterministic header order.
	cols := make([]string, 0, len(mapping))
	for col := range mapping {
		cols = append(cols, col)
	}
	sort.Strings(cols)

	parsed := make([]parsedMapping, 0, len(cols))
	for _, col := range cols {
		segs, err := parsePath(mapping[col])
		if err != nil {
			return nil, nil, fmt.Errorf("json: field mapping %q: %w", col, err)
		}
		parsed = append(parsed, parsedMapping{col: col, segments: segs})
	}

	headers := cols
	rows := make([]map[string]string, len(records))

	for i, rec := range records {
		row := make(map[string]string, len(parsed))
		for _, pm := range parsed {
			val := traversePath(rec.fields, pm.segments)
			row[pm.col] = stringify(val)
		}
		rows[i] = row
	}

	return headers, rows, nil
}
