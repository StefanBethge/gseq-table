package json

import (
	stdjson "encoding/json"
	"fmt"
	"strconv"
)

// extractDefault converts decoded records into flat key-value pairs.
// Nested objects and arrays are serialised as compact JSON strings.
// Returns headers in first-seen order (preserving JSON key order within
// each object) and corresponding row maps.
func extractDefault(records []record) ([]string, []map[string]string) {
	var headers []string
	seen := make(map[string]bool)
	rows := make([]map[string]string, len(records))

	for i, rec := range records {
		row := make(map[string]string, len(rec.fields))
		for _, k := range rec.keys {
			if !seen[k] {
				seen[k] = true
				headers = append(headers, k)
			}
			row[k] = stringify(rec.fields[k])
		}
		rows[i] = row
	}

	return headers, rows
}

// extractFlatten recursively flattens nested JSON objects into dot-separated
// column names. Arrays are expanded with index-based keys (e.g. "tags.0").
// maxDepth limits recursion; 0 means unlimited. At the depth limit, values
// are serialised as compact JSON strings.
func extractFlatten(records []record, sep string, maxDepth int) ([]string, []map[string]string) {
	var headers []string
	seen := make(map[string]bool)
	rows := make([]map[string]string, len(records))

	for i, rec := range records {
		row := make(map[string]string)
		keys := flattenRecord(rec, "", sep, 0, maxDepth, row)
		for _, k := range keys {
			if !seen[k] {
				seen[k] = true
				headers = append(headers, k)
			}
		}
		rows[i] = row
	}

	return headers, rows
}

// flattenRecord recursively walks a record using key order from the record.
// It populates row and returns the keys in deterministic order.
func flattenRecord(rec record, prefix, sep string, depth, maxDepth int, row map[string]string) []string {
	var keys []string
	for _, k := range rec.keys {
		key := k
		if prefix != "" {
			key = prefix + sep + k
		}
		keys = append(keys, flattenValueOrdered(rec.fields[k], key, sep, depth+1, maxDepth, row)...)
	}
	return keys
}

// flattenValueOrdered recursively walks v, populates row, and returns keys
// in deterministic order (preserving JSON key order at the top level,
// map iteration order for nested objects, array index order for arrays).
func flattenValueOrdered(v any, prefix, sep string, depth, maxDepth int, row map[string]string) []string {
	if maxDepth > 0 && depth > maxDepth {
		row[prefix] = stringify(v)
		return []string{prefix}
	}

	switch val := v.(type) {
	case map[string]any:
		if len(val) == 0 {
			if prefix != "" {
				row[prefix] = "{}"
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
			keys = append(keys, flattenValueOrdered(child, childKey, sep, depth+1, maxDepth, row)...)
		}
		return keys
	case []any:
		if len(val) == 0 {
			if prefix != "" {
				row[prefix] = "[]"
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
			keys = append(keys, flattenValueOrdered(child, childKey, sep, depth+1, maxDepth, row)...)
		}
		return keys
	default:
		if prefix != "" {
			row[prefix] = stringify(v)
			return []string{prefix}
		}
		return nil
	}
}

// stringify converts a decoded JSON value to its string representation.
// Strings pass through, numbers use json.Number.String(), booleans become
// "true"/"false", null becomes "", and nested values are compact JSON.
func stringify(v any) string {
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
