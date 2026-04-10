package table

import (
	"fmt"

	"github.com/stefanbethge/gseq/slice"
)

func normalizeHeaders(headers slice.Slice[string]) slice.Slice[string] {
	if len(headers) == 0 {
		return headers
	}

	used := make(map[string]bool, len(headers))
	var out slice.Slice[string]
	for i, h := range headers {
		if !used[h] {
			used[h] = true
			continue
		}
		if out == nil {
			out = copyHeaders(headers)
		}
		out[i] = nextUniqueHeaderName(h, used)
		used[out[i]] = true
	}
	if out == nil {
		return headers
	}
	return out
}

func nextUniqueHeaderName(base string, used map[string]bool) string {
	for i := 2; ; i++ {
		name := fmt.Sprintf("%s_%d", base, i)
		if !used[name] {
			return name
		}
	}
}

func clampRowValues(values slice.Slice[string], width int) slice.Slice[string] {
	if width <= 0 {
		return values
	}
	if len(values) <= width {
		return values
	}
	return values[:width:width]
}

func clampRecordValues(values []string, width int) []string {
	if width <= 0 {
		return values
	}
	if len(values) <= width {
		return values
	}
	return values[:width:width]
}
