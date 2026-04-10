package table

import "github.com/stefanbethge/gseq/slice"

type pairKey struct {
	a string
	b string
}

func valueAt(values []string, idx int) string {
	if idx >= 0 && idx < len(values) {
		return values[idx]
	}
	return ""
}

func valueAtRow(values slice.Slice[string], idx int) string {
	if idx >= 0 && idx < len(values) {
		return values[idx]
	}
	return ""
}

func keyFromValues(values []string, idxs []int, scratch []byte) (string, []byte) {
	switch len(idxs) {
	case 0:
		return "", scratch[:0]
	case 1:
		return valueAt(values, idxs[0]), scratch[:0]
	}

	scratch = scratch[:0]
	for i, idx := range idxs {
		if i > 0 {
			scratch = append(scratch, 0)
		}
		if idx >= 0 && idx < len(values) {
			scratch = append(scratch, values[idx]...)
		}
	}
	return string(scratch), scratch
}

func keyFromRowValues(values slice.Slice[string], idxs []int, scratch []byte) (string, []byte) {
	return keyFromValues([]string(values), idxs, scratch)
}
