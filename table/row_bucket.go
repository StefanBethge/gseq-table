package table

import "github.com/stefanbethge/gseq/slice"

type rowBucket struct {
	first    slice.Slice[string]
	hasFirst bool
	rest     []slice.Slice[string]
}

func (b rowBucket) len() int {
	if b.hasFirst {
		return 1 + len(b.rest)
	}
	return len(b.rest)
}

func addRowBucket(index map[string]rowBucket, key string, row slice.Slice[string]) {
	b := index[key]
	if !b.hasFirst && len(b.rest) == 0 {
		b.first = row
		b.hasFirst = true
		index[key] = b
		return
	}
	if b.hasFirst && len(b.rest) == 0 {
		b.rest = append(b.rest, b.first, row)
		b.first = nil
		b.hasFirst = false
		index[key] = b
		return
	}
	b.rest = append(b.rest, row)
	index[key] = b
}

func forEachRowBucket(b rowBucket, fn func(slice.Slice[string])) {
	if b.hasFirst {
		fn(b.first)
		return
	}
	for _, row := range b.rest {
		fn(row)
	}
}
