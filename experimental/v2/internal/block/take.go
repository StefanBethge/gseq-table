package block

import (
	"fmt"
	"math"
	"time"
)

// Ref addresses row Row of block Blk in a list of blocks. A Row of -1 yields
// a null cell, as for a left join without a partner.
type Ref struct{ Blk, Row int32 }

// Take returns a new column with one owner that holds the cells rows of c,
// in that order. A row may appear more than once, as after a 1:n join; -1
// yields a null cell, as for a left join without a partner.
func (c Column) Take(rows []int) Column {
	d := c.d
	for _, r := range rows {
		if r >= 0 {
			c.checkIndex(r)
		}
	}
	out := &data{kind: d.kind, length: len(rows)}
	if d.kind == Text {
		size := 0
		for _, r := range rows {
			if r >= 0 {
				size += int(d.offs[r+1] - d.offs[r])
			}
		}
		out.makeText(len(rows), size)
		for i, r := range rows {
			if r < 0 || d.isNull(r) {
				out.markNull(i)
			} else {
				out.text = append(out.text, d.text[d.offs[r]:d.offs[r+1]]...)
			}
			out.offs[i+1] = uint32(len(out.text))
		}
		return newColumn(out)
	}
	out.makeValues(len(rows))
	switch d.kind {
	case Int:
		takeValues(out, out.ints, d, d.ints, rows)
	case Float:
		takeValues(out, out.floats, d, d.floats, rows)
	case Bool:
		takeValues(out, out.bools, d, d.bools, rows)
	case Timestamp:
		takeValues(out, out.times, d, d.times, rows)
	}
	return newColumn(out)
}

// Gather returns a new column with one owner that holds the cells refs of
// cols, in that order: cell Row of cols[Blk]. All columns must have kind k.
// Every cell is copied once, into storage of the right size (D113, G75).
func Gather(k Kind, cols []Column, refs []Ref) Column {
	ds := make([]*data, len(cols))
	for i, c := range cols {
		if c.d.kind != k {
			panic(fmt.Sprintf("block: gather of a %s column into %s", c.d.kind, k))
		}
		ds[i] = c.d
	}
	for _, r := range refs {
		if r.Row >= 0 {
			cols[r.Blk].checkIndex(int(r.Row))
		}
	}
	out := &data{kind: k, length: len(refs)}
	if k == Text {
		size := 0
		for _, r := range refs {
			if r.Row >= 0 {
				d := ds[r.Blk]
				size += int(d.offs[r.Row+1] - d.offs[r.Row])
			}
		}
		out.makeText(len(refs), size)
		for i, r := range refs {
			if d := ds[r.Blk]; r.Row < 0 || d.isNull(int(r.Row)) {
				out.markNull(i)
			} else {
				out.text = append(out.text, d.text[d.offs[r.Row]:d.offs[r.Row+1]]...)
			}
			out.offs[i+1] = uint32(len(out.text))
		}
		return newColumn(out)
	}
	out.makeValues(len(refs))
	for i, r := range refs {
		if d := ds[r.Blk]; r.Row < 0 || d.isNull(int(r.Row)) {
			out.markNull(i)
		} else {
			out.copyValue(i, d, int(r.Row))
		}
	}
	return newColumn(out)
}

// takeValues copies the values rows of src into dst, the values of out; a
// null cell keeps the zero value.
func takeValues[T any](out *data, dst []T, src *data, vals []T, rows []int) {
	if src.nulls == nil {
		for i, r := range rows {
			if r < 0 {
				out.markNull(i)
				continue
			}
			dst[i] = vals[r]
		}
		return
	}
	for i, r := range rows {
		if r < 0 || src.isNull(r) {
			out.markNull(i)
			continue
		}
		dst[i] = vals[r]
	}
}

// makeText makes the storage of a text column of n cells and size bytes.
func (d *data) makeText(n, size int) {
	if size > math.MaxUint32 {
		panic("block: text column over 4 GiB")
	}
	d.text = make([]byte, 0, size)
	d.offs = make([]uint32, n+1)
}

// makeValues makes the value slice of a column of n cells of another kind
// than text.
func (d *data) makeValues(n int) {
	switch d.kind {
	case Int:
		d.ints = make([]int64, n)
	case Float:
		d.floats = make([]float64, n)
	case Bool:
		d.bools = make([]bool, n)
	case Timestamp:
		d.times = make([]time.Time, n)
	default:
		panic(fmt.Sprintf("block: unknown kind %v", d.kind))
	}
}

// copyValue copies value r of src, which has d's kind, into cell i.
func (d *data) copyValue(i int, src *data, r int) {
	switch d.kind {
	case Int:
		d.ints[i] = src.ints[r]
	case Float:
		d.floats[i] = src.floats[r]
	case Bool:
		d.bools[i] = src.bools[r]
	case Timestamp:
		d.times[i] = src.times[r]
	}
}

// markNull marks cell i of a column under construction as null.
func (d *data) markNull(i int) {
	if d.nulls == nil {
		d.nulls = make([]uint64, (d.length+63)/64)
	}
	d.nulls[i/64] |= 1 << (i % 64)
	d.nnull++
}

// Concat returns a new column with one owner that holds the cells of cols one
// after another. All columns must have kind k.
func Concat(k Kind, cols ...Column) Column {
	n := 0
	for _, c := range cols {
		n += c.Len()
	}
	refs := make([]Ref, 0, n)
	for j, c := range cols {
		for i := range c.Len() {
			refs = append(refs, Ref{int32(j), int32(i)})
		}
	}
	return Gather(k, cols, refs)
}

// Take returns a new block with the rows of b, in that order (see
// Column.Take).
func (b Block) Take(rows []int) Block {
	out := Block{length: len(rows), cols: make([]Column, len(b.cols))}
	for i, c := range b.cols {
		out.cols[i] = c.Take(rows)
	}
	return out
}

// GatherBlocks returns a new block with the rows refs of blks, which have
// the same columns (see Gather).
func GatherBlocks(blks []Block, refs []Ref) Block {
	if len(blks) == 0 {
		return Block{length: len(refs)}
	}
	width := blks[0].Width()
	out := Block{length: len(refs), cols: make([]Column, width)}
	cols := make([]Column, len(blks))
	for i := range width {
		for j, b := range blks {
			cols[j] = b.cols[i]
		}
		out.cols[i] = Gather(cols[0].Kind(), cols, refs)
	}
	return out
}
