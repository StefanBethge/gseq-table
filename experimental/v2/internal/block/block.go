package block

import (
	"errors"
	"fmt"
)

// Block is a run of rows stored as typed columns of equal length (D29). The
// engine processes data block by block; how many rows a block holds is set
// by the caller, not fixed here.
//
// Copying a Block value shares its column slice. Use Share to hand the block
// to another owner, for example raw and working state (D55).
type Block struct {
	length int
	cols   []Column
}

// New returns a block of the given columns. All columns must have the same
// length.
func New(cols ...Column) (Block, error) {
	b := Block{cols: make([]Column, 0, len(cols))}
	for _, c := range cols {
		if err := b.AppendColumn(c); err != nil {
			return Block{}, err
		}
	}
	return b, nil
}

// Len returns the number of rows.
func (b Block) Len() int { return b.length }

// Width returns the number of columns.
func (b Block) Width() int { return len(b.cols) }

// Column returns column i as a read-only view.
func (b Block) Column(i int) Column { return b.cols[i] }

// AppendColumn adds c as the last column. Its length must match the block,
// unless the block has no columns yet.
func (b *Block) AppendColumn(c Column) error {
	if len(b.cols) > 0 && c.Len() != b.length {
		return fmt.Errorf("block: column %d has %d rows, block has %d", len(b.cols), c.Len(), b.length)
	}
	b.length = c.Len()
	b.cols = append(b.cols, c)
	return nil
}

// SetColumn replaces column i with c, which must have the block's length.
// The block gives up its ownership of the old column.
func (b *Block) SetColumn(i int, c Column) error {
	if c.Len() != b.length {
		return fmt.Errorf("block: column %d has %d rows, block has %d", i, c.Len(), b.length)
	}
	b.cols[i].Release()
	b.cols[i] = c
	return nil
}

// Share returns a block over the same columns for another owner. No values
// are copied; every column counts as shared until one side changes it (D55).
func (b Block) Share() Block {
	cols := make([]Column, len(b.cols))
	for i, c := range b.cols {
		cols[i] = c.Share()
	}
	return Block{length: b.length, cols: cols}
}

// Release gives up the block's ownership of its columns. The block must not
// be used afterwards.
func (b *Block) Release() {
	for i := range b.cols {
		b.cols[i].Release()
	}
	b.cols = nil
}

// Mutable is a column of a block that may be changed in place.
type Mutable struct {
	Column *Column
	// Copied is true if the column was shared and had to be copied first.
	// The engine records such copies in the trace (D64).
	Copied bool
}

// MutableColumn prepares column i for a change and returns it. A shared
// column is copied once; the other owners keep the original values (D55).
func (b *Block) MutableColumn(i int) Mutable {
	copied := b.cols[i].Mutate()
	return Mutable{Column: &b.cols[i], Copied: copied}
}

// TextBuilder collects rows of raw text fields, as a reader delivers them,
// into blocks of text columns (D29). An empty field stays empty text; raw
// columns contain no nulls (D30).
type TextBuilder struct {
	width, length int
	cols          []*Builder
}

// NewTextBuilder returns a builder for blocks of width text columns and at
// most length rows. The engine sets length; it must be positive.
func NewTextBuilder(width, length int) (*TextBuilder, error) {
	if width < 0 {
		return nil, fmt.Errorf("block: width %d is negative", width)
	}
	if length <= 0 {
		return nil, errors.New("block: block length must be positive")
	}
	tb := &TextBuilder{width: width, length: length, cols: make([]*Builder, width)}
	for i := range tb.cols {
		tb.cols[i] = NewBuilder(Text, length)
	}
	return tb, nil
}

// Add appends one row. When the row fills the block, Add returns the block
// and full is true.
func (tb *TextBuilder) Add(fields []string) (blk Block, full bool, err error) {
	if len(fields) != tb.width {
		return Block{}, false, fmt.Errorf("block: row has %d fields, want %d", len(fields), tb.width)
	}
	for i, f := range fields {
		tb.cols[i].AppendText(f)
	}
	if tb.rows() < tb.length {
		return Block{}, false, nil
	}
	return tb.build(), true, nil
}

// Flush returns the rows collected since the last full block, if any.
func (tb *TextBuilder) Flush() (Block, bool) {
	if tb.rows() == 0 {
		return Block{}, false
	}
	return tb.build(), true
}

func (tb *TextBuilder) rows() int {
	if tb.width == 0 {
		return 0
	}
	return tb.cols[0].Len()
}

func (tb *TextBuilder) build() Block {
	b := Block{length: tb.rows(), cols: make([]Column, tb.width)}
	for i, cb := range tb.cols {
		b.cols[i] = cb.Build()
	}
	return b
}
