package gtable

import (
	"fmt"
	"time"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

// Type is the type of the values in a column (design decision D29). Columns
// that a reader reads from a delivery are TypeText until a step casts them.
type Type uint8

const (
	TypeText      = Type(block.Text)
	TypeInt       = Type(block.Int)
	TypeFloat     = Type(block.Float)
	TypeBool      = Type(block.Bool)
	TypeTimestamp = Type(block.Timestamp)
)

func (t Type) String() string { return block.Kind(t).String() }

func typeOf(k block.Kind) Type { return Type(k) }

// Column is a named, typed column. Every cell either holds a value or is
// null, and empty text is a value, not null (D30). Accessors return the
// value and whether the cell holds one; they panic if the column has another
// type or the index is out of range.
type Column struct {
	name string
	col  block.Column
}

func newColumn(name string, c block.Column) Column {
	return Column{name: name, col: c}
}

// Name returns the column name.
func (c Column) Name() string { return c.name }

// Type returns the type of the values.
func (c Column) Type() Type { return typeOf(c.col.Kind()) }

// Len returns the number of cells.
func (c Column) Len() int { return c.col.Len() }

// NullCount returns the number of null cells.
func (c Column) NullCount() int { return c.col.NullCount() }

// IsNull reports whether cell i has no value.
func (c Column) IsNull(i int) bool { return c.col.IsNull(i) }

// Text returns cell i of a TypeText column; ok is false for null.
func (c Column) Text(i int) (v string, ok bool) { return c.col.Text(i) }

// Int returns cell i of a TypeInt column; ok is false for null.
func (c Column) Int(i int) (v int64, ok bool) { return c.col.Int(i) }

// Float returns cell i of a TypeFloat column; ok is false for null.
func (c Column) Float(i int) (v float64, ok bool) { return c.col.Float(i) }

// Bool returns cell i of a TypeBool column; ok is false for null.
func (c Column) Bool(i int) (v bool, ok bool) { return c.col.Bool(i) }

// Timestamp returns cell i of a TypeTimestamp column; ok is false for null.
func (c Column) Timestamp(i int) (v time.Time, ok bool) { return c.col.Timestamp(i) }

// Format returns cell i as text, in the form Table.String shows: integers
// in decimal, floats in the shortest form, timestamps in RFC 3339; ok is
// false for null.
func (c Column) Format(i int) (v string, ok bool) {
	if c.col.IsNull(i) {
		return "", false
	}
	if c.col.Kind() == block.Text {
		return c.col.Text(i)
	}
	vv := &vec{kind: c.col.Kind(), ints: c.col.Ints(), flts: c.col.Floats(),
		bools: c.col.Bools(), times: c.col.Timestamps()}
	return formatCell(vv.kind, vv, i), true
}

// Texts returns a text column with the given values. Empty text is a value,
// not null (D30); use WithNulls for null cells.
func Texts(name string, values ...string) Column {
	b := block.NewBuilder(block.Text, len(values))
	size := 0
	for _, v := range values {
		size += len(v)
	}
	b.ReserveText(size)
	for _, v := range values {
		b.AppendText(v)
	}
	return newColumn(name, b.Build())
}

// Ints returns an integer column with the given values.
func Ints(name string, values ...int64) Column {
	b := block.NewBuilder(block.Int, len(values))
	for _, v := range values {
		b.AppendInt(v)
	}
	return newColumn(name, b.Build())
}

// Floats returns a floating-point column with the given values.
func Floats(name string, values ...float64) Column {
	b := block.NewBuilder(block.Float, len(values))
	for _, v := range values {
		b.AppendFloat(v)
	}
	return newColumn(name, b.Build())
}

// Bools returns a boolean column with the given values.
func Bools(name string, values ...bool) Column {
	b := block.NewBuilder(block.Bool, len(values))
	for _, v := range values {
		b.AppendBool(v)
	}
	return newColumn(name, b.Build())
}

// Timestamps returns a timestamp column with the given values.
func Timestamps(name string, values ...time.Time) Column {
	b := block.NewBuilder(block.Timestamp, len(values))
	for _, v := range values {
		b.AppendTimestamp(v)
	}
	return newColumn(name, b.Build())
}

// WithNulls returns a copy of c in which the given cells are null. It panics
// if a row is out of range.
func (c Column) WithNulls(rows ...int) Column {
	all := make([]int, c.Len())
	for i := range all {
		all[i] = i
	}
	for _, r := range rows {
		if r < 0 || r >= len(all) {
			panic(fmt.Sprintf("gtable: WithNulls: row %d out of range [0, %d)", r, len(all)))
		}
		all[r] = -1
	}
	return newColumn(c.name, c.col.Take(all))
}
