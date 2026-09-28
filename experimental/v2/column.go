package gtable

import (
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
