// Package block holds the unit the v2 engine processes: blocks of typed
// columns in which every cell can be null (design decisions D29, D30). It also
// tracks which columns are shared between owners, so that raw and working
// state can share a column until a step changes it (D55).
package block

import (
	"fmt"
	"math"
	"sync/atomic"
	"time"
	"unsafe"
)

// Kind is the type of the values in a column (D29).
type Kind uint8

const (
	Text Kind = iota + 1
	Int
	Float
	Bool
	Timestamp
)

func (k Kind) String() string {
	switch k {
	case Text:
		return "text"
	case Int:
		return "int"
	case Float:
		return "float"
	case Bool:
		return "bool"
	case Timestamp:
		return "timestamp"
	}
	return fmt.Sprintf("Kind(%d)", uint8(k))
}

// data is the storage of a column. Exactly one of the value slices is set,
// matching kind; a text column has text and offs. A null cell keeps the zero
// value in its slot, a null text cell no bytes.
//
// A text column holds the bytes of its values one after another in text,
// and value i is text[offs[i]:offs[i+1]], as in the layout of Apache Arrow
// (D113, G75). Neither slice holds pointers, so the garbage collector does
// not scan them. Bytes once written to text never change: Text returns views
// into the buffer, and a changed column gets a new one (D55).
type data struct {
	kind   Kind
	length int
	nulls  []uint64 // bit i set: cell i is null; nil while there is no null
	nnull  int

	text   []byte
	offs   []uint32
	ints   []int64
	floats []float64
	bools  []bool
	times  []time.Time

	owners atomic.Int32 // number of Column handles that hold this data
}

// Column is a handle to the typed values of one column. A column obtained
// from a Builder has one owner. Share hands the same values to another owner
// without copying; Mutate copies them on the first change while they are
// shared (D55).
//
// Copying a Column value does not register a new owner: only Share does. A
// copied value is a read-only view.
type Column struct {
	d *data
}

func newColumn(d *data) Column {
	d.owners.Store(1)
	return Column{d: d}
}

// Kind returns the type of the values.
func (c Column) Kind() Kind { return c.d.kind }

// Len returns the number of cells.
func (c Column) Len() int { return c.d.length }

// NullCount returns the number of null cells.
func (c Column) NullCount() int { return c.d.nnull }

// IsNull reports whether cell i has no value. Empty text is a value (D30).
func (c Column) IsNull(i int) bool {
	c.checkIndex(i)
	return c.d.isNull(i)
}

func (d *data) isNull(i int) bool {
	return d.nulls != nil && d.nulls[i/64]&(1<<(i%64)) != 0
}

// Text returns cell i of a text column; ok is false for null. The value is
// a view into the buffer of the column, not a copy.
func (c Column) Text(i int) (v string, ok bool) {
	c.checkRead(Text, i)
	if c.d.isNull(i) {
		return "", false
	}
	return c.d.textAt(i), true
}

// TextBytes returns the bytes of cell i of a text column, nil for null. The
// bytes must not be modified.
func (c Column) TextBytes(i int) []byte {
	c.checkRead(Text, i)
	if c.d.isNull(i) {
		return nil
	}
	return c.d.text[c.d.offs[i]:c.d.offs[i+1]:c.d.offs[i+1]]
}

func (d *data) textAt(i int) string {
	a, b := d.offs[i], d.offs[i+1]
	if a == b {
		return ""
	}
	return unsafe.String(&d.text[a], b-a)
}

// Int returns cell i of an integer column; ok is false for null.
func (c Column) Int(i int) (v int64, ok bool) {
	c.checkRead(Int, i)
	return c.d.ints[i], !c.d.isNull(i)
}

// Float returns cell i of a floating-point column; ok is false for null.
func (c Column) Float(i int) (v float64, ok bool) {
	c.checkRead(Float, i)
	return c.d.floats[i], !c.d.isNull(i)
}

// Bool returns cell i of a boolean column; ok is false for null.
func (c Column) Bool(i int) (v bool, ok bool) {
	c.checkRead(Bool, i)
	return c.d.bools[i], !c.d.isNull(i)
}

// Timestamp returns cell i of a timestamp column; ok is false for null.
func (c Column) Timestamp(i int) (v time.Time, ok bool) {
	c.checkRead(Timestamp, i)
	return c.d.times[i], !c.d.isNull(i)
}

// TextData returns the buffer and the offsets of a text column, or nil for
// another kind: value i is text[offs[i]:offs[i+1]], and a null cell has no
// bytes. The slices must not be modified.
func (c Column) TextData() (text []byte, offs []uint32) { return c.d.text, c.d.offs }

// Ints returns the values of an integer column, or nil for another kind. Null
// cells hold 0. The slice must not be modified.
func (c Column) Ints() []int64 { return c.d.ints }

// Floats returns the values of a floating-point column, or nil for another
// kind. Null cells hold 0. The slice must not be modified.
func (c Column) Floats() []float64 { return c.d.floats }

// Bools returns the values of a boolean column, or nil for another kind. Null
// cells hold false. The slice must not be modified.
func (c Column) Bools() []bool { return c.d.bools }

// Timestamps returns the values of a timestamp column, or nil for another
// kind. Null cells hold the zero time. The slice must not be modified.
func (c Column) Timestamps() []time.Time { return c.d.times }

// Shared reports whether another owner holds the same values.
func (c Column) Shared() bool { return c.d.owners.Load() > 1 }

// Share registers another owner of the values and returns its handle. No
// values are copied.
func (c Column) Share() Column {
	c.d.owners.Add(1)
	return Column{d: c.d}
}

// Release gives up this handle's ownership. The handle must not be used
// afterwards.
func (c *Column) Release() {
	c.d.owners.Add(-1)
	c.d = nil
}

// Mutate makes this handle the only owner of its values, so the setters may
// change them. If the values are shared, they are copied once and copied is
// true; the other owners keep the original values (D55, D64).
func (c *Column) Mutate() (copied bool) {
	if !c.Shared() {
		return false
	}
	old := c.d
	d := &data{
		kind:   old.kind,
		length: old.length,
		nnull:  old.nnull,
		nulls:  cloneNil(old.nulls),
		text:   cloneNil(old.text),
		offs:   cloneNil(old.offs),
		ints:   cloneNil(old.ints),
		floats: cloneNil(old.floats),
		bools:  cloneNil(old.bools),
		times:  cloneNil(old.times),
	}
	*c = newColumn(d)
	old.owners.Add(-1)
	return true
}

func cloneNil[T any](s []T) []T {
	if s == nil {
		return nil
	}
	return append(make([]T, 0, len(s)), s...)
}

// SetNull makes cell i null.
func (c *Column) SetNull(i int) {
	c.checkWrite(c.d.kind, i)
	c.d.setNull(i)
	switch c.d.kind {
	case Text:
		// The bytes stay: they may be read through a view (D55).
	case Int:
		c.d.ints[i] = 0
	case Float:
		c.d.floats[i] = 0
	case Bool:
		c.d.bools[i] = false
	case Timestamp:
		c.d.times[i] = time.Time{}
	}
}

// SetText stores v in cell i of a text column. The bytes of a text column
// never change once written (see data), so it writes a new buffer; set
// many cells by building a new column instead.
func (c *Column) SetText(i int, v string) {
	c.checkWrite(Text, i)
	d := c.d
	a, b := d.offs[i], d.offs[i+1]
	text := make([]byte, 0, len(d.text)-int(b-a)+len(v))
	text = append(append(append(text, d.text[:a]...), v...), d.text[b:]...)
	if len(text) > math.MaxUint32 {
		panic("block: text column over 4 GiB")
	}
	offs := make([]uint32, len(d.offs))
	copy(offs, d.offs[:i+1])
	for j := i + 1; j < len(offs); j++ {
		offs[j] = d.offs[j] - b + a + uint32(len(v))
	}
	d.text, d.offs = text, offs
	d.clearNull(i)
}

// SetInt stores v in cell i of an integer column.
func (c *Column) SetInt(i int, v int64) {
	c.checkWrite(Int, i)
	c.d.ints[i] = v
	c.d.clearNull(i)
}

// SetFloat stores v in cell i of a floating-point column.
func (c *Column) SetFloat(i int, v float64) {
	c.checkWrite(Float, i)
	c.d.floats[i] = v
	c.d.clearNull(i)
}

// SetBool stores v in cell i of a boolean column.
func (c *Column) SetBool(i int, v bool) {
	c.checkWrite(Bool, i)
	c.d.bools[i] = v
	c.d.clearNull(i)
}

// SetTimestamp stores v in cell i of a timestamp column.
func (c *Column) SetTimestamp(i int, v time.Time) {
	c.checkWrite(Timestamp, i)
	c.d.times[i] = v
	c.d.clearNull(i)
}

func (d *data) setNull(i int) {
	if d.nulls == nil {
		d.nulls = make([]uint64, (d.length+63)/64)
	}
	if d.nulls[i/64]&(1<<(i%64)) == 0 {
		d.nulls[i/64] |= 1 << (i % 64)
		d.nnull++
	}
}

func (d *data) clearNull(i int) {
	if d.isNull(i) {
		d.nulls[i/64] &^= 1 << (i % 64)
		d.nnull--
	}
}

func (c Column) checkIndex(i int) {
	if i < 0 || i >= c.d.length {
		panic(fmt.Sprintf("block: cell %d out of range [0, %d)", i, c.d.length))
	}
}

func (c Column) checkRead(k Kind, i int) {
	if c.d.kind != k {
		panic(fmt.Sprintf("block: %s access on a %s column", k, c.d.kind))
	}
	c.checkIndex(i)
}

func (c Column) checkWrite(k Kind, i int) {
	c.checkRead(k, i)
	if c.Shared() {
		panic("block: change of a shared column without Mutate")
	}
}

// Builder appends cells to a new column of one kind.
type Builder struct {
	kind     Kind
	capacity int // cells to make room for
	reserve  int // bytes to make room for in a text column
	d        *data
}

// NewBuilder returns a builder for a column of kind k with room for capacity
// cells.
func NewBuilder(k Kind, capacity int) *Builder {
	switch k {
	case Text, Int, Float, Bool, Timestamp:
	default:
		panic(fmt.Sprintf("block: unknown kind %v", k))
	}
	return &Builder{kind: k, capacity: capacity}
}

// ReserveText makes room for n bytes of text in the column being built. It
// is an estimate; a builder grows past it.
func (b *Builder) ReserveText(n int) {
	b.reserve = n
	if b.d != nil && b.kind == Text && cap(b.d.text)-len(b.d.text) < n {
		b.d.text = append(make([]byte, 0, len(b.d.text)+n), b.d.text...)
	}
}

// start makes the storage of the column on the first cell, so that a
// builder that is built and dropped costs nothing more.
func (b *Builder) start() *data {
	if b.d != nil {
		return b.d
	}
	d := &data{kind: b.kind}
	switch b.kind {
	case Text:
		d.text = make([]byte, 0, b.reserve)
		d.offs = make([]uint32, 1, b.capacity+1)
	case Int:
		d.ints = make([]int64, 0, b.capacity)
	case Float:
		d.floats = make([]float64, 0, b.capacity)
	case Bool:
		d.bools = make([]bool, 0, b.capacity)
	case Timestamp:
		d.times = make([]time.Time, 0, b.capacity)
	}
	b.d = d
	return d
}

// Len returns the number of cells appended so far.
func (b *Builder) Len() int {
	if b.d == nil {
		return 0
	}
	return b.d.length
}

// AppendNull appends a null cell.
func (b *Builder) AppendNull() {
	d := b.start()
	switch d.kind {
	case Text:
		d.offs = append(d.offs, uint32(len(d.text)))
	case Int:
		d.ints = append(d.ints, 0)
	case Float:
		d.floats = append(d.floats, 0)
	case Bool:
		d.bools = append(d.bools, false)
	case Timestamp:
		d.times = append(d.times, time.Time{})
	}
	i := d.length
	d.length++
	if need := (d.length + 63) / 64; len(d.nulls) < need {
		d.nulls = append(d.nulls, make([]uint64, need-len(d.nulls))...)
	}
	d.nulls[i/64] |= 1 << (i % 64)
	d.nnull++
}

// AppendText appends a text value. Empty text is a value, not null (D30).
func (b *Builder) AppendText(v string) {
	d := b.check(Text)
	d.text = append(d.text, v...)
	d.endText()
	b.grow()
}

// AppendTextBytes appends a text value given as bytes, which are copied.
func (b *Builder) AppendTextBytes(v []byte) {
	d := b.check(Text)
	d.text = append(d.text, v...)
	d.endText()
	b.grow()
}

// endText ends the value appended last to the buffer of a text column.
func (d *data) endText() {
	if len(d.text) > math.MaxUint32 {
		panic("block: text column over 4 GiB")
	}
	d.offs = append(d.offs, uint32(len(d.text)))
}

// AppendInt appends an integer value.
func (b *Builder) AppendInt(v int64) {
	d := b.check(Int)
	d.ints = append(d.ints, v)
	b.grow()
}

// AppendFloat appends a floating-point value.
func (b *Builder) AppendFloat(v float64) {
	d := b.check(Float)
	d.floats = append(d.floats, v)
	b.grow()
}

// AppendBool appends a boolean value.
func (b *Builder) AppendBool(v bool) {
	d := b.check(Bool)
	d.bools = append(d.bools, v)
	b.grow()
}

// AppendTimestamp appends a timestamp value.
func (b *Builder) AppendTimestamp(v time.Time) {
	d := b.check(Timestamp)
	d.times = append(d.times, v)
	b.grow()
}

func (b *Builder) check(k Kind) *data {
	if b.kind != k {
		panic(fmt.Sprintf("block: %s value appended to a %s column", k, b.kind))
	}
	return b.start()
}

func (b *Builder) grow() {
	b.d.length++
	if b.d.nulls != nil && len(b.d.nulls) < (b.d.length+63)/64 {
		b.d.nulls = append(b.d.nulls, 0)
	}
}

// Build returns the column built so far, with one owner, and resets the
// builder to an empty column of the same kind. The next column is made
// with room for as many cells and bytes as this one had.
func (b *Builder) Build() Column {
	d := b.start()
	if d.nnull == 0 {
		d.nulls = nil
	}
	b.d = nil
	b.capacity = max(b.capacity, d.length)
	if d.kind == Text {
		b.reserve = max(b.reserve, len(d.text))
	}
	return newColumn(d)
}

// Bytes estimates the memory the values of the column take: the value
// slice, the bytes of the texts and the null bitmap. The engine counts it
// against the memory budget (D28).
func (c Column) Bytes() int64 {
	d := c.d
	n := int64(len(d.nulls)) * 8
	switch d.kind {
	case Text:
		n += int64(len(d.text)) + 4*int64(len(d.offs))
	case Int, Float:
		n += int64(d.length) * 8
	case Bool:
		n += int64(d.length)
	case Timestamp:
		n += int64(d.length) * 24
	}
	return n
}

// AppendFrom appends cell i of c, which has the builder's kind.
func (b *Builder) AppendFrom(c Column, i int) {
	if c.IsNull(i) {
		b.AppendNull()
		return
	}
	switch c.Kind() {
	case Text:
		b.AppendTextBytes(c.d.text[c.d.offs[i]:c.d.offs[i+1]])
	case Int:
		b.AppendInt(c.d.ints[i])
	case Float:
		b.AppendFloat(c.d.floats[i])
	case Bool:
		b.AppendBool(c.d.bools[i])
	case Timestamp:
		b.AppendTimestamp(c.d.times[i])
	}
}
