package spill

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

// Writer encodes values and blocks into a spill file. Errors are sticky:
// Flush returns the first one.
type Writer struct {
	w   *bufio.Writer
	n   int64
	err error
	buf [binary.MaxVarintLen64]byte
}

// NewWriter returns a writer that appends to w.
func NewWriter(w io.Writer) *Writer { return &Writer{w: bufio.NewWriterSize(w, 64<<10)} }

// Offset returns the number of bytes written so far.
func (w *Writer) Offset() int64 { return w.n }

func (w *Writer) write(p []byte) {
	if w.err != nil {
		return
	}
	n, err := w.w.Write(p)
	w.n += int64(n)
	w.err = err
}

// Uvarint writes v.
func (w *Writer) Uvarint(v uint64) { w.write(binary.AppendUvarint(w.buf[:0], v)) }

// Varint writes v.
func (w *Writer) Varint(v int64) { w.write(binary.AppendVarint(w.buf[:0], v)) }

// String writes s with its length.
func (w *Writer) String(s string) {
	w.Uvarint(uint64(len(s)))
	if w.err == nil {
		n, err := w.w.WriteString(s)
		w.n += int64(n)
		w.err = err
	}
}

// Bytes writes p with its length, like String.
func (w *Writer) Bytes(p []byte) {
	w.Uvarint(uint64(len(p)))
	w.write(p)
}

// Bool writes b.
func (w *Writer) Bool(b bool) {
	if b {
		w.write([]byte{1})
	} else {
		w.write([]byte{0})
	}
}

// Block writes the rows of b with the kind and the null cells of every
// column.
func (w *Writer) Block(b block.Block) {
	w.Uvarint(uint64(b.Len()))
	w.Uvarint(uint64(b.Width()))
	for i := range b.Width() {
		w.column(b.Column(i))
	}
}

func (w *Writer) column(c block.Column) {
	w.Uvarint(uint64(c.Kind()))
	nulls := c.NullCount() > 0
	w.Bool(nulls)
	for i := range c.Len() {
		if nulls {
			null := c.IsNull(i)
			w.Bool(null)
			if null {
				continue
			}
		}
		switch c.Kind() {
		case block.Text:
			w.Bytes(c.TextBytes(i))
		case block.Int:
			w.Varint(c.Ints()[i])
		case block.Float:
			w.write(binary.LittleEndian.AppendUint64(w.buf[:0], math.Float64bits(c.Floats()[i])))
		case block.Bool:
			w.Bool(c.Bools()[i])
		case block.Timestamp:
			p, err := c.Timestamps()[i].MarshalBinary()
			if err != nil && w.err == nil {
				w.err = err
			}
			w.String(string(p))
		}
	}
}

// Flush writes buffered data and returns the first error.
func (w *Writer) Flush() error {
	if w.err == nil {
		w.err = w.w.Flush()
	}
	return w.err
}

// Reader decodes what a Writer wrote. Errors are sticky: Err returns the
// first one, and the values read after it are zero.
type Reader struct {
	r   *bufio.Reader
	err error
	buf []byte // scratch for the bytes of a text cell
}

// NewReader returns a reader over r.
func NewReader(r io.Reader) *Reader { return &Reader{r: bufio.NewReaderSize(r, 32<<10)} }

// Section returns a reader over n bytes of f from off.
func Section(f io.ReaderAt, off, n int64) *Reader {
	return NewReader(io.NewSectionReader(f, off, n))
}

// Err returns the first error; io.ErrUnexpectedEOF for data that ends
// early.
func (r *Reader) Err() error { return r.err }

func (r *Reader) fail(err error) {
	if r.err == nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		r.err = err
	}
}

// Uvarint reads a value written by Writer.Uvarint.
func (r *Reader) Uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, err := binary.ReadUvarint(r.r)
	if err != nil {
		r.fail(err)
	}
	return v
}

// Varint reads a value written by Writer.Varint.
func (r *Reader) Varint() int64 {
	if r.err != nil {
		return 0
	}
	v, err := binary.ReadVarint(r.r)
	if err != nil {
		r.fail(err)
	}
	return v
}

// Int reads a non-negative value written by Writer.Uvarint as an int.
func (r *Reader) Int() int {
	v := r.Uvarint()
	if v > math.MaxInt32*1024 {
		r.fail(fmt.Errorf("spill: value %d out of range", v))
		return 0
	}
	return int(v)
}

// String reads a value written by Writer.String.
func (r *Reader) String() string {
	n := r.Int()
	if r.err != nil || n == 0 {
		return ""
	}
	p := make([]byte, n)
	if _, err := io.ReadFull(r.r, p); err != nil {
		r.fail(err)
		return ""
	}
	return string(p)
}

// bytes reads a value written by Writer.Bytes or Writer.String into a
// scratch buffer that the next call overwrites.
func (r *Reader) bytes() []byte {
	n := r.Int()
	if r.err != nil || n == 0 {
		return nil
	}
	if cap(r.buf) < n {
		r.buf = make([]byte, n)
	}
	p := r.buf[:n]
	if _, err := io.ReadFull(r.r, p); err != nil {
		r.fail(err)
		return nil
	}
	return p
}

// Bool reads a value written by Writer.Bool.
func (r *Reader) Bool() bool {
	if r.err != nil {
		return false
	}
	b, err := r.r.ReadByte()
	if err != nil {
		r.fail(err)
	}
	return b == 1
}

// Block reads a block written by Writer.Block.
func (r *Reader) Block() block.Block {
	n, width := r.Int(), r.Int()
	cols := make([]block.Column, 0, width)
	for range width {
		if r.err != nil {
			break
		}
		cols = append(cols, r.column(n))
	}
	if r.err != nil {
		return block.Block{}
	}
	if width == 0 {
		return block.Block{}.Take(make([]int, n))
	}
	b, err := block.New(cols...)
	if err != nil {
		r.fail(err)
	}
	return b
}

func (r *Reader) column(n int) block.Column {
	k := block.Kind(r.Uvarint())
	if k < block.Text || k > block.Timestamp {
		r.fail(fmt.Errorf("spill: unknown kind %d", k))
		return block.Column{}
	}
	b := block.NewBuilder(k, n)
	nulls := r.Bool()
	var p [8]byte
	for range n {
		if nulls && r.Bool() {
			b.AppendNull()
			continue
		}
		switch k {
		case block.Text:
			b.AppendTextBytes(r.bytes())
		case block.Int:
			b.AppendInt(r.Varint())
		case block.Float:
			if _, err := io.ReadFull(r.r, p[:]); err != nil {
				r.fail(err)
			}
			b.AppendFloat(math.Float64frombits(binary.LittleEndian.Uint64(p[:])))
		case block.Bool:
			b.AppendBool(r.Bool())
		case block.Timestamp:
			var t time.Time
			if s := r.String(); r.err == nil {
				if err := t.UnmarshalBinary([]byte(s)); err != nil {
					r.fail(err)
				}
			}
			b.AppendTimestamp(t)
		}
	}
	return b.Build()
}
