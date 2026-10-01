package gtable

import (
	"strings"
	"time"
	"unsafe"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

// vec is the working form of a column while an expression is evaluated over a
// block: one value slice matching kind, and a null flag per cell (D30). A
// vec that reads a text column keeps its buffer and offsets (tbuf, toffs);
// one that is computed holds texts. Read text cells with text.
type vec struct {
	kind  block.Kind
	n     int
	null  []bool
	texts []string
	tbuf  []byte
	toffs []uint32
	arena []byte // bytes of computed texts, which texts views (see appendText)
	ints  []int64
	flts  []float64
	bools []bool
	times []time.Time
}

func newVec(k block.Kind, n int) *vec {
	v := &vec{kind: k, n: n, null: make([]bool, n)}
	switch k {
	case block.Text:
		v.texts = make([]string, n)
	case block.Int:
		v.ints = make([]int64, n)
	case block.Float:
		v.flts = make([]float64, n)
	case block.Bool:
		v.bools = make([]bool, n)
	case block.Timestamp:
		v.times = make([]time.Time, n)
	}
	return v
}

// vecOf reads a block column into a vec. The value slices are shared with
// the column and must not be modified.
func vecOf(c block.Column) *vec {
	v := &vec{kind: c.Kind(), n: c.Len(), null: make([]bool, c.Len())}
	if c.NullCount() > 0 {
		for i := range v.null {
			v.null[i] = c.IsNull(i)
		}
	}
	switch v.kind {
	case block.Text:
		v.tbuf, v.toffs = c.TextData()
	case block.Int:
		v.ints = c.Ints()
	case block.Float:
		v.flts = c.Floats()
	case block.Bool:
		v.bools = c.Bools()
	case block.Timestamp:
		v.times = c.Timestamps()
	}
	return v
}

// text returns text cell i; for a vec that reads a column, a view into its
// buffer.
func (v *vec) text(i int) string {
	if v.toffs == nil {
		return v.texts[i]
	}
	a, b := v.toffs[i], v.toffs[i+1]
	if a == b {
		return ""
	}
	return unsafe.String(&v.tbuf[a], b-a)
}

// appendText sets text cell i of a computed vec to the parts one after
// another, in the arena of v instead of a string of its own (D113). Bytes
// in the arena are never written again, so the views stay valid when it
// grows.
func (v *vec) appendText(i int, parts []*vec, j int) {
	start := len(v.arena)
	for _, p := range parts {
		v.arena = append(v.arena, p.text(j)...)
	}
	if n := len(v.arena) - start; n > 0 {
		v.texts[i] = unsafe.String(&v.arena[start], n)
	} else {
		v.texts[i] = ""
	}
}

// float returns cell i as a float, widening an integer (D71).
func (v *vec) float(i int) float64 {
	if v.kind == block.Int {
		return float64(v.ints[i])
	}
	return v.flts[i]
}

// column builds a block column from the cells of v.
func (v *vec) column() block.Column {
	b := block.NewBuilder(v.kind, v.n)
	if v.kind == block.Text {
		size := 0
		for i := range v.n {
			if !v.null[i] {
				size += len(v.text(i))
			}
		}
		b.ReserveText(size)
	}
	for i := range v.n {
		if v.null[i] {
			b.AppendNull()
			continue
		}
		switch v.kind {
		case block.Text:
			b.AppendText(v.text(i))
		case block.Int:
			b.AppendInt(v.ints[i])
		case block.Float:
			b.AppendFloat(v.flts[i])
		case block.Bool:
			b.AppendBool(v.bools[i])
		case block.Timestamp:
			b.AppendTimestamp(v.times[i])
		}
	}
	return b.Build()
}

// set copies cell j of src into cell i of v; both have the same kind.
func (v *vec) set(i int, src *vec, j int) {
	if src.null[j] {
		v.null[i] = true
		return
	}
	v.null[i] = false
	switch v.kind {
	case block.Text:
		// A copy: the view would hold the whole buffer of src (D113).
		v.texts[i] = strings.Clone(src.text(j))
	case block.Int:
		v.ints[i] = src.ints[j]
	case block.Float:
		v.flts[i] = src.float(j)
	case block.Bool:
		v.bools[i] = src.bools[j]
	case block.Timestamp:
		v.times[i] = src.times[j]
	}
}

// format returns cell i as text for a reject's value.
func (v *vec) format(i int) (string, bool) {
	if v.null[i] {
		return "", false
	}
	return formatCell(v.kind, v, i), true
}

// writeInto writes the cells of v into c, which has v's kind and length and
// may be changed in place. A text column is never written in place (see
// setColumn).
func (v *vec) writeInto(c *block.Column) {
	for i := range v.n {
		if v.null[i] {
			c.SetNull(i)
			continue
		}
		switch v.kind {
		case block.Int:
			c.SetInt(i, v.ints[i])
		case block.Float:
			c.SetFloat(i, v.flts[i])
		case block.Bool:
			c.SetBool(i, v.bools[i])
		case block.Timestamp:
			c.SetTimestamp(i, v.times[i])
		}
	}
}

// aliases reports whether v holds the values of c, as a vec that reads a
// column does (vecOf).
func (v *vec) aliases(c block.Column) bool {
	if v.kind != c.Kind() || v.n == 0 || c.Len() == 0 {
		return false
	}
	switch v.kind {
	case block.Text:
		_, offs := c.TextData()
		return v.toffs != nil && &v.toffs[0] == &offs[0]
	case block.Int:
		return &v.ints[0] == &c.Ints()[0]
	case block.Float:
		return &v.flts[0] == &c.Floats()[0]
	case block.Bool:
		return &v.bools[0] == &c.Bools()[0]
	case block.Timestamp:
		return &v.times[0] == &c.Timestamps()[0]
	}
	return false
}
