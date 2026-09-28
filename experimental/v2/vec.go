package gtable

import (
	"time"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

// vec is the working form of a column while an expression is evaluated over a
// block: one value slice matching kind, and a null flag per cell (D30).
type vec struct {
	kind  block.Kind
	n     int
	null  []bool
	texts []string
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
		v.texts = c.Texts()
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
	for i := range v.n {
		if v.null[i] {
			b.AppendNull()
			continue
		}
		switch v.kind {
		case block.Text:
			b.AppendText(v.texts[i])
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
		v.texts[i] = src.texts[j]
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
