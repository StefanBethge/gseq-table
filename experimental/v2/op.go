package gtable

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

// Op is an operation as a value (D31). The same Op runs immediately on a
// Table (Table.Apply and the Table methods) or as a step of a Pipeline, both
// through the same engine.
type Op struct {
	impl opImpl
}

// opImpl is one operation. plan checks it against the input schema before
// any row is read and returns the output schema (D19). Every operation is
// either a streamOp, applied block by block, or a fullOp that needs all rows
// (join, group by, sort; D6).
type opImpl interface {
	kind() string
	plan(in schema) (schema, error)
}

type streamOp interface {
	opImpl
	apply(blk block.Block, in schema, sc *stepCtx) (block.Block, error)
}

type fullOp interface {
	opImpl
	applyAll(blks []block.Block, in schema, sc *stepCtx) (block.Block, error)
}

// stepCtx is what a step needs while it runs: its name for rejects and the
// rejector of the run.
type stepCtx struct {
	step string
	rx   *rejector
}

func (sc *stepCtx) reject(column, value string, hasValue bool, reason, code string) error {
	return sc.rx.add(Reject{Step: sc.step, Column: column, Value: value, HasValue: hasValue, Reason: reason, Code: code})
}

// dropRows returns blk without the rows marked in drop.
func dropRows(blk block.Block, drop []bool) block.Block {
	keep := make([]int, 0, len(drop))
	for i, d := range drop {
		if !d {
			keep = append(keep, i)
		}
	}
	if len(keep) == len(drop) {
		return blk
	}
	return blk.Take(keep)
}

// shareColumns returns a block of the given columns of blk, sharing their
// values (D55).
func shareColumns(blk block.Block, idx []int) block.Block {
	cols := make([]block.Column, len(idx))
	for i, j := range idx {
		cols[i] = blk.Column(j).Share()
	}
	out, err := block.New(cols...)
	if err != nil {
		panic("gtable: " + err.Error())
	}
	return out
}

func allIndexes(n int) []int {
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	return idx
}

// withColumn returns blk with c as column i, or appended if i is -1.
func withColumn(blk block.Block, i int, c block.Column) block.Block {
	out := shareColumns(blk, allIndexes(blk.Width()))
	var err error
	if i < 0 {
		err = out.AppendColumn(c)
	} else {
		err = out.SetColumn(i, c)
	}
	if err != nil {
		panic("gtable: " + err.Error())
	}
	return out
}

// ---------------------------------------------------------------------------
// Select and Rename

// Select keeps the given columns in that order. An unknown or repeated
// column is a plan error.
func Select(cols ...string) Op { return Op{selectOp{cols}} }

type selectOp struct{ cols []string }

func (selectOp) kind() string { return "select" }

func (o selectOp) plan(in schema) (schema, error) {
	if len(o.cols) == 0 {
		return nil, errors.New("no columns selected")
	}
	out := make(schema, 0, len(o.cols))
	for _, c := range o.cols {
		i, err := in.lookup(c)
		if err != nil {
			return nil, err
		}
		if out.index(c) >= 0 {
			return nil, fmt.Errorf("column %q selected twice", c)
		}
		out = append(out, in[i])
	}
	return out, nil
}

func (o selectOp) apply(blk block.Block, in schema, _ *stepCtx) (block.Block, error) {
	idx := make([]int, len(o.cols))
	for i, c := range o.cols {
		idx[i] = in.index(c)
	}
	return shareColumns(blk, idx), nil
}

// Rename renames column old to new. An unknown column, or a new name that
// another column has, is a plan error.
func Rename(old, new string) Op { return Op{renameOp{old, new}} }

type renameOp struct{ old, new string }

func (renameOp) kind() string { return "rename" }

func (o renameOp) plan(in schema) (schema, error) {
	i, err := in.lookup(o.old)
	if err != nil {
		return nil, err
	}
	if o.new == "" {
		return nil, errors.New("empty column name")
	}
	if j := in.index(o.new); j >= 0 && j != i {
		return nil, fmt.Errorf("column %q exists", o.new)
	}
	out := in.clone()
	out[i].name = o.new
	return out, nil
}

func (o renameOp) apply(blk block.Block, _ schema, _ *stepCtx) (block.Block, error) {
	return shareColumns(blk, allIndexes(blk.Width())), nil
}

// ---------------------------------------------------------------------------
// Where and With

// Where keeps the rows for which cond is true. A row for which cond is false
// or null is dropped, not rejected (D30, D43). If cond fails for a row at run
// time, the row is rejected with code "expr" (D54). cond must be boolean.
func Where(cond Expr) Op { return Op{whereOp{cond}} }

type whereOp struct{ cond Expr }

func (whereOp) kind() string { return "where" }

func (o whereOp) plan(in schema) (schema, error) {
	k, err := o.cond.check(in)
	if err != nil {
		return nil, err
	}
	if k != block.Bool {
		return nil, fmt.Errorf("condition is %s, want bool", k)
	}
	return in, nil
}

func (o whereOp) apply(blk block.Block, in schema, sc *stepCtx) (block.Block, error) {
	c := newEvalCtx(blk, in)
	v := o.cond.n.eval(c)
	drop := make([]bool, c.n)
	for i := range c.n {
		if r := c.reasons[i]; r != "" {
			if err := sc.reject("", "", false, r, CodeExpr); err != nil {
				return block.Block{}, err
			}
			drop[i] = true
			continue
		}
		drop[i] = v.null[i] || !v.bools[i]
	}
	return dropRows(blk, drop), nil
}

// With sets column name to the value of e, replacing a column of that name
// or adding it at the end. If e fails for a row at run time, the row is
// rejected with code "expr" (D54).
func With(name string, e Expr) Op { return Op{withOp{name, e}} }

type withOp struct {
	name string
	e    Expr
}

func (withOp) kind() string { return "with" }

func (o withOp) plan(in schema) (schema, error) {
	if o.name == "" {
		return nil, errors.New("empty column name")
	}
	k, err := o.e.check(in)
	if err != nil {
		return nil, err
	}
	return setField(in, o.name, k), nil
}

func setField(in schema, name string, k block.Kind) schema {
	out := in.clone()
	if i := out.index(name); i >= 0 {
		out[i].kind = k
		return out
	}
	return append(out, field{name, k})
}

func (o withOp) apply(blk block.Block, in schema, sc *stepCtx) (block.Block, error) {
	c := newEvalCtx(blk, in)
	v := o.e.n.eval(c)
	drop, err := rejectFailed(c.reasons, o.name, sc, CodeExpr)
	if err != nil {
		return block.Block{}, err
	}
	return dropRows(withColumn(blk, in.index(o.name), v.column()), drop), nil
}

// rejectFailed rejects every row with a reason and marks it for dropping.
func rejectFailed(reasons []string, column string, sc *stepCtx, code string) ([]bool, error) {
	drop := make([]bool, len(reasons))
	for i, r := range reasons {
		if r == "" {
			continue
		}
		if err := sc.reject(column, "", false, r, code); err != nil {
			return nil, err
		}
		drop[i] = true
	}
	return drop, nil
}

// ---------------------------------------------------------------------------
// Cast

// CastOption configures a Cast.
type CastOption func(*castOp)

// DateFormat sets the Go time layout for casting text to a timestamp, or a
// timestamp to text. Without it, text in the forms 2006-01-02 and RFC 3339
// is accepted and timestamps are formatted in RFC 3339.
func DateFormat(layout string) CastOption { return func(o *castOp) { o.layout = layout } }

// NullTexts sets texts that become null when cast from text, in addition to
// empty text, such as "NULL", "n/a" or "-" (D30).
func NullTexts(texts ...string) CastOption {
	return func(o *castOp) { o.nullTexts = append(o.nullTexts, texts...) }
}

// Cast converts column col to type to. Raw columns are text until a step
// casts them (D29). From text, empty text and the NullTexts become null
// (D30), and a text that does not parse rejects the row with code "parse".
// Integers widen to floats; a float casts to an integer only if it is
// integral and in range. Every type casts to text. Other conversions are a
// plan error.
func Cast(col string, to Type, opts ...CastOption) Op {
	o := castOp{col: col, to: block.Kind(to)}
	for _, opt := range opts {
		opt(&o)
	}
	return Op{o}
}

type castOp struct {
	col       string
	to        block.Kind
	layout    string
	nullTexts []string
}

func (castOp) kind() string { return "cast" }

func (o castOp) plan(in schema) (schema, error) {
	i, err := in.lookup(o.col)
	if err != nil {
		return nil, err
	}
	from := in[i].kind
	switch {
	case o.to < block.Text || o.to > block.Timestamp:
		return nil, fmt.Errorf("unknown type %v", o.to)
	case from == o.to, from == block.Text, o.to == block.Text:
	case isNumeric(from) && isNumeric(o.to):
	default:
		return nil, fmt.Errorf("cannot cast %s to %s", from, o.to)
	}
	out := in.clone()
	out[i].kind = o.to
	return out, nil
}

func (o castOp) apply(blk block.Block, in schema, sc *stepCtx) (block.Block, error) {
	ci := in.index(o.col)
	src := vecOf(blk.Column(ci))
	out := newVec(o.to, src.n)
	drop := make([]bool, src.n)
	for i := range src.n {
		if src.null[i] {
			out.null[i] = true
			continue
		}
		if reason := o.cell(src, i, out); reason != "" {
			val, _ := src.format(i)
			if err := sc.reject(o.col, val, true, reason, CodeParse); err != nil {
				return block.Block{}, err
			}
			drop[i] = true
		}
	}
	return dropRows(withColumn(blk, ci, out.column()), drop), nil
}

// cell casts cell i of src into out and returns a reason if it fails.
func (o castOp) cell(src *vec, i int, out *vec) string {
	if src.kind == block.Text {
		s := src.texts[i]
		if o.to != block.Text && s == "" {
			out.null[i] = true
			return ""
		}
		for _, nt := range o.nullTexts {
			if s == nt {
				out.null[i] = true
				return ""
			}
		}
		return o.parse(s, i, out)
	}
	switch o.to {
	case src.kind:
		out.set(i, src, i)
	case block.Text:
		if src.kind == block.Timestamp {
			out.texts[i] = src.times[i].Format(o.timeLayout())
		} else {
			out.texts[i] = formatCell(src.kind, src, i)
		}
	case block.Float:
		out.flts[i] = float64(src.ints[i])
	case block.Int:
		f := src.flts[i]
		if f != math.Trunc(f) || f < math.MinInt64 || f >= math.MaxInt64 {
			return fmt.Sprintf("float %v is not an integer in range", f)
		}
		out.ints[i] = int64(f)
	}
	return ""
}

func (o castOp) timeLayout() string {
	if o.layout != "" {
		return o.layout
	}
	return time.RFC3339Nano
}

func (o castOp) parse(s string, i int, out *vec) string {
	switch o.to {
	case block.Text:
		out.texts[i] = s
	case block.Int:
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Sprintf("not an integer: %q", s)
		}
		out.ints[i] = v
	case block.Float:
		v, err := strconv.ParseFloat(s, 64)
		if err != nil || math.IsInf(v, 0) || math.IsNaN(v) {
			return fmt.Sprintf("not a number: %q", s)
		}
		out.flts[i] = v
	case block.Bool:
		v, err := strconv.ParseBool(s)
		if err != nil {
			return fmt.Sprintf("not a boolean: %q", s)
		}
		out.bools[i] = v
	case block.Timestamp:
		layouts := []string{"2006-01-02", time.RFC3339Nano}
		if o.layout != "" {
			layouts = []string{o.layout}
		}
		for _, l := range layouts {
			if t, err := time.Parse(l, s); err == nil {
				out.times[i] = t
				return ""
			}
		}
		if o.layout != "" {
			return fmt.Sprintf("not a date in format %q: %q", o.layout, s)
		}
		return fmt.Sprintf("not a date: %q", s)
	}
	return ""
}
