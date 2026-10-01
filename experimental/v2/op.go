package gtable

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
	"github.com/stefanbethge/gseq-table/internal/cell"
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
	// apply returns the output block and, for every output row, the input
	// row it comes from; nil means row for row.
	apply(blk block.Block, in schema, sc *stepCtx) (block.Block, []int, error)
}

type fullOp interface {
	opImpl
	// applyAll returns the output block and the origin of every output row
	// (D11, D12). sc.orig holds the origins of the input rows in order.
	applyAll(blks []block.Block, in schema, sc *stepCtx) (block.Block, []origin, error)
}

// stepCtx is what a step needs while it runs: its name for rejects, the
// rejector of the run, and the input rows with their origins.
type stepCtx struct {
	step  string
	ref   *stepRef
	rx    *rejector
	in    schema
	blk   block.Block
	orig  []origin
	ids   map[int]string   // reject_id per failed row of the current input
	codes map[int][]string // error codes per failed row of the current input
	cnt   *counter         // counts of the step in a run (D43); nil for a Table method

	// With a fail branch, the errors of the step wait in pending until the
	// branch decides (D25), and held says that the branch holds the input
	// (D9).
	deferring bool
	held      bool
	pending   []pendingReject
}

// begin sets the input rows of the next apply.
func (sc *stepCtx) begin(blk block.Block, orig []origin) {
	sc.blk, sc.orig, sc.ids, sc.codes = blk, orig, nil, nil
}

// rejected reports whether input row i of the current input was rejected.
func (sc *stepCtx) rejected(i int) bool {
	_, ok := sc.ids[i]
	return ok
}

// reject rejects input row i. Errors of one row share its reject_id (D15).
func (sc *stepCtx) reject(i int, column, value string, hasValue bool, reason, code string) error {
	snap := func() snapshot {
		// The values of the row, without the info columns of a fail
		// branch (D91).
		var s schema
		var idx []int
		for j, f := range sc.in {
			if !hasPrefix(f.name, sc.rx.infoPrefix()) {
				s, idx = append(s, f), append(idx, j)
			}
		}
		return snapshot{s, shareColumns(sc.blk, idx).Take([]int{i})}
	}
	return sc.rejectRow(i, sc.orig[i], snap, column, value, hasValue, reason, code)
}

// rejectRow rejects the row with the given key and origin; snap returns
// its values if it is an aggregated row (D77). A row that failed before and
// went into a fail branch keeps its reject_id, its path and the previous
// reason (D27, D46, D92).
func (sc *stepCtx) rejectRow(key int, o origin, snap func() snapshot, column, value string, hasValue bool, reason, code string) error {
	if sc.ids == nil {
		sc.ids = make(map[int]string)
	}
	h := o.history()
	id, ok := sc.ids[key]
	first := !ok
	if first {
		id = sc.rx.run.nextID()
		if h != nil {
			id = h.id
		}
		sc.ids[key] = id
	}
	// A row counts once per code in a step (D84).
	if sc.codes == nil {
		sc.codes = make(map[int][]string)
	}
	fresh := !slices.Contains(sc.codes[key], code)
	if fresh {
		sc.codes[key] = append(sc.codes[key], code)
	}
	path, prev := sc.step, ""
	if h != nil {
		path, prev = h.path+" › "+sc.step, h.reason
	}
	e := rejectEntry{
		// The value may be a view into a block; the entry outlives it (D113).
		Reject: Reject{ID: id, Step: path, Column: column, Value: strings.Clone(value), HasValue: hasValue, Reason: reason, PrevReason: prev, Code: code},
		runID:  sc.rx.run.id,
		orig:   o.detached(),
		step:   sc.ref,
	}
	if o.aggregated() > 0 {
		s := snap()
		e.snap = &s
	}
	if sc.deferring {
		sc.pending = append(sc.pending, pendingReject{key, e})
		return nil
	}
	return sc.rx.add(e, first, fresh)
}

// dropRows returns blk without the rows marked in drop, and the kept rows;
// nil if none is dropped.
func dropRows(blk block.Block, drop []bool) (block.Block, []int) {
	keep := make([]int, 0, len(drop))
	for i, d := range drop {
		if !d {
			keep = append(keep, i)
		}
	}
	if len(keep) == len(drop) {
		return blk, nil
	}
	return blk.Take(keep), keep
}

// shareColumns returns a block of the given columns of blk. The columns
// move to the new block without a copy: a step does not use its input
// block after apply, so a column nothing else holds can then be changed in
// place (D7, D55). Whoever keeps the input, such as a table or a fail
// branch, holds it with Block.Share.
func shareColumns(blk block.Block, idx []int) block.Block {
	cols := make([]block.Column, len(idx))
	for i, j := range idx {
		cols[i] = blk.Column(j)
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

func (o selectOp) apply(blk block.Block, in schema, _ *stepCtx) (block.Block, []int, error) {
	idx := make([]int, len(o.cols))
	for i, c := range o.cols {
		idx[i] = in.index(c)
	}
	return shareColumns(blk, idx), nil, nil
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

func (o renameOp) apply(blk block.Block, _ schema, _ *stepCtx) (block.Block, []int, error) {
	return shareColumns(blk, allIndexes(blk.Width())), nil, nil
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

func (o whereOp) apply(blk block.Block, in schema, sc *stepCtx) (block.Block, []int, error) {
	c := newEvalCtx(blk, in)
	v := o.cond.n.eval(c)
	drop := make([]bool, c.n)
	for i := range c.n {
		if r := c.reasons.at(i); r != "" {
			if err := sc.reject(i, "", "", false, r, CodeExpr); err != nil {
				return block.Block{}, nil, err
			}
			drop[i] = true
			continue
		}
		drop[i] = v.null[i] || !v.bools[i]
	}
	out, keep := dropRows(blk, drop)
	return out, keep, nil
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

func (o withOp) apply(blk block.Block, in schema, sc *stepCtx) (block.Block, []int, error) {
	return applyParts(blk, sc, []part{o.part(blk, in)})
}

// part evaluates e over blk.
func (o withOp) part(blk block.Block, in schema) part {
	c := newEvalCtx(blk, in)
	v := o.e.n.eval(c)
	return part{name: o.name, idx: in.index(o.name), vals: v, reasons: c.reasons, code: CodeExpr}
}

// part is the result of an operation for one column: the new column, a
// reason for every row that failed, and how to show the failed value.
type part struct {
	name    string
	idx     int // column to replace, or -1 to append
	vals    *vec
	reasons reasons
	value   func(i int) (string, bool) // nil: no value
	code    string
}

// applyParts rejects every row for which a part failed, one entry per
// failed column under one reject_id (D15), sets the columns of the parts
// and drops the rejected rows.
func applyParts(blk block.Block, sc *stepCtx, parts []part) (block.Block, []int, error) {
	drop := make([]bool, blk.Len())
	for i := range blk.Len() {
		for _, p := range parts {
			r := p.reasons.at(i)
			if r == "" {
				continue
			}
			var val string
			var ok bool
			if p.value != nil {
				val, ok = p.value(i)
			}
			if err := sc.reject(i, p.name, val, ok, r, p.code); err != nil {
				return block.Block{}, nil, err
			}
			drop[i] = true
		}
	}
	// A part that reads the values of a column another part sets must see
	// them before the step, so those columns are not changed in place.
	inPlace := true
	for i, p := range parts {
		for j, q := range parts {
			if i != j && q.idx >= 0 && p.vals.aliases(blk.Column(q.idx)) {
				inPlace = false
			}
		}
	}
	out := shareColumns(blk, allIndexes(blk.Width()))
	for _, p := range parts {
		setColumn(&out, sc, p.name, p.idx, p.vals, inPlace)
	}
	res, keep := dropRows(out, drop)
	return res, keep, nil
}

// ---------------------------------------------------------------------------
// Cast

// CastOption configures a Cast.
type CastOption func(*castOp)

// DateFormat sets the Go time layout for casting text to a timestamp, or a
// timestamp to text. Without it, text in the forms 2006-01-02 and RFC 3339
// is accepted and timestamps are formatted in RFC 3339; see Lenient for
// more layouts.
func DateFormat(layout string) CastOption { return func(o *castOp) { o.layout = layout } }

// Lenient makes the cast behave like v1 (D74): it trims surrounding
// whitespace before parsing and, without a DateFormat, tries the common
// date layouts of v1 (internal/cell, DateLayouts) in order. Without it,
// Cast is strict.
func Lenient() CastOption { return func(o *castOp) { o.lenient = true } }

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
// plan error. Cast is strict: it does not trim whitespace; see Lenient
// (D74).
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
	lenient   bool
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

func (o castOp) apply(blk block.Block, in schema, sc *stepCtx) (block.Block, []int, error) {
	return applyParts(blk, sc, []part{o.part(blk, in)})
}

// part casts the column of blk.
func (o castOp) part(blk block.Block, in schema) part {
	ci := in.index(o.col)
	src := vecOf(blk.Column(ci))
	out := newVec(o.to, src.n)
	var reasons reasons
	for i := range src.n {
		if src.null[i] {
			out.null[i] = true
			continue
		}
		reasons.set(i, o.cell(src, i, out), src.n)
	}
	return part{name: o.col, idx: ci, vals: out, reasons: reasons, value: src.format, code: CodeParse}
}

// cell casts cell i of src into out and returns a reason if it fails.
func (o castOp) cell(src *vec, i int, out *vec) string {
	if src.kind == block.Text {
		s := src.text(i)
		if o.lenient && o.to != block.Text {
			s = strings.TrimSpace(s)
		}
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
		if o.lenient {
			layouts = cell.DateLayouts
		}
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
