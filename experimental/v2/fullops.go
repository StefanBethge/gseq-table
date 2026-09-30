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
)

// concatBlocks joins the blocks of a step over all rows into one block, with
// the columns of s.
func concatBlocks(blks []block.Block, s schema) block.Block {
	if len(blks) == 1 {
		return blks[0]
	}
	cols := make([]block.Column, len(s))
	parts := make([]block.Column, len(blks))
	for i, f := range s {
		for j, b := range blks {
			parts[j] = b.Column(i)
		}
		cols[i] = block.Concat(f.kind, parts...)
	}
	n := 0
	for _, b := range blks {
		n += b.Len()
	}
	return newBlock(cols, n)
}

// keyOf encodes the cells of the given columns in row i as a map key, and
// reports whether one of them is null.
func keyOf(cols []*vec, i int, sb *strings.Builder) (string, bool) {
	sb.Reset()
	hasNull := false
	for _, c := range cols {
		if c.null[i] {
			sb.WriteString("n;")
			hasNull = true
			continue
		}
		var s string
		switch c.kind {
		case block.Float:
			f := c.flts[i]
			if f == 0 {
				f = 0 // -0 and 0 are one key
			}
			s = strconv.FormatFloat(f, 'g', -1, 64)
		case block.Timestamp:
			s = c.times[i].UTC().Format(time.RFC3339Nano) // one key per instant
		default:
			s = formatCell(c.kind, c, i)
		}
		sb.WriteString(strconv.Itoa(len(s)))
		sb.WriteByte(':')
		sb.WriteString(s)
		sb.WriteByte(';')
	}
	return sb.String(), hasNull
}

// ---------------------------------------------------------------------------
// Join

// JoinKey names the key columns of a join: one name on both sides, or a
// pair of names.
type JoinKey struct{ left, right string }

// On joins on a column that has the same name on both sides.
func On(col string) JoinKey { return JoinKey{col, col} }

// OnPair joins column left of the left side on column right of the right
// side.
func OnPair(left, right string) JoinKey { return JoinKey{left, right} }

// InnerJoin keeps the rows of the left side that have a partner in right,
// once per partner. The right key columns are dropped. A clash of other
// column names, or keys of different types, is a plan error (D72). A null
// key has no partner (D30). The result carries the rejects of both sides
// (D69). In the prototype, the right side is a Table held in memory (P1).
func InnerJoin(right Table, keys ...JoinKey) Op { return Op{joinOp{right, keys, false}} }

// LeftJoin is InnerJoin that keeps left rows without a partner, with null in
// the right columns.
func LeftJoin(right Table, keys ...JoinKey) Op { return Op{joinOp{right, keys, true}} }

type joinOp struct {
	right Table
	keys  []JoinKey
	left  bool
}

func (o joinOp) kind() string {
	if o.left {
		return "left_join"
	}
	return "inner_join"
}

func (o joinOp) plan(in schema) (schema, error) {
	if o.right.err != nil {
		return nil, fmt.Errorf("right side has an error: %w", o.right.err)
	}
	if len(o.keys) == 0 {
		return nil, errors.New("no join key")
	}
	rs := o.right.s
	rightKey := make(map[string]bool, len(o.keys))
	for _, k := range o.keys {
		li, err := in.lookup(k.left)
		if err != nil {
			return nil, fmt.Errorf("left side: %w", err)
		}
		ri, err := rs.lookup(k.right)
		if err != nil {
			return nil, fmt.Errorf("right side: %w", err)
		}
		if in[li].kind != rs[ri].kind {
			return nil, fmt.Errorf("key %q is %s, right key %q is %s", k.left, in[li].kind, k.right, rs[ri].kind)
		}
		rightKey[k.right] = true
	}
	out := in.clone()
	for _, f := range rs {
		if rightKey[f.name] {
			continue
		}
		if out.index(f.name) >= 0 {
			return nil, fmt.Errorf("column %q exists on both sides; rename it first", f.name)
		}
		out = append(out, f)
	}
	return out, nil
}

// rightRejects are the rejects the right side brings into the result (D69).
func (o joinOp) rightRejects() []rejectEntry { return o.right.rejects }

// applyAll joins the rows. A join row keeps the source rows of both sides,
// so that its failure rejects all of them together (D11).
func (o joinOp) applyAll(blks []block.Block, in schema, sc *stepCtx) (block.Block, []origin, error) {
	out, orig, _ := o.probe(o.prepare(), concatBlocks(blks, in), in, sc.orig)
	return out, orig, nil
}

// joinIndex is the right side of a join, held in memory (P1), with its
// rows by key.
type joinIndex struct {
	rb    block.Block
	index map[string][]int
	rorig []origin
}

func (o joinOp) prepare() *joinIndex {
	rb := concatBlocks(o.right.blocks, o.right.s)
	rk := make([]*vec, len(o.keys))
	for i, k := range o.keys {
		rk[i] = vecOf(rb.Column(o.right.s.index(k.right)))
	}
	var sb strings.Builder
	index := make(map[string][]int)
	for i := range rb.Len() {
		if key, null := keyOf(rk, i, &sb); !null {
			index[key] = append(index[key], i)
		}
	}
	return &joinIndex{rb: rb, index: index, rorig: o.right.origins()}
}

// probe joins the left rows lb with origins lorig to the right side, and
// returns the number of partners of every left row. The join rows keep the
// order of the left rows, so that probing block by block gives the rows of
// probing all at once (D6).
func (o joinOp) probe(ix *joinIndex, lb block.Block, in schema, lorig []origin) (block.Block, []origin, []int) {
	lk := make([]*vec, len(o.keys))
	for i, k := range o.keys {
		lk[i] = vecOf(lb.Column(in.index(k.left)))
	}
	var sb strings.Builder
	var li, ri []int
	var orig []origin
	counts := make([]int, lb.Len())
	for i := range lb.Len() {
		var matches []int
		if key, null := keyOf(lk, i, &sb); !null {
			matches = ix.index[key]
		}
		counts[i] = len(matches)
		for _, j := range matches {
			li = append(li, i)
			ri = append(ri, j)
			orig = append(orig, lorig[i].join(ix.rorig[j]))
		}
		if len(matches) == 0 && o.left {
			li = append(li, i)
			ri = append(ri, -1)
			orig = append(orig, lorig[i])
		}
	}
	cols := make([]block.Column, 0, len(in)+ix.rb.Width())
	for i := range in {
		cols = append(cols, lb.Column(i).Take(li))
	}
	for i, f := range o.right.s {
		if !o.isRightKey(f.name) {
			cols = append(cols, ix.rb.Column(i).Take(ri))
		}
	}
	return newBlock(cols, len(li)), orig, counts
}

func (o joinOp) isRightKey(name string) bool {
	return slices.ContainsFunc(o.keys, func(k JoinKey) bool { return k.right == name })
}

// newBlock returns a block of cols with n rows.
func newBlock(cols []block.Column, n int) block.Block {
	if len(cols) == 0 {
		return block.Block{}.Take(make([]int, n))
	}
	b, err := block.New(cols...)
	if err != nil {
		panic("gtable: " + err.Error())
	}
	return b
}

// ---------------------------------------------------------------------------
// Group by

// Agg is an aggregation for GroupBy. Aggregations skip nulls (D30); over no
// values they yield null, except Count and CountDistinct, which yield 0.
type Agg struct {
	fn   string
	col  string
	as   string
	sep  string
	q    float64
	isQ  bool
	kind func(block.Kind) (block.Kind, error)
}

// As sets the name of the output column; the default is the input column.
func (a Agg) As(name string) Agg { a.as = name; return a }

func (a Agg) name() string {
	if a.as != "" {
		return a.as
	}
	return a.col
}

func numericAgg(res block.Kind) func(block.Kind) (block.Kind, error) {
	return func(k block.Kind) (block.Kind, error) {
		if !isNumeric(k) {
			return 0, fmt.Errorf("%s column, want a number", k)
		}
		if res == 0 {
			return k, nil
		}
		return res, nil
	}
}

func anyKind(res block.Kind) func(block.Kind) (block.Kind, error) {
	return func(k block.Kind) (block.Kind, error) {
		if res == 0 {
			return k, nil
		}
		return res, nil
	}
}

// Sum adds the values; the sum of integers is an integer (D71), and an
// integer overflow rejects the group's row with code "expr".
func Sum(col string) Agg { return Agg{fn: "Sum", col: col, kind: numericAgg(0)} }

// Mean is the arithmetic mean, as a float.
func Mean(col string) Agg { return Agg{fn: "Mean", col: col, kind: numericAgg(block.Float)} }

// Count counts the values that are not null.
func Count(col string) Agg { return Agg{fn: "Count", col: col, kind: anyKind(block.Int)} }

// CountDistinct counts the distinct values that are not null.
func CountDistinct(col string) Agg {
	return Agg{fn: "CountDistinct", col: col, kind: anyKind(block.Int)}
}

// StringJoin joins the texts with sep.
func StringJoin(col, sep string) Agg {
	return Agg{fn: "StringJoin", col: col, sep: sep, kind: func(k block.Kind) (block.Kind, error) {
		if k != block.Text {
			return 0, fmt.Errorf("%s column, want text", k)
		}
		return block.Text, nil
	}}
}

// First is the first value of the group.
func First(col string) Agg { return Agg{fn: "First", col: col, kind: anyKind(0)} }

// Last is the last value of the group.
func Last(col string) Agg { return Agg{fn: "Last", col: col, kind: anyKind(0)} }

func ordered(k block.Kind) (block.Kind, error) {
	if k == block.Bool {
		return 0, errors.New("bool column has no order for Min or Max")
	}
	return k, nil
}

// Min is the smallest value; numbers, text and timestamps.
func Min(col string) Agg { return Agg{fn: "Min", col: col, kind: ordered} }

// Max is the largest value; numbers, text and timestamps.
func Max(col string) Agg { return Agg{fn: "Max", col: col, kind: ordered} }

// Median is the median, the mean of the two middle values for an even count.
func Median(col string) Agg { return Agg{fn: "Median", col: col, kind: numericAgg(block.Float)} }

// Quantile is the p-quantile with linear interpolation, like v1. p outside
// [0, 1] is a plan error.
func Quantile(col string, p float64) Agg {
	return Agg{fn: "Quantile", col: col, q: p, isQ: true, kind: numericAgg(block.Float)}
}

// Var is the population variance.
func Var(col string) Agg { return Agg{fn: "Var", col: col, kind: numericAgg(block.Float)} }

// StdDev is the population standard deviation.
func StdDev(col string) Agg { return Agg{fn: "StdDev", col: col, kind: numericAgg(block.Float)} }

// GroupBy groups the rows by the key columns and computes one row per group:
// the keys, then the aggregations in the order given. Groups appear in the
// order of their first row. Null keys form a group of their own, as in SQL.
func GroupBy(keys []string, aggs ...Agg) Op { return Op{groupOp{keys, aggs}} }

type groupOp struct {
	keys []string
	aggs []Agg
}

func (groupOp) kind() string { return "group_by" }

func (o groupOp) plan(in schema) (schema, error) {
	if len(o.keys) == 0 {
		return nil, errors.New("no group key")
	}
	out := make(schema, 0, len(o.keys)+len(o.aggs))
	for _, k := range o.keys {
		i, err := in.lookup(k)
		if err != nil {
			return nil, err
		}
		if out.index(k) >= 0 {
			return nil, fmt.Errorf("group key %q given twice", k)
		}
		out = append(out, in[i])
	}
	for _, a := range o.aggs {
		if a.kind == nil {
			return nil, errors.New("empty aggregation")
		}
		i, err := in.lookup(a.col)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", a.fn, err)
		}
		k, err := a.kind(in[i].kind)
		if err != nil {
			return nil, fmt.Errorf("%s(%q): %w", a.fn, a.col, err)
		}
		if a.isQ && (math.IsNaN(a.q) || a.q < 0 || a.q > 1) {
			return nil, fmt.Errorf("Quantile(%q): p %v outside [0, 1]", a.col, a.q)
		}
		if out.index(a.name()) >= 0 {
			return nil, fmt.Errorf("output column %q exists; name the aggregation with As", a.name())
		}
		out = append(out, field{a.name(), k})
	}
	return out, nil
}

// applyAll groups the rows. Each group becomes an aggregated row that
// stands for its input rows (D12). If aggregations fail for a group, its row
// is rejected once, with an entry per failed aggregation (D15, D77).
func (o groupOp) applyAll(blks []block.Block, in schema, sc *stepCtx) (block.Block, []origin, error) {
	out, orig, fails, _ := o.aggregate(concatBlocks(blks, in), in, sc.orig, sc.rx.shared)
	keys := allIndexes(out.Len())
	return o.rejectFailed(out, orig, fails, keys, in, sc)
}

// withKey returns a function that adds the key of the group of every row
// as a last text column, for sorting the rows by group when they are
// spilled. The key is the one the groups are formed by.
func (o groupOp) withKey(in schema) func(batch) batch {
	return func(b batch) batch {
		kv := make([]*vec, len(o.keys))
		for i, k := range o.keys {
			kv[i] = vecOf(b.blk.Column(in.index(k)))
		}
		var sb strings.Builder
		kb := block.NewBuilder(block.Text, b.blk.Len())
		for i := range b.blk.Len() {
			key, _ := keyOf(kv, i, &sb)
			kb.AppendText(key)
		}
		cols := make([]block.Column, 0, len(in)+1)
		for i := range in {
			cols = append(cols, b.blk.Column(i))
		}
		return batch{newBlock(append(cols, kb.Build()), b.blk.Len()), b.orig}
	}
}

// aggFailure is a failed aggregation of a group.
type aggFailure struct{ column, reason string }

// aggregate computes a row per group of the rows of b with origins rorig,
// in the order of the groups' first rows. It returns the rows, their
// origins, the failed aggregations and the first row of every group. A
// failed aggregation is null in its row.
func (o groupOp) aggregate(b block.Block, in schema, rorig []origin, sh *shared) (block.Block, []origin, [][]aggFailure, []int) {
	kv := make([]*vec, len(o.keys))
	for i, k := range o.keys {
		kv[i] = vecOf(b.Column(in.index(k)))
	}
	var sb strings.Builder
	ids := make(map[string]int)
	var groups [][]int
	for i := range b.Len() {
		key, _ := keyOf(kv, i, &sb)
		g, ok := ids[key]
		if !ok {
			g = len(groups)
			ids[key] = g
			groups = append(groups, nil)
		}
		groups[g] = append(groups[g], i)
	}

	firsts := make([]int, len(groups))
	orig := make([]origin, len(groups))
	for g, rows := range groups {
		firsts[g] = rows[0]
		ab := newAggBuilder(sh)
		for _, r := range rows {
			ab.add(rorig[r])
		}
		orig[g] = aggOf(ab.build())
	}
	fails := make([][]aggFailure, len(groups))
	outs := make([]*vec, len(o.aggs))
	for j, a := range o.aggs {
		src := vecOf(b.Column(in.index(a.col)))
		k, _ := a.kind(src.kind)
		outs[j] = newVec(k, len(groups))
		for g, rows := range groups {
			if reason := a.reduce(src, rows, outs[j], g); reason != "" {
				outs[j].null[g] = true
				fails[g] = append(fails[g], aggFailure{a.name(), reason})
			}
		}
	}
	cols := make([]block.Column, 0, len(o.keys)+len(o.aggs))
	for i := range o.keys {
		cols = append(cols, b.Column(in.index(o.keys[i])).Take(firsts))
	}
	for _, v := range outs {
		cols = append(cols, v.column())
	}
	return newBlock(cols, len(groups)), orig, fails, firsts
}

// rejectFailed rejects the rows of out with failed aggregations, keyed by
// keys for their reject_id, and returns the others.
func (o groupOp) rejectFailed(out block.Block, orig []origin, fails [][]aggFailure, keys []int, in schema, sc *stepCtx) (block.Block, []origin, error) {
	outSchema, _ := o.plan(in)
	drop := make([]bool, out.Len())
	for g, fs := range fails {
		snap := func() snapshot { return snapshot{outSchema, out.Take([]int{g})} }
		for _, f := range fs {
			if err := sc.rejectRow(keys[g], orig[g], snap, f.column, "", false, f.reason, CodeExpr); err != nil {
				return block.Block{}, nil, err
			}
			drop[g] = true
		}
	}
	res, keep := dropRows(out, drop)
	if keep != nil {
		orig = pick(orig, keep)
	}
	return res, orig, nil
}

// reduce computes the aggregation over the given rows of src into cell g of
// out, and returns a reason if it fails.
func (a Agg) reduce(src *vec, rows []int, out *vec, g int) string {
	vals := rows[:0:0]
	for _, r := range rows {
		if !src.null[r] {
			vals = append(vals, r)
		}
	}
	switch a.fn {
	case "Count":
		out.ints[g] = int64(len(vals))
		return ""
	case "CountDistinct":
		seen := make(map[string]bool)
		var sb strings.Builder
		for _, r := range vals {
			k, _ := keyOf([]*vec{src}, r, &sb)
			seen[k] = true
		}
		out.ints[g] = int64(len(seen))
		return ""
	}
	if len(vals) == 0 {
		out.null[g] = true
		return ""
	}
	switch a.fn {
	case "Sum":
		if src.kind == block.Int {
			var s int64
			for _, r := range vals {
				v, ok := intOp("+", s, src.ints[r])
				if !ok {
					return fmt.Sprintf("integer overflow in Sum(%q)", a.col)
				}
				s = v
			}
			out.ints[g] = s
			return ""
		}
		var s float64
		for _, r := range vals {
			s += src.flts[r]
		}
		out.flts[g] = s
		return finite(s)
	case "Mean":
		var s float64
		for _, r := range vals {
			s += src.float(r)
		}
		out.flts[g] = s / float64(len(vals))
		return finite(out.flts[g])
	case "StringJoin":
		parts := make([]string, len(vals))
		for i, r := range vals {
			parts[i] = src.texts[r]
		}
		out.texts[g] = strings.Join(parts, a.sep)
	case "First":
		out.set(g, src, vals[0])
	case "Last":
		out.set(g, src, vals[len(vals)-1])
	case "Min", "Max":
		best := vals[0]
		for _, r := range vals[1:] {
			c := compareCells(src, r, src, best)
			if (a.fn == "Min" && c < 0) || (a.fn == "Max" && c > 0) {
				best = r
			}
		}
		out.set(g, src, best)
	case "Median", "Quantile", "Var", "StdDev":
		fs := make([]float64, len(vals))
		for i, r := range vals {
			fs[i] = src.float(r)
		}
		switch a.fn {
		case "Median":
			out.flts[g] = quantile(fs, 0.5)
		case "Quantile":
			out.flts[g] = quantile(fs, a.q)
		default:
			v := variance(fs)
			if a.fn == "StdDev" {
				v = math.Sqrt(v)
			}
			out.flts[g] = v
		}
		return finite(out.flts[g])
	}
	return ""
}

// quantile is the p-quantile of vals with linear interpolation between the
// closest ranks, as in v1. The median of an even count is the mean of the two
// middle values. vals is reordered.
func quantile(vals []float64, p float64) float64 {
	slices.Sort(vals)
	h := p * float64(len(vals)-1)
	k := int(h)
	if frac := h - float64(k); frac > 0 && k+1 < len(vals) {
		return vals[k] + frac*(vals[k+1]-vals[k])
	}
	return vals[k]
}

// variance is the population variance of vals, as in v1.
func variance(vals []float64) float64 {
	var sum float64
	for _, f := range vals {
		sum += f
	}
	mean := sum / float64(len(vals))
	var sq float64
	for _, f := range vals {
		d := f - mean
		sq += d * d
	}
	return sq / float64(len(vals))
}

// ---------------------------------------------------------------------------
// Sort

// SortKey is a column to sort by and its direction.
type SortKey struct {
	col  string
	desc bool
}

// Asc sorts by col in ascending order.
func Asc(col string) SortKey { return SortKey{col, false} }

// Desc sorts by col in descending order.
func Desc(col string) SortKey { return SortKey{col, true} }

// Sort orders the rows by the keys. Nulls come last in both directions, and
// rows with equal keys keep their order (D70).
func Sort(keys ...SortKey) Op { return Op{sortOp{keys}} }

type sortOp struct{ keys []SortKey }

func (sortOp) kind() string { return "sort" }

func (o sortOp) plan(in schema) (schema, error) {
	if len(o.keys) == 0 {
		return nil, errors.New("no sort key")
	}
	for _, k := range o.keys {
		if _, err := in.lookup(k.col); err != nil {
			return nil, err
		}
	}
	return in, nil
}

func (o sortOp) applyAll(blks []block.Block, in schema, sc *stepCtx) (block.Block, []origin, error) {
	b := concatBlocks(blks, in)
	idx := sortIndex(b, in, o.keys)
	return b.Take(idx), pick(sc.orig, idx), nil
}

// sortIndex returns the rows of b in the order of the keys, stable (D70).
func sortIndex(b block.Block, s schema, keys []SortKey) []int {
	kv := keyVecs(b, s, keys)
	idx := allIndexes(b.Len())
	slices.SortStableFunc(idx, func(x, y int) int { return compareKeys(kv, x, kv, y, keys) })
	return idx
}

// keyVecs returns the key columns of b.
func keyVecs(b block.Block, s schema, keys []SortKey) []*vec {
	kv := make([]*vec, len(keys))
	for i, k := range keys {
		kv[i] = vecOf(b.Column(s.index(k.col)))
	}
	return kv
}

// compareKeys compares row x of the key columns a with row y of b. Nulls
// come last in both directions (D70).
func compareKeys(a []*vec, x int, b []*vec, y int, keys []SortKey) int {
	for i := range keys {
		xn, yn := a[i].null[x], b[i].null[y]
		switch {
		case xn && yn:
			continue
		case xn:
			return 1
		case yn:
			return -1
		}
		c := compareCells(a[i], x, b[i], y)
		if keys[i].desc {
			c = -c
		}
		if c != 0 {
			return c
		}
	}
	return 0
}
