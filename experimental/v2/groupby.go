package gtable

import (
	"fmt"
	"time"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

// Group by as the rows come (D112). Count, Sum, Mean, Min, Max, First and
// Last need no more than a running state per group, and each row goes into
// it in the order of the input, so the values are those of the group by in
// memory, bit for bit. The states count against the budget; once it is
// reached, the step takes no new groups, and the rows of new keys go the
// way of the other aggregations: sorted by key and spilled (G68).

// streamable reports whether every aggregation of o runs as the rows come.
func (o groupOp) streamable() bool {
	for _, a := range o.aggs {
		switch a.fn {
		case "Count", "Sum", "Mean", "Min", "Max", "First", "Last":
		default:
			return false
		}
	}
	return true
}

// streamGroup holds the running groups of a group by.
type streamGroup struct {
	op    groupOp
	in    schema
	sh    *shared
	m     *runMem
	ids   map[string]int
	keys  []*block.Builder // key cells of the first row of every group
	accs  []*aggAcc
	orig  []*aggBuilder
	first []int64 // sequence number of the first row of every group
	next  int64   // sequence number of the next row
	bytes int64
	full  bool // the budget was reached: no new groups
}

func newStreamGroup(op groupOp, in schema, sh *shared, m *runMem) *streamGroup {
	g := &streamGroup{op: op, in: in, sh: sh, m: m, ids: map[string]int{}}
	for _, k := range op.keys {
		g.keys = append(g.keys, block.NewBuilder(in[in.index(k)].kind, 0))
	}
	for _, a := range op.aggs {
		k, _ := a.kind(in[in.index(a.col)].kind)
		g.accs = append(g.accs, &aggAcc{a: a, out: &vec{kind: k}})
	}
	return g
}

// add puts the rows of b into their groups and returns the rows whose key
// is new while the budget is reached, with their sequence numbers.
func (g *streamGroup) add(b batch) ([]int, []int64) {
	kv := make([]*vec, len(g.op.keys))
	for i, k := range g.op.keys {
		kv[i] = vecOf(b.blk.Column(g.in.index(k)))
	}
	src := make([]*vec, len(g.accs))
	for j, acc := range g.accs {
		src[j] = vecOf(b.blk.Column(g.in.index(acc.a.col)))
	}
	var kb []byte
	var rest []int
	var restSeq []int64
	for i := range b.blk.Len() {
		seq := g.next
		g.next++
		key, _ := keyBytes(kv, i, &kb)
		n, ok := g.ids[string(key)]
		if !ok {
			if g.full || g.m.over() {
				g.full = true
				rest, restSeq = append(rest, i), append(restSeq, seq)
				continue
			}
			n = g.newGroup(string(key), b.blk, i, seq)
		}
		for j, acc := range g.accs {
			acc.add(n, src[j], i)
		}
		g.orig[n].add(b.orig[i])
	}
	return rest, restSeq
}

func (g *streamGroup) newGroup(key string, blk block.Block, i int, seq int64) int {
	n := len(g.first)
	g.ids[key] = n
	for k, name := range g.op.keys {
		g.keys[k].AppendFrom(blk.Column(g.in.index(name)), i)
	}
	for _, acc := range g.accs {
		acc.grow()
	}
	g.orig = append(g.orig, newAggBuilder(g.sh))
	g.first = append(g.first, seq)
	// The key, the key cells, a state per aggregation and the origin.
	bytes := int64(2*len(key)) + 64 + int64(48*len(g.accs))
	g.bytes += bytes
	g.m.grow(bytes)
	return n
}

// result returns a row per group in the order of their first rows, their
// origins, their failed aggregations and the sequence numbers of their
// first rows. It stops counting the groups.
func (g *streamGroup) result() (block.Block, []origin, [][]aggFailure, []int64) {
	n := len(g.first)
	cols := make([]block.Column, 0, len(g.keys)+len(g.accs))
	for _, b := range g.keys {
		cols = append(cols, b.Build())
	}
	fails := make([][]aggFailure, n)
	for _, acc := range g.accs {
		for i := range n {
			if reason := acc.finish(i); reason != "" {
				acc.out.null[i] = true
				fails[i] = append(fails[i], aggFailure{acc.a.name(), reason})
			}
		}
		cols = append(cols, acc.out.column())
	}
	orig := make([]origin, n)
	for i, b := range g.orig {
		orig[i] = aggOf(b.build())
	}
	g.m.shrink(g.bytes)
	g.bytes = 0
	return newBlock(cols, n), orig, fails, g.first
}

// aggAcc is the running state of one aggregation over the groups, in the
// way Agg.reduce computes it over all rows of a group.
type aggAcc struct {
	a     Agg
	out   *vec    // the value so far per group, of the output kind
	count []int64 // values that are not null
	sum   []float64
	isum  []int64
	fail  []bool // an integer sum overflowed
}

func (acc *aggAcc) grow() {
	v := acc.out
	v.n++
	v.null = append(v.null, true)
	switch v.kind {
	case block.Text:
		v.texts = append(v.texts, "")
	case block.Int:
		v.ints = append(v.ints, 0)
	case block.Float:
		v.flts = append(v.flts, 0)
	case block.Bool:
		v.bools = append(v.bools, false)
	case block.Timestamp:
		v.times = append(v.times, time.Time{})
	}
	acc.count = append(acc.count, 0)
	switch acc.a.fn {
	case "Sum", "Mean":
		acc.sum = append(acc.sum, 0)
		acc.isum = append(acc.isum, 0)
		acc.fail = append(acc.fail, false)
	}
}

// add adds cell i of src to group g; nulls are skipped (D30).
func (acc *aggAcc) add(g int, src *vec, i int) {
	if src.null[i] {
		return
	}
	acc.count[g]++
	switch acc.a.fn {
	case "Sum":
		if src.kind == block.Int {
			if acc.fail[g] {
				return
			}
			v, ok := intOp("+", acc.isum[g], src.ints[i])
			if !ok {
				acc.fail[g] = true
				return
			}
			acc.isum[g] = v
			return
		}
		acc.sum[g] += src.flts[i]
	case "Mean":
		acc.sum[g] += src.float(i)
	case "First":
		if acc.count[g] == 1 {
			acc.out.set(g, src, i)
		}
	case "Last":
		acc.out.set(g, src, i)
	case "Min", "Max":
		if acc.count[g] == 1 {
			acc.out.set(g, src, i)
			return
		}
		c := compareCells(src, i, acc.out, g)
		if (acc.a.fn == "Min" && c < 0) || (acc.a.fn == "Max" && c > 0) {
			acc.out.set(g, src, i)
		}
	}
}

// finish sets the value of group g and returns a reason if the
// aggregation failed, as Agg.reduce does.
func (acc *aggAcc) finish(g int) string {
	v := acc.out
	switch acc.a.fn {
	case "Count":
		v.null[g] = false
		v.ints[g] = acc.count[g]
		return ""
	}
	if acc.count[g] == 0 {
		v.null[g] = true
		return ""
	}
	switch acc.a.fn {
	case "Sum":
		v.null[g] = false
		if v.kind == block.Int {
			if acc.fail[g] {
				return fmt.Sprintf("integer overflow in Sum(%q)", acc.a.col)
			}
			v.ints[g] = acc.isum[g]
			return ""
		}
		v.flts[g] = acc.sum[g]
		return finite(acc.sum[g])
	case "Mean":
		v.null[g] = false
		v.flts[g] = acc.sum[g] / float64(acc.count[g])
		return finite(v.flts[g])
	}
	return ""
}
