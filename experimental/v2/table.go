package gtable

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strings"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

// Table is a finished table in memory (D31). Tables are immutable (D5):
// every method returns a new Table and leaves its receiver unchanged.
//
// A Table carries the rows its operations rejected and a sticky error (D50).
// Data errors reject the row by default (D5). A plan error, such as an
// unknown column, sets the sticky error, and the following operations of the
// chain do not run; the table keeps the data from before the failed
// operation (D73). An error whose code is set to ModeStop sets the sticky
// error too.
//
// A table built with NewTable is a source of its own, named "code" or as
// set with AsSource (D75). Its raw state is the values at creation, and
// its rows are located by their row index (D50).
type Table struct {
	s       schema
	blocks  []block.Block
	orig    [][]origin // per block, the origin of every row
	srcs    []*rawSource
	rejects []rejectEntry
	finds   []Finding
	err     error
	policy  errorPolicy
	prefix  string // info column prefix; empty means DefaultInfoPrefix
	run     *runInfo
	fresh   bool // built by NewTable, no operation applied yet
}

// NewTable returns a table of the given columns. Columns of different
// length or a repeated name set the sticky error.
func NewTable(cols ...Column) Table {
	s := make(schema, 0, len(cols))
	bcs := make([]block.Column, 0, len(cols))
	for _, c := range cols {
		if c.name == "" {
			return Table{err: &PlanError{Step: "new_table", Err: errors.New("empty column name")}}
		}
		if s.index(c.name) >= 0 {
			return Table{err: &PlanError{Step: "new_table", Err: fmt.Errorf("column %q given twice", c.name)}}
		}
		s = append(s, field{c.name, c.col.Kind()})
		bcs = append(bcs, c.col.Share())
	}
	if len(bcs) == 0 {
		return Table{run: newRun(), fresh: true}
	}
	b, err := block.New(bcs...)
	if err != nil {
		return Table{err: &PlanError{Step: "new_table", Err: err}}
	}
	src := newRawSource("code", s, bcs)
	return Table{
		s:      s,
		blocks: []block.Block{b},
		orig:   [][]origin{sourceOrigins(src, b.Len())},
		srcs:   []*rawSource{src},
		run:    newRun(),
		fresh:  true,
	}
}

// AsSource names the source of a table built with NewTable; the default is
// "code" (D75). The name stands in the info column source and names the
// table of rejected rows of the source. Called after an operation, or with
// an empty name, it sets the sticky error.
func (t Table) AsSource(name string) Table {
	if t.err != nil {
		return t
	}
	switch {
	case !t.fresh:
		t.err = &PlanError{Step: "as_source", Err: errors.New("AsSource names a table right after NewTable, not after an operation")}
		return t
	case name == "":
		t.err = &PlanError{Step: "as_source", Err: errors.New("empty source name")}
		return t
	case len(t.srcs) == 0:
		return t
	}
	old := t.srcs[0]
	src := &rawSource{name: name, s: old.s, chunks: old.chunks, starts: old.starts, n: old.n}
	t.srcs = []*rawSource{src}
	t.orig = [][]origin{sourceOrigins(src, t.Len())}
	return t
}

// Len returns the number of rows.
func (t Table) Len() int {
	n := 0
	for _, b := range t.blocks {
		n += b.Len()
	}
	return n
}

// Columns returns the column names.
func (t Table) Columns() []string { return t.s.names() }

// Column returns the column with the given name.
func (t Table) Column(name string) (Column, bool) {
	i := t.s.index(name)
	if i < 0 {
		return Column{}, false
	}
	return newColumn(name, concatBlocks(t.blocks, t.s).Column(i)), true
}

// Rows returns the rows in order.
func (t Table) Rows() iter.Seq[Row] {
	return func(yield func(Row) bool) {
		for _, b := range t.blocks {
			for i := range b.Len() {
				if !yield(Row{t.s, b, i}) {
					return
				}
			}
		}
	}
}

// Rejects returns the errors of the rows the table's operations rejected, in
// the order they happened: one entry per error, so a row that failed in
// several columns appears once per column, under one ID (D15). The rejected
// rows as tables are in RejectedRows.
func (t Table) Rejects() []Reject {
	out := make([]Reject, len(t.rejects))
	for i, e := range t.rejects {
		out[i] = e.Reject
	}
	return out
}

// Findings returns the findings about the build of the deliveries the table
// was read from: missing, new and probably renamed columns (D22, D79).
func (t Table) Findings() []Finding { return append([]Finding(nil), t.finds...) }

// Err returns the sticky error, or nil. It is a *PlanError or, for an error
// whose code is set to ModeStop, a *DataError.
func (t Table) Err() error { return t.err }

// ErrorMode returns the table's general error mode.
func (t Table) ErrorMode() ErrorMode { return t.policy.mode }

// WithErrorMode returns the table with the given error mode for the
// operations that follow (D50). Settings per kind and per code take
// precedence (D19).
func (t Table) WithErrorMode(m ErrorMode) Table {
	t.policy.mode = m
	return t
}

// WithErrorKindMode sets the error mode for errors of kind k (D19).
func (t Table) WithErrorKindMode(k ErrorKind, m ErrorMode) Table {
	t.policy = t.policy.withKind(k, m)
	return t
}

// WithErrorCodeMode sets the error mode for errors with the given code,
// such as CodeParse or "custom:vip" (D19).
func (t Table) WithErrorCodeMode(code string, m ErrorMode) Table {
	t.policy = t.policy.withCode(code, m)
	return t
}

// origins returns the origin of every row, in order.
func (t Table) origins() []origin {
	var out []origin
	for _, o := range t.orig {
		out = append(out, o...)
	}
	return out
}

// batches returns the blocks of the table with the origins of their rows.
func (t Table) batches() []batch {
	out := make([]batch, len(t.blocks))
	for i, b := range t.blocks {
		out[i] = batch{b, t.orig[i]}
	}
	return out
}

// Apply runs op on the table at once, through the same engine as a pipeline
// step (D31).
func (t Table) Apply(op Op) Table {
	if t.err != nil {
		return t
	}
	name := "?"
	if op.impl != nil {
		name = op.impl.kind()
	}
	srcs, finds := t.srcs, t.finds
	if j, ok := op.impl.(joinOp); ok {
		if j.right.err != nil {
			t.err = j.right.err // the result carries the right side's error (D69)
			return t
		}
		if err := sourceClash(t.srcs, j.right.srcs); err != nil {
			t.err = &PlanError{Step: name, Err: err}
			return t
		}
		srcs = unionSources(t.srcs, j.right.srcs)
		finds = append(append([]Finding(nil), t.finds...), j.right.finds...)
	}
	steps, out, err := checkPlan(t.s, []step{{name, op}})
	if err != nil {
		t.err = err
		return t
	}
	if t.run == nil {
		t.run = newRun()
	}
	rx := &rejector{policy: t.policy, run: t.run}
	bs := t.batches()
	if len(bs) == 0 {
		bs = []batch{{emptyBlock(t.s), nil}}
	}
	res, err := run(context.Background(), func(yield func(batch) error) error {
		for _, b := range bs {
			if err := yield(b); err != nil {
				return err
			}
		}
		return nil
	}, steps, rx, 0)
	if err != nil {
		t.err = err
		return t
	}
	rejects := append(append([]rejectEntry(nil), t.rejects...), rx.entries...)
	return Table{s: out, blocks: blocksOf(res), orig: originsOf(res), srcs: srcs, rejects: rejects, finds: finds,
		policy: t.policy, prefix: t.prefix, run: t.run}
}

func blocksOf(bs []batch) []block.Block {
	out := make([]block.Block, len(bs))
	for i, b := range bs {
		out[i] = b.blk
	}
	return out
}

func originsOf(bs []batch) [][]origin {
	out := make([][]origin, len(bs))
	for i, b := range bs {
		out[i] = b.orig
	}
	return out
}

// Select keeps the given columns; see the Op Select.
func (t Table) Select(cols ...string) Table { return t.Apply(Select(cols...)) }

// Rename renames a column; see the Op Rename.
func (t Table) Rename(old, new string) Table { return t.Apply(Rename(old, new)) }

// Where keeps the rows for which cond is true; see the Op Where.
func (t Table) Where(cond Expr) Table { return t.Apply(Where(cond)) }

// WhereFunc keeps the rows for which f returns true; see the Op WhereFunc.
func (t Table) WhereFunc(f func(Row) (bool, error)) Table { return t.Apply(WhereFunc(f)) }

// With sets a column to an expression; see the Op With.
func (t Table) With(name string, e Expr) Table { return t.Apply(With(name, e)) }

// Cast converts a column to a type; see the Op Cast.
func (t Table) Cast(col string, to Type, opts ...CastOption) Table {
	return t.Apply(Cast(col, to, opts...))
}

// InnerJoin joins right; see the Op InnerJoin.
func (t Table) InnerJoin(right Table, keys ...JoinKey) Table {
	return t.Apply(InnerJoin(right, keys...))
}

// LeftJoin joins right and keeps rows without a partner; see the Op
// LeftJoin.
func (t Table) LeftJoin(right Table, keys ...JoinKey) Table {
	return t.Apply(LeftJoin(right, keys...))
}

// GroupBy groups and aggregates; see the Op GroupBy.
func (t Table) GroupBy(keys []string, aggs ...Agg) Table { return t.Apply(GroupBy(keys, aggs...)) }

// Sort orders the rows; see the Op Sort.
func (t Table) Sort(keys ...SortKey) Table { return t.Apply(Sort(keys...)) }

// String formats the table as aligned text, with null cells as <null>.
func (t Table) String() string {
	rows := [][]string{t.s.names()}
	for _, b := range t.blocks {
		vs := make([]*vec, b.Width())
		for i := range vs {
			vs[i] = vecOf(b.Column(i))
		}
		for i := range b.Len() {
			row := make([]string, len(vs))
			for j, v := range vs {
				if s, ok := v.format(i); ok {
					row[j] = s
				} else {
					row[j] = "<null>"
				}
			}
			rows = append(rows, row)
		}
	}
	width := make([]int, len(t.s))
	for _, r := range rows {
		for j, c := range r {
			width[j] = max(width[j], len([]rune(c)))
		}
	}
	var sb strings.Builder
	for _, r := range rows {
		for j, c := range r {
			if j > 0 {
				sb.WriteString("  ")
			}
			sb.WriteString(c)
			if j < len(r)-1 {
				sb.WriteString(strings.Repeat(" ", width[j]-len([]rune(c))))
			}
		}
		sb.WriteByte('\n')
	}
	return sb.String()
}
