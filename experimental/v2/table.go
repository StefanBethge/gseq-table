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
// operation. In ModeStop the first data error sets the sticky error.
type Table struct {
	s       schema
	blocks  []block.Block
	rejects []Reject
	err     error
	mode    ErrorMode
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
		return Table{}
	}
	b, err := block.New(bcs...)
	if err != nil {
		return Table{err: &PlanError{Step: "new_table", Err: err}}
	}
	return Table{s: s, blocks: []block.Block{b}}
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

// Rejects returns the rows the table's operations rejected, in the order
// they were rejected.
func (t Table) Rejects() []Reject { return append([]Reject(nil), t.rejects...) }

// Err returns the sticky error, or nil. It is a *PlanError or, in ModeStop,
// a *DataError.
func (t Table) Err() error { return t.err }

// ErrorMode returns the table's error mode.
func (t Table) ErrorMode() ErrorMode { return t.mode }

// WithErrorMode returns the table with the given error mode for the
// operations that follow (D50).
func (t Table) WithErrorMode(m ErrorMode) Table {
	t.mode = m
	return t
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
	if j, ok := op.impl.(joinOp); ok && j.right.err != nil {
		t.err = j.right.err // the result carries the right side's error (D69)
		return t
	}
	steps, out, err := checkPlan(t.s, []step{{name, op}})
	if err != nil {
		t.err = err
		return t
	}
	rx := &rejector{mode: t.mode}
	blks := t.blocks
	if len(blks) == 0 {
		blks = []block.Block{emptyBlock(t.s)}
	}
	res, err := run(context.Background(), func(yield func(block.Block) error) error {
		for _, b := range blks {
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
	return Table{s: out, blocks: res, rejects: append(t.Rejects(), rx.rejects...), mode: t.mode}
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
