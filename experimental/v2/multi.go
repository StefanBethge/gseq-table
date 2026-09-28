package gtable

import (
	"errors"
	"fmt"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

// CastAll runs several Cast operations as one step (D78), for example to
// type a delivery at once. Every cast works on the columns before the step.
// A row for which casts fail is rejected once, with an entry per failed
// column under one reject_id (D15). Two casts of the same column, an empty
// list and an operation other than Cast are plan errors.
func CastAll(casts ...Op) Op { return Op{castAllOp{casts}} }

type castAllOp struct{ ops []Op }

func (castAllOp) kind() string { return "cast_all" }

func (o castAllOp) plan(in schema) (schema, error) {
	if len(o.ops) == 0 {
		return nil, errors.New("no casts")
	}
	out := in.clone()
	seen := make(map[string]bool, len(o.ops))
	for _, op := range o.ops {
		c, ok := op.impl.(castOp)
		if !ok {
			return nil, fmt.Errorf("CastAll takes Cast operations, got %s", kindName(op))
		}
		if seen[c.col] {
			return nil, fmt.Errorf("column %q cast twice", c.col)
		}
		seen[c.col] = true
		s, err := c.plan(in)
		if err != nil {
			return nil, err
		}
		i := in.index(c.col)
		out[i] = s[i]
	}
	return out, nil
}

func (o castAllOp) apply(blk block.Block, in schema, sc *stepCtx) (block.Block, []int, error) {
	parts := make([]part, len(o.ops))
	for i, op := range o.ops {
		parts[i] = op.impl.(castOp).part(blk, in)
	}
	return applyParts(blk, sc, parts)
}

// WithAll runs several With operations as one step (D78), like with_columns
// in Polars. Every expression sees the columns before the step, not those
// another part of the step sets. New columns are appended in order. A row
// for which expressions fail is rejected once, with an entry per failed
// column under one reject_id (D15). Two parts setting the same column, an
// empty list and an operation other than With are plan errors.
func WithAll(withs ...Op) Op { return Op{withAllOp{withs}} }

type withAllOp struct{ ops []Op }

func (withAllOp) kind() string { return "with_all" }

func (o withAllOp) plan(in schema) (schema, error) {
	if len(o.ops) == 0 {
		return nil, errors.New("no expressions")
	}
	out := in
	seen := make(map[string]bool, len(o.ops))
	for _, op := range o.ops {
		w, ok := op.impl.(withOp)
		if !ok {
			return nil, fmt.Errorf("WithAll takes With operations, got %s", kindName(op))
		}
		if seen[w.name] {
			return nil, fmt.Errorf("column %q set twice", w.name)
		}
		seen[w.name] = true
		s, err := w.plan(in)
		if err != nil {
			return nil, err
		}
		out = setField(out, w.name, s[s.index(w.name)].kind)
	}
	return out, nil
}

func (o withAllOp) apply(blk block.Block, in schema, sc *stepCtx) (block.Block, []int, error) {
	parts := make([]part, len(o.ops))
	for i, op := range o.ops {
		parts[i] = op.impl.(withOp).part(blk, in)
	}
	return applyParts(blk, sc, parts)
}

// kindName names the operation of op for a message.
func kindName(op Op) string {
	if op.impl == nil {
		return "an empty operation"
	}
	return op.impl.kind()
}
