package gtable

import (
	"errors"
	"fmt"
	"time"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

// Row is a read-only view of one row, for closures and for iterating over a
// Table. The accessors return the value and whether the cell holds one; they
// panic for an unknown column or a column of another type. In a closure such
// a panic rejects the row like any other panic (D39).
type Row struct {
	s   schema
	blk block.Block
	i   int
}

func (r Row) col(name string) block.Column {
	j := r.s.index(name)
	if j < 0 {
		panic(fmt.Sprintf("gtable: unknown column %q", name))
	}
	return r.blk.Column(j)
}

// Columns returns the column names of the row.
func (r Row) Columns() []string { return r.s.names() }

// IsNull reports whether the cell of column name has no value.
func (r Row) IsNull(name string) bool { return r.col(name).IsNull(r.i) }

// Text returns the cell of a text column.
func (r Row) Text(name string) (string, bool) { return r.col(name).Text(r.i) }

// Int returns the cell of an integer column.
func (r Row) Int(name string) (int64, bool) { return r.col(name).Int(r.i) }

// Float returns the cell of a floating-point column.
func (r Row) Float(name string) (float64, bool) { return r.col(name).Float(r.i) }

// Bool returns the cell of a boolean column.
func (r Row) Bool(name string) (bool, bool) { return r.col(name).Bool(r.i) }

// Timestamp returns the cell of a timestamp column.
func (r Row) Timestamp(name string) (time.Time, bool) { return r.col(name).Timestamp(r.i) }

// Value is a Go type a column can hold.
type Value interface {
	string | int64 | float64 | bool | time.Time
}

func kindOf[T Value]() block.Kind {
	var z T
	switch any(z).(type) {
	case string:
		return block.Text
	case int64:
		return block.Int
	case float64:
		return block.Float
	case bool:
		return block.Bool
	}
	return block.Timestamp
}

// callRow runs f for one row and turns a panic into an error (D39).
func callRow[T any](f func(Row) (T, error), r Row) (v T, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = errors.New(panicReason(p))
		}
	}()
	return f(r)
}

// WithFunc sets column name to the result of f for every row: the escape
// hatch for logic that no expression can state (D32). If f returns an error,
// the row is rejected with code "custom" and the error text as reason, or
// "custom:<code>" for a CustomError (D54). A panic in f counts as a returned
// error, with the panic message as reason (D39).
func WithFunc[T Value](name string, f func(Row) (T, error)) Op {
	return Op{withFuncOp[T]{name, f}}
}

type withFuncOp[T Value] struct {
	name string
	f    func(Row) (T, error)
}

func (withFuncOp[T]) kind() string { return "with_func" }

func (o withFuncOp[T]) plan(in schema) (schema, error) {
	if o.name == "" {
		return nil, errors.New("empty column name")
	}
	if o.f == nil {
		return nil, errors.New("nil function")
	}
	return setField(in, o.name, kindOf[T]()), nil
}

func (o withFuncOp[T]) apply(blk block.Block, in schema, sc *stepCtx) (block.Block, error) {
	b := block.NewBuilder(kindOf[T](), blk.Len())
	drop := make([]bool, blk.Len())
	for i := range blk.Len() {
		v, err := callRow(o.f, Row{in, blk, i})
		if err != nil {
			if err := sc.reject(o.name, "", false, err.Error(), customCode(err)); err != nil {
				return block.Block{}, err
			}
			drop[i] = true
			b.AppendNull()
			continue
		}
		switch x := any(v).(type) {
		case string:
			b.AppendText(x)
		case int64:
			b.AppendInt(x)
		case float64:
			b.AppendFloat(x)
		case bool:
			b.AppendBool(x)
		case time.Time:
			b.AppendTimestamp(x)
		}
	}
	return dropRows(withColumn(blk, in.index(o.name), b.Build()), drop), nil
}

// WhereFunc keeps the rows for which f returns true. Errors and panics in f
// reject the row as for WithFunc (D32, D39, D54).
func WhereFunc(f func(Row) (bool, error)) Op { return Op{whereFuncOp{f}} }

type whereFuncOp struct {
	f func(Row) (bool, error)
}

func (whereFuncOp) kind() string { return "where_func" }

func (o whereFuncOp) plan(in schema) (schema, error) {
	if o.f == nil {
		return nil, errors.New("nil function")
	}
	return in, nil
}

func (o whereFuncOp) apply(blk block.Block, in schema, sc *stepCtx) (block.Block, error) {
	drop := make([]bool, blk.Len())
	for i := range blk.Len() {
		ok, err := callRow(o.f, Row{in, blk, i})
		if err != nil {
			if err := sc.reject("", "", false, err.Error(), customCode(err)); err != nil {
				return block.Block{}, err
			}
		}
		drop[i] = err != nil || !ok
	}
	return dropRows(blk, drop), nil
}
