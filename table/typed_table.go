//go:build go1.27

package table

import (
	"errors"
	"fmt"

	"github.com/stefanbethge/gseq/option"
)

// ColAs parses every value of col as T. It returns an error if col does not
// exist or any cell cannot be parsed (empty cells count as unparseable for
// non-string T). The result is aligned with t.Rows.
//
//	ages, err := t.ColAs[int]("age")
//
// Use ColOptAs to keep bad cells as None instead of failing, or the
// package-level ColAs function to skip them.
func (t Table) ColAs[T Value](col string) ([]T, error) {
	return t.colWith("ColAs", col, parseValueErr[T])
}

// ColWith parses every value of col with parse. It returns an error if col
// does not exist or parse fails for any cell. T is inferred from parse:
//
//	ns, err := t.ColWith("n", strconv.Atoi) // []int
func (t Table) ColWith[T any](col string, parse func(string) (T, error)) ([]T, error) {
	return t.colWith("ColWith", col, parse)
}

func (t Table) colWith[T any](op, col string, parse func(string) (T, error)) ([]T, error) {
	idx, ok := t.headerIdx[col]
	if !ok {
		return nil, typedErrf(t.source, "%s: unknown column %q", op, col)
	}
	return colWith(t.source, op, col, len(t.Rows), t.cellFn(idx), parse)
}

// ColOptAs parses every value of col as T and returns one Option per row:
// Some for parseable cells, None for empty or unparseable ones. Returns nil if
// col does not exist.
//
//	prices := t.ColOptAs[float64]("price")
//	for i, p := range prices {
//	    if p.IsNone() { log.Printf("row %d: bad price", i) }
//	}
func (t Table) ColOptAs[T Value](col string) []option.Option[T] {
	idx, ok := t.headerIdx[col]
	if !ok {
		return nil
	}
	return colOpt[T](len(t.Rows), t.cellFn(idx))
}

// MapAs parses every value of col as T, transforms it with fn and stores the
// result formatted as a string. Cells that are empty or cannot be parsed as T
// are left unchanged. Returns the table unchanged (with an error recorded) if
// col does not exist. T and U are inferred from fn:
//
//	t.MapAs("price", func(p float64) float64 { return p * 1.19 })
//	t.MapAs("qty", func(n int) bool { return n > 0 })
func (t Table) MapAs[T, U Value](col string, fn func(T) U) Table {
	if _, ok := t.headerIdx[col]; !ok {
		return t.withErrf("MapAs: unknown column %q", col)
	}
	return t.Map(col, typedMapFn(fn))
}

// AddColAs appends a new column whose value per row is computed by fn and
// formatted as a string. T is inferred from fn:
//
//	t.AddColAs("total", func(r table.Row) float64 {
//	    return r.GetAs[float64]("price").UnwrapOr(0) * float64(r.GetAs[int]("qty").UnwrapOr(0))
//	})
func (t Table) AddColAs[T Value](name string, fn func(Row) T) Table {
	return AddColOf(t, name, fn, formatValue[T])
}

// SumAs returns the sum of all values in col parsed as T. Empty and
// unparseable cells are skipped. Returns 0 if col does not exist.
//
//	total := t.SumAs[int64]("qty")
func (t Table) SumAs[T Number](col string) T {
	return t.ReduceAs(col, T(0), func(acc, v T) T { return acc + v })
}

// MinAs returns the smallest value in col parsed as T. Empty and unparseable
// cells are skipped. Returns None if col does not exist or has no parseable
// values.
//
//	cheapest := t.MinAs[float64]("price")
func (t Table) MinAs[T Ordered](col string) option.Option[T] {
	return t.ReduceAs(col, option.None[T](), minStep[T])
}

// MaxAs returns the largest value in col parsed as T. Empty and unparseable
// cells are skipped. Returns None if col does not exist or has no parseable
// values.
//
//	latest := t.MaxAs[string]("sku")
func (t Table) MaxAs[T Ordered](col string) option.Option[T] {
	return t.ReduceAs(col, option.None[T](), maxStep[T])
}

// ReduceAs folds all values in col parsed as T into an accumulator of type A,
// starting from init. Empty and unparseable cells are skipped. Returns init if
// col does not exist. T and A are inferred from fn:
//
//	positives := t.ReduceAs("delta", 0, func(n int, v float64) int {
//	    if v > 0 { n++ }
//	    return n
//	})
func (t Table) ReduceAs[T Value, A any](col string, init A, fn func(A, T) A) A {
	idx, ok := t.headerIdx[col]
	if !ok {
		return init
	}
	return reduceCol(len(t.Rows), t.cellFn(idx), init, fn)
}

// cellFn returns an accessor for column idx of row i.
func (t Table) cellFn(idx int) func(int) string {
	return func(i int) string { return valueAtRow(t.Rows[i].values, idx) }
}

// --- shared helpers for Table and MutableTable ---

func typedErrf(source, format string, args ...any) error {
	msg := fmt.Sprintf(format, args...)
	if source != "" {
		msg = "[" + source + "] " + msg
	}
	return errors.New(msg)
}

func colWith[T any](source, op, col string, n int, cell func(int) string, parse func(string) (T, error)) ([]T, error) {
	out := make([]T, n)
	for i := range n {
		v, err := parse(cell(i))
		if err != nil {
			return nil, typedErrf(source, "%s: column %q row %d: %v", op, col, i, err)
		}
		out[i] = v
	}
	return out, nil
}

func colOpt[T Value](n int, cell func(int) string) []option.Option[T] {
	out := make([]option.Option[T], n)
	for i := range n {
		out[i] = optionOf(parseValue[T](cell(i)))
	}
	return out
}

func reduceCol[T Value, A any](n int, cell func(int) string, acc A, fn func(A, T) A) A {
	for i := range n {
		if v, ok := parseValue[T](cell(i)); ok {
			acc = fn(acc, v)
		}
	}
	return acc
}

func typedMapFn[T, U Value](fn func(T) U) func(string) string {
	return func(raw string) string {
		v, ok := parseValue[T](raw)
		if !ok {
			return raw
		}
		return formatValue(fn(v))
	}
}

func minStep[T Ordered](acc option.Option[T], v T) option.Option[T] {
	if cur, ok := acc.Get(); ok && cur <= v {
		return acc
	}
	return option.Some(v)
}

func maxStep[T Ordered](acc option.Option[T], v T) option.Option[T] {
	if cur, ok := acc.Get(); ok && cur >= v {
		return acc
	}
	return option.Some(v)
}
