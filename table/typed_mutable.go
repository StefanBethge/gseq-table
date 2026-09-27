//go:build go1.27

package table

import "github.com/stefanbethge/gseq/option"

// ColAs parses every value of col as T. It returns an error if col does not
// exist or any cell cannot be parsed (empty cells count as unparseable for
// non-string T). See Table.ColAs.
func (m *MutableTable) ColAs[T Value](col string) ([]T, error) {
	return m.colWith("ColAs", col, parseValueErr[T])
}

// ColWith parses every value of col with parse. It returns an error if col
// does not exist or parse fails for any cell. See Table.ColWith.
func (m *MutableTable) ColWith[T any](col string, parse func(string) (T, error)) ([]T, error) {
	return m.colWith("ColWith", col, parse)
}

func (m *MutableTable) colWith[T any](op, col string, parse func(string) (T, error)) ([]T, error) {
	idx, ok := m.headerIdx[col]
	if !ok {
		return nil, typedErrf(m.source, "%s: unknown column %q", op, col)
	}
	return colWith(m.source, op, col, len(m.rows), m.cellFn(idx), parse)
}

// ColOptAs parses every value of col as T and returns one Option per row.
// Returns nil if col does not exist. See Table.ColOptAs.
func (m *MutableTable) ColOptAs[T Value](col string) []option.Option[T] {
	idx, ok := m.headerIdx[col]
	if !ok {
		return nil
	}
	return colOpt[T](len(m.rows), m.cellFn(idx))
}

// MapAs parses every value of col as T, transforms it with fn and stores the
// formatted result in place. Cells that are empty or cannot be parsed as T are
// left unchanged. See Table.MapAs.
//
//	m.MapAs("price", func(p float64) float64 { return p * 1.19 })
func (m *MutableTable) MapAs[T, U Value](col string, fn func(T) U) *MutableTable {
	if _, ok := m.headerIdx[col]; !ok {
		m.addErrf("MapAs: unknown column %q", col)
		return m
	}
	return m.Map(col, typedMapFn(fn))
}

// AddColAs appends a derived column in place whose value per row is computed
// by fn and formatted as a string. See Table.AddColAs.
func (m *MutableTable) AddColAs[T Value](name string, fn func(Row) T) *MutableTable {
	return m.AddCol(name, func(r Row) string { return formatValue(fn(r)) })
}

// SetAs formats val and stores it in a single cell in place. See Set.
//
//	m.SetAs(0, "qty", 42)
//	m.SetAs(0, "active", true)
func (m *MutableTable) SetAs[T Value](row int, col string, val T) *MutableTable {
	return m.Set(row, col, formatValue(val))
}

// SumAs returns the sum of all values in col parsed as T. See Table.SumAs.
func (m *MutableTable) SumAs[T Number](col string) T {
	return m.ReduceAs(col, T(0), func(acc, v T) T { return acc + v })
}

// MinAs returns the smallest value in col parsed as T. See Table.MinAs.
func (m *MutableTable) MinAs[T Ordered](col string) option.Option[T] {
	return m.ReduceAs(col, option.None[T](), minStep[T])
}

// MaxAs returns the largest value in col parsed as T. See Table.MaxAs.
func (m *MutableTable) MaxAs[T Ordered](col string) option.Option[T] {
	return m.ReduceAs(col, option.None[T](), maxStep[T])
}

// ReduceAs folds all values in col parsed as T into an accumulator of type A.
// See Table.ReduceAs.
func (m *MutableTable) ReduceAs[T Value, A any](col string, init A, fn func(A, T) A) A {
	idx, ok := m.headerIdx[col]
	if !ok {
		return init
	}
	return reduceCol(len(m.rows), m.cellFn(idx), init, fn)
}

// cellFn returns an accessor for column idx of row i.
func (m *MutableTable) cellFn(idx int) func(int) string {
	return func(i int) string { return valueAt(m.rows[i], idx) }
}
