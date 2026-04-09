package table

// AddColOf is the generic base for typed column creation. fn computes a value
// of any type T for each row; stringify converts it to the string stored in the
// cell. Use the concrete wrappers (AddColFloat, AddColInt) for common types.
//
//	table.AddColOf(t, "ratio", func(r table.Row) float64 {
//	    return schema.Float(r, "a").UnwrapOr(0) / schema.Float(r, "b").UnwrapOr(1)
//	}, func(f float64) string { return fmt.Sprintf("%.4f", f) })
func AddColOf[T any](t Table, name string, fn func(Row) T, stringify func(T) string) Table {
	return t.AddCol(name, func(r Row) string {
		return stringify(fn(r))
	})
}

// ColAs extracts column col from t, applies parse to each value, and returns
// only the successfully parsed results. Unparseable and empty values are
// silently skipped.
//
//	// Extract age column as []int64
//	ages := table.ColAs(t, "age", func(v string) (int64, error) {
//	    return strconv.ParseInt(v, 10, 64)
//	})
func ColAs[T any](t Table, col string, parse func(string) (T, error)) []T {
	idx := t.headerIdx[col]
	var result []T
	for _, row := range t.Rows {
		v := ""
		if idx < len(row.values) {
			v = row.values[idx]
		}
		if parsed, err := parse(v); err == nil {
			result = append(result, parsed)
		}
	}
	return result
}

// MapColTo transforms all values in col using fn and returns them as a
// []T. Unlike ColAs, fn always succeeds — use it for infallible conversions
// like strings.ToUpper or strings.TrimSpace.
//
//	upper := table.MapColTo(t, "name", strings.ToUpper)
func MapColTo[T any](t Table, col string, fn func(string) T) []T {
	idx := t.headerIdx[col]
	result := make([]T, len(t.Rows))
	for i, row := range t.Rows {
		v := ""
		if idx < len(row.values) {
			v = row.values[idx]
		}
		result[i] = fn(v)
	}
	return result
}
