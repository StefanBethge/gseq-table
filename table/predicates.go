package table

import (
	"regexp"
	"strings"
)

// Predicate helpers return reusable func(Row) bool values for use with
// Where, AddColSwitch, Partition, and any other method that accepts a
// row predicate.
//
// Column-based predicates are Table methods so the column index can be
// pre-computed once at predicate creation time (O(1) per row instead of O(H)).
//
//	t.Where(t.Eq("status", "active"))
//	t.Where(table.And(t.Eq("status", "active"), t.NotEmpty("email")))

// Eq returns a predicate: col == val.
func (t Table) Eq(col, val string) func(Row) bool {
	idx := t.headerIdx[col]
	return func(r Row) bool {
		v := ""
		if idx < len(r.values) {
			v = r.values[idx]
		}
		return v == val
	}
}

// Ne returns a predicate: col != val.
func (t Table) Ne(col, val string) func(Row) bool {
	idx := t.headerIdx[col]
	return func(r Row) bool {
		v := ""
		if idx < len(r.values) {
			v = r.values[idx]
		}
		return v != val
	}
}

// Contains returns a predicate: strings.Contains(col, sub).
func (t Table) Contains(col, sub string) func(Row) bool {
	idx := t.headerIdx[col]
	return func(r Row) bool {
		v := ""
		if idx < len(r.values) {
			v = r.values[idx]
		}
		return strings.Contains(v, sub)
	}
}

// Prefix returns a predicate: strings.HasPrefix(col, prefix).
func (t Table) Prefix(col, prefix string) func(Row) bool {
	idx := t.headerIdx[col]
	return func(r Row) bool {
		v := ""
		if idx < len(r.values) {
			v = r.values[idx]
		}
		return strings.HasPrefix(v, prefix)
	}
}

// Suffix returns a predicate: strings.HasSuffix(col, suffix).
func (t Table) Suffix(col, suffix string) func(Row) bool {
	idx := t.headerIdx[col]
	return func(r Row) bool {
		v := ""
		if idx < len(r.values) {
			v = r.values[idx]
		}
		return strings.HasSuffix(v, suffix)
	}
}

// Matches returns a predicate that checks col against the compiled regexp.
// Panics if pattern is not a valid regular expression.
//
//	t.Where(t.Matches("email", `^[^@]+@gmail\.com$`))
func (t Table) Matches(col, pattern string) func(Row) bool {
	re := regexp.MustCompile(pattern)
	idx := t.headerIdx[col]
	return func(r Row) bool {
		v := ""
		if idx < len(r.values) {
			v = r.values[idx]
		}
		return re.MatchString(v)
	}
}

// Empty returns a predicate: col == "".
func (t Table) Empty(col string) func(Row) bool {
	idx := t.headerIdx[col]
	return func(r Row) bool {
		if idx < len(r.values) {
			return r.values[idx] == ""
		}
		return true
	}
}

// NotEmpty returns a predicate: col != "".
func (t Table) NotEmpty(col string) func(Row) bool {
	idx := t.headerIdx[col]
	return func(r Row) bool {
		if idx < len(r.values) {
			return r.values[idx] != ""
		}
		return false
	}
}

// And combines predicates with logical AND. All must return true.
//
//	t.Where(table.And(t.Eq("status", "active"), t.NotEmpty("email")))
func And(fns ...func(Row) bool) func(Row) bool {
	return func(r Row) bool {
		for _, fn := range fns {
			if !fn(r) {
				return false
			}
		}
		return true
	}
}

// Or combines predicates with logical OR. At least one must return true.
//
//	t.Where(table.Or(t.Eq("city", "Berlin"), t.Eq("city", "Munich")))
func Or(fns ...func(Row) bool) func(Row) bool {
	return func(r Row) bool {
		for _, fn := range fns {
			if fn(r) {
				return true
			}
		}
		return false
	}
}

// Not negates a predicate.
//
//	t.Where(table.Not(t.Empty("email")))
func Not(fn func(Row) bool) func(Row) bool {
	return func(r Row) bool { return !fn(r) }
}
