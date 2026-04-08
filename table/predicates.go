package table

import (
	"regexp"
	"strings"
)

// Predicate helpers return reusable func(Row) bool values for use with
// Where, AddColSwitch, Partition, and any other method that accepts a
// row predicate.

// Eq returns a predicate: col == val.
func Eq(col, val string) func(Row) bool {
	return func(r Row) bool { return r.Get(col).UnwrapOr("") == val }
}

// Ne returns a predicate: col != val.
func Ne(col, val string) func(Row) bool {
	return func(r Row) bool { return r.Get(col).UnwrapOr("") != val }
}

// Contains returns a predicate: strings.Contains(col, sub).
func Contains(col, sub string) func(Row) bool {
	return func(r Row) bool { return strings.Contains(r.Get(col).UnwrapOr(""), sub) }
}

// Prefix returns a predicate: strings.HasPrefix(col, prefix).
func Prefix(col, prefix string) func(Row) bool {
	return func(r Row) bool { return strings.HasPrefix(r.Get(col).UnwrapOr(""), prefix) }
}

// Suffix returns a predicate: strings.HasSuffix(col, suffix).
func Suffix(col, suffix string) func(Row) bool {
	return func(r Row) bool { return strings.HasSuffix(r.Get(col).UnwrapOr(""), suffix) }
}

// Matches returns a predicate that checks col against the compiled regexp.
// Panics if pattern is not a valid regular expression.
//
//	t.Where(table.Matches("email", `^[^@]+@gmail\.com$`))
func Matches(col, pattern string) func(Row) bool {
	re := regexp.MustCompile(pattern)
	return func(r Row) bool { return re.MatchString(r.Get(col).UnwrapOr("")) }
}

// Empty returns a predicate: col == "".
func Empty(col string) func(Row) bool {
	return func(r Row) bool { return r.Get(col).UnwrapOr("") == "" }
}

// NotEmpty returns a predicate: col != "".
func NotEmpty(col string) func(Row) bool {
	return func(r Row) bool { return r.Get(col).UnwrapOr("") != "" }
}

// And combines predicates with logical AND. All must return true.
//
//	t.Where(table.And(table.Eq("status", "active"), table.NotEmpty("email")))
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
//	t.Where(table.Or(table.Eq("city", "Berlin"), table.Eq("city", "Munich")))
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
//	t.Where(table.Not(table.Empty("email")))
func Not(fn func(Row) bool) func(Row) bool {
	return func(r Row) bool { return !fn(r) }
}
