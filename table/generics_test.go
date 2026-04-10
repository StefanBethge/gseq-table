package table

import (
	"strconv"
	"strings"
	"testing"
)

func TestAddColOf_Float(t *testing.T) {
	tb := New([]string{"v"}, [][]string{{"3"}, {"4"}})
	result := AddColOf(tb, "sq", func(r Row) float64 {
		n, _ := strconv.ParseFloat(r.Get("v").UnwrapOr("0"), 64)
		return n * n
	}, func(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) })
	assertEqual(t, result.Rows[0].Get("sq").UnwrapOr(""), "9")
	assertEqual(t, result.Rows[1].Get("sq").UnwrapOr(""), "16")
}

func TestColAs_Int(t *testing.T) {
	tb := New([]string{"n"}, [][]string{{"1"}, {"abc"}, {"3"}, {""}})
	vals := ColAs(tb, "n", func(v string) (int64, error) {
		return strconv.ParseInt(v, 10, 64)
	})
	assertEqual(t, len(vals), 2) // "abc" and "" skipped
	assertEqual(t, vals[0], int64(1))
	assertEqual(t, vals[1], int64(3))
}

func TestColAs_Empty(t *testing.T) {
	tb := New([]string{"n"}, nil)
	vals := ColAs(tb, "n", func(v string) (int64, error) {
		return strconv.ParseInt(v, 10, 64)
	})
	assertEqual(t, len(vals), 0)
}

func TestMapColTo(t *testing.T) {
	tb := New([]string{"name"}, [][]string{{"alice"}, {"bob"}})
	result := MapColTo(tb, "name", strings.ToUpper)
	assertEqual(t, len(result), 2)
	assertEqual(t, result[0], "ALICE")
	assertEqual(t, result[1], "BOB")
}

func TestAddColFloat(t *testing.T) {
	tb := New([]string{"a", "b"}, [][]string{{"3", "4"}, {"1", "2"}})
	result := tb.AddColFloat("sum", func(r Row) float64 {
		a, _ := strconv.ParseFloat(r.Get("a").UnwrapOr("0"), 64)
		b, _ := strconv.ParseFloat(r.Get("b").UnwrapOr("0"), 64)
		return a + b
	})
	assertEqual(t, result.Rows[0].Get("sum").UnwrapOr(""), "7")
	assertEqual(t, result.Rows[1].Get("sum").UnwrapOr(""), "3")
}

func TestAddColInt(t *testing.T) {
	tb := New([]string{"v"}, [][]string{{"5"}, {"10"}})
	result := tb.AddColInt("doubled", func(r Row) int64 {
		n, _ := strconv.ParseInt(r.Get("v").UnwrapOr("0"), 10, 64)
		return n * 2
	})
	assertEqual(t, result.Rows[0].Get("doubled").UnwrapOr(""), "10")
	assertEqual(t, result.Rows[1].Get("doubled").UnwrapOr(""), "20")
}

// --- Missing column edge cases ---

func TestColAs_MissingCol(t *testing.T) {
	tb := New([]string{"a"}, [][]string{{"1"}, {"2"}})
	vals := ColAs(tb, "nonexistent", func(v string) (int64, error) {
		return strconv.ParseInt(v, 10, 64)
	})
	// nil result for missing column
	assertEqual(t, len(vals), 0)
}

func TestMapColTo_MissingCol(t *testing.T) {
	tb := New([]string{"a"}, [][]string{{"1"}, {"2"}})
	result := MapColTo(tb, "nonexistent", strings.ToUpper)
	// nil result for missing column
	assertEqual(t, len(result), 0)
}
