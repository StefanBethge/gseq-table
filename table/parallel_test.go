package table

import (
	"strings"
	"testing"
)

func TestTransformParallel(t *testing.T) {
	tb := New([]string{"name", "city"}, [][]string{
		{"Alice", "berlin"},
		{"Bob", "munich"},
		{"Carol", "hamburg"},
	})
	result := tb.TransformParallel(func(r Row) map[string]string {
		return map[string]string{
			"city": strings.ToUpper(r.Get("city").UnwrapOr("")),
		}
	})
	assertEqual(t, len(result.Rows), 3)
	// names unchanged, cities uppercased
	assertEqual(t, result.Rows[0].Get("name").UnwrapOr(""), "Alice")
	// order preserved
	cities := result.Col("city")
	for _, c := range cities {
		if c != strings.ToUpper(c) {
			t.Errorf("expected uppercase city, got %q", c)
		}
	}
}

func TestMapParallel(t *testing.T) {
	tb := New([]string{"name"}, [][]string{{"alice"}, {"bob"}, {"carol"}})
	result := tb.MapParallel("name", strings.ToUpper)
	assertEqual(t, len(result.Rows), 3)
	assertEqual(t, result.Rows[0].Get("name").UnwrapOr(""), "ALICE")
	assertEqual(t, result.Rows[2].Get("name").UnwrapOr(""), "CAROL")
}

func TestMapParallel_UnknownCol(t *testing.T) {
	tb := New([]string{"a"}, [][]string{{"x"}})
	result := tb.MapParallel("unknown", strings.ToUpper)
	assertEqual(t, result.Rows[0].Get("a").UnwrapOr(""), "x") // unchanged
}

func TestTransformParallel_OrderPreserved(t *testing.T) {
	// build a large table and verify row order is stable after parallel transform
	n := 500
	records := make([][]string, n)
	for i := range n {
		records[i] = []string{strings.Repeat("x", i%10+1)}
	}
	tb := New([]string{"v"}, records)
	result := tb.TransformParallel(func(r Row) map[string]string {
		return map[string]string{"v": r.Get("v").UnwrapOr("") + "!"}
	})
	assertEqual(t, len(result.Rows), n)
	for i, row := range result.Rows {
		expected := strings.Repeat("x", i%10+1) + "!"
		assertEqual(t, row.Get("v").UnwrapOr(""), expected)
	}
}
