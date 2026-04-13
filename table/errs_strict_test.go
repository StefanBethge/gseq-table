//go:build strict

package table

import "testing"

// TestGroupByAgg_MissingGroupCol_StrictPanics verifies that unknown group
// columns cause a panic in the strict build. Use -tags strict in CI to surface
// programming errors as panics with stack traces.
func TestGroupByAgg_MissingGroupCol_StrictPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for unknown group column, got none")
		}
	}()
	salesTable().GroupByAgg(
		[]string{"nonexistent"},
		[]AggDef{{Col: "total", Agg: Sum("revenue")}},
	)
}

// TestWithSource_StrictPanicsWithPrefix verifies that panics in strict mode
// include the source name when WithSource has been set.
func TestWithSource_StrictPanicsWithPrefix(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic value, got %T", r)
		}
		if len(msg) < 12 || msg[:12] != "[sales.csv] " {
			t.Errorf("expected '[sales.csv] ' prefix in panic, got %q", msg)
		}
	}()
	New([]string{"city"}, [][]string{{"Berlin"}}).
		WithSource("sales.csv").
		Select("nonexistent")
}
