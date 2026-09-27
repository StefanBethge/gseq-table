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

// TestMutableTable_AppendMapUnknownKey_StrictPanics verifies that unknown map
// keys in AppendMap panic in the strict build.
func TestMutableTable_AppendMapUnknownKey_StrictPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for unknown column, got none")
		}
	}()
	NewMutable([]string{"id"}, nil).AppendMap(map[string]string{"missing": "x"})
}

// expectPanic runs fn and fails the test if it does not panic.
func expectPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic, got none")
		}
	}()
	fn()
}

// TestJSON_InvalidInput_StrictPanics verifies that unknown columns and invalid
// JSON paths in ExpandJSON/MapJSON/TryMapJSON panic in the strict build. The
// lenient counterparts live in errs_lenient_test.go.
func TestJSON_InvalidInput_StrictPanics(t *testing.T) {
	idTbl := func() Table { return makeTestTable([]string{"id"}, [][]string{{"1"}}) }
	dataTbl := func() Table { return makeTestTable([]string{"data"}, [][]string{{`{"a":"1"}`}}) }
	badMapping := WithJSONFieldMapping(map[string]string{"val": "bad"})

	cases := []struct {
		name string
		fn   func()
	}{
		{"ExpandJSON_UnknownColumn", func() { idTbl().ExpandJSON("nonexistent") }},
		{"ExpandJSON_InvalidFieldMappingPath", func() { dataTbl().ExpandJSON("data", badMapping) }},
		{"MapJSON_UnknownColumn", func() { idTbl().MapJSON("nonexistent", ".a") }},
		{"MapJSON_InvalidPath", func() { dataTbl().MapJSON("data", "bad.path") }},
		{"MapJSON_InvalidFieldMappingPath", func() { dataTbl().MapJSON("data", badMapping) }},
		{"TryMapJSON_UnknownColumn", func() { idTbl().TryMapJSON("nonexistent", ".key") }},
		{"TryMapJSON_InvalidPath", func() { dataTbl().TryMapJSON("data", "bad.path") }},
		{"Mutable_ExpandJSON_UnknownColumn", func() { idTbl().Mutable().ExpandJSON("nonexistent") }},
		{"Mutable_ExpandJSON_InvalidFieldMappingPath", func() { dataTbl().Mutable().ExpandJSON("data", badMapping) }},
		{"Mutable_MapJSON_UnknownColumn", func() { idTbl().Mutable().MapJSON("nonexistent", ".key") }},
		{"Mutable_MapJSON_InvalidPath", func() { dataTbl().Mutable().MapJSON("data", "bad.path") }},
		{"Mutable_TryMapJSON_UnknownColumn", func() { idTbl().Mutable().TryMapJSON("nonexistent", ".key") }},
		{"Mutable_TryMapJSON_InvalidPath", func() { dataTbl().Mutable().TryMapJSON("data", "bad.path") }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { expectPanic(t, c.fn) })
	}
}
