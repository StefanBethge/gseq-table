//go:build strict

package table_test

import (
	"strings"
	"testing"

	"github.com/stefanbethge/gseq-table/table"
)

// expectStrictPanic runs fn and fails the test if it does not panic. It
// returns the panic message so callers can inspect it.
func expectStrictPanic(t *testing.T, fn func()) (msg string) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Error("expected panic, got none")
			return
		}
		msg, _ = r.(string)
	}()
	fn()
	return ""
}

// TestCoverage_Table_InvalidInput_StrictPanics verifies that the Table error
// cases in coverage_lenient_test.go panic in the strict build.
func TestCoverage_Table_InvalidInput_StrictPanics(t *testing.T) {
	one := func() table.Table { return newTable([]string{"a"}, [][]string{{"1"}, {"2"}}) }
	ids := func() table.Table { return newTable([]string{"id"}, [][]string{{"1"}}) }

	cases := []struct {
		name string
		fn   func()
	}{
		{"Errs", func() { newTable([]string{"x"}, nil).Select("missing") }},
		{"Sort_MissingCol", func() { one().Sort("nonexistent", true) }},
		{"SortMulti_MissingCol", func() { one().SortMulti(table.Asc("nonexistent")) }},
		{"Rename_UnknownCol", func() { one().Rename("nonexistent", "b") }},
		{"FillBackward_MissingCol", func() { one().FillBackward("nonexistent") }},
		{"FillForward_MissingCol", func() { one().FillForward("nonexistent") }},
		{"Pivot_MissingCol", func() {
			newTable([]string{"a", "b", "c"}, [][]string{{"1", "x", "v"}}).Pivot("nonexistent", "b", "c")
		}},
		{"LeftJoin_MissingLeftCol", func() { ids().LeftJoin(ids(), "nonexistent", "id") }},
		{"LeftJoin_MissingRightCol", func() { ids().LeftJoin(ids(), "id", "nonexistent") }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { expectStrictPanic(t, c.fn) })
	}
}

// TestCoverage_Mutable_InvalidInput_StrictPanics verifies that the
// MutableTable error cases in coverage_lenient_test.go panic in the strict
// build.
func TestCoverage_Mutable_InvalidInput_StrictPanics(t *testing.T) {
	one := func() *table.MutableTable { return table.NewMutable([]string{"a"}, [][]string{{"1"}}) }
	ids := func() *table.MutableTable { return table.NewMutable([]string{"id"}, [][]string{{"1"}}) }
	right := func() table.Table { return newTable([]string{"id"}, [][]string{{"1"}}) }
	lookup := func() table.Table { return newTable([]string{"code", "name"}, [][]string{{"A", "Alpha"}}) }
	codes := func() *table.MutableTable { return table.NewMutable([]string{"code"}, [][]string{{"A"}}) }

	cases := []struct {
		name string
		fn   func()
	}{
		{"Errs", func() { table.NewMutable([]string{"x"}, nil).Select("nonexistent") }},
		{"AssertColumns_Missing", func() { ids().AssertColumns("id", "missing") }},
		{"AssertNoEmpty_Empty", func() {
			table.NewMutable([]string{"id"}, [][]string{{"1"}, {""}}).AssertNoEmpty("id")
		}},
		{"AssertNoEmpty_WithEmpty_AllCols", func() {
			table.NewMutable([]string{"a", "b"}, [][]string{{"1", ""}, {"2", "x"}}).AssertNoEmpty()
		}},
		{"Sort_MissingCol", func() { one().Sort("nonexistent", true) }},
		{"SortMulti_MissingCol", func() { one().SortMulti(table.Asc("nonexistent")) }},
		{"GroupBy_MissingCol", func() { one().GroupBy("nonexistent") }},
		{"Explode_MissingCol", func() { one().Explode("nonexistent", ",") }},
		{"FillForward_MissingCol", func() { one().FillForward("nonexistent") }},
		{"FillBackward_MissingCol", func() { one().FillBackward("nonexistent") }},
		{"Lag_MissingCol", func() { one().Lag("nonexistent", "out", 1) }},
		{"Lead_MissingCol", func() { one().Lead("nonexistent", "out", 1) }},
		{"CumSum_MissingCol", func() { one().CumSum("nonexistent", "out") }},
		{"Rank_MissingCol", func() { one().Rank("nonexistent", "out", true) }},
		{"Rename_MissingCol", func() { one().Rename("nonexistent", "b") }},
		{"Map_MissingCol", func() { one().Map("nonexistent", func(v string) string { return v }) }},
		{"Select_MissingCol", func() {
			table.NewMutable([]string{"a", "b"}, [][]string{{"1", "2"}}).Select("a", "nonexistent")
		}},
		{"AntiJoin_MissingLeftCol", func() {
			ids().AntiJoin(newTable([]string{"other"}, [][]string{{"1"}}), "nonexistent", "other")
		}},
		{"AntiJoin_MissingRightCol", func() { ids().AntiJoin(right(), "id", "nonexistent") }},
		{"OuterJoin_MissingLeftCol", func() { ids().OuterJoin(right(), "nonexistent", "id") }},
		{"OuterJoin_MissingRightCol", func() { ids().OuterJoin(right(), "id", "nonexistent") }},
		{"Lookup_MissingCol", func() { codes().Lookup("nonexistent", "name", lookup(), "code", "name") }},
		{"Lookup_MissingKeyCol", func() { codes().Lookup("code", "name", lookup(), "nonexistent", "name") }},
		{"Lookup_MissingValCol", func() { codes().Lookup("code", "name", lookup(), "code", "nonexistent") }},
		{"Intersect_MissingCol", func() { ids().Intersect(right(), "nonexistent") }},
		{"Bin_MissingCol", func() {
			one().Bin("nonexistent", "group", []table.BinDef{{Max: 100, Label: "low"}})
		}},
		{"FormatCol_MissingCol", func() { one().FormatCol("nonexistent", 2) }},
		{"GroupByAgg_MissingGroupCol", func() {
			table.NewMutable([]string{"a", "val"}, [][]string{{"x", "10"}}).GroupByAgg(
				[]string{"nonexistent"},
				[]table.AggDef{{Col: "total", Agg: table.Sum("val")}},
			)
		}},
		{"DropEmpty_MissingCol", func() { one().DropEmpty("nonexistent") }},
		{"Distinct_MissingCol", func() { one().Distinct("nonexistent") }},
		{"Pivot_MissingCol", func() {
			table.NewMutable([]string{"a", "b", "c"}, [][]string{{"1", "x", "v"}}).Pivot("nonexistent", "b", "c")
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { expectStrictPanic(t, c.fn) })
	}
}

// TestCoverage_Mutable_SourcePrefixedError_StrictPanics verifies that strict
// panics carry the source prefix set via WithSource.
func TestCoverage_Mutable_SourcePrefixedError_StrictPanics(t *testing.T) {
	msg := expectStrictPanic(t, func() {
		m := table.NewMutable([]string{"x"}, nil)
		m.WithSource("data.csv")
		m.Select("nonexistent")
	})
	if !strings.Contains(msg, "data.csv") {
		t.Errorf("expected source prefix in panic, got: %q", msg)
	}
}
