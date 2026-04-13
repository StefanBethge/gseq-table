//go:build !strict

package table

import (
	"strings"
	"testing"
)

// TestGroupByAgg_MissingGroupCol verifies that unknown group columns accumulate
// an error in the lenient (default) build. In the strict build this panics —
// see errs_strict_test.go for the corresponding coverage.
func TestGroupByAgg_MissingGroupCol(t *testing.T) {
	result := salesTable().GroupByAgg(
		[]string{"nonexistent"},
		[]AggDef{{Col: "total", Agg: Sum("revenue")}},
	)
	assertEqual(t, result.HasErrs(), true)
	assertEqual(t, len(result.Rows), 5)
}

// TestWithSource_ErrorPrefix verifies that error messages include the source
// name when WithSource has been set.
func TestWithSource_ErrorPrefix(t *testing.T) {
	tbl := New([]string{"city"}, [][]string{{"Berlin"}}).WithSource("sales.csv")
	out := tbl.Select("nonexistent")
	if !out.HasErrs() {
		t.Fatal("expected error")
	}
	msg := out.Errs()[0].Error()
	if len(msg) < 12 || msg[:12] != "[sales.csv] " {
		t.Errorf("expected '[sales.csv] ' prefix, got %q", msg)
	}
}

// ─── Join missing column tests ─────────────────────────────────────────────

func TestRightJoin_MissingLeftCol(t *testing.T) {
	left, right := joinTables()
	result := left.RightJoin(right, "nonexistent", "dept_id")
	assertEqual(t, len(result.Rows), len(left.Rows))
}

func TestRightJoin_MissingRightCol(t *testing.T) {
	left, right := joinTables()
	result := left.RightJoin(right, "dept_id", "nonexistent")
	assertEqual(t, len(result.Rows), len(left.Rows))
}

func TestOuterJoin_MissingCol(t *testing.T) {
	left, right := joinTables()
	result := left.OuterJoin(right, "nonexistent", "dept_id")
	assertEqual(t, len(result.Rows), len(left.Rows))
}

func TestAntiJoin_MissingLeftCol(t *testing.T) {
	left := New([]string{"id"}, [][]string{{"1"}, {"2"}})
	right := New([]string{"id"}, [][]string{{"1"}})
	result := left.AntiJoin(right, "nonexistent", "id")
	assertEqual(t, len(result.Rows), 2)
}

func TestAntiJoin_MissingRightCol(t *testing.T) {
	left := New([]string{"id"}, [][]string{{"1"}, {"2"}})
	right := New([]string{"id"}, [][]string{{"1"}})
	result := left.AntiJoin(right, "id", "nonexistent")
	assertEqual(t, len(result.Rows), 2)
}

// ─── Ops missing column tests ──────────────────────────────────────────────

func TestLookup_MissingCol(t *testing.T) {
	orders := New([]string{"id"}, [][]string{{"1"}})
	customers := New([]string{"id", "name"}, [][]string{{"1", "Alice"}})
	result := orders.Lookup("nonexistent", "cust_name", customers, "id", "name")
	assertEqual(t, len(result.Rows), 1)
	assertEqual(t, result.Rows[0].Get("cust_name").IsNone(), true)
}

func TestLookup_MissingLookupKeyCol(t *testing.T) {
	orders := New([]string{"id"}, [][]string{{"1"}})
	customers := New([]string{"name"}, [][]string{{"Alice"}})
	result := orders.Lookup("id", "cust_name", customers, "nonexistent", "name")
	assertEqual(t, len(result.Rows), 1)
}

func TestBin_MissingCol(t *testing.T) {
	tb := New([]string{"age"}, [][]string{{"25"}})
	result := tb.Bin("nonexistent", "group", []BinDef{{Max: 65, Label: "adult"}})
	assertEqual(t, len(result.Rows), 1)
	assertEqual(t, result.Rows[0].Get("group").IsNone(), true)
}

func TestIntersect_MissingCol(t *testing.T) {
	a := New([]string{"id"}, [][]string{{"1"}, {"2"}})
	b := New([]string{"id"}, [][]string{{"1"}})
	result := a.Intersect(b, "nonexistent")
	assertEqual(t, len(result.Rows), 2)
}

func TestFillForward_MissingCol(t *testing.T) {
	tb := New([]string{"a"}, [][]string{{"1"}, {""}, {"3"}})
	result := tb.FillForward("nonexistent")
	assertEqual(t, len(result.Rows), 3)
	assertEqual(t, result.Rows[1].Get("a").UnwrapOr(""), "")
}

func TestFillBackward_MissingCol(t *testing.T) {
	tb := New([]string{"a"}, [][]string{{""}, {"2"}, {"3"}})
	result := tb.FillBackward("nonexistent")
	assertEqual(t, result.Rows[0].Get("a").UnwrapOr(""), "")
}

// ─── Table missing column tests ────────────────────────────────────────────

func TestTable_Map_UnknownCol(t *testing.T) {
	tb := makeTable()
	result := tb.Map("unknown", func(v string) string { return "x" })
	assertEqual(t, result.Rows[0].Get("name").UnwrapOr(""), "Alice")
}

func TestTable_ValueCounts_MissingCol(t *testing.T) {
	vc := makeTable().ValueCounts("nonexistent")
	assertEqual(t, len(vc.Rows), 0)
	assertEqual(t, len(vc.Headers), 2)
}

func TestTable_Join_MissingLeftCol(t *testing.T) {
	left := New([]string{"name"}, [][]string{{"Alice"}})
	right := New([]string{"id", "city"}, [][]string{{"1", "Berlin"}})
	result := left.Join(right, "nonexistent", "id")
	assertEqual(t, len(result.Rows), 1)
	assertEqual(t, result.Rows[0].Get("name").UnwrapOr(""), "Alice")
}

func TestTable_Join_MissingRightCol(t *testing.T) {
	left := New([]string{"id", "name"}, [][]string{{"1", "Alice"}})
	right := New([]string{"city"}, [][]string{{"Berlin"}})
	result := left.Join(right, "id", "nonexistent")
	assertEqual(t, len(result.Rows), 1)
	assertEqual(t, result.Rows[0].Get("name").UnwrapOr(""), "Alice")
}

func TestTable_LeftJoin_MissingCol(t *testing.T) {
	left := New([]string{"name"}, [][]string{{"Alice"}})
	right := New([]string{"id"}, [][]string{{"1"}})
	result := left.LeftJoin(right, "nonexistent", "id")
	assertEqual(t, len(result.Rows), 1)
}

func TestTable_Pivot_MissingCol(t *testing.T) {
	tb := New([]string{"a", "b", "c"}, [][]string{{"1", "2", "3"}})
	result := tb.Pivot("a", "nonexistent", "c")
	assertEqual(t, len(result.Rows), 1)
}

func TestTable_Distinct_MissingCol(t *testing.T) {
	tb := New([]string{"a"}, [][]string{{"1"}, {"2"}})
	result := tb.Distinct("nonexistent")
	assertEqual(t, len(result.Rows), 2)
}

func TestTable_DropEmpty_MissingCol(t *testing.T) {
	tb := New([]string{"a"}, [][]string{{"1"}, {"2"}, {"3"}})
	result := tb.DropEmpty("nonexistent")
	assertEqual(t, len(result.Rows), 3)
}

func TestTable_DropEmpty_MixedExistingAndMissing(t *testing.T) {
	tb := New([]string{"a", "b"}, [][]string{{"1", "x"}, {"", "y"}})
	result := tb.DropEmpty("a", "nonexistent")
	assertEqual(t, len(result.Rows), 1)
	assertEqual(t, result.Rows[0].Get("a").UnwrapOr(""), "1")
}

// ─── Ops missing column tests (from ops_test.go) ───────────────────────────

func TestExplode_UnknownCol(t *testing.T) {
	tb := New([]string{"v"}, [][]string{{"x"}})
	result := tb.Explode("unknown", ",")
	assertEqual(t, len(result.Rows), 1)
}

// ─── Parallel missing column tests ─────────────────────────────────────────

func TestMapParallel_UnknownCol(t *testing.T) {
	tb := New([]string{"a"}, [][]string{{"x"}})
	result := tb.MapParallel("unknown", strings.ToUpper)
	assertEqual(t, result.Rows[0].Get("a").UnwrapOr(""), "x")
}

// ─── Timeseries missing column tests ───────────────────────────────────────

func TestLag_MissingCol(t *testing.T) {
	tb := tsTable()
	result := tb.Lag("nonexistent", "prev", 1)
	assertEqual(t, len(result.Rows), 5)
	assertEqual(t, len(result.Headers), 2)
}

func TestLead_MissingCol(t *testing.T) {
	tb := tsTable()
	result := tb.Lead("nonexistent", "next", 1)
	assertEqual(t, len(result.Rows), 5)
	assertEqual(t, len(result.Headers), 2)
}

func TestCumSum_MissingCol(t *testing.T) {
	tb := tsTable()
	result := tb.CumSum("nonexistent", "cum")
	assertEqual(t, len(result.Rows), 5)
	assertEqual(t, len(result.Headers), 2)
}

func TestRank_MissingCol(t *testing.T) {
	tb := tsTable()
	result := tb.Rank("nonexistent", "rank", true)
	assertEqual(t, len(result.Rows), 5)
	assertEqual(t, len(result.Headers), 2)
}

// ─── TryMap unknown column ──────────────────────────────────────────────────

func TestTryMap_UnknownCol(t *testing.T) {
	tb := New([]string{"a"}, [][]string{{"1"}})
	res := tb.TryMap("unknown", func(v string) (string, error) { return v, nil })
	assertEqual(t, res.IsOk(), true)
}

// ─── Mutable missing column / out-of-range tests ───────────────────────────

func TestMutableTable_SetErrors(t *testing.T) {
	m := NewMutable([]string{"id"}, [][]string{{"1"}})

	m.Set(5, "id", "x")
	if !m.HasErrs() {
		t.Fatal("expected row error")
	}
	m.ResetErrs()
	m.Set(0, "missing", "x")
	if !m.HasErrs() {
		t.Fatal("expected column error")
	}
}
