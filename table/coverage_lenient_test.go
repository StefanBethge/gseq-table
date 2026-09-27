//go:build !strict

package table_test

import (
	"strings"
	"testing"

	"github.com/stefanbethge/gseq-table/table"
)

// Tests in this file assert errors that accumulate on the table in the lenient
// (default) build. In the strict build the same calls panic — see
// coverage_strict_test.go for the corresponding coverage.

func TestCoverage_TableErrs(t *testing.T) {
	tb := newTable([]string{"x"}, nil).Select("missing")
	errs := tb.Errs()
	if len(errs) == 0 {
		t.Fatal("expected errors")
	}
}

func TestCoverage_Mutable_Errs(t *testing.T) {
	m := table.NewMutable([]string{"x"}, nil)
	m.Select("nonexistent")
	errs := m.Errs()
	if len(errs) == 0 {
		t.Fatal("expected errors on mutable table")
	}
}

func TestCoverage_Mutable_AssertColumns_Missing(t *testing.T) {
	m := table.NewMutable([]string{"id"}, [][]string{{"1"}})
	m.AssertColumns("id", "missing")
	if !m.HasErrs() {
		t.Fatal("expected error for missing column")
	}
}

func TestCoverage_Mutable_AssertNoEmpty_Empty(t *testing.T) {
	m := table.NewMutable([]string{"id"}, [][]string{{"1"}, {""}})
	m.AssertNoEmpty("id")
	if !m.HasErrs() {
		t.Fatal("expected error for empty cell")
	}
}

func TestCoverage_Mutable_SourcePrefixedError(t *testing.T) {
	m := table.NewMutable([]string{"x"}, nil)
	m.WithSource("data.csv")
	m.Select("nonexistent")
	errs := m.Errs()
	if len(errs) == 0 {
		t.Fatal("expected errors")
	}
	if !strings.Contains(errs[0].Error(), "data.csv") {
		t.Errorf("expected source prefix in error, got: %v", errs[0])
	}
}

func TestCoverage_Table_Sort_MissingCol(t *testing.T) {
	tb := newTable([]string{"a"}, [][]string{{"1"}, {"2"}})
	result := tb.Sort("nonexistent", true)
	checkInt(t, result.Len(), 2)
}

func TestCoverage_Table_Rename_UnknownCol(t *testing.T) {
	tb := newTable([]string{"a"}, [][]string{{"1"}})
	result := tb.Rename("nonexistent", "b")
	if result.Headers[0] != "a" {
		t.Errorf("unexpected header: %v", result.Headers)
	}
}

func TestCoverage_Table_FillBackward_MissingCol(t *testing.T) {
	tb := newTable([]string{"a"}, [][]string{{"1"}})
	result := tb.FillBackward("nonexistent")
	if !result.HasErrs() {
		t.Error("expected error for missing col")
	}
}

func TestCoverage_Table_FillForward_MissingCol(t *testing.T) {
	tb := newTable([]string{"a"}, [][]string{{"1"}})
	result := tb.FillForward("nonexistent")
	if !result.HasErrs() {
		t.Error("expected error for missing col")
	}
}

func TestCoverage_Mutable_Sort_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a"}, [][]string{{"1"}})
	m.Sort("nonexistent", true)
	if !m.HasErrs() {
		t.Error("expected error")
	}
}

func TestCoverage_Mutable_GroupBy_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a"}, [][]string{{"1"}})
	groups := m.GroupBy("nonexistent")
	checkInt(t, len(groups), 0)
}

func TestCoverage_Mutable_Explode_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a"}, [][]string{{"1"}})
	m.Explode("nonexistent", ",")
	if !m.HasErrs() {
		t.Error("expected error")
	}
}

func TestCoverage_Mutable_FillForward_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a"}, [][]string{{"1"}})
	m.FillForward("nonexistent")
	if !m.HasErrs() {
		t.Error("expected error")
	}
}

func TestCoverage_Mutable_FillBackward_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a"}, [][]string{{"1"}})
	m.FillBackward("nonexistent")
	if !m.HasErrs() {
		t.Error("expected error")
	}
}

func TestCoverage_Mutable_Lag_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a"}, [][]string{{"1"}})
	m.Lag("nonexistent", "out", 1)
	if !m.HasErrs() {
		t.Error("expected error")
	}
}

func TestCoverage_Mutable_Lead_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a"}, [][]string{{"1"}})
	m.Lead("nonexistent", "out", 1)
	if !m.HasErrs() {
		t.Error("expected error")
	}
}

func TestCoverage_Mutable_CumSum_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a"}, [][]string{{"1"}})
	m.CumSum("nonexistent", "out")
	if !m.HasErrs() {
		t.Error("expected error")
	}
}

func TestCoverage_Mutable_Rank_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a"}, [][]string{{"1"}})
	m.Rank("nonexistent", "out", true)
	if !m.HasErrs() {
		t.Error("expected error")
	}
}

func TestCoverage_Mutable_Rename_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a"}, [][]string{{"1"}})
	m.Rename("nonexistent", "b")
	if !m.HasErrs() {
		t.Error("expected error for Rename with missing col")
	}
}

func TestCoverage_Mutable_Map_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a"}, [][]string{{"1"}})
	m.Map("nonexistent", func(v string) string { return v })
	if !m.HasErrs() {
		t.Error("expected error for Map with missing col")
	}
}

func TestCoverage_Mutable_SortMulti_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a"}, [][]string{{"1"}, {"2"}})
	m.SortMulti(table.Asc("nonexistent"))
	if !m.HasErrs() {
		t.Error("expected error for SortMulti with missing col")
	}
}

func TestCoverage_Mutable_Select_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a", "b"}, [][]string{{"1", "2"}})
	m.Select("a", "nonexistent")
	if !m.HasErrs() {
		t.Error("expected error for Select with missing col")
	}
}

func TestCoverage_Mutable_AntiJoin_MissingLeftCol(t *testing.T) {
	m := table.NewMutable([]string{"id"}, [][]string{{"1"}})
	right := newTable([]string{"other"}, [][]string{{"1"}})
	m.AntiJoin(right, "nonexistent", "other")
	if !m.HasErrs() {
		t.Error("expected error for missing left col in AntiJoin")
	}
}

func TestCoverage_Mutable_AntiJoin_MissingRightCol(t *testing.T) {
	m := table.NewMutable([]string{"id"}, [][]string{{"1"}})
	right := newTable([]string{"id"}, [][]string{{"1"}})
	m.AntiJoin(right, "id", "nonexistent")
	if !m.HasErrs() {
		t.Error("expected error for missing right col in AntiJoin")
	}
}

func TestCoverage_Mutable_Lookup_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"code"}, [][]string{{"A"}})
	lookup := newTable([]string{"code", "name"}, [][]string{{"A", "Alpha"}})
	m.Lookup("nonexistent", "name", lookup, "code", "name")
	if !m.HasErrs() {
		t.Error("expected error for missing source col")
	}
}

func TestCoverage_Mutable_Lookup_MissingKeyCol(t *testing.T) {
	m := table.NewMutable([]string{"code"}, [][]string{{"A"}})
	lookup := newTable([]string{"code", "name"}, [][]string{{"A", "Alpha"}})
	m.Lookup("code", "name", lookup, "nonexistent", "name")
	if !m.HasErrs() {
		t.Error("expected error for missing key col in lookup")
	}
}

func TestCoverage_Mutable_Lookup_MissingValCol(t *testing.T) {
	m := table.NewMutable([]string{"code"}, [][]string{{"A"}})
	lookup := newTable([]string{"code", "name"}, [][]string{{"A", "Alpha"}})
	m.Lookup("code", "name", lookup, "code", "nonexistent")
	if !m.HasErrs() {
		t.Error("expected error for missing val col in lookup")
	}
}

func TestCoverage_Mutable_Intersect_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"id"}, [][]string{{"1"}})
	other := newTable([]string{"id"}, [][]string{{"1"}})
	m.Intersect(other, "nonexistent")
	if !m.HasErrs() {
		t.Error("expected error for missing col in Intersect")
	}
}

func TestCoverage_Mutable_Bin_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a"}, [][]string{{"1"}})
	m.Bin("nonexistent", "group", []table.BinDef{{Max: 100, Label: "low"}})
	if !m.HasErrs() {
		t.Error("expected error for missing col in Bin")
	}
}

func TestCoverage_Mutable_FormatCol_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a"}, [][]string{{"1"}})
	m.FormatCol("nonexistent", 2)
	if !m.HasErrs() {
		t.Error("expected error for missing col in FormatCol")
	}
}

func TestCoverage_Mutable_GroupByAgg_MissingGroupCol(t *testing.T) {
	m := table.NewMutable([]string{"a", "val"}, [][]string{{"x", "10"}})
	m.GroupByAgg([]string{"nonexistent"}, []table.AggDef{
		{Col: "total", Agg: table.Sum("val")},
	})
	if !m.HasErrs() {
		t.Error("expected error for missing group col")
	}
}

func TestCoverage_Mutable_DropEmpty_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a"}, [][]string{{"1"}})
	m.DropEmpty("nonexistent")
	if !m.HasErrs() {
		t.Error("expected error for missing col in DropEmpty")
	}
}

func TestCoverage_Mutable_Distinct_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a"}, [][]string{{"1"}})
	m.Distinct("nonexistent")
	if !m.HasErrs() {
		t.Error("expected error for missing col in Distinct")
	}
}

func TestCoverage_Mutable_OuterJoin_MissingLeftCol(t *testing.T) {
	m := table.NewMutable([]string{"id"}, [][]string{{"1"}})
	right := newTable([]string{"id"}, [][]string{{"1"}})
	m.OuterJoin(right, "nonexistent", "id")
	if !m.HasErrs() {
		t.Error("expected error for missing left col")
	}
}

func TestCoverage_Mutable_OuterJoin_MissingRightCol(t *testing.T) {
	m := table.NewMutable([]string{"id"}, [][]string{{"1"}})
	right := newTable([]string{"id"}, [][]string{{"1"}})
	m.OuterJoin(right, "id", "nonexistent")
	if !m.HasErrs() {
		t.Error("expected error for missing right col")
	}
}

func TestCoverage_Table_Pivot_MissingCol(t *testing.T) {
	tb := newTable([]string{"a", "b", "c"}, [][]string{{"1", "x", "v"}})
	result := tb.Pivot("nonexistent", "b", "c")
	if !result.HasErrs() {
		t.Error("expected error for missing index col in Pivot")
	}
}

func TestCoverage_Table_LeftJoin_MissingLeftCol(t *testing.T) {
	left := newTable([]string{"id"}, [][]string{{"1"}})
	right := newTable([]string{"id"}, [][]string{{"1"}})
	result := left.LeftJoin(right, "nonexistent", "id")
	if !result.HasErrs() {
		t.Error("expected error for missing left col")
	}
}

func TestCoverage_Table_LeftJoin_MissingRightCol(t *testing.T) {
	left := newTable([]string{"id"}, [][]string{{"1"}})
	right := newTable([]string{"id"}, [][]string{{"1"}})
	result := left.LeftJoin(right, "id", "nonexistent")
	if !result.HasErrs() {
		t.Error("expected error for missing right col")
	}
}

func TestCoverage_Table_SortMulti_MissingCol(t *testing.T) {
	tb := newTable([]string{"a"}, [][]string{{"1"}, {"2"}})
	result := tb.SortMulti(table.Asc("nonexistent"))
	// Should still return result (error accumulated internally)
	checkInt(t, result.Len(), 2)
}

func TestCoverage_Mutable_Pivot_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a", "b", "c"}, [][]string{{"1", "x", "v"}})
	m.Pivot("nonexistent", "b", "c")
	if !m.HasErrs() {
		t.Error("expected error for missing col in Pivot")
	}
}

func TestCoverage_Mutable_AssertNoEmpty_WithEmpty_AllCols(t *testing.T) {
	m := table.NewMutable([]string{"a", "b"}, [][]string{{"1", ""}, {"2", "x"}})
	m.AssertNoEmpty()
	if !m.HasErrs() {
		t.Error("expected error for empty cell in AllCols check")
	}
}
