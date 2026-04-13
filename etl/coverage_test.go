package etl_test

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stefanbethge/gseq-table/etl"
	"github.com/stefanbethge/gseq-table/schema"
	"github.com/stefanbethge/gseq-table/table"
	"github.com/stefanbethge/gseq/result"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

func assertEq[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func baseTable() table.Table {
	return table.New(
		[]string{"name", "city", "score"},
		[][]string{
			{"Alice", "Berlin", "80"},
			{"Bob", "Munich", "90"},
			{"Carol", "Berlin", "70"},
		},
	)
}

func baseMutable() *table.MutableTable {
	return table.NewMutable(
		[]string{"name", "city", "score"},
		[][]string{
			{"Alice", "Berlin", "80"},
			{"Bob", "Munich", "90"},
			{"Carol", "Berlin", "70"},
		},
	)
}

// ─────────────────────────────────────────────────────────────────────────────
// ops.go — TableFunc factory functions
// ─────────────────────────────────────────────────────────────────────────────

func TestCoverage_Compose(t *testing.T) {
	normalize := etl.Compose(
		etl.Map("name", strings.ToUpper),
		etl.Select("name", "score"),
	)
	out := etl.From(baseTable()).Then(normalize).Unwrap()
	assertEq(t, len(out.Headers), 2)
	assertEq(t, out.Rows[0].Get("name").UnwrapOr(""), "ALICE")
}

func TestCoverage_Compose_Empty(t *testing.T) {
	noop := etl.Compose()
	out := etl.From(baseTable()).Then(noop).Unwrap()
	assertEq(t, out.Len(), 3)
}

func TestCoverage_Select(t *testing.T) {
	out := etl.From(baseTable()).Then(etl.Select("name")).Unwrap()
	assertEq(t, len(out.Headers), 1)
	assertEq(t, out.Headers[0], "name")
}

func TestCoverage_Map(t *testing.T) {
	out := etl.From(baseTable()).Then(etl.Map("city", strings.ToUpper)).Unwrap()
	assertEq(t, out.Rows[0].Get("city").UnwrapOr(""), "BERLIN")
}

func TestCoverage_Where(t *testing.T) {
	out := etl.From(baseTable()).Then(etl.Where(func(r table.Row) bool {
		return r.Get("city").UnwrapOr("") == "Berlin"
	})).Unwrap()
	assertEq(t, out.Len(), 2)
}

func TestCoverage_AddCol(t *testing.T) {
	out := etl.From(baseTable()).Then(etl.AddCol("tag", func(r table.Row) string {
		return r.Get("name").UnwrapOr("") + "@" + r.Get("city").UnwrapOr("")
	})).Unwrap()
	assertEq(t, out.Rows[0].Get("tag").UnwrapOr(""), "Alice@Berlin")
}

func TestCoverage_AddColFloat(t *testing.T) {
	out := etl.From(baseTable()).Then(etl.AddColFloat("doubled", func(r table.Row) float64 {
		v, _ := strconv.ParseFloat(r.Get("score").UnwrapOr("0"), 64)
		return v * 2
	})).Unwrap()
	assertEq(t, out.Rows[0].Get("doubled").UnwrapOr(""), "160")
}

func TestCoverage_AddColInt(t *testing.T) {
	out := etl.From(baseTable()).Then(etl.AddColInt("rank", func(r table.Row) int64 {
		return 1
	})).Unwrap()
	assertEq(t, out.Rows[0].Get("rank").UnwrapOr(""), "1")
}

func TestCoverage_Rename(t *testing.T) {
	out := etl.From(baseTable()).Then(etl.Rename("city", "location")).Unwrap()
	assertEq(t, out.Headers[1], "location")
}

func TestCoverage_RenameMany(t *testing.T) {
	out := etl.From(baseTable()).Then(etl.RenameMany(map[string]string{
		"name": "person", "city": "place",
	})).Unwrap()
	assertEq(t, out.Headers[0], "person")
	assertEq(t, out.Headers[1], "place")
}

func TestCoverage_Drop(t *testing.T) {
	out := etl.From(baseTable()).Then(etl.Drop("score")).Unwrap()
	assertEq(t, len(out.Headers), 2)
	assertEq(t, out.Rows[0].Get("score").IsNone(), true)
}

func TestCoverage_DropEmpty(t *testing.T) {
	t2 := table.New(
		[]string{"name", "city"},
		[][]string{{"Alice", "Berlin"}, {"Bob", ""}, {"Carol", "Munich"}},
	)
	out := etl.From(t2).Then(etl.DropEmpty("city")).Unwrap()
	assertEq(t, out.Len(), 2)
}

func TestCoverage_FillEmpty(t *testing.T) {
	t2 := table.New(
		[]string{"name", "region"},
		[][]string{{"Alice", ""}, {"Bob", "EU"}},
	)
	out := etl.From(t2).Then(etl.FillEmpty("region", "unknown")).Unwrap()
	assertEq(t, out.Rows[0].Get("region").UnwrapOr(""), "unknown")
	assertEq(t, out.Rows[1].Get("region").UnwrapOr(""), "EU")
}

func TestCoverage_FillForward(t *testing.T) {
	t2 := table.New(
		[]string{"name", "region"},
		[][]string{{"Alice", "EU"}, {"Bob", ""}, {"Carol", ""}},
	)
	out := etl.From(t2).Then(etl.FillForward("region")).Unwrap()
	assertEq(t, out.Rows[1].Get("region").UnwrapOr(""), "EU")
	assertEq(t, out.Rows[2].Get("region").UnwrapOr(""), "EU")
}

func TestCoverage_FillBackward(t *testing.T) {
	t2 := table.New(
		[]string{"name", "region"},
		[][]string{{"Alice", ""}, {"Bob", ""}, {"Carol", "US"}},
	)
	out := etl.From(t2).Then(etl.FillBackward("region")).Unwrap()
	assertEq(t, out.Rows[0].Get("region").UnwrapOr(""), "US")
	assertEq(t, out.Rows[1].Get("region").UnwrapOr(""), "US")
}

func TestCoverage_Sort(t *testing.T) {
	out := etl.From(baseTable()).Then(etl.Sort("score", true)).Unwrap()
	assertEq(t, out.Rows[0].Get("name").UnwrapOr(""), "Carol")
}

func TestCoverage_SortMulti(t *testing.T) {
	out := etl.From(baseTable()).Then(etl.SortMulti(
		table.Asc("city"), table.Desc("score"),
	)).Unwrap()
	assertEq(t, out.Rows[0].Get("city").UnwrapOr(""), "Berlin")
}

func TestCoverage_Distinct(t *testing.T) {
	t2 := table.New(
		[]string{"city"},
		[][]string{{"Berlin"}, {"Munich"}, {"Berlin"}},
	)
	out := etl.From(t2).Then(etl.Distinct("city")).Unwrap()
	assertEq(t, out.Len(), 2)
}

func TestCoverage_Head(t *testing.T) {
	out := etl.From(baseTable()).Then(etl.Head(2)).Unwrap()
	assertEq(t, out.Len(), 2)
	assertEq(t, out.Rows[0].Get("name").UnwrapOr(""), "Alice")
}

func TestCoverage_Tail(t *testing.T) {
	out := etl.From(baseTable()).Then(etl.Tail(1)).Unwrap()
	assertEq(t, out.Len(), 1)
	assertEq(t, out.Rows[0].Get("name").UnwrapOr(""), "Carol")
}

func TestCoverage_Sample(t *testing.T) {
	out := etl.From(baseTable()).Then(etl.Sample(2)).Unwrap()
	assertEq(t, out.Len(), 2)
}

func TestCoverage_SampleFrac(t *testing.T) {
	out := etl.From(baseTable()).Then(etl.SampleFrac(0.5)).Unwrap()
	if out.Len() > 3 {
		t.Errorf("expected ≤3 rows, got %d", out.Len())
	}
}

func TestCoverage_Append(t *testing.T) {
	other := table.New(
		[]string{"name", "city", "score"},
		[][]string{{"Dave", "Hamburg", "85"}},
	)
	out := etl.From(baseTable()).Then(etl.Append(other)).Unwrap()
	assertEq(t, out.Len(), 4)
}

func TestCoverage_Join(t *testing.T) {
	lookup := table.New(
		[]string{"city", "country"},
		[][]string{{"Berlin", "DE"}, {"Munich", "DE"}},
	)
	out := etl.From(baseTable()).Then(etl.Join(lookup, "city", "city")).Unwrap()
	assertEq(t, out.Len(), 3)
	assertEq(t, out.Rows[0].Get("country").UnwrapOr(""), "DE")
}

func TestCoverage_LeftJoin(t *testing.T) {
	lookup := table.New(
		[]string{"city", "country"},
		[][]string{{"Berlin", "DE"}},
	)
	out := etl.From(baseTable()).Then(etl.LeftJoin(lookup, "city", "city")).Unwrap()
	assertEq(t, out.Len(), 3)
	assertEq(t, out.Rows[1].Get("country").UnwrapOr(""), "") // Munich not in lookup
}

func TestCoverage_Intersect(t *testing.T) {
	other := table.New(
		[]string{"name", "city", "score"},
		[][]string{{"Alice", "Berlin", "80"}, {"Dave", "Hamburg", "85"}},
	)
	out := etl.From(baseTable()).Then(etl.Intersect(other, "name")).Unwrap()
	assertEq(t, out.Len(), 1)
	assertEq(t, out.Rows[0].Get("name").UnwrapOr(""), "Alice")
}

func TestCoverage_AddRowIndex(t *testing.T) {
	out := etl.From(baseTable()).Then(etl.AddRowIndex("idx")).Unwrap()
	assertEq(t, out.Rows[0].Get("idx").UnwrapOr(""), "0")
	assertEq(t, out.Rows[2].Get("idx").UnwrapOr(""), "2")
}

func TestCoverage_Explode(t *testing.T) {
	t2 := table.New(
		[]string{"name", "tags"},
		[][]string{{"Alice", "a,b,c"}, {"Bob", "x,y"}},
	)
	out := etl.From(t2).Then(etl.Explode("tags", ",")).Unwrap()
	assertEq(t, out.Len(), 5)
}

func TestCoverage_Transpose(t *testing.T) {
	t2 := table.New(
		[]string{"col1", "col2"},
		[][]string{{"a", "b"}, {"c", "d"}},
	)
	out := etl.From(t2).Then(etl.Transpose()).Unwrap()
	assertEq(t, out.Len(), 2) // 2 original columns → 2 rows
}

func TestCoverage_ValueCounts(t *testing.T) {
	out := etl.From(baseTable()).Then(etl.ValueCounts("city")).Unwrap()
	assertEq(t, out.Len(), 2) // Berlin, Munich
}

func TestCoverage_Melt(t *testing.T) {
	t2 := table.New(
		[]string{"id", "jan", "feb"},
		[][]string{{"1", "100", "200"}},
	)
	out := etl.From(t2).Then(etl.Melt([]string{"id"}, "month", "value")).Unwrap()
	assertEq(t, out.Len(), 2)
}

func TestCoverage_Pivot(t *testing.T) {
	t2 := table.New(
		[]string{"id", "month", "value"},
		[][]string{{"1", "jan", "100"}, {"1", "feb", "200"}},
	)
	out := etl.From(t2).Then(etl.Pivot("id", "month", "value")).Unwrap()
	assertEq(t, out.Len(), 1)
}

func TestCoverage_AddColSwitch(t *testing.T) {
	cases := []table.Case{
		{
			When: func(r table.Row) bool { return r.Get("city").UnwrapOr("") == "Berlin" },
			Then: func(r table.Row) string { return "capital" },
		},
	}
	out := etl.From(baseTable()).Then(etl.AddColSwitch("label", cases, func(r table.Row) string {
		return "other"
	})).Unwrap()
	assertEq(t, out.Rows[0].Get("label").UnwrapOr(""), "capital")
	assertEq(t, out.Rows[1].Get("label").UnwrapOr(""), "other")
}

func TestCoverage_TransformRows(t *testing.T) {
	out := etl.From(baseTable()).Then(etl.TransformRows(func(r table.Row) map[string]string {
		return map[string]string{"name": strings.ToLower(r.Get("name").UnwrapOr(""))}
	})).Unwrap()
	assertEq(t, out.Rows[0].Get("name").UnwrapOr(""), "alice")
}

func TestCoverage_GroupByAgg(t *testing.T) {
	out := etl.From(baseTable()).Then(etl.GroupByAgg(
		[]string{"city"},
		[]table.AggDef{{Col: "total", Agg: table.Sum("score")}},
	)).Unwrap()
	assertEq(t, out.Len(), 2)
}

func TestCoverage_RollingAgg(t *testing.T) {
	t2 := table.New(
		[]string{"val"},
		[][]string{{"10"}, {"20"}, {"30"}, {"40"}},
	)
	out := etl.From(t2).Then(etl.RollingAgg("rolling", 2, table.Mean("val"))).Unwrap()
	assertEq(t, out.Len(), 4)
}

func TestCoverage_Coalesce(t *testing.T) {
	t2 := table.New(
		[]string{"a", "b", "c"},
		[][]string{{"", "", "z"}, {"x", "", "z"}},
	)
	out := etl.From(t2).Then(etl.Coalesce("result", "a", "b", "c")).Unwrap()
	assertEq(t, out.Rows[0].Get("result").UnwrapOr(""), "z")
	assertEq(t, out.Rows[1].Get("result").UnwrapOr(""), "x")
}

func TestCoverage_Lookup(t *testing.T) {
	lut := table.New(
		[]string{"code", "label"},
		[][]string{{"DE", "Germany"}, {"US", "United States"}},
	)
	t2 := table.New(
		[]string{"name", "code"},
		[][]string{{"Alice", "DE"}, {"Bob", "US"}},
	)
	out := etl.From(t2).Then(etl.Lookup("code", "country", lut, "code", "label")).Unwrap()
	assertEq(t, out.Rows[0].Get("country").UnwrapOr(""), "Germany")
}

func TestCoverage_FormatCol(t *testing.T) {
	t2 := table.New(
		[]string{"val"},
		[][]string{{"3.14159"}},
	)
	out := etl.From(t2).Then(etl.FormatCol("val", 2)).Unwrap()
	assertEq(t, out.Rows[0].Get("val").UnwrapOr(""), "3.14")
}

func TestCoverage_Bin(t *testing.T) {
	bins := []table.BinDef{
		{Max: 75, Label: "low"},
		{Max: 85, Label: "mid"},
		{Max: 100, Label: "high"},
	}
	out := etl.From(baseTable()).Then(etl.Bin("score", "grade", bins)).Unwrap()
	assertEq(t, out.Rows[0].Get("grade").UnwrapOr(""), "mid")  // 80
	assertEq(t, out.Rows[1].Get("grade").UnwrapOr(""), "high") // 90
	assertEq(t, out.Rows[2].Get("grade").UnwrapOr(""), "low")  // 70
}

// ─────────────────────────────────────────────────────────────────────────────
// mutable_ops.go — Mut.* factory functions
// ─────────────────────────────────────────────────────────────────────────────

func TestCoverage_MutCompose(t *testing.T) {
	prep := etl.MutCompose(
		etl.Mut.Map("city", strings.ToUpper),
		etl.Mut.FillEmpty("score", "0"),
	)
	out := etl.FromMutable(baseMutable()).Then(prep).Frozen().Unwrap()
	assertEq(t, out.Rows[0].Get("city").UnwrapOr(""), "BERLIN")
}

func TestCoverage_MutCompose_Empty(t *testing.T) {
	noop := etl.MutCompose()
	out := etl.FromMutable(baseMutable()).Then(noop).Frozen().Unwrap()
	assertEq(t, out.Len(), 3)
}

func TestCoverage_Mut_Select(t *testing.T) {
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.Select("name")).Frozen().Unwrap()
	assertEq(t, len(out.Headers), 1)
}

func TestCoverage_Mut_Where(t *testing.T) {
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.Where(func(r table.Row) bool {
		return r.Get("city").UnwrapOr("") == "Berlin"
	})).Frozen().Unwrap()
	assertEq(t, out.Len(), 2)
}

func TestCoverage_Mut_Map(t *testing.T) {
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.Map("name", strings.ToUpper)).Frozen().Unwrap()
	assertEq(t, out.Rows[0].Get("name").UnwrapOr(""), "ALICE")
}

func TestCoverage_Mut_FillEmpty(t *testing.T) {
	m := table.NewMutable(
		[]string{"name", "region"},
		[][]string{{"Alice", ""}, {"Bob", "EU"}},
	)
	out := etl.FromMutable(m).Then(etl.Mut.FillEmpty("region", "unknown")).Frozen().Unwrap()
	assertEq(t, out.Rows[0].Get("region").UnwrapOr(""), "unknown")
}

func TestCoverage_Mut_AddCol(t *testing.T) {
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.AddCol("tag", func(r table.Row) string {
		return r.Get("name").UnwrapOr("") + "@" + r.Get("city").UnwrapOr("")
	})).Frozen().Unwrap()
	assertEq(t, out.Rows[0].Get("tag").UnwrapOr(""), "Alice@Berlin")
}

func TestCoverage_Mut_AddColFloat(t *testing.T) {
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.AddColFloat("doubled", func(r table.Row) float64 {
		v, _ := strconv.ParseFloat(r.Get("score").UnwrapOr("0"), 64)
		return v * 2
	})).Frozen().Unwrap()
	assertEq(t, out.Rows[0].Get("doubled").UnwrapOr(""), "160")
}

func TestCoverage_Mut_AddColInt(t *testing.T) {
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.AddColInt("rank", func(r table.Row) int64 {
		return 42
	})).Frozen().Unwrap()
	assertEq(t, out.Rows[0].Get("rank").UnwrapOr(""), "42")
}

func TestCoverage_Mut_Drop(t *testing.T) {
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.Drop("score")).Frozen().Unwrap()
	assertEq(t, len(out.Headers), 2)
}

func TestCoverage_Mut_DropEmpty(t *testing.T) {
	m := table.NewMutable(
		[]string{"name", "city"},
		[][]string{{"Alice", "Berlin"}, {"Bob", ""}, {"Carol", "Munich"}},
	)
	out := etl.FromMutable(m).Then(etl.Mut.DropEmpty("city")).Frozen().Unwrap()
	assertEq(t, out.Len(), 2)
}

func TestCoverage_Mut_Rename(t *testing.T) {
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.Rename("city", "location")).Frozen().Unwrap()
	assertEq(t, out.Headers[1], "location")
}

func TestCoverage_Mut_RenameMany(t *testing.T) {
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.RenameMany(map[string]string{
		"name": "person",
	})).Frozen().Unwrap()
	assertEq(t, out.Headers[0], "person")
}

func TestCoverage_Mut_Sort(t *testing.T) {
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.Sort("score", true)).Frozen().Unwrap()
	assertEq(t, out.Rows[0].Get("name").UnwrapOr(""), "Carol")
}

func TestCoverage_Mut_SortMulti(t *testing.T) {
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.SortMulti(
		table.Asc("city"), table.Desc("score"),
	)).Frozen().Unwrap()
	assertEq(t, out.Rows[0].Get("city").UnwrapOr(""), "Berlin")
}

func TestCoverage_Mut_Head(t *testing.T) {
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.Head(1)).Frozen().Unwrap()
	assertEq(t, out.Len(), 1)
}

func TestCoverage_Mut_Tail(t *testing.T) {
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.Tail(1)).Frozen().Unwrap()
	assertEq(t, out.Len(), 1)
	assertEq(t, out.Rows[0].Get("name").UnwrapOr(""), "Carol")
}

func TestCoverage_Mut_Distinct(t *testing.T) {
	m := table.NewMutable(
		[]string{"city"},
		[][]string{{"Berlin"}, {"Munich"}, {"Berlin"}},
	)
	out := etl.FromMutable(m).Then(etl.Mut.Distinct("city")).Frozen().Unwrap()
	assertEq(t, out.Len(), 2)
}

func TestCoverage_Mut_AddColSwitch(t *testing.T) {
	cases := []table.Case{
		{
			When: func(r table.Row) bool { return r.Get("city").UnwrapOr("") == "Berlin" },
			Then: func(r table.Row) string { return "capital" },
		},
	}
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.AddColSwitch("label", cases, func(r table.Row) string {
		return "other"
	})).Frozen().Unwrap()
	assertEq(t, out.Rows[0].Get("label").UnwrapOr(""), "capital")
}

func TestCoverage_Mut_Transform(t *testing.T) {
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.Transform(func(r table.Row) map[string]string {
		return map[string]string{"name": strings.ToLower(r.Get("name").UnwrapOr(""))}
	})).Frozen().Unwrap()
	assertEq(t, out.Rows[0].Get("name").UnwrapOr(""), "alice")
}

func TestCoverage_Mut_TransformParallel(t *testing.T) {
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.TransformParallel(func(r table.Row) map[string]string {
		return map[string]string{"score": r.Get("score").UnwrapOr("0") + "!"}
	})).Frozen().Unwrap()
	assertEq(t, out.Len(), 3)
}

func TestCoverage_Mut_AddRowIndex(t *testing.T) {
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.AddRowIndex("idx")).Frozen().Unwrap()
	assertEq(t, out.Rows[0].Get("idx").UnwrapOr(""), "0")
}

func TestCoverage_Mut_Explode(t *testing.T) {
	m := table.NewMutable(
		[]string{"name", "tags"},
		[][]string{{"Alice", "a,b"}, {"Bob", "c"}},
	)
	out := etl.FromMutable(m).Then(etl.Mut.Explode("tags", ",")).Frozen().Unwrap()
	assertEq(t, out.Len(), 3)
}

func TestCoverage_Mut_Transpose(t *testing.T) {
	m := table.NewMutable(
		[]string{"col1", "col2"},
		[][]string{{"a", "b"}, {"c", "d"}},
	)
	out := etl.FromMutable(m).Then(etl.Mut.Transpose()).Frozen().Unwrap()
	assertEq(t, out.Len(), 2)
}

func TestCoverage_Mut_FillForward(t *testing.T) {
	m := table.NewMutable(
		[]string{"name", "region"},
		[][]string{{"Alice", "EU"}, {"Bob", ""}, {"Carol", ""}},
	)
	out := etl.FromMutable(m).Then(etl.Mut.FillForward("region")).Frozen().Unwrap()
	assertEq(t, out.Rows[1].Get("region").UnwrapOr(""), "EU")
}

func TestCoverage_Mut_FillBackward(t *testing.T) {
	m := table.NewMutable(
		[]string{"name", "region"},
		[][]string{{"Alice", ""}, {"Bob", ""}, {"Carol", "US"}},
	)
	out := etl.FromMutable(m).Then(etl.Mut.FillBackward("region")).Frozen().Unwrap()
	assertEq(t, out.Rows[0].Get("region").UnwrapOr(""), "US")
}

func TestCoverage_Mut_Sample(t *testing.T) {
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.Sample(2)).Frozen().Unwrap()
	assertEq(t, out.Len(), 2)
}

func TestCoverage_Mut_SampleFrac(t *testing.T) {
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.SampleFrac(0.5)).Frozen().Unwrap()
	if out.Len() > 3 {
		t.Errorf("expected ≤3 rows, got %d", out.Len())
	}
}

func TestCoverage_Mut_Coalesce(t *testing.T) {
	m := table.NewMutable(
		[]string{"a", "b", "c"},
		[][]string{{"", "", "z"}, {"x", "", "z"}},
	)
	out := etl.FromMutable(m).Then(etl.Mut.Coalesce("result", "a", "b", "c")).Frozen().Unwrap()
	assertEq(t, out.Rows[0].Get("result").UnwrapOr(""), "z")
}

func TestCoverage_Mut_Lookup(t *testing.T) {
	lut := table.New(
		[]string{"code", "label"},
		[][]string{{"DE", "Germany"}},
	)
	m := table.NewMutable(
		[]string{"name", "code"},
		[][]string{{"Alice", "DE"}},
	)
	out := etl.FromMutable(m).Then(etl.Mut.Lookup("code", "country", lut, "code", "label")).Frozen().Unwrap()
	assertEq(t, out.Rows[0].Get("country").UnwrapOr(""), "Germany")
}

func TestCoverage_Mut_FormatCol(t *testing.T) {
	m := table.NewMutable(
		[]string{"val"},
		[][]string{{"3.14159"}},
	)
	out := etl.FromMutable(m).Then(etl.Mut.FormatCol("val", 2)).Frozen().Unwrap()
	assertEq(t, out.Rows[0].Get("val").UnwrapOr(""), "3.14")
}

func TestCoverage_Mut_Intersect(t *testing.T) {
	other := table.New(
		[]string{"name", "city", "score"},
		[][]string{{"Alice", "Berlin", "80"}},
	)
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.Intersect(other, "name")).Frozen().Unwrap()
	assertEq(t, out.Len(), 1)
}

func TestCoverage_Mut_Bin(t *testing.T) {
	bins := []table.BinDef{
		{Max: 75, Label: "low"},
		{Max: 100, Label: "high"},
	}
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.Bin("score", "grade", bins)).Frozen().Unwrap()
	assertEq(t, out.Rows[2].Get("grade").UnwrapOr(""), "low") // score=70
}

func TestCoverage_Mut_Join(t *testing.T) {
	lookup := table.New(
		[]string{"city", "country"},
		[][]string{{"Berlin", "DE"}, {"Munich", "DE"}},
	)
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.Join(lookup, "city", "city")).Frozen().Unwrap()
	assertEq(t, out.Len(), 3)
}

func TestCoverage_Mut_LeftJoin(t *testing.T) {
	lookup := table.New(
		[]string{"city", "country"},
		[][]string{{"Berlin", "DE"}},
	)
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.LeftJoin(lookup, "city", "city")).Frozen().Unwrap()
	assertEq(t, out.Len(), 3)
}

func TestCoverage_Mut_RightJoin(t *testing.T) {
	lookup := table.New(
		[]string{"city", "country"},
		[][]string{{"Berlin", "DE"}, {"Hamburg", "DE"}},
	)
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.RightJoin(lookup, "city", "city")).Frozen().Unwrap()
	if out.Len() < 2 {
		t.Errorf("expected ≥2 rows, got %d", out.Len())
	}
}

func TestCoverage_Mut_OuterJoin(t *testing.T) {
	lookup := table.New(
		[]string{"city", "country"},
		[][]string{{"Berlin", "DE"}, {"Hamburg", "DE"}},
	)
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.OuterJoin(lookup, "city", "city")).Frozen().Unwrap()
	if out.Len() < 3 {
		t.Errorf("expected ≥3 rows, got %d", out.Len())
	}
}

func TestCoverage_Mut_AntiJoin(t *testing.T) {
	lookup := table.New(
		[]string{"city", "country"},
		[][]string{{"Berlin", "DE"}},
	)
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.AntiJoin(lookup, "city", "city")).Frozen().Unwrap()
	assertEq(t, out.Len(), 1) // only Munich not matched
}

func TestCoverage_Mut_ValueCounts(t *testing.T) {
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.ValueCounts("city")).Frozen().Unwrap()
	assertEq(t, out.Len(), 2)
}

func TestCoverage_Mut_Melt(t *testing.T) {
	m := table.NewMutable(
		[]string{"id", "jan", "feb"},
		[][]string{{"1", "100", "200"}},
	)
	out := etl.FromMutable(m).Then(etl.Mut.Melt([]string{"id"}, "month", "value")).Frozen().Unwrap()
	assertEq(t, out.Len(), 2)
}

func TestCoverage_Mut_Pivot(t *testing.T) {
	m := table.NewMutable(
		[]string{"id", "month", "value"},
		[][]string{{"1", "jan", "100"}, {"1", "feb", "200"}},
	)
	out := etl.FromMutable(m).Then(etl.Mut.Pivot("id", "month", "value")).Frozen().Unwrap()
	assertEq(t, out.Len(), 1)
}

func TestCoverage_Mut_Append(t *testing.T) {
	other := table.New(
		[]string{"name", "city", "score"},
		[][]string{{"Dave", "Hamburg", "85"}},
	)
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.Append(other)).Frozen().Unwrap()
	assertEq(t, out.Len(), 4)
}

func TestCoverage_Mut_AppendMutable(t *testing.T) {
	other := table.NewMutable(
		[]string{"name", "city", "score"},
		[][]string{{"Dave", "Hamburg", "85"}},
	)
	out := etl.FromMutable(baseMutable()).Then(etl.Mut.AppendMutable(other)).Frozen().Unwrap()
	assertEq(t, out.Len(), 4)
}

// ─────────────────────────────────────────────────────────────────────────────
// MutablePipeline new methods: TryMap, GroupBy, Partition, Chunk, FanOut
// ─────────────────────────────────────────────────────────────────────────────

func TestCoverage_MutablePipeline_TryMap_Ok(t *testing.T) {
	mp := etl.FromMutable(baseMutable()).TryMap("score", func(v string) (string, error) {
		return v + "pts", nil
	})
	out := mp.Frozen().Unwrap()
	assertEq(t, out.Rows[0].Get("score").UnwrapOr(""), "80pts")
}

func TestCoverage_MutablePipeline_TryMap_WithErrors(t *testing.T) {
	m := table.NewMutable(
		[]string{"val"},
		[][]string{{"good"}, {"bad"}, {"good"}},
	)
	mp := etl.FromMutable(m).TryMap("val", func(v string) (string, error) {
		if v == "bad" {
			return "", errors.New("nope")
		}
		return strings.ToUpper(v), nil
	})
	assertEq(t, mp.IsOk(), true)
}

func TestCoverage_MutablePipeline_GroupBy_Ok(t *testing.T) {
	groups := etl.FromMutable(baseMutable()).GroupBy("city")
	assertEq(t, len(groups), 2)
	assertEq(t, groups["Berlin"].IsOk(), true)
	assertEq(t, groups["Berlin"].Unwrap().Len(), 2)
	assertEq(t, groups["Munich"].Unwrap().Len(), 1)
}

func TestCoverage_MutablePipeline_GroupBy_ErrorForwarded(t *testing.T) {
	sentinel := errors.New("boom")
	mp := etl.FromMutable(baseMutable()).ThenErr(func(m *table.MutableTable) result.Result[*table.MutableTable, error] {
		return result.Err[*table.MutableTable, error](sentinel)
	})
	groups := mp.GroupBy("city")
	assertEq(t, len(groups), 1)
	assertEq(t, groups[""].IsErr(), true)
}

func TestCoverage_MutablePipeline_Partition_Ok(t *testing.T) {
	matched, rest := etl.FromMutable(baseMutable()).Partition(func(r table.Row) bool {
		return r.Get("city").UnwrapOr("") == "Berlin"
	})
	assertEq(t, matched.IsOk(), true)
	assertEq(t, matched.Unwrap().Len(), 2)
	assertEq(t, rest.Unwrap().Len(), 1)
}

func TestCoverage_MutablePipeline_Partition_ErrorForwarded(t *testing.T) {
	sentinel := errors.New("boom")
	mp := etl.FromMutable(baseMutable()).ThenErr(func(m *table.MutableTable) result.Result[*table.MutableTable, error] {
		return result.Err[*table.MutableTable, error](sentinel)
	})
	matched, rest := mp.Partition(func(r table.Row) bool { return true })
	assertEq(t, matched.IsErr(), true)
	assertEq(t, rest.IsErr(), true)
}

func TestCoverage_MutablePipeline_Chunk_Ok(t *testing.T) {
	chunks := etl.FromMutable(baseMutable()).Chunk(2)
	assertEq(t, len(chunks), 2)
	assertEq(t, chunks[0].Unwrap().Len(), 2)
	assertEq(t, chunks[1].Unwrap().Len(), 1)
}

func TestCoverage_MutablePipeline_Chunk_ErrorForwarded(t *testing.T) {
	sentinel := errors.New("boom")
	mp := etl.FromMutable(baseMutable()).ThenErr(func(m *table.MutableTable) result.Result[*table.MutableTable, error] {
		return result.Err[*table.MutableTable, error](sentinel)
	})
	chunks := mp.Chunk(2)
	assertEq(t, len(chunks), 1)
	assertEq(t, chunks[0].IsErr(), true)
}

func TestCoverage_MutablePipeline_FanOut_Ok(t *testing.T) {
	branches := etl.FromMutable(baseMutable()).FanOut(
		func(m *table.MutableTable) *table.MutableTable {
			return m.Where(func(r table.Row) bool { return r.Get("city").UnwrapOr("") == "Berlin" })
		},
		func(m *table.MutableTable) *table.MutableTable {
			return m.Where(func(r table.Row) bool { return r.Get("city").UnwrapOr("") == "Munich" })
		},
	)
	assertEq(t, len(branches), 2)
	assertEq(t, branches[0].IsOk(), true)
	assertEq(t, branches[1].IsOk(), true)
	assertEq(t, branches[0].Frozen().Unwrap().Len(), 2)
	assertEq(t, branches[1].Frozen().Unwrap().Len(), 1)
}

func TestCoverage_MutablePipeline_FanOut_ErrorForwarded(t *testing.T) {
	sentinel := errors.New("boom")
	mp := etl.FromMutable(baseMutable()).ThenErr(func(m *table.MutableTable) result.Result[*table.MutableTable, error] {
		return result.Err[*table.MutableTable, error](sentinel)
	})
	branches := mp.FanOut(
		func(m *table.MutableTable) *table.MutableTable { return m },
		func(m *table.MutableTable) *table.MutableTable { return m },
	)
	for _, b := range branches {
		assertEq(t, b.IsErr(), true)
	}
}

func TestCoverage_MutablePipeline_FanOut_PreservesOrder(t *testing.T) {
	branches := etl.FromMutable(baseMutable()).FanOut(
		func(m *table.MutableTable) *table.MutableTable { return m },
		func(m *table.MutableTable) *table.MutableTable { return m },
		func(m *table.MutableTable) *table.MutableTable { return m },
	)
	assertEq(t, len(branches), 3)
	for _, b := range branches {
		assertEq(t, b.IsOk(), true)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// MutablePipeline — partially covered methods
// ─────────────────────────────────────────────────────────────────────────────

func TestCoverage_MutablePipeline_IfThenErr_Applied(t *testing.T) {
	sentinel := errors.New("injected")
	mp := etl.FromMutable(baseMutable()).IfThenErr(true, func(m *table.MutableTable) result.Result[*table.MutableTable, error] {
		return result.Err[*table.MutableTable, error](sentinel)
	})
	assertEq(t, mp.IsErr(), true)
	assertEq(t, errors.Is(mp.Result().UnwrapErr(), sentinel), true)
}

func TestCoverage_MutablePipeline_IfThenErr_Skipped(t *testing.T) {
	mp := etl.FromMutable(baseMutable()).IfThenErr(false, func(m *table.MutableTable) result.Result[*table.MutableTable, error] {
		return result.Err[*table.MutableTable, error](errors.New("must not be called"))
	})
	assertEq(t, mp.IsOk(), true)
}

func TestCoverage_MutablePipeline_RecoverWith_OkPipeline(t *testing.T) {
	fallback := table.NewMutable([]string{"x"}, [][]string{{"fallback"}})
	mp := etl.FromMutable(baseMutable()).RecoverWith(fallback)
	assertEq(t, mp.IsOk(), true)
	assertEq(t, mp.Unwrap().Len(), 3) // original table, not fallback
}

func TestCoverage_MutablePipeline_OnError_OkPipeline(t *testing.T) {
	called := false
	mp := etl.FromMutable(baseMutable()).OnError(func(err error) (*table.MutableTable, error) {
		called = true
		return nil, err
	})
	assertEq(t, mp.IsOk(), true)
	assertEq(t, called, false)
}

func TestCoverage_MutablePipeline_OnError_Recovers(t *testing.T) {
	sentinel := errors.New("boom")
	fallback := table.NewMutable([]string{"x"}, [][]string{{"recovered"}})
	mp := etl.FromMutable(baseMutable()).
		ThenErr(func(m *table.MutableTable) result.Result[*table.MutableTable, error] {
			return result.Err[*table.MutableTable, error](sentinel)
		}).
		OnError(func(err error) (*table.MutableTable, error) {
			return fallback, nil
		})
	assertEq(t, mp.IsOk(), true)
	row, _ := mp.Unwrap().Row(0)
	assertEq(t, row.Get("x").UnwrapOr(""), "recovered")
}

func TestCoverage_MutablePipeline_OnError_ReplacesError(t *testing.T) {
	orig := errors.New("original")
	replaced := errors.New("replaced")
	mp := etl.FromMutable(baseMutable()).
		ThenErr(func(m *table.MutableTable) result.Result[*table.MutableTable, error] {
			return result.Err[*table.MutableTable, error](orig)
		}).
		OnError(func(err error) (*table.MutableTable, error) {
			return nil, replaced
		})
	assertEq(t, mp.IsErr(), true)
	assertEq(t, errors.Is(mp.Result().UnwrapErr(), replaced), true)
}

func TestCoverage_MutablePipeline_Trace_Nil(t *testing.T) {
	mp := etl.FromMutable(baseMutable())
	assertEq(t, mp.Trace() == nil, true)
}

func TestCoverage_MutablePipeline_Step_NoTrace(t *testing.T) {
	// Step without WithTracing should still apply the fn
	mp := etl.FromMutable(baseMutable()).Step("noop", func(m *table.MutableTable) *table.MutableTable {
		return m.Map("city", strings.ToUpper)
	})
	assertEq(t, mp.IsOk(), true)
	out := mp.Frozen().Unwrap()
	assertEq(t, out.Rows[0].Get("city").UnwrapOr(""), "BERLIN")
}

// ─────────────────────────────────────────────────────────────────────────────
// pipeline.go — partially covered methods
// ─────────────────────────────────────────────────────────────────────────────

func TestCoverage_Pipeline_TryTransform_Strict(t *testing.T) {
	t2 := table.New(
		[]string{"id", "val"},
		[][]string{{"1", "ok"}, {"2", "bad"}, {"3", "ok"}},
	)
	p := etl.From(t2).TryTransform(func(r table.Row) (map[string]string, error) {
		v := r.Get("val").UnwrapOr("")
		if v == "bad" {
			return nil, fmt.Errorf("bad row")
		}
		return map[string]string{"val": strings.ToUpper(v)}, nil
	})
	assertEq(t, p.IsErr(), true) // strict mode short-circuits on first error
}

func TestCoverage_Pipeline_ApplySchema(t *testing.T) {
	t2 := table.New(
		[]string{"name", "score"},
		[][]string{{"Alice", "90"}, {"Bob", "85"}},
	)
	s := schema.Infer(t2)
	p := etl.From(t2).ApplySchema(s)
	assertEq(t, p.IsOk(), true)
	assertEq(t, p.Unwrap().Len(), 2)
}

func TestCoverage_Pipeline_ApplySchemaStrict(t *testing.T) {
	t2 := table.New(
		[]string{"name", "score"},
		[][]string{{"Alice", "90"}, {"Bob", "85"}},
	)
	s := schema.Infer(t2)
	p := etl.From(t2).ApplySchemaStrict(s)
	assertEq(t, p.IsOk(), true)
}

func TestCoverage_Pipeline_Partition_Ok(t *testing.T) {
	matched, rest := etl.From(baseTable()).Partition(func(r table.Row) bool {
		return r.Get("city").UnwrapOr("") == "Berlin"
	})
	assertEq(t, matched.IsOk(), true)
	assertEq(t, matched.Unwrap().Len(), 2)
	assertEq(t, rest.Unwrap().Len(), 1)
}

func TestCoverage_Pipeline_Partition_Error(t *testing.T) {
	errP := etl.From(baseTable()).AssertColumns("nonexistent")
	matched, rest := errP.Partition(func(r table.Row) bool { return true })
	assertEq(t, matched.IsErr(), true)
	assertEq(t, rest.IsErr(), true)
}

func TestCoverage_Pipeline_Chunk_Ok(t *testing.T) {
	chunks := etl.From(baseTable()).Chunk(2)
	assertEq(t, len(chunks), 2)
	assertEq(t, chunks[0].Unwrap().Len(), 2)
	assertEq(t, chunks[1].Unwrap().Len(), 1)
}

func TestCoverage_Pipeline_Chunk_Error(t *testing.T) {
	errP := etl.From(baseTable()).AssertColumns("nonexistent")
	chunks := errP.Chunk(2)
	assertEq(t, len(chunks), 1)
	assertEq(t, chunks[0].IsErr(), true)
}

// ─────────────────────────────────────────────────────────────────────────────
// error_log.go — ErrorLog.Len
// ─────────────────────────────────────────────────────────────────────────────

func TestCoverage_ErrorLog_Len_Empty(t *testing.T) {
	log := etl.NewErrorLog()
	assertEq(t, log.Len(), 0)
}

func TestCoverage_ErrorLog_Len_WithErrors(t *testing.T) {
	log := etl.NewErrorLog()
	t2 := table.New(
		[]string{"val"},
		[][]string{{"good"}, {"bad"}, {"good"}},
	)
	_ = etl.From(t2).WithErrorLog(log).TryMap("val", func(v string) (string, error) {
		if v == "bad" {
			return "", errors.New("nope")
		}
		return v, nil
	})
	assertEq(t, log.Len(), 1)
}

func TestCoverage_ErrorLog_Entries_Empty(t *testing.T) {
	log := etl.NewErrorLog()
	entries := log.Entries()
	assertEq(t, len(entries), 0)
}

func TestCoverage_ErrorLog_ToTable_Empty(t *testing.T) {
	log := etl.NewErrorLog()
	tbl := log.ToTable()
	assertEq(t, tbl.Len(), 0)
}
