package table_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stefanbethge/gseq-table/table"
)

// --- helpers ---

func newTable(headers []string, rows [][]string) table.Table {
	return table.New(headers, rows)
}

func check(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func checkInt(t *testing.T, got, want int) {
	t.Helper()
	if got != want {
		t.Errorf("got %d, want %d", got, want)
	}
}

// ============================================================
// Row.Headers
// ============================================================

func TestCoverage_RowHeaders(t *testing.T) {
	row := table.NewRow([]string{"a", "b"}, []string{"1", "2"})
	h := row.Headers()
	if len(h) != 2 || h[0] != "a" || h[1] != "b" {
		t.Errorf("unexpected headers: %v", h)
	}
}

// ============================================================
// Table.Source / Table.WithSource / Table.Freeze
// ============================================================

func TestCoverage_TableSource(t *testing.T) {
	tb := newTable([]string{"x"}, [][]string{{"1"}}).WithSource("test.csv")
	check(t, tb.Source(), "test.csv")
}

func TestCoverage_TableFreeze(t *testing.T) {
	tb := newTable([]string{"x"}, [][]string{{"1"}})
	frozen := tb.Freeze()
	checkInt(t, frozen.Len(), 1)
}

func TestCoverage_CopyErrsFrom_NoErrs(t *testing.T) {
	src := newTable([]string{"x"}, [][]string{{"1"}}).WithSource("file.csv")
	dst := newTable([]string{"a"}, [][]string{{"1"}})
	result := dst.CopyErrsFrom(src)
	check(t, result.Source(), "file.csv")
	if result.HasErrs() {
		t.Error("expected no errors")
	}
}

// ============================================================
// Table.Desc / Table.SortMulti with Desc
// ============================================================

func TestCoverage_Desc(t *testing.T) {
	tb := newTable([]string{"val"}, [][]string{{"1"}, {"3"}, {"2"}})
	sorted := tb.SortMulti(table.Desc("val"))
	check(t, sorted.Rows[0].Get("val").UnwrapOr(""), "3")
	check(t, sorted.Rows[2].Get("val").UnwrapOr(""), "1")
}

// ============================================================
// Table.Errs
// ============================================================

func TestCoverage_TableErrs(t *testing.T) {
	tb := newTable([]string{"x"}, nil).Select("missing")
	errs := tb.Errs()
	if len(errs) == 0 {
		t.Fatal("expected errors")
	}
}

// ============================================================
// RollingAgg with Count / StringJoin / First / Last
// (covers the agg.reduce methods at 0%)
// ============================================================

func TestCoverage_RollingAgg_Count(t *testing.T) {
	tb := newTable([]string{"v"}, [][]string{{"a"}, {"b"}, {""}})
	result := tb.RollingAgg("cnt", 2, table.Count("v"))
	check(t, result.Rows[0].Get("cnt").UnwrapOr(""), "1")
	check(t, result.Rows[1].Get("cnt").UnwrapOr(""), "2")
	check(t, result.Rows[2].Get("cnt").UnwrapOr(""), "1") // last is empty, only "b" counted
}

func TestCoverage_RollingAgg_StringJoin(t *testing.T) {
	tb := newTable([]string{"v"}, [][]string{{"a"}, {"b"}, {"c"}})
	result := tb.RollingAgg("joined", 2, table.StringJoin("v", "|"))
	check(t, result.Rows[0].Get("joined").UnwrapOr(""), "a")
	check(t, result.Rows[1].Get("joined").UnwrapOr(""), "a|b")
	check(t, result.Rows[2].Get("joined").UnwrapOr(""), "b|c")
}

func TestCoverage_RollingAgg_First(t *testing.T) {
	tb := newTable([]string{"v"}, [][]string{{"x"}, {"y"}, {"z"}})
	result := tb.RollingAgg("first", 3, table.First("v"))
	check(t, result.Rows[2].Get("first").UnwrapOr(""), "x")
}

func TestCoverage_RollingAgg_Last(t *testing.T) {
	tb := newTable([]string{"v"}, [][]string{{"x"}, {"y"}, {"z"}})
	result := tb.RollingAgg("last", 3, table.Last("v"))
	check(t, result.Rows[2].Get("last").UnwrapOr(""), "z")
}

// ============================================================
// Intersect with 3+ columns (covers keyFromValues/keyFromRowValues)
// ============================================================

func TestCoverage_Intersect_ThreeCols(t *testing.T) {
	a := newTable([]string{"a", "b", "c"}, [][]string{
		{"1", "x", "p"},
		{"2", "y", "q"},
		{"3", "z", "r"},
	})
	b := newTable([]string{"a", "b", "c"}, [][]string{
		{"1", "x", "p"},
		{"3", "z", "s"}, // different "c"
	})
	result := a.Intersect(b, "a", "b", "c")
	checkInt(t, len(result.Rows), 1)
	check(t, result.Rows[0].Get("a").UnwrapOr(""), "1")
}

// ============================================================
// GroupByAgg with 3+ group columns (covers keyFromRowValues default case)
// ============================================================

func TestCoverage_GroupByAgg_ThreeGroupCols(t *testing.T) {
	tb := newTable([]string{"a", "b", "c", "val"}, [][]string{
		{"x", "1", "p", "10"},
		{"x", "1", "p", "20"},
		{"y", "2", "q", "30"},
	})
	result := tb.GroupByAgg([]string{"a", "b", "c"}, []table.AggDef{
		{Col: "total", Agg: table.Sum("val")},
	})
	checkInt(t, len(result.Rows), 2)
	check(t, result.Rows[0].Get("total").UnwrapOr(""), "30")
}

// ============================================================
// Join with 3+ matching rows (covers rowBucket multi-match path)
// ============================================================

func TestCoverage_Join_ThreeMatchingRows(t *testing.T) {
	left := newTable([]string{"id", "name"}, [][]string{
		{"1", "a"},
		{"1", "b"},
		{"1", "c"},
	})
	right := newTable([]string{"id", "val"}, [][]string{{"1", "X"}})
	result := left.Join(right, "id", "id")
	checkInt(t, len(result.Rows), 3)
	check(t, result.Rows[0].Get("val").UnwrapOr(""), "X")
	check(t, result.Rows[2].Get("val").UnwrapOr(""), "X")
}

// ============================================================
// Rank with float values (covers denseRankValues float path)
// ============================================================

func TestCoverage_Rank_FloatValues(t *testing.T) {
	tb := newTable([]string{"score"}, [][]string{{"1.5"}, {"3.5"}, {"2.5"}})
	result := tb.Rank("score", "rank", true)
	check(t, result.Rows[0].Get("rank").UnwrapOr(""), "1")
	check(t, result.Rows[1].Get("rank").UnwrapOr(""), "3")
	check(t, result.Rows[2].Get("rank").UnwrapOr(""), "2")
}

func TestCoverage_Rank_FloatDesc(t *testing.T) {
	tb := newTable([]string{"score"}, [][]string{{"1.5"}, {"3.5"}, {"2.5"}})
	result := tb.Rank("score", "rank", false)
	check(t, result.Rows[0].Get("rank").UnwrapOr(""), "3")
	check(t, result.Rows[1].Get("rank").UnwrapOr(""), "1")
	check(t, result.Rows[2].Get("rank").UnwrapOr(""), "2")
}

// ============================================================
// MutableTable.Errs / WithSource / Source
// ============================================================

func TestCoverage_Mutable_Errs(t *testing.T) {
	m := table.NewMutable([]string{"x"}, nil)
	m.Select("nonexistent")
	errs := m.Errs()
	if len(errs) == 0 {
		t.Fatal("expected errors on mutable table")
	}
}

func TestCoverage_Mutable_WithSource_Source(t *testing.T) {
	m := table.NewMutable([]string{"x"}, nil)
	m.WithSource("data.csv")
	check(t, m.Source(), "data.csv")
}

// ============================================================
// MutableTable.Headers
// ============================================================

func TestCoverage_Mutable_Headers(t *testing.T) {
	m := table.NewMutable([]string{"a", "b", "c"}, nil)
	h := m.Headers()
	if len(h) != 3 || h[0] != "a" {
		t.Errorf("unexpected headers: %v", h)
	}
}

// ============================================================
// MutableTable.Row
// ============================================================

func TestCoverage_Mutable_Row(t *testing.T) {
	m := table.NewMutable([]string{"name", "age"}, [][]string{{"Alice", "30"}, {"Bob", "25"}})
	row, ok := m.Row(0)
	if !ok {
		t.Fatal("Row(0) should be ok")
	}
	check(t, row.Get("name").UnwrapOr(""), "Alice")

	_, ok = m.Row(99)
	if ok {
		t.Error("Row(99) should not be ok")
	}
}

// ============================================================
// MutableTable.Table (alias for Freeze)
// ============================================================

func TestCoverage_Mutable_Table(t *testing.T) {
	m := table.NewMutable([]string{"x"}, [][]string{{"hello"}})
	tb := m.Table()
	checkInt(t, tb.Len(), 1)
	check(t, tb.Rows[0].Get("x").UnwrapOr(""), "hello")
}

// ============================================================
// MutableTable.Col
// ============================================================

func TestCoverage_Mutable_Col(t *testing.T) {
	m := table.NewMutable([]string{"id", "name"}, [][]string{{"1", "Alice"}, {"2", "Bob"}})
	ids := m.Col("id")
	if len(ids) != 2 || ids[0] != "1" || ids[1] != "2" {
		t.Errorf("unexpected col values: %v", ids)
	}
	missing := m.Col("nonexistent")
	checkInt(t, len(missing), 0)
}

// ============================================================
// MutableTable.SortMulti
// ============================================================

func TestCoverage_Mutable_SortMulti(t *testing.T) {
	m := table.NewMutable([]string{"city", "name"}, [][]string{
		{"Berlin", "Carol"},
		{"Munich", "Bob"},
		{"Berlin", "Alice"},
	})
	m.SortMulti(table.Asc("city"), table.Asc("name"))
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("name").UnwrapOr(""), "Alice")
	check(t, tb.Rows[1].Get("name").UnwrapOr(""), "Carol")
	check(t, tb.Rows[2].Get("name").UnwrapOr(""), "Bob")
}

// ============================================================
// MutableTable.AppendMutable
// ============================================================

func TestCoverage_Mutable_AppendMutable(t *testing.T) {
	a := table.NewMutable([]string{"name"}, [][]string{{"Alice"}})
	b := table.NewMutable([]string{"name"}, [][]string{{"Bob"}, {"Carol"}})
	a.AppendMutable(b)
	checkInt(t, a.Len(), 3)
	tb := a.Freeze()
	check(t, tb.Rows[2].Get("name").UnwrapOr(""), "Carol")
}

// ============================================================
// MutableTable.Head / Tail
// ============================================================

func TestCoverage_Mutable_Head(t *testing.T) {
	m := table.NewMutable([]string{"v"}, [][]string{{"1"}, {"2"}, {"3"}, {"4"}})
	m.Head(2)
	checkInt(t, m.Len(), 2)
}

func TestCoverage_Mutable_Head_Zero(t *testing.T) {
	m := table.NewMutable([]string{"v"}, [][]string{{"1"}, {"2"}})
	m.Head(0)
	checkInt(t, m.Len(), 0)
}

func TestCoverage_Mutable_Tail(t *testing.T) {
	m := table.NewMutable([]string{"v"}, [][]string{{"1"}, {"2"}, {"3"}})
	m.Tail(1)
	checkInt(t, m.Len(), 1)
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("v").UnwrapOr(""), "3")
}

func TestCoverage_Mutable_Tail_Zero(t *testing.T) {
	m := table.NewMutable([]string{"v"}, [][]string{{"1"}, {"2"}})
	m.Tail(0)
	checkInt(t, m.Len(), 0)
}

// ============================================================
// MutableTable.DropEmpty
// ============================================================

func TestCoverage_Mutable_DropEmpty(t *testing.T) {
	m := table.NewMutable([]string{"a", "b"}, [][]string{
		{"1", "x"},
		{"", "y"},
		{"3", ""},
	})
	m.DropEmpty("a")
	checkInt(t, m.Len(), 2)
}

func TestCoverage_Mutable_DropEmpty_AllCols(t *testing.T) {
	m := table.NewMutable([]string{"a", "b"}, [][]string{
		{"1", "x"},
		{"", "y"},
		{"3", ""},
	})
	m.DropEmpty()
	checkInt(t, m.Len(), 1)
}

// ============================================================
// MutableTable.Distinct
// ============================================================

func TestCoverage_Mutable_Distinct_SingleCol(t *testing.T) {
	m := table.NewMutable([]string{"city"}, [][]string{{"Berlin"}, {"Munich"}, {"Berlin"}})
	m.Distinct("city")
	checkInt(t, m.Len(), 2)
}

func TestCoverage_Mutable_Distinct_TwoCols(t *testing.T) {
	m := table.NewMutable([]string{"a", "b"}, [][]string{
		{"1", "x"},
		{"1", "x"},
		{"1", "y"},
	})
	m.Distinct("a", "b")
	checkInt(t, m.Len(), 2)
}

func TestCoverage_Mutable_Distinct_ThreeCols(t *testing.T) {
	m := table.NewMutable([]string{"a", "b", "c"}, [][]string{
		{"1", "x", "p"},
		{"1", "x", "p"},
		{"1", "x", "q"},
	})
	m.Distinct("a", "b", "c")
	checkInt(t, m.Len(), 2)
}

// ============================================================
// MutableTable.AddColSwitch
// ============================================================

func TestCoverage_Mutable_AddColSwitch(t *testing.T) {
	m := table.NewMutable([]string{"status"}, [][]string{{"active"}, {"inactive"}, {"other"}})
	m.AddColSwitch("label",
		[]table.Case{
			{When: m.Eq("status", "active"), Then: func(table.Row) string { return "yes" }},
			{When: m.Eq("status", "inactive"), Then: func(table.Row) string { return "no" }},
		},
		func(table.Row) string { return "unknown" },
	)
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("label").UnwrapOr(""), "yes")
	check(t, tb.Rows[1].Get("label").UnwrapOr(""), "no")
	check(t, tb.Rows[2].Get("label").UnwrapOr(""), "unknown")
}

// ============================================================
// MutableTable.Transform
// ============================================================

func TestCoverage_Mutable_Transform(t *testing.T) {
	m := table.NewMutable([]string{"name"}, [][]string{{"alice"}, {"bob"}})
	m.Transform(func(r table.Row) map[string]string {
		return map[string]string{"name": strings.ToUpper(r.Get("name").UnwrapOr(""))}
	})
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("name").UnwrapOr(""), "ALICE")
	check(t, tb.Rows[1].Get("name").UnwrapOr(""), "BOB")
}

// ============================================================
// MutableTable.Explode
// ============================================================

func TestCoverage_Mutable_Explode(t *testing.T) {
	m := table.NewMutable([]string{"tags"}, [][]string{{"go,etl"}, {"data"}})
	m.Explode("tags", ",")
	checkInt(t, m.Len(), 3)
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("tags").UnwrapOr(""), "go")
	check(t, tb.Rows[1].Get("tags").UnwrapOr(""), "etl")
}

// ============================================================
// MutableTable.Transpose
// ============================================================

func TestCoverage_Mutable_Transpose(t *testing.T) {
	m := table.NewMutable([]string{"name", "age"}, [][]string{
		{"Alice", "30"},
		{"Bob", "25"},
	})
	m.Transpose()
	tb := m.Freeze()
	checkInt(t, len(tb.Rows), 2) // one row per original column
	check(t, tb.Rows[0].Get("column").UnwrapOr(""), "name")
	check(t, tb.Rows[0].Get("0").UnwrapOr(""), "Alice")
}

// ============================================================
// MutableTable.FillBackward
// ============================================================

func TestCoverage_Mutable_FillBackward(t *testing.T) {
	m := table.NewMutable([]string{"region"}, [][]string{{""}, {""}, {"EU"}, {""}, {"US"}})
	m.FillBackward("region")
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("region").UnwrapOr(""), "EU")
	check(t, tb.Rows[3].Get("region").UnwrapOr(""), "US")
}

// ============================================================
// MutableTable.Sample / SampleFrac
// ============================================================

func TestCoverage_Mutable_Sample(t *testing.T) {
	m := table.NewMutable([]string{"v"}, [][]string{{"1"}, {"2"}, {"3"}, {"4"}, {"5"}})
	m.Sample(3)
	checkInt(t, m.Len(), 3)
}

func TestCoverage_Mutable_Sample_LargerThanLen(t *testing.T) {
	m := table.NewMutable([]string{"v"}, [][]string{{"1"}, {"2"}})
	m.Sample(100)
	checkInt(t, m.Len(), 2)
}

func TestCoverage_Mutable_SampleFrac(t *testing.T) {
	m := table.NewMutable([]string{"v"}, func() [][]string {
		rows := make([][]string, 100)
		for i := range rows {
			rows[i] = []string{strconv.Itoa(i)}
		}
		return rows
	}())
	m.SampleFrac(0.1)
	checkInt(t, m.Len(), 10)
}

// ============================================================
// MutableTable.AddColFloat / AddColInt
// ============================================================

func TestCoverage_Mutable_AddColFloat(t *testing.T) {
	m := table.NewMutable([]string{"x"}, [][]string{{"3"}, {"5"}})
	m.AddColFloat("half", func(r table.Row) float64 {
		v, _ := strconv.ParseFloat(r.Get("x").UnwrapOr("0"), 64)
		return v / 2.0
	})
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("half").UnwrapOr(""), "1.5")
	check(t, tb.Rows[1].Get("half").UnwrapOr(""), "2.5")
}

func TestCoverage_Mutable_AddColInt(t *testing.T) {
	m := table.NewMutable([]string{"year_month"}, [][]string{{"202301"}, {"202406"}})
	m.AddColInt("year", func(r table.Row) int64 {
		v, _ := strconv.ParseInt(r.Get("year_month").UnwrapOr("0"), 10, 64)
		return v / 100
	})
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("year").UnwrapOr(""), "2023")
	check(t, tb.Rows[1].Get("year").UnwrapOr(""), "2024")
}

// ============================================================
// MutableTable.Coalesce
// ============================================================

func TestCoverage_Mutable_Coalesce(t *testing.T) {
	m := table.NewMutable([]string{"a", "b", "c"}, [][]string{
		{"", "", "fallback"},
		{"first", "second", "third"},
	})
	m.Coalesce("result", "a", "b", "c")
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("result").UnwrapOr(""), "fallback")
	check(t, tb.Rows[1].Get("result").UnwrapOr(""), "first")
}

// ============================================================
// MutableTable.Lookup
// ============================================================

func TestCoverage_Mutable_Lookup(t *testing.T) {
	m := table.NewMutable([]string{"code"}, [][]string{{"A"}, {"B"}, {"Z"}})
	lookup := newTable([]string{"code", "name"}, [][]string{{"A", "Alpha"}, {"B", "Beta"}})
	m.Lookup("code", "name", lookup, "code", "name")
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("name").UnwrapOr(""), "Alpha")
	check(t, tb.Rows[1].Get("name").UnwrapOr(""), "Beta")
	check(t, tb.Rows[2].Get("name").UnwrapOr("x"), "")
}

// ============================================================
// MutableTable.FormatCol
// ============================================================

func TestCoverage_Mutable_FormatCol(t *testing.T) {
	m := table.NewMutable([]string{"price"}, [][]string{{"3.14159"}, {"abc"}})
	m.FormatCol("price", 2)
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("price").UnwrapOr(""), "3.14")
	check(t, tb.Rows[1].Get("price").UnwrapOr(""), "abc")
}

// ============================================================
// MutableTable.Intersect
// ============================================================

func TestCoverage_Mutable_Intersect_SingleCol(t *testing.T) {
	m := table.NewMutable([]string{"id"}, [][]string{{"1"}, {"2"}, {"3"}})
	other := newTable([]string{"id"}, [][]string{{"2"}, {"3"}, {"4"}})
	m.Intersect(other, "id")
	checkInt(t, m.Len(), 2)
}

func TestCoverage_Mutable_Intersect_TwoCols(t *testing.T) {
	m := table.NewMutable([]string{"a", "b"}, [][]string{{"1", "x"}, {"2", "y"}, {"1", "z"}})
	other := newTable([]string{"a", "b"}, [][]string{{"1", "x"}, {"2", "y"}})
	m.Intersect(other, "a", "b")
	checkInt(t, m.Len(), 2)
}

func TestCoverage_Mutable_Intersect_ThreeCols(t *testing.T) {
	m := table.NewMutable([]string{"a", "b", "c"}, [][]string{
		{"1", "x", "p"},
		{"2", "y", "q"},
		{"3", "z", "r"},
	})
	other := newTable([]string{"a", "b", "c"}, [][]string{{"1", "x", "p"}, {"3", "z", "r"}})
	m.Intersect(other, "a", "b", "c")
	checkInt(t, m.Len(), 2)
}

// ============================================================
// MutableTable.Bin
// ============================================================

func TestCoverage_Mutable_Bin(t *testing.T) {
	m := table.NewMutable([]string{"age"}, [][]string{{"15"}, {"40"}, {"70"}, {"abc"}})
	m.Bin("age", "group", []table.BinDef{
		{Max: 18, Label: "minor"},
		{Max: 65, Label: "adult"},
		{Max: 999, Label: "senior"},
	})
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("group").UnwrapOr(""), "minor")
	check(t, tb.Rows[1].Get("group").UnwrapOr(""), "adult")
	check(t, tb.Rows[2].Get("group").UnwrapOr(""), "senior")
	check(t, tb.Rows[3].Get("group").UnwrapOr("x"), "")
}

// ============================================================
// MutableTable.LeftJoin
// ============================================================

func TestCoverage_Mutable_LeftJoin(t *testing.T) {
	m := table.NewMutable([]string{"name", "city"}, [][]string{
		{"Alice", "Berlin"},
		{"Bob", "Hamburg"},
	})
	right := newTable([]string{"city", "country"}, [][]string{{"Berlin", "DE"}})
	m.LeftJoin(right, "city", "city")
	tb := m.Freeze()
	checkInt(t, len(tb.Rows), 2)
	check(t, tb.Rows[0].Get("country").UnwrapOr(""), "DE")
	check(t, tb.Rows[1].Get("country").UnwrapOr(""), "")
}

// ============================================================
// MutableTable.RightJoin
// ============================================================

func TestCoverage_Mutable_RightJoin(t *testing.T) {
	m := table.NewMutable([]string{"id", "val"}, [][]string{{"1", "A"}, {"2", "B"}})
	right := newTable([]string{"id", "info"}, [][]string{{"1", "X"}, {"3", "Z"}})
	m.RightJoin(right, "id", "id")
	tb := m.Freeze()
	checkInt(t, len(tb.Rows), 2)
}

// ============================================================
// MutableTable.OuterJoin
// ============================================================

func TestCoverage_Mutable_OuterJoin(t *testing.T) {
	m := table.NewMutable([]string{"id", "left_val"}, [][]string{{"1", "A"}, {"2", "B"}})
	right := newTable([]string{"id", "right_val"}, [][]string{{"1", "X"}, {"3", "Z"}})
	m.OuterJoin(right, "id", "id")
	tb := m.Freeze()
	checkInt(t, len(tb.Rows), 3) // 1 matched + 1 left-only + 1 right-only
}

// ============================================================
// MutableTable.AntiJoin
// ============================================================

func TestCoverage_Mutable_AntiJoin(t *testing.T) {
	m := table.NewMutable([]string{"id"}, [][]string{{"1"}, {"2"}, {"3"}})
	right := newTable([]string{"id"}, [][]string{{"2"}})
	m.AntiJoin(right, "id", "id")
	checkInt(t, m.Len(), 2)
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("id").UnwrapOr(""), "1")
	check(t, tb.Rows[1].Get("id").UnwrapOr(""), "3")
}

// ============================================================
// MutableTable.Melt
// ============================================================

func TestCoverage_Mutable_Melt(t *testing.T) {
	m := table.NewMutable([]string{"name", "q1", "q2"}, [][]string{
		{"Alice", "100", "200"},
	})
	m.Melt([]string{"name"}, "quarter", "value")
	checkInt(t, m.Len(), 2)
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("quarter").UnwrapOr(""), "q1")
	check(t, tb.Rows[0].Get("value").UnwrapOr(""), "100")
}

// ============================================================
// MutableTable.Pivot
// ============================================================

func TestCoverage_Mutable_Pivot(t *testing.T) {
	m := table.NewMutable([]string{"name", "quarter", "revenue"}, [][]string{
		{"Alice", "q1", "100"},
		{"Alice", "q2", "200"},
	})
	m.Pivot("name", "quarter", "revenue")
	tb := m.Freeze()
	checkInt(t, len(tb.Rows), 1)
	check(t, tb.Rows[0].Get("q1").UnwrapOr(""), "100")
	check(t, tb.Rows[0].Get("q2").UnwrapOr(""), "200")
}

// ============================================================
// MutableTable.GroupByAgg
// ============================================================

func TestCoverage_Mutable_GroupByAgg_SingleGroup(t *testing.T) {
	m := table.NewMutable([]string{"region", "revenue"}, [][]string{
		{"EU", "100"},
		{"EU", "200"},
		{"US", "300"},
	})
	m.GroupByAgg([]string{"region"}, []table.AggDef{
		{Col: "total", Agg: table.Sum("revenue")},
	})
	tb := m.Freeze()
	checkInt(t, len(tb.Rows), 2)
	check(t, tb.Rows[0].Get("total").UnwrapOr(""), "300")
}

func TestCoverage_Mutable_GroupByAgg_TwoGroups(t *testing.T) {
	m := table.NewMutable([]string{"a", "b", "val"}, [][]string{
		{"x", "1", "10"},
		{"x", "1", "20"},
		{"y", "2", "30"},
	})
	m.GroupByAgg([]string{"a", "b"}, []table.AggDef{
		{Col: "n", Agg: table.Count("val")},
	})
	tb := m.Freeze()
	checkInt(t, len(tb.Rows), 2)
}

func TestCoverage_Mutable_GroupByAgg_ThreeGroups(t *testing.T) {
	m := table.NewMutable([]string{"a", "b", "c", "val"}, [][]string{
		{"x", "1", "p", "10"},
		{"x", "1", "p", "20"},
		{"y", "2", "q", "30"},
	})
	m.GroupByAgg([]string{"a", "b", "c"}, []table.AggDef{
		{Col: "total", Agg: table.Sum("val")},
	})
	tb := m.Freeze()
	checkInt(t, len(tb.Rows), 2)
	check(t, tb.Rows[0].Get("total").UnwrapOr(""), "30")
}

// ============================================================
// MutableTable.Lead
// ============================================================

func TestCoverage_Mutable_Lead(t *testing.T) {
	m := table.NewMutable([]string{"val"}, [][]string{{"a"}, {"b"}, {"c"}})
	m.Lead("val", "val_lead", 1)
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("val_lead").UnwrapOr(""), "b")
	check(t, tb.Rows[1].Get("val_lead").UnwrapOr(""), "c")
	check(t, tb.Rows[2].Get("val_lead").UnwrapOr(""), "")
}

// ============================================================
// MutableTable.Rank
// ============================================================

func TestCoverage_Mutable_Rank(t *testing.T) {
	m := table.NewMutable([]string{"score"}, [][]string{{"10"}, {"30"}, {"20"}})
	m.Rank("score", "rank", true)
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("rank").UnwrapOr(""), "1")
	check(t, tb.Rows[1].Get("rank").UnwrapOr(""), "3")
	check(t, tb.Rows[2].Get("rank").UnwrapOr(""), "2")
}

// ============================================================
// MutableTable.ForEach
// ============================================================

func TestCoverage_Mutable_ForEach(t *testing.T) {
	m := table.NewMutable([]string{"v"}, [][]string{{"a"}, {"b"}, {"c"}})
	count := 0
	m.ForEach(func(i int, r table.Row) { count++ })
	checkInt(t, count, 3)
}

// ============================================================
// MutableTable.AssertColumns / AssertNoEmpty
// ============================================================

func TestCoverage_Mutable_AssertColumns_Ok(t *testing.T) {
	m := table.NewMutable([]string{"id", "name"}, [][]string{{"1", "Alice"}})
	m.AssertColumns("id", "name")
	if m.HasErrs() {
		t.Fatal("expected no errors")
	}
}

func TestCoverage_Mutable_AssertColumns_Missing(t *testing.T) {
	m := table.NewMutable([]string{"id"}, [][]string{{"1"}})
	m.AssertColumns("id", "missing")
	if !m.HasErrs() {
		t.Fatal("expected error for missing column")
	}
}

func TestCoverage_Mutable_AssertNoEmpty_Ok(t *testing.T) {
	m := table.NewMutable([]string{"id"}, [][]string{{"1"}, {"2"}})
	m.AssertNoEmpty("id")
	if m.HasErrs() {
		t.Fatal("expected no errors")
	}
}

func TestCoverage_Mutable_AssertNoEmpty_Empty(t *testing.T) {
	m := table.NewMutable([]string{"id"}, [][]string{{"1"}, {""}})
	m.AssertNoEmpty("id")
	if !m.HasErrs() {
		t.Fatal("expected error for empty cell")
	}
}

func TestCoverage_Mutable_AssertNoEmpty_AllCols(t *testing.T) {
	m := table.NewMutable([]string{"a", "b"}, [][]string{{"1", "x"}, {"2", "y"}})
	m.AssertNoEmpty()
	if m.HasErrs() {
		t.Fatal("expected no errors")
	}
}

// ============================================================
// MutableTable predicates: Ne, Matches, Empty, NotEmpty
// ============================================================

func TestCoverage_Mutable_Ne(t *testing.T) {
	m := table.NewMutable([]string{"status"}, [][]string{{"active"}, {"inactive"}, {"active"}})
	pred := m.Ne("status", "active")
	result := m.Freeze().Where(pred)
	checkInt(t, result.Len(), 1)
	check(t, result.Rows[0].Get("status").UnwrapOr(""), "inactive")
}

func TestCoverage_Mutable_Matches(t *testing.T) {
	m := table.NewMutable([]string{"email"}, [][]string{{"a@gmail.com"}, {"b@yahoo.com"}, {"c@gmail.com"}})
	pred := m.Matches("email", `@gmail\.com$`)
	result := m.Freeze().Where(pred)
	checkInt(t, result.Len(), 2)
}

func TestCoverage_Mutable_Empty(t *testing.T) {
	m := table.NewMutable([]string{"v"}, [][]string{{"x"}, {""}, {"y"}})
	pred := m.Empty("v")
	result := m.Freeze().Where(pred)
	checkInt(t, result.Len(), 1)
}

func TestCoverage_Mutable_NotEmpty(t *testing.T) {
	m := table.NewMutable([]string{"v"}, [][]string{{"x"}, {""}, {"y"}})
	pred := m.NotEmpty("v")
	result := m.Freeze().Where(pred)
	checkInt(t, result.Len(), 2)
}

func TestCoverage_Mutable_Prefix(t *testing.T) {
	m := table.NewMutable([]string{"name"}, [][]string{{"Alice"}, {"Bob"}, {"Anna"}})
	pred := m.Prefix("name", "A")
	result := m.Freeze().Where(pred)
	checkInt(t, result.Len(), 2)
}

func TestCoverage_Mutable_Suffix(t *testing.T) {
	m := table.NewMutable([]string{"name"}, [][]string{{"Alice"}, {"Bob"}, {"Grace"}})
	pred := m.Suffix("name", "ce")
	result := m.Freeze().Where(pred)
	checkInt(t, result.Len(), 2)
}

func TestCoverage_Mutable_Contains(t *testing.T) {
	m := table.NewMutable([]string{"name"}, [][]string{{"Alice"}, {"Bob"}, {"Charlie"}})
	pred := m.Contains("name", "li")
	result := m.Freeze().Where(pred)
	checkInt(t, result.Len(), 2)
}

// ============================================================
// Table predicates: Empty / NotEmpty row index edge cases
// ============================================================

func TestCoverage_Table_Empty_ShortRow(t *testing.T) {
	// Empty predicate when row is shorter than header count
	row := table.NewRow([]string{"a", "b"}, []string{"x"}) // short row: no value for "b"
	tb := table.NewFromRows([]string{"a", "b"}, []table.Row{row})
	pred := tb.Empty("b")
	// "b" at index 1, row has no value at index 1 → should be considered empty
	if !pred(row) {
		t.Error("expected Empty predicate to return true for missing value")
	}
}

func TestCoverage_Table_NotEmpty_ShortRow(t *testing.T) {
	row := table.NewRow([]string{"a", "b"}, []string{"x"}) // short row
	tb := table.NewFromRows([]string{"a", "b"}, []table.Row{row})
	pred := tb.NotEmpty("b")
	if pred(row) {
		t.Error("expected NotEmpty predicate to return false for missing value")
	}
}

// ============================================================
// Table.AddColFloat / AddColInt (immutable)
// ============================================================

func TestCoverage_Table_AddColFloat(t *testing.T) {
	tb := newTable([]string{"x"}, [][]string{{"4"}, {"9"}})
	result := tb.AddColFloat("sqrt_x", func(r table.Row) float64 {
		v, _ := strconv.ParseFloat(r.Get("x").UnwrapOr("0"), 64)
		return v * 0.5
	})
	check(t, result.Rows[0].Get("sqrt_x").UnwrapOr(""), "2")
	check(t, result.Rows[1].Get("sqrt_x").UnwrapOr(""), "4.5")
}

func TestCoverage_Table_AddColInt(t *testing.T) {
	tb := newTable([]string{"x"}, [][]string{{"10"}, {"20"}})
	result := tb.AddColInt("double", func(r table.Row) int64 {
		v, _ := strconv.ParseInt(r.Get("x").UnwrapOr("0"), 10, 64)
		return v * 2
	})
	check(t, result.Rows[0].Get("double").UnwrapOr(""), "20")
	check(t, result.Rows[1].Get("double").UnwrapOr(""), "40")
}

// ============================================================
// Table.AddColSwitch (immutable)
// ============================================================

func TestCoverage_Table_AddColSwitch(t *testing.T) {
	tb := newTable([]string{"grade"}, [][]string{{"A"}, {"B"}, {"C"}})
	result := tb.AddColSwitch("label",
		[]table.Case{
			{When: tb.Eq("grade", "A"), Then: func(table.Row) string { return "excellent" }},
			{When: tb.Eq("grade", "B"), Then: func(table.Row) string { return "good" }},
		},
		func(table.Row) string { return "other" },
	)
	check(t, result.Rows[0].Get("label").UnwrapOr(""), "excellent")
	check(t, result.Rows[1].Get("label").UnwrapOr(""), "good")
	check(t, result.Rows[2].Get("label").UnwrapOr(""), "other")
}

// ============================================================
// Table.Empty (immutable, predicate)
// ============================================================

func TestCoverage_Table_Empty_Pred(t *testing.T) {
	tb := newTable([]string{"v"}, [][]string{{"x"}, {""}, {"y"}})
	result := tb.Where(tb.Empty("v"))
	checkInt(t, result.Len(), 1)
}

// ============================================================
// Table.Tail edge case: overflow returns all rows
// ============================================================

func TestCoverage_Table_Tail_Overflow(t *testing.T) {
	tb := newTable([]string{"v"}, [][]string{{"1"}, {"2"}, {"3"}})
	result := tb.Tail(99)
	checkInt(t, result.Len(), 3)
}

// ============================================================
// MutableTable Eq predicate (missing col edge case)
// ============================================================

func TestCoverage_Mutable_Eq_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"x"}, [][]string{{"1"}})
	pred := m.Eq("nonexistent", "")
	// missing col with val "" → always true
	row := m.Freeze().Rows[0]
	if !pred(row) {
		t.Error("Eq missing col with empty val should be true")
	}
}

func TestCoverage_Mutable_Ne_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"x"}, [][]string{{"1"}})
	pred := m.Ne("nonexistent", "something")
	row := m.Freeze().Rows[0]
	if !pred(row) {
		t.Error("Ne missing col with non-empty val should be true")
	}
}

// ============================================================
// Table.Distinct with 2 columns (pairKey path)
// ============================================================

func TestCoverage_Table_Distinct_TwoCols(t *testing.T) {
	tb := newTable([]string{"a", "b"}, [][]string{
		{"1", "x"},
		{"1", "x"},
		{"1", "y"},
	})
	result := tb.Distinct("a", "b")
	checkInt(t, result.Len(), 2)
}

func TestCoverage_Table_Distinct_ThreeCols(t *testing.T) {
	tb := newTable([]string{"a", "b", "c"}, [][]string{
		{"1", "x", "p"},
		{"1", "x", "p"},
		{"1", "x", "q"},
	})
	result := tb.Distinct("a", "b", "c")
	checkInt(t, result.Len(), 2)
}

// ============================================================
// Table.LeftJoin multi-match (row_bucket path)
// ============================================================

func TestCoverage_Table_LeftJoin_MultiMatch(t *testing.T) {
	left := newTable([]string{"id"}, [][]string{{"1"}})
	right := newTable([]string{"id", "v"}, [][]string{{"1", "a"}, {"1", "b"}, {"1", "c"}})
	result := left.LeftJoin(right, "id", "id")
	checkInt(t, result.Len(), 3)
}

// ============================================================
// Table.Append with different headers (column alignment)
// ============================================================

func TestCoverage_Table_Append_DifferentHeaders(t *testing.T) {
	a := newTable([]string{"name", "city"}, [][]string{{"Alice", "Berlin"}})
	b := newTable([]string{"name", "age"}, [][]string{{"Bob", "25"}})
	result := a.Append(b)
	checkInt(t, result.Len(), 2)
	check(t, result.Rows[1].Get("name").UnwrapOr(""), "Bob")
}

// ============================================================
// Table.Partition edge case: all rows match
// ============================================================

func TestCoverage_Table_Partition_AllMatch(t *testing.T) {
	tb := newTable([]string{"v"}, [][]string{{"a"}, {"a"}})
	matched, rest := tb.Partition(func(r table.Row) bool { return true })
	checkInt(t, matched.Len(), 2)
	checkInt(t, rest.Len(), 0)
}

// ============================================================
// Table.Chunk edge case: zero/negative size
// ============================================================

func TestCoverage_Table_Chunk_ZeroSize(t *testing.T) {
	tb := newTable([]string{"v"}, [][]string{{"1"}, {"2"}})
	chunks := tb.Chunk(0)
	checkInt(t, len(chunks), 1)
}

// ============================================================
// Table.SortMulti: empty table
// ============================================================

func TestCoverage_Table_SortMulti_Empty(t *testing.T) {
	tb := newTable([]string{"v"}, nil)
	result := tb.SortMulti(table.Asc("v"))
	checkInt(t, result.Len(), 0)
}

// ============================================================
// Table.Coalesce: all empty
// ============================================================

func TestCoverage_Table_Coalesce_AllEmpty(t *testing.T) {
	tb := newTable([]string{"a", "b"}, [][]string{{"", ""}})
	result := tb.Coalesce("c", "a", "b")
	check(t, result.Rows[0].Get("c").UnwrapOr("x"), "")
}

// ============================================================
// Table.Intersect: single col (fast path) and two cols
// ============================================================

func TestCoverage_Table_Intersect_TwoCols(t *testing.T) {
	a := newTable([]string{"a", "b"}, [][]string{{"1", "x"}, {"2", "y"}, {"1", "z"}})
	b := newTable([]string{"a", "b"}, [][]string{{"1", "x"}, {"2", "y"}})
	result := a.Intersect(b, "a", "b")
	checkInt(t, result.Len(), 2)
}

// ============================================================
// Table.Bin: value exceeds all bins (falls to last label)
// ============================================================

func TestCoverage_Table_Bin_ExceedsAllBins(t *testing.T) {
	tb := newTable([]string{"v"}, [][]string{{"999"}})
	result := tb.Bin("v", "group", []table.BinDef{
		{Max: 100, Label: "low"},
		{Max: 500, Label: "mid"},
	})
	check(t, result.Rows[0].Get("group").UnwrapOr(""), "mid")
}

// ============================================================
// Table.GroupByAgg: two group cols (pairKey path)
// ============================================================

func TestCoverage_Table_GroupByAgg_TwoGroupCols(t *testing.T) {
	tb := newTable([]string{"a", "b", "val"}, [][]string{
		{"x", "1", "10"},
		{"x", "1", "20"},
		{"y", "2", "30"},
	})
	result := tb.GroupByAgg([]string{"a", "b"}, []table.AggDef{
		{Col: "total", Agg: table.Sum("val")},
		{Col: "n", Agg: table.Count("val")},
	})
	checkInt(t, result.Len(), 2)
	check(t, result.Rows[0].Get("total").UnwrapOr(""), "30")
	check(t, result.Rows[0].Get("n").UnwrapOr(""), "2")
}

// ============================================================
// MutableTable.ValueCounts
// ============================================================

func TestCoverage_Mutable_ValueCounts(t *testing.T) {
	m := table.NewMutable([]string{"city"}, [][]string{
		{"Berlin"}, {"Munich"}, {"Berlin"},
	})
	m.ValueCounts("city")
	tb := m.Freeze()
	checkInt(t, len(tb.Rows), 2)
}

// ============================================================
// Table.AddColSwitch with nil else_ func
// ============================================================

func TestCoverage_Table_AddColSwitch_NilElse(t *testing.T) {
	tb := newTable([]string{"v"}, [][]string{{"a"}, {"b"}})
	result := tb.AddColSwitch("out",
		[]table.Case{
			{When: tb.Eq("v", "a"), Then: func(table.Row) string { return "matched" }},
		},
		nil,
	)
	check(t, result.Rows[0].Get("out").UnwrapOr(""), "matched")
	check(t, result.Rows[1].Get("out").UnwrapOr(""), "")
}

// ============================================================
// MutableTable source-prefixed errors
// ============================================================

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

// ============================================================
// Table.RollingAgg size < 1 (clamped to 1)
// ============================================================

func TestCoverage_RollingAgg_SizeLessThanOne(t *testing.T) {
	tb := newTable([]string{"v"}, [][]string{{"10"}, {"20"}, {"30"}})
	result := tb.RollingAgg("s", 0, table.Sum("v"))
	// size clamped to 1, each window is just the current row
	check(t, result.Rows[0].Get("s").UnwrapOr(""), "10")
	check(t, result.Rows[1].Get("s").UnwrapOr(""), "20")
}

// ============================================================
// MutableTable.CumSum (more coverage)
// ============================================================

func TestCoverage_Mutable_CumSum(t *testing.T) {
	m := table.NewMutable([]string{"val"}, [][]string{{"10"}, {"20"}, {"30"}})
	m.CumSum("val", "cumsum")
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("cumsum").UnwrapOr(""), "10")
	check(t, tb.Rows[1].Get("cumsum").UnwrapOr(""), "30")
	check(t, tb.Rows[2].Get("cumsum").UnwrapOr(""), "60")
}

// ============================================================
// MutableTable.Lag
// ============================================================

func TestCoverage_Mutable_Lag(t *testing.T) {
	m := table.NewMutable([]string{"val"}, [][]string{{"a"}, {"b"}, {"c"}})
	m.Lag("val", "val_lag", 1)
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("val_lag").UnwrapOr(""), "")
	check(t, tb.Rows[1].Get("val_lag").UnwrapOr(""), "a")
	check(t, tb.Rows[2].Get("val_lag").UnwrapOr(""), "b")
}

// ============================================================
// Table.Melt with explicit value columns (tests melt varName/valName path)
// ============================================================

func TestCoverage_Table_Melt_Explicit(t *testing.T) {
	tb := newTable([]string{"id", "jan", "feb"}, [][]string{
		{"1", "100", "200"},
	})
	result := tb.Melt([]string{"id"}, "month", "amount")
	checkInt(t, result.Len(), 2)
	check(t, result.Rows[0].Get("month").UnwrapOr(""), "jan")
	check(t, result.Rows[1].Get("month").UnwrapOr(""), "feb")
}

// ============================================================
// CartesianProduct
// ============================================================

func TestCoverage_CartesianProduct(t *testing.T) {
	a := newTable([]string{"color"}, [][]string{{"red"}, {"blue"}})
	b := newTable([]string{"size"}, [][]string{{"S"}, {"M"}, {"L"}})
	result := table.CartesianProduct(a, b)
	checkInt(t, result.Len(), 6)
}

// ============================================================
// Table.Sort: missing column returns unchanged table
// ============================================================

func TestCoverage_Table_Sort_MissingCol(t *testing.T) {
	tb := newTable([]string{"a"}, [][]string{{"1"}, {"2"}})
	result := tb.Sort("nonexistent", true)
	checkInt(t, result.Len(), 2)
}

// ============================================================
// Table.Rename: unknown column (no-op)
// ============================================================

func TestCoverage_Table_Rename_UnknownCol(t *testing.T) {
	tb := newTable([]string{"a"}, [][]string{{"1"}})
	result := tb.Rename("nonexistent", "b")
	if result.Headers[0] != "a" {
		t.Errorf("unexpected header: %v", result.Headers)
	}
}

// ============================================================
// Table.Join: missing left/right col handled via HasErrs
// ============================================================

func TestCoverage_Table_Join_MultiMatchThreeRows(t *testing.T) {
	left := newTable([]string{"city"}, [][]string{{"Berlin"}})
	right := newTable([]string{"city", "v"}, [][]string{
		{"Berlin", "1"},
		{"Berlin", "2"},
		{"Berlin", "3"},
	})
	result := left.Join(right, "city", "city")
	checkInt(t, result.Len(), 3)
}

// ============================================================
// Table.AddRowIndex: preserves existing columns
// ============================================================

func TestCoverage_Table_AddRowIndex_WithData(t *testing.T) {
	tb := newTable([]string{"name"}, [][]string{{"Alice"}, {"Bob"}})
	result := tb.AddRowIndex("idx")
	check(t, result.Rows[0].Get("idx").UnwrapOr(""), "0")
	check(t, result.Rows[1].Get("idx").UnwrapOr(""), "1")
	check(t, result.Rows[0].Get("name").UnwrapOr(""), "Alice")
}

// ============================================================
// Table.FillBackward: missing col error
// ============================================================

func TestCoverage_Table_FillBackward_MissingCol(t *testing.T) {
	tb := newTable([]string{"a"}, [][]string{{"1"}})
	result := tb.FillBackward("nonexistent")
	if !result.HasErrs() {
		t.Error("expected error for missing col")
	}
}

// ============================================================
// Table.FillForward: missing col error
// ============================================================

func TestCoverage_Table_FillForward_MissingCol(t *testing.T) {
	tb := newTable([]string{"a"}, [][]string{{"1"}})
	result := tb.FillForward("nonexistent")
	if !result.HasErrs() {
		t.Error("expected error for missing col")
	}
}

// ============================================================
// MutableTable.Join with 3 matching rows
// (covers addRowBucket 3-path and forEachRowBucket multi-row path)
// ============================================================

func TestCoverage_Mutable_Join_ThreeMatchingRows(t *testing.T) {
	m := table.NewMutable([]string{"id"}, [][]string{{"1"}})
	right := newTable([]string{"id", "v"}, [][]string{
		{"1", "a"},
		{"1", "b"},
		{"1", "c"},
	})
	m.Join(right, "id", "id")
	checkInt(t, m.Len(), 3)
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("v").UnwrapOr(""), "a")
	check(t, tb.Rows[2].Get("v").UnwrapOr(""), "c")
}

func TestCoverage_Mutable_LeftJoin_ThreeMatchingRows(t *testing.T) {
	m := table.NewMutable([]string{"id"}, [][]string{{"1"}, {"2"}})
	right := newTable([]string{"id", "v"}, [][]string{
		{"1", "a"},
		{"1", "b"},
		{"1", "c"},
	})
	m.LeftJoin(right, "id", "id")
	checkInt(t, m.Len(), 4) // 3 matches for "1" + 1 unmatched for "2"
}

func TestCoverage_Mutable_RightJoin_ThreeMatchingRows(t *testing.T) {
	m := table.NewMutable([]string{"id", "lv"}, [][]string{
		{"1", "x"},
		{"1", "y"},
		{"1", "z"},
	})
	right := newTable([]string{"id", "rv"}, [][]string{{"1", "R"}})
	m.RightJoin(right, "id", "id")
	checkInt(t, m.Len(), 3)
}

// ============================================================
// MutableTable error cases: Sort/GroupBy/Explode/FillForward/
// FillBackward/Lag/Lead/CumSum/Rank missing columns
// ============================================================

func TestCoverage_Mutable_Sort_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a"}, [][]string{{"1"}})
	m.Sort("nonexistent", true)
	if !m.HasErrs() {
		t.Error("expected error")
	}
}

func TestCoverage_Mutable_Sort_Desc(t *testing.T) {
	m := table.NewMutable([]string{"v"}, [][]string{{"1"}, {"3"}, {"2"}})
	m.Sort("v", false)
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("v").UnwrapOr(""), "3")
	check(t, tb.Rows[2].Get("v").UnwrapOr(""), "1")
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

func TestCoverage_Mutable_Rank_Desc(t *testing.T) {
	m := table.NewMutable([]string{"score"}, [][]string{{"10"}, {"30"}, {"20"}})
	m.Rank("score", "rank", false)
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("rank").UnwrapOr(""), "3")
	check(t, tb.Rows[1].Get("rank").UnwrapOr(""), "1")
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

// ============================================================
// MutableTable.SortMulti error case
// ============================================================

func TestCoverage_Mutable_SortMulti_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a"}, [][]string{{"1"}, {"2"}})
	m.SortMulti(table.Asc("nonexistent"))
	if !m.HasErrs() {
		t.Error("expected error for SortMulti with missing col")
	}
}

// ============================================================
// Table.Get: col exists but row is shorter than header count
// ============================================================

func TestCoverage_Row_Get_ShortRow(t *testing.T) {
	row := table.NewRow([]string{"a", "b", "c"}, []string{"x"}) // only 1 value
	if row.Get("b").IsSome() {
		t.Error("expected None for missing value at index 1")
	}
}

// ============================================================
// MutableTable.Drop: error on unknown column
// ============================================================

func TestCoverage_Mutable_Drop_Unknown(t *testing.T) {
	m := table.NewMutable([]string{"a", "b"}, [][]string{{"1", "2"}})
	m.Drop("nonexistent")
	// Drop on unknown columns is silently ignored (or keeps table intact)
	checkInt(t, m.Len(), 1)
}

// ============================================================
// MutableTable.Select: error on unknown column
// ============================================================

func TestCoverage_Mutable_Select_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a", "b"}, [][]string{{"1", "2"}})
	m.Select("a", "nonexistent")
	if !m.HasErrs() {
		t.Error("expected error for Select with missing col")
	}
}

// ============================================================
// MutableTable.AntiJoin: error cases
// ============================================================

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

// ============================================================
// MutableTable.Lookup: error cases
// ============================================================

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

// ============================================================
// MutableTable.Intersect: error cases
// ============================================================

func TestCoverage_Mutable_Intersect_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"id"}, [][]string{{"1"}})
	other := newTable([]string{"id"}, [][]string{{"1"}})
	m.Intersect(other, "nonexistent")
	if !m.HasErrs() {
		t.Error("expected error for missing col in Intersect")
	}
}

// ============================================================
// MutableTable.Bin: error case
// ============================================================

func TestCoverage_Mutable_Bin_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a"}, [][]string{{"1"}})
	m.Bin("nonexistent", "group", []table.BinDef{{Max: 100, Label: "low"}})
	if !m.HasErrs() {
		t.Error("expected error for missing col in Bin")
	}
}

// ============================================================
// MutableTable.FormatCol: error case
// ============================================================

func TestCoverage_Mutable_FormatCol_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a"}, [][]string{{"1"}})
	m.FormatCol("nonexistent", 2)
	if !m.HasErrs() {
		t.Error("expected error for missing col in FormatCol")
	}
}

// ============================================================
// MutableTable.GroupByAgg: error case
// ============================================================

func TestCoverage_Mutable_GroupByAgg_MissingGroupCol(t *testing.T) {
	m := table.NewMutable([]string{"a", "val"}, [][]string{{"x", "10"}})
	m.GroupByAgg([]string{"nonexistent"}, []table.AggDef{
		{Col: "total", Agg: table.Sum("val")},
	})
	if !m.HasErrs() {
		t.Error("expected error for missing group col")
	}
}

// ============================================================
// MutableTable.Coalesce: all empty
// ============================================================

func TestCoverage_Mutable_Coalesce_AllEmpty(t *testing.T) {
	m := table.NewMutable([]string{"a", "b"}, [][]string{{"", ""}})
	m.Coalesce("c", "a", "b")
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("c").UnwrapOr("x"), "")
}

// ============================================================
// MutableTable.ValueCounts: sort by count (covers more paths)
// ============================================================

func TestCoverage_Mutable_ValueCounts_Sort(t *testing.T) {
	m := table.NewMutable([]string{"v"}, [][]string{
		{"a"}, {"b"}, {"a"}, {"a"}, {"b"},
	})
	m.ValueCounts("v")
	tb := m.Freeze()
	// "a" appears 3 times, "b" appears 2 times → sorted by count desc
	check(t, tb.Rows[0].Get("value").UnwrapOr(""), "a")
	check(t, tb.Rows[0].Get("count").UnwrapOr(""), "3")
}

// ============================================================
// MutableTable.Head/Tail with overflow
// ============================================================

func TestCoverage_Mutable_Head_Overflow(t *testing.T) {
	m := table.NewMutable([]string{"v"}, [][]string{{"1"}, {"2"}})
	m.Head(100)
	checkInt(t, m.Len(), 2)
}

func TestCoverage_Mutable_Tail_Overflow(t *testing.T) {
	m := table.NewMutable([]string{"v"}, [][]string{{"1"}, {"2"}})
	m.Tail(100)
	checkInt(t, m.Len(), 2)
}

// ============================================================
// MutableTable.DropEmpty: missing column
// ============================================================

func TestCoverage_Mutable_DropEmpty_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a"}, [][]string{{"1"}})
	m.DropEmpty("nonexistent")
	if !m.HasErrs() {
		t.Error("expected error for missing col in DropEmpty")
	}
}

// ============================================================
// MutableTable.Distinct: missing column
// ============================================================

func TestCoverage_Mutable_Distinct_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a"}, [][]string{{"1"}})
	m.Distinct("nonexistent")
	if !m.HasErrs() {
		t.Error("expected error for missing col in Distinct")
	}
}

// ============================================================
// CumSum with float transition (covers intOnly false path)
// ============================================================

func TestCoverage_Mutable_CumSum_FloatTransition(t *testing.T) {
	m := table.NewMutable([]string{"val"}, [][]string{{"10"}, {"1.5"}, {"2.5"}})
	m.CumSum("val", "cumsum")
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("cumsum").UnwrapOr(""), "10")
	check(t, tb.Rows[1].Get("cumsum").UnwrapOr(""), "11.5")
	check(t, tb.Rows[2].Get("cumsum").UnwrapOr(""), "14")
}

// ============================================================
// Table.CumSum with float transition (immutable, covers timeseries.go)
// ============================================================

func TestCoverage_Table_CumSum_FloatTransition(t *testing.T) {
	tb := newTable([]string{"val"}, [][]string{{"5"}, {"1.5"}, {"3.5"}})
	result := tb.CumSum("val", "cumsum")
	check(t, result.Rows[0].Get("cumsum").UnwrapOr(""), "5")
	check(t, result.Rows[1].Get("cumsum").UnwrapOr(""), "6.5")
	check(t, result.Rows[2].Get("cumsum").UnwrapOr(""), "10")
}

// ============================================================
// MutableTable.AddRowIndex: preserves existing columns
// ============================================================

func TestCoverage_Mutable_AddRowIndex(t *testing.T) {
	m := table.NewMutable([]string{"name"}, [][]string{{"Alice"}, {"Bob"}})
	m.AddRowIndex("idx")
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("idx").UnwrapOr(""), "0")
	check(t, tb.Rows[1].Get("idx").UnwrapOr(""), "1")
}

// ============================================================
// MutableTable.Partition
// ============================================================

func TestCoverage_Mutable_Partition(t *testing.T) {
	m := table.NewMutable([]string{"city"}, [][]string{{"Berlin"}, {"Munich"}, {"Berlin"}})
	pred := m.Eq("city", "Berlin")
	matched, rest := m.Partition(pred)
	checkInt(t, matched.Len(), 2)
	checkInt(t, rest.Len(), 1)
}

// ============================================================
// MutableTable.Chunk
// ============================================================

func TestCoverage_Mutable_Chunk(t *testing.T) {
	m := table.NewMutable([]string{"v"}, [][]string{{"1"}, {"2"}, {"3"}, {"4"}, {"5"}})
	chunks := m.Chunk(2)
	checkInt(t, len(chunks), 3)
	checkInt(t, chunks[0].Len(), 2)
	checkInt(t, chunks[2].Len(), 1)
}

func TestCoverage_Mutable_Chunk_Empty(t *testing.T) {
	m := table.NewMutable([]string{"v"}, nil)
	chunks := m.Chunk(2)
	checkInt(t, len(chunks), 1)
}

// ============================================================
// MutableTable.RollingAgg with Count/StringJoin/First/Last
// ============================================================

func TestCoverage_Mutable_RollingAgg_Count(t *testing.T) {
	m := table.NewMutable([]string{"v"}, [][]string{{"a"}, {"b"}, {""}})
	m.RollingAgg("cnt", 2, table.Count("v"))
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("cnt").UnwrapOr(""), "1")
	check(t, tb.Rows[1].Get("cnt").UnwrapOr(""), "2")
}

func TestCoverage_Mutable_RollingAgg_First(t *testing.T) {
	m := table.NewMutable([]string{"v"}, [][]string{{"x"}, {"y"}, {"z"}})
	m.RollingAgg("first", 3, table.First("v"))
	tb := m.Freeze()
	check(t, tb.Rows[2].Get("first").UnwrapOr(""), "x")
}

func TestCoverage_Mutable_RollingAgg_Last(t *testing.T) {
	m := table.NewMutable([]string{"v"}, [][]string{{"x"}, {"y"}, {"z"}})
	m.RollingAgg("last", 3, table.Last("v"))
	tb := m.Freeze()
	check(t, tb.Rows[2].Get("last").UnwrapOr(""), "z")
}

func TestCoverage_Mutable_RollingAgg_StringJoin(t *testing.T) {
	m := table.NewMutable([]string{"v"}, [][]string{{"a"}, {"b"}, {"c"}})
	m.RollingAgg("joined", 2, table.StringJoin("v", "|"))
	tb := m.Freeze()
	check(t, tb.Rows[1].Get("joined").UnwrapOr(""), "a|b")
}

// ============================================================
// Table.Lag/Lead (immutable) with negative n (clamped to 0)
// ============================================================

func TestCoverage_Table_Lag_NegativeN(t *testing.T) {
	tb := newTable([]string{"v"}, [][]string{{"a"}, {"b"}})
	result := tb.Lag("v", "out", -1)
	check(t, result.Rows[0].Get("out").UnwrapOr(""), "a")
}

func TestCoverage_Table_Lead_NegativeN(t *testing.T) {
	tb := newTable([]string{"v"}, [][]string{{"a"}, {"b"}})
	result := tb.Lead("v", "out", -1)
	check(t, result.Rows[0].Get("out").UnwrapOr(""), "a")
}

// ============================================================
// parseIntFast edge cases (+ and - prefix)
// ============================================================

func TestCoverage_ParseIntFast_Via_Rank(t *testing.T) {
	// +5 is a valid integer for parseIntFast (covers + prefix path)
	tb := newTable([]string{"v"}, [][]string{{"+5"}, {"10"}, {"-3"}})
	result := tb.Rank("v", "rank", true)
	// -3 ranks 1st (smallest), +5 ranks 2nd, 10 ranks 3rd
	check(t, result.Rows[2].Get("rank").UnwrapOr(""), "1") // -3
	check(t, result.Rows[0].Get("rank").UnwrapOr(""), "2") // +5
	check(t, result.Rows[1].Get("rank").UnwrapOr(""), "3") // 10
}

// ============================================================
// MutableTable.Lead/Lag with negative n (clamped to 0)
// ============================================================

func TestCoverage_Mutable_Lag_NegativeN(t *testing.T) {
	m := table.NewMutable([]string{"v"}, [][]string{{"a"}, {"b"}})
	m.Lag("v", "out", -5)
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("out").UnwrapOr(""), "a")
}

func TestCoverage_Mutable_Lead_NegativeN(t *testing.T) {
	m := table.NewMutable([]string{"v"}, [][]string{{"a"}, {"b"}})
	m.Lead("v", "out", -5)
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("out").UnwrapOr(""), "a")
}

// ============================================================
// MutableTable.OuterJoin error cases
// ============================================================

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

// ============================================================
// Table.Pivot: missing column error
// ============================================================

func TestCoverage_Table_Pivot_MissingCol(t *testing.T) {
	tb := newTable([]string{"a", "b", "c"}, [][]string{{"1", "x", "v"}})
	result := tb.Pivot("nonexistent", "b", "c")
	if !result.HasErrs() {
		t.Error("expected error for missing index col in Pivot")
	}
}

// ============================================================
// Table.LeftJoin: missing col errors
// ============================================================

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

// ============================================================
// Table.SortMulti: missing column
// ============================================================

func TestCoverage_Table_SortMulti_MissingCol(t *testing.T) {
	tb := newTable([]string{"a"}, [][]string{{"1"}, {"2"}})
	result := tb.SortMulti(table.Asc("nonexistent"))
	// Should still return result (error accumulated internally)
	checkInt(t, result.Len(), 2)
}

// ============================================================
// Table.GroupByAgg: missing agg col
// ============================================================

func TestCoverage_Table_GroupByAgg_MissingAggCol(t *testing.T) {
	tb := newTable([]string{"group", "val"}, [][]string{{"A", "10"}})
	result := tb.GroupByAgg([]string{"group"}, []table.AggDef{
		{Col: "total", Agg: table.Sum("nonexistent")},
	})
	// The agg col missing returns empty string for that column
	checkInt(t, result.Len(), 1)
}

// ============================================================
// Table.RollingAgg: missing agg col
// ============================================================

func TestCoverage_Table_RollingAgg_MissingAggCol(t *testing.T) {
	tb := newTable([]string{"v"}, [][]string{{"1"}, {"2"}})
	result := tb.RollingAgg("out", 2, table.Sum("nonexistent"))
	// Missing col returns 0 (no valid values)
	checkInt(t, result.Len(), 2)
}

// ============================================================
// MutableTable.Melt: edge case with no value cols
// ============================================================

func TestCoverage_Mutable_Melt_MultipleIdCols(t *testing.T) {
	m := table.NewMutable([]string{"id", "name", "q1", "q2"}, [][]string{
		{"1", "Alice", "100", "200"},
	})
	m.Melt([]string{"id", "name"}, "quarter", "value")
	checkInt(t, m.Len(), 2)
	tb := m.Freeze()
	check(t, tb.Rows[0].Get("id").UnwrapOr(""), "1")
	check(t, tb.Rows[0].Get("name").UnwrapOr(""), "Alice")
}

// ============================================================
// MutableTable.Pivot: missing column error
// ============================================================

func TestCoverage_Mutable_Pivot_MissingCol(t *testing.T) {
	m := table.NewMutable([]string{"a", "b", "c"}, [][]string{{"1", "x", "v"}})
	m.Pivot("nonexistent", "b", "c")
	if !m.HasErrs() {
		t.Error("expected error for missing col in Pivot")
	}
}

// ============================================================
// Table.Append preserves source
// ============================================================

func TestCoverage_Table_Append_PreservesSource(t *testing.T) {
	a := newTable([]string{"v"}, [][]string{{"1"}}).WithSource("file.csv")
	b := newTable([]string{"v"}, [][]string{{"2"}})
	result := a.Append(b)
	check(t, result.Source(), "file.csv")
}

// ============================================================
// MutableTable.Explode: empty cell (keeps row)
// ============================================================

func TestCoverage_Mutable_Explode_EmptyCell(t *testing.T) {
	m := table.NewMutable([]string{"v"}, [][]string{{""}, {"a,b"}})
	m.Explode("v", ",")
	checkInt(t, m.Len(), 3) // "" → 1 row (kept), "a,b" → 2 rows
}

// ============================================================
// Table.Explode with empty sep (no split)
// ============================================================

func TestCoverage_Table_Explode_EmptySep(t *testing.T) {
	tb := newTable([]string{"v"}, [][]string{{"hello"}})
	result := tb.Explode("v", "")
	checkInt(t, result.Len(), 1)
	check(t, result.Rows[0].Get("v").UnwrapOr(""), "hello")
}

// ============================================================
// MutableTable.AssertNoEmpty all columns (no explicit cols)
// ============================================================

func TestCoverage_Mutable_AssertNoEmpty_WithEmpty_AllCols(t *testing.T) {
	m := table.NewMutable([]string{"a", "b"}, [][]string{{"1", ""}, {"2", "x"}})
	m.AssertNoEmpty()
	if !m.HasErrs() {
		t.Error("expected error for empty cell in AllCols check")
	}
}
