package table

import (
	"strconv"
	"testing"
)

func salesTable() Table {
	return New(
		[]string{"region", "product", "revenue", "label"},
		[][]string{
			{"EU", "Widget", "100", "A"},
			{"EU", "Widget", "200", "B"},
			{"EU", "Gadget", "150", "C"},
			{"US", "Widget", "300", "D"},
			{"US", "Gadget", "50", ""},
		},
	)
}

// --- GroupByAgg ---

func TestGroupByAgg_Sum(t *testing.T) {
	result := salesTable().GroupByAgg(
		[]string{"region"},
		[]AggDef{{Col: "total", Agg: Sum("revenue")}},
	)
	assertEqual(t, len(result.Rows), 2)
	// EU: 100+200+150 = 450
	euRow := result.Where(func(r Row) bool { return r.Get("region").UnwrapOr("") == "EU" }).Rows[0]
	assertEqual(t, euRow.Get("total").UnwrapOr(""), "450")
	// US: 300+50 = 350
	usRow := result.Where(func(r Row) bool { return r.Get("region").UnwrapOr("") == "US" }).Rows[0]
	assertEqual(t, usRow.Get("total").UnwrapOr(""), "350")
}

func TestGroupByAgg_MultiKey(t *testing.T) {
	result := salesTable().GroupByAgg(
		[]string{"region", "product"},
		[]AggDef{{Col: "total", Agg: Sum("revenue")}},
	)
	assertEqual(t, len(result.Rows), 4) // EU/Widget, EU/Gadget, US/Widget, US/Gadget
}

func TestGroupByAgg_Mean(t *testing.T) {
	result := salesTable().GroupByAgg(
		[]string{"region"},
		[]AggDef{{Col: "avg", Agg: Mean("revenue")}},
	)
	euRow := result.Where(func(r Row) bool { return r.Get("region").UnwrapOr("") == "EU" }).Rows[0]
	// EU mean: (100+200+150)/3 = 150
	assertEqual(t, euRow.Get("avg").UnwrapOr(""), "150")
}

func TestGroupByAgg_Count(t *testing.T) {
	result := salesTable().GroupByAgg(
		[]string{"region"},
		[]AggDef{{Col: "n", Agg: Count("revenue")}},
	)
	eu := result.Where(func(r Row) bool { return r.Get("region").UnwrapOr("") == "EU" }).Rows[0]
	assertEqual(t, eu.Get("n").UnwrapOr(""), "3")
}

func TestGroupByAgg_Count_SkipsEmpty(t *testing.T) {
	result := salesTable().GroupByAgg(
		[]string{"region"},
		[]AggDef{{Col: "n", Agg: Count("label")}},
	)
	// US has 2 rows but one empty label
	us := result.Where(func(r Row) bool { return r.Get("region").UnwrapOr("") == "US" }).Rows[0]
	assertEqual(t, us.Get("n").UnwrapOr(""), "1")
}

func TestGroupByAgg_StringJoin(t *testing.T) {
	result := salesTable().GroupByAgg(
		[]string{"region"},
		[]AggDef{{Col: "labels", Agg: StringJoin("label", ", ")}},
	)
	eu := result.Where(func(r Row) bool { return r.Get("region").UnwrapOr("") == "EU" }).Rows[0]
	assertEqual(t, eu.Get("labels").UnwrapOr(""), "A, B, C")
}

func TestGroupByAgg_StringJoin_SkipsEmpty(t *testing.T) {
	result := salesTable().GroupByAgg(
		[]string{"region"},
		[]AggDef{{Col: "labels", Agg: StringJoin("label", ", ")}},
	)
	us := result.Where(func(r Row) bool { return r.Get("region").UnwrapOr("") == "US" }).Rows[0]
	assertEqual(t, us.Get("labels").UnwrapOr(""), "D") // empty label skipped
}

func TestGroupByAgg_First(t *testing.T) {
	result := salesTable().GroupByAgg(
		[]string{"region"},
		[]AggDef{{Col: "first_label", Agg: First("label")}},
	)
	eu := result.Where(func(r Row) bool { return r.Get("region").UnwrapOr("") == "EU" }).Rows[0]
	assertEqual(t, eu.Get("first_label").UnwrapOr(""), "A")
}

func TestGroupByAgg_Last(t *testing.T) {
	result := salesTable().GroupByAgg(
		[]string{"region"},
		[]AggDef{{Col: "last_label", Agg: Last("label")}},
	)
	eu := result.Where(func(r Row) bool { return r.Get("region").UnwrapOr("") == "EU" }).Rows[0]
	assertEqual(t, eu.Get("last_label").UnwrapOr(""), "C")
}

func TestGroupByAgg_MultipleAggs(t *testing.T) {
	result := salesTable().GroupByAgg(
		[]string{"region"},
		[]AggDef{
			{Col: "total", Agg: Sum("revenue")},
			{Col: "count", Agg: Count("revenue")},
			{Col: "labels", Agg: StringJoin("label", ";")},
		},
	)
	assertEqual(t, len(result.Headers), 4) // region + 3 aggs
	eu := result.Where(func(r Row) bool { return r.Get("region").UnwrapOr("") == "EU" }).Rows[0]
	assertEqual(t, eu.Get("total").UnwrapOr(""), "450")
	assertEqual(t, eu.Get("count").UnwrapOr(""), "3")
	assertEqual(t, eu.Get("labels").UnwrapOr(""), "A;B;C")
}

func TestGroupByAgg_PreservesOrder(t *testing.T) {
	result := salesTable().GroupByAgg([]string{"region"}, []AggDef{{Col: "n", Agg: Count("revenue")}})
	// EU appears first in input → first in output
	assertEqual(t, result.Rows[0].Get("region").UnwrapOr(""), "EU")
	assertEqual(t, result.Rows[1].Get("region").UnwrapOr(""), "US")
}

// --- Missing column edge cases ---

func TestGroupByAgg_MissingAggCol(t *testing.T) {
	result := salesTable().GroupByAgg(
		[]string{"region"},
		[]AggDef{{Col: "total", Agg: Sum("nonexistent")}},
	)
	// grouping works, but Sum on missing col returns "0"
	assertEqual(t, len(result.Rows), 2)
	assertEqual(t, result.Rows[0].Get("total").UnwrapOr(""), "0")
}

func TestMean_MissingCol(t *testing.T) {
	result := salesTable().GroupByAgg(
		[]string{"region"},
		[]AggDef{{Col: "avg", Agg: Mean("nonexistent")}},
	)
	assertEqual(t, result.Rows[0].Get("avg").UnwrapOr("x"), "")
}

func TestCount_MissingCol(t *testing.T) {
	result := salesTable().GroupByAgg(
		[]string{"region"},
		[]AggDef{{Col: "n", Agg: Count("nonexistent")}},
	)
	assertEqual(t, result.Rows[0].Get("n").UnwrapOr(""), "0")
}

func TestStringJoin_MissingCol(t *testing.T) {
	result := salesTable().GroupByAgg(
		[]string{"region"},
		[]AggDef{{Col: "labels", Agg: StringJoin("nonexistent", ",")}},
	)
	assertEqual(t, result.Rows[0].Get("labels").UnwrapOr("x"), "")
}

func TestFirst_MissingCol(t *testing.T) {
	result := salesTable().GroupByAgg(
		[]string{"region"},
		[]AggDef{{Col: "f", Agg: First("nonexistent")}},
	)
	assertEqual(t, result.Rows[0].Get("f").UnwrapOr("x"), "")
}

func TestLast_MissingCol(t *testing.T) {
	result := salesTable().GroupByAgg(
		[]string{"region"},
		[]AggDef{{Col: "l", Agg: Last("nonexistent")}},
	)
	assertEqual(t, result.Rows[0].Get("l").UnwrapOr("x"), "")
}

// --- AddColSwitch ---

func TestAddColSwitch_FirstMatch(t *testing.T) {
	tb := New([]string{"score"}, [][]string{{"A"}, {"B"}, {"C"}})
	result := tb.AddColSwitch("grade",
		[]Case{
			{When: func(r Row) bool { return r.Get("score").UnwrapOr("") == "A" },
				Then: func(r Row) string { return "excellent" }},
			{When: func(r Row) bool { return r.Get("score").UnwrapOr("") == "B" },
				Then: func(r Row) string { return "good" }},
		},
		func(r Row) string { return "other" },
	)
	assertEqual(t, result.Rows[0].Get("grade").UnwrapOr(""), "excellent")
	assertEqual(t, result.Rows[1].Get("grade").UnwrapOr(""), "good")
	assertEqual(t, result.Rows[2].Get("grade").UnwrapOr(""), "other") // default
}

func TestAddColSwitch_NilDefault(t *testing.T) {
	tb := New([]string{"v"}, [][]string{{"x"}})
	result := tb.AddColSwitch("out",
		[]Case{{When: func(r Row) bool { return false }, Then: func(r Row) string { return "hit" }}},
		nil,
	)
	assertEqual(t, result.Rows[0].Get("out").UnwrapOr("x"), "") // nil default → ""
}

func TestAddColSwitch_RowValueInThen(t *testing.T) {
	tb := New([]string{"v"}, [][]string{{"hello"}})
	result := tb.AddColSwitch("upper",
		[]Case{{
			When: func(r Row) bool { return r.Get("v").UnwrapOr("") != "" },
			Then: func(r Row) string { return "[" + r.Get("v").UnwrapOr("") + "]" },
		}},
		nil,
	)
	assertEqual(t, result.Rows[0].Get("upper").UnwrapOr(""), "[hello]")
}

// --- CartesianProduct ---

func TestCartesianProduct_Basic(t *testing.T) {
	a := New([]string{"region"}, [][]string{{"EU"}, {"US"}})
	b := New([]string{"product"}, [][]string{{"Widget"}, {"Gadget"}})
	result := CartesianProduct(a, b)
	assertEqual(t, len(result.Rows), 4)
	assertEqual(t, len(result.Headers), 2)
}

func TestCartesianProduct_AllCombinations(t *testing.T) {
	a := New([]string{"x"}, [][]string{{"1"}, {"2"}})
	b := New([]string{"y"}, [][]string{{"a"}, {"b"}, {"c"}})
	result := CartesianProduct(a, b)
	assertEqual(t, len(result.Rows), 6) // 2×3
	assertEqual(t, result.Rows[0].Get("x").UnwrapOr(""), "1")
	assertEqual(t, result.Rows[0].Get("y").UnwrapOr(""), "a")
	assertEqual(t, result.Rows[1].Get("x").UnwrapOr(""), "1")
	assertEqual(t, result.Rows[1].Get("y").UnwrapOr(""), "b")
	assertEqual(t, result.Rows[2].Get("y").UnwrapOr(""), "c")
	assertEqual(t, result.Rows[3].Get("x").UnwrapOr(""), "2")
}

func TestCartesianProduct_EmptyTable(t *testing.T) {
	a := New([]string{"x"}, [][]string{{"1"}})
	b := New([]string{"y"}, nil)
	result := CartesianProduct(a, b)
	assertEqual(t, len(result.Rows), 0)
}

func BenchmarkGroupByAgg(b *testing.B) {
	records := make([][]string, 50_000)
	for i := 0; i < len(records); i++ {
		region := "EU"
		if i%3 == 1 {
			region = "US"
		}
		if i%3 == 2 {
			region = "APAC"
		}
		records[i] = []string{
			region,
			"product_" + strconv.Itoa(i%50),
			strconv.Itoa(100 + i%1000),
			"label_" + strconv.Itoa(i%20),
		}
	}
	tb := New([]string{"region", "product", "revenue", "label"}, records)
	aggs := []AggDef{
		{Col: "total", Agg: Sum("revenue")},
		{Col: "count", Agg: Count("revenue")},
		{Col: "labels", Agg: StringJoin("label", ",")},
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = tb.GroupByAgg([]string{"region", "product"}, aggs)
	}
}

// --- RollingAgg ---

func TestRollingAgg_Sum(t *testing.T) {
	tb := New([]string{"v"}, [][]string{{"1"}, {"2"}, {"3"}, {"4"}, {"5"}})
	result := tb.RollingAgg("roll", 3, Sum("v"))
	// window size 3:
	// row 0: [1]       → 1
	// row 1: [1,2]     → 3
	// row 2: [1,2,3]   → 6
	// row 3: [2,3,4]   → 9
	// row 4: [3,4,5]   → 12
	assertEqual(t, result.Rows[0].Get("roll").UnwrapOr(""), "1")
	assertEqual(t, result.Rows[1].Get("roll").UnwrapOr(""), "3")
	assertEqual(t, result.Rows[2].Get("roll").UnwrapOr(""), "6")
	assertEqual(t, result.Rows[3].Get("roll").UnwrapOr(""), "9")
	assertEqual(t, result.Rows[4].Get("roll").UnwrapOr(""), "12")
}

func TestRollingAgg_Mean(t *testing.T) {
	tb := New([]string{"v"}, [][]string{{"2"}, {"4"}, {"6"}})
	result := tb.RollingAgg("avg", 2, Mean("v"))
	assertEqual(t, result.Rows[0].Get("avg").UnwrapOr(""), "2") // window=[2], mean=2
	assertEqual(t, result.Rows[1].Get("avg").UnwrapOr(""), "3") // window=[2,4], mean=3
	assertEqual(t, result.Rows[2].Get("avg").UnwrapOr(""), "5") // window=[4,6], mean=5
}

func TestRollingAgg_WindowSize1(t *testing.T) {
	tb := New([]string{"v"}, [][]string{{"10"}, {"20"}, {"30"}})
	result := tb.RollingAgg("same", 1, Sum("v"))
	assertEqual(t, result.Rows[0].Get("same").UnwrapOr(""), "10")
	assertEqual(t, result.Rows[1].Get("same").UnwrapOr(""), "20")
	assertEqual(t, result.Rows[2].Get("same").UnwrapOr(""), "30")
}

func TestRollingAgg_OriginalColsPreserved(t *testing.T) {
	tb := New([]string{"name", "v"}, [][]string{{"Alice", "5"}, {"Bob", "10"}})
	result := tb.RollingAgg("roll", 2, Sum("v"))
	assertEqual(t, result.Rows[1].Get("name").UnwrapOr(""), "Bob")
	assertEqual(t, result.Rows[1].Get("roll").UnwrapOr(""), "15")
}
