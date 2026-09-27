package table

import (
	"math"
	"testing"

	"github.com/stefanbethge/gseq-table/internal/cell"
)

// statsTable has one group with mixed values (including blanks and
// unparseable cells), one group with a single value and one with none.
func statsTable() Table {
	return New(
		[]string{"g", "v", "tag"},
		[][]string{
			{"a", "4", "x"},
			{"a", " 2 ", "y"},
			{"a", "", "x"},
			{"a", "n/a", ""},
			{"a", "8", "z"},
			{"a", "6", "x"},
			{"b", "5", "q"},
			{"c", "", ""},
			{"c", "abc", "abc"},
		},
	)
}

func statsAggs() []AggDef {
	return []AggDef{
		{Col: "min", Agg: Min("v")},
		{Col: "max", Agg: Max("v")},
		{Col: "median", Agg: Median("v")},
		{Col: "p25", Agg: Quantile("v", 0.25)},
		{Col: "var", Agg: Var("v")},
		{Col: "std", Agg: StdDev("v")},
		{Col: "distinct", Agg: CountDistinct("tag")},
	}
}

// statsWant is the expected output for statsTable grouped by "g":
// a: values 4 2 8 6 → sorted 2 4 6 8, mean 5, variance 5.
var statsWant = map[string]map[string]string{
	"a": {"min": "2", "max": "8", "median": "5", "p25": "3.5", "var": "5", "std": cell.FormatFloat(math.Sqrt(5), 64), "distinct": "3"},
	"b": {"min": "5", "max": "5", "median": "5", "p25": "5", "var": "0", "std": "0", "distinct": "1"},
	"c": {"min": "", "max": "", "median": "", "p25": "", "var": "", "std": "", "distinct": "1"},
}

func checkStatsResult(t *testing.T, result Table) {
	t.Helper()
	assertEqual(t, len(result.Rows), 3)
	for _, row := range result.Rows {
		g := row.Get("g").UnwrapOr("")
		for col, want := range statsWant[g] {
			if got := row.Get(col).UnwrapOr("<missing>"); got != want {
				t.Errorf("group %q, %s: got %q, want %q", g, col, got, want)
			}
		}
	}
}

func TestGroupByAgg_StatsAggs(t *testing.T) {
	checkStatsResult(t, statsTable().GroupByAgg([]string{"g"}, statsAggs()))
}

func TestMutableGroupByAgg_StatsAggs(t *testing.T) {
	m := statsTable().Mutable().GroupByAgg([]string{"g"}, statsAggs())
	checkStatsResult(t, m.Freeze())
}

func TestGroupByAgg_StatsAggs_MultiKey(t *testing.T) {
	tb := statsTable().AddColConstValue("k", "1")
	result := tb.GroupByAgg([]string{"g", "k"}, statsAggs())
	checkStatsResult(t, result)
	result = tb.AddColConstValue("k2", "2").GroupByAgg([]string{"g", "k", "k2"}, statsAggs())
	checkStatsResult(t, result)
}

func TestGroupByAgg_StatsHeaders(t *testing.T) {
	result := statsTable().GroupByAgg([]string{"g"}, statsAggs())
	want := []string{"g", "min", "max", "median", "p25", "var", "std", "distinct"}
	assertEqual(t, len(result.Headers), len(want))
	for i, h := range want {
		assertEqual(t, result.Headers[i], h)
	}
}

func TestMedian_EvenAndOdd(t *testing.T) {
	result := salesTable().GroupByAgg(
		[]string{"region"},
		[]AggDef{{Col: "median", Agg: Median("revenue")}},
	)
	// EU: 100 150 200 → 150; US: 50 300 → 175
	assertEqual(t, result.Rows[0].Get("median").UnwrapOr(""), "150")
	assertEqual(t, result.Rows[1].Get("median").UnwrapOr(""), "175")
}

func TestQuantile_Bounds(t *testing.T) {
	result := salesTable().GroupByAgg(
		[]string{"region"},
		[]AggDef{
			{Col: "q0", Agg: Quantile("revenue", 0)},
			{Col: "q1", Agg: Quantile("revenue", 1)},
			{Col: "q50", Agg: Quantile("revenue", 0.5)},
			{Col: "q75", Agg: Quantile("revenue", 0.75)},
		},
	)
	eu := result.Rows[0]
	assertEqual(t, eu.Get("q0").UnwrapOr(""), "100")
	assertEqual(t, eu.Get("q1").UnwrapOr(""), "200")
	assertEqual(t, eu.Get("q50").UnwrapOr(""), "150")
	assertEqual(t, eu.Get("q75").UnwrapOr(""), "175")
	us := result.Rows[1]
	assertEqual(t, us.Get("q50").UnwrapOr(""), "175")
	assertEqual(t, us.Get("q75").UnwrapOr(""), "237.5")
}

func TestQuantile_InvalidP(t *testing.T) {
	for _, p := range []float64{-0.01, 1.01, math.NaN(), math.Inf(-1)} {
		result := salesTable().GroupByAgg(
			[]string{"region"},
			[]AggDef{{Col: "q", Agg: Quantile("revenue", p)}},
		)
		assertEqual(t, result.HasErrs(), false)
		for _, row := range result.Rows {
			assertEqual(t, row.Get("q").UnwrapOr("x"), "")
		}
	}
}

func TestMinMax_Negative(t *testing.T) {
	tb := New([]string{"g", "v"}, [][]string{{"a", "-1.5"}, {"a", "-10"}, {"a", "3e2"}})
	result := tb.GroupByAgg([]string{"g"}, []AggDef{
		{Col: "min", Agg: Min("v")},
		{Col: "max", Agg: Max("v")},
	})
	assertEqual(t, result.Rows[0].Get("min").UnwrapOr(""), "-10")
	assertEqual(t, result.Rows[0].Get("max").UnwrapOr(""), "300")
}

func TestCountDistinct_RawStrings(t *testing.T) {
	tb := New([]string{"g", "v"}, [][]string{{"a", "1"}, {"a", "1.0"}, {"a", "1"}, {"a", ""}, {"a", " "}})
	result := tb.GroupByAgg([]string{"g"}, []AggDef{{Col: "n", Agg: CountDistinct("v")}})
	// "1", "1.0" and " " are distinct; "" is skipped like Count does.
	assertEqual(t, result.Rows[0].Get("n").UnwrapOr(""), "3")
}

func TestStatsAggs_MissingCol(t *testing.T) {
	aggs := []AggDef{
		{Col: "min", Agg: Min("nonexistent")},
		{Col: "max", Agg: Max("nonexistent")},
		{Col: "median", Agg: Median("nonexistent")},
		{Col: "q", Agg: Quantile("nonexistent", 0.9)},
		{Col: "var", Agg: Var("nonexistent")},
		{Col: "std", Agg: StdDev("nonexistent")},
		{Col: "distinct", Agg: CountDistinct("nonexistent")},
	}
	result := salesTable().GroupByAgg([]string{"region"}, aggs)
	assertEqual(t, result.HasErrs(), false)
	row := result.Rows[0]
	for _, col := range []string{"min", "max", "median", "q", "var", "std"} {
		assertEqual(t, row.Get(col).UnwrapOr("x"), "")
	}
	assertEqual(t, row.Get("distinct").UnwrapOr(""), "0")
}

func TestRollingAgg_StatsAggs(t *testing.T) {
	tb := New([]string{"v"}, [][]string{{"3"}, {"1"}, {"x"}, {"2"}, {"1"}})
	cases := []struct {
		agg  Agg
		want []string
	}{
		{Min("v"), []string{"3", "1", "1", "1", "1"}},
		{Max("v"), []string{"3", "3", "3", "2", "2"}},
		{Median("v"), []string{"3", "2", "2", "1.5", "1.5"}},
		{Quantile("v", 1), []string{"3", "3", "3", "2", "2"}},
		{Var("v"), []string{"0", "1", "1", "0.25", "0.25"}},
		{StdDev("v"), []string{"0", "1", "1", "0.5", "0.5"}},
		{CountDistinct("v"), []string{"1", "2", "3", "3", "3"}},
	}
	for i, c := range cases {
		result := tb.RollingAgg("r", 3, c.agg)
		mresult := tb.Mutable().RollingAgg("r", 3, c.agg).Freeze()
		for j, want := range c.want {
			if got := result.Rows[j].Get("r").UnwrapOr(""); got != want {
				t.Errorf("case %d row %d: got %q, want %q", i, j, got, want)
			}
			if got := mresult.Rows[j].Get("r").UnwrapOr(""); got != want {
				t.Errorf("mutable case %d row %d: got %q, want %q", i, j, got, want)
			}
		}
	}
}

func BenchmarkGroupByAgg_Stats(b *testing.B) {
	cases := []struct {
		name string
		agg  Agg
	}{
		{"Sum", Sum("revenue")},
		{"Min", Min("revenue")},
		{"Median", Median("revenue")},
		{"Quantile", Quantile("revenue", 0.95)},
		{"StdDev", StdDev("revenue")},
		{"CountDistinct", CountDistinct("name")},
	}
	tb := benchTable(50_000)
	for _, c := range cases {
		aggs := []AggDef{{Col: "out", Agg: c.agg}}
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = tb.GroupByAgg([]string{"city"}, aggs)
			}
		})
	}
}

func BenchmarkMutableGroupByAgg_Stats(b *testing.B) {
	headers, records := benchmarkMutableRecords(50_000)
	aggs := []AggDef{
		{Col: "min", Agg: Min("revenue")},
		{Col: "median", Agg: Median("revenue")},
		{Col: "std", Agg: StdDev("revenue")},
		{Col: "distinct", Agg: CountDistinct("name")},
	}
	benchMutableOp(b, benchFixture{src: NewMutable(headers, records)}, func(m *MutableTable) {
		m.GroupByAgg([]string{"city"}, aggs)
	})
}
