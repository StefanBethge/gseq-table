package table

import (
	"slices"
	"testing"
)

// windowTable is intentionally interleaved and unsorted by date so tests
// verify both partitioning and that the original row order is preserved.
func windowTable() Table {
	return New([]string{"customer", "date", "revenue"}, [][]string{
		{"a", "03", "30"},
		{"b", "01", "5"},
		{"a", "01", "10"},
		{"b", "02", "7"},
		{"a", "02", "20"},
	})
}

func assertCol(t *testing.T, tb Table, col string, want ...string) {
	t.Helper()
	got := []string(tb.Col(col))
	if !slices.Equal(got, want) {
		t.Errorf("column %q: got %q, want %q", col, got, want)
	}
}

func assertWindowOrderPreserved(t *testing.T, tb Table) {
	t.Helper()
	assertCol(t, tb, "customer", "a", "b", "a", "b", "a")
	assertCol(t, tb, "date", "03", "01", "01", "02", "02")
}

func TestWindow_CumSum(t *testing.T) {
	out := windowTable().PartitionBy("customer").CumSum("revenue", "cum")
	assertWindowOrderPreserved(t, out)
	assertCol(t, out, "cum", "30", "5", "40", "12", "60")
	assertEqual(t, len(out.Errs()), 0)
}

func TestWindow_CumSum_OrderBy(t *testing.T) {
	out := windowTable().PartitionBy("customer").OrderBy(Asc("date")).CumSum("revenue", "cum")
	assertWindowOrderPreserved(t, out)
	assertCol(t, out, "cum", "60", "5", "10", "12", "30")
}

func TestWindow_CumSum_FloatTransition(t *testing.T) {
	tb := New([]string{"k", "v"}, [][]string{{"x", "1"}, {"y", "2"}, {"x", "0.5"}, {"y", "x"}, {"x", "2"}})
	out := tb.PartitionBy("k").CumSum("v", "cum")
	assertCol(t, out, "cum", "1", "2", "1.5", "2", "3.5")
}

func TestWindow_Lag(t *testing.T) {
	out := windowTable().PartitionBy("customer").Lag("revenue", "prev", 1)
	assertWindowOrderPreserved(t, out)
	assertCol(t, out, "prev", "", "", "30", "5", "10")
}

func TestWindow_Lag_OrderBy(t *testing.T) {
	out := windowTable().PartitionBy("customer").OrderBy(Asc("date")).Lag("revenue", "prev", 1)
	assertWindowOrderPreserved(t, out)
	assertCol(t, out, "prev", "20", "", "", "5", "10")
}

func TestWindow_Lag_NegativeN(t *testing.T) {
	out := windowTable().PartitionBy("customer").Lag("revenue", "prev", -3)
	assertCol(t, out, "prev", "30", "5", "10", "7", "20")
}

func TestWindow_Lead_OrderByDesc(t *testing.T) {
	out := windowTable().PartitionBy("customer").OrderBy(Desc("date")).Lead("revenue", "next", 1)
	assertWindowOrderPreserved(t, out)
	// a desc: 03(30) → 02(20) → 01(10); b desc: 02(7) → 01(5)
	assertCol(t, out, "next", "20", "", "", "5", "10")
}

func TestWindow_Lead_N2(t *testing.T) {
	out := windowTable().PartitionBy("customer").OrderBy(Asc("date")).Lead("revenue", "next2", 2)
	assertCol(t, out, "next2", "", "", "30", "", "")
}

func TestWindow_Rank(t *testing.T) {
	tb := New([]string{"g", "score"}, [][]string{
		{"x", "10"}, {"y", "3"}, {"x", "30"}, {"y", "3"}, {"x", "10"}, {"y", "bad"}, {"y", "1.5"},
	})
	out := tb.PartitionBy("g").Rank("score", "rank", false)
	assertCol(t, out, "rank", "2", "1", "1", "1", "2", "", "2")
}

func TestWindow_RollingAgg(t *testing.T) {
	out := windowTable().PartitionBy("customer").OrderBy(Asc("date")).RollingAgg("roll", 2, Sum("revenue"))
	assertWindowOrderPreserved(t, out)
	assertCol(t, out, "roll", "50", "5", "10", "12", "30")
}

func TestWindow_RollingAgg_SizeLessThanOne(t *testing.T) {
	out := windowTable().PartitionBy("customer").RollingAgg("roll", 0, Sum("revenue"))
	assertCol(t, out, "roll", "30", "5", "10", "7", "20")
}

func TestWindow_MultiColumnPartition(t *testing.T) {
	tb := New([]string{"a", "b", "v"}, [][]string{
		{"x", "1", "1"}, {"x", "2", "2"}, {"x", "1", "3"}, {"x", "2", "4"},
	})
	out := tb.PartitionBy("a", "b").CumSum("v", "cum")
	assertCol(t, out, "cum", "1", "2", "4", "6")
}

func TestWindow_NoPartitionCols_WholeTable(t *testing.T) {
	tb := windowTable()
	assertCol(t, tb.PartitionBy().CumSum("revenue", "cum"), "cum",
		[]string(tb.CumSum("revenue", "cum").Col("cum"))...)
	// Ordering without partitions: a global sorted window, original order kept.
	out := tb.PartitionBy().OrderBy(Asc("date"), Asc("customer")).Lag("revenue", "prev", 1)
	// sorted: (a,01,10) (b,01,5) (a,02,20) (b,02,7) (a,03,30)
	assertCol(t, out, "prev", "7", "10", "", "20", "5")
}

func TestWindow_EmptyTable(t *testing.T) {
	tb := New([]string{"k", "v"}, nil)
	out := tb.PartitionBy("k").CumSum("v", "cum")
	assertEqual(t, out.Len(), 0)
	assertEqual(t, len(out.Headers), 3)
}

func TestWindow_ShortRows(t *testing.T) {
	tb := New([]string{"k", "v"}, [][]string{{"x", "1"}, {"x"}, {}, {"x", "2"}})
	out := tb.PartitionBy("k").CumSum("v", "cum")
	assertCol(t, out, "cum", "1", "1", "0", "3")
	assertEqual(t, len(out.Rows[1].Values()), 3)
}

func TestWindow_DoesNotMutateSource(t *testing.T) {
	tb := windowTable()
	w := tb.PartitionBy("customer").OrderBy(Asc("date"))
	_ = w.CumSum("revenue", "cum")
	_ = w.Lag("revenue", "prev", 1)
	assertEqual(t, len(tb.Headers), 3)
	assertWindowOrderPreserved(t, tb)
}

func TestWindow_WindowReusable(t *testing.T) {
	w := windowTable().PartitionBy("customer")
	a := w.CumSum("revenue", "cum")
	b := w.OrderBy(Asc("date")).CumSum("revenue", "cum")
	assertCol(t, a, "cum", "30", "5", "40", "12", "60")
	assertCol(t, b, "cum", "60", "5", "10", "12", "30")
	// OrderBy returned a copy; w itself is still unordered.
	assertCol(t, w.CumSum("revenue", "cum"), "cum", "30", "5", "40", "12", "60")
}

func TestWindow_PreservesSource(t *testing.T) {
	out := windowTable().WithSource("sales.csv").PartitionBy("customer").CumSum("revenue", "cum")
	assertEqual(t, out.Source(), "sales.csv")
}

// ── MutableTable ─────────────────────────────────────────────────────────────

func windowMutable() *MutableTable {
	tb := windowTable()
	records := make([][]string, len(tb.Rows))
	for i, r := range tb.Rows {
		records[i] = r.Values()
	}
	return NewMutable(tb.Headers, records)
}

func TestMutableWindow_MatchesImmutable(t *testing.T) {
	cases := []struct {
		name string
		imm  func(Window) Table
		mut  func(*MutableWindow) *MutableTable
	}{
		{"CumSum", func(w Window) Table { return w.CumSum("revenue", "out") },
			func(w *MutableWindow) *MutableTable { return w.CumSum("revenue", "out") }},
		{"Lag", func(w Window) Table { return w.Lag("revenue", "out", 1) },
			func(w *MutableWindow) *MutableTable { return w.Lag("revenue", "out", 1) }},
		{"Lead", func(w Window) Table { return w.Lead("revenue", "out", 1) },
			func(w *MutableWindow) *MutableTable { return w.Lead("revenue", "out", 1) }},
		{"Rank", func(w Window) Table { return w.Rank("revenue", "out", true) },
			func(w *MutableWindow) *MutableTable { return w.Rank("revenue", "out", true) }},
		{"RollingAgg", func(w Window) Table { return w.RollingAgg("out", 2, Mean("revenue")) },
			func(w *MutableWindow) *MutableTable { return w.RollingAgg("out", 2, Mean("revenue")) }},
	}
	for _, tc := range cases {
		for _, ordered := range []bool{false, true} {
			iw := windowTable().PartitionBy("customer")
			mw := windowMutable().PartitionBy("customer")
			if ordered {
				iw = iw.OrderBy(Desc("date"))
				mw = mw.OrderBy(Desc("date"))
			}
			want := []string(tc.imm(iw).Col("out"))
			m := tc.mut(mw)
			got := []string(m.Col("out"))
			if !slices.Equal(got, want) {
				t.Errorf("%s ordered=%v: got %q, want %q", tc.name, ordered, got, want)
			}
			assertEqual(t, len(m.Errs()), 0)
			assertWindowOrderPreserved(t, m.Freeze())
		}
	}
}

func TestMutableWindow_ReturnsSameTable(t *testing.T) {
	m := windowMutable()
	out := m.PartitionBy("customer").OrderBy(Asc("date")).CumSum("revenue", "cum")
	if out != m {
		t.Fatal("expected MutableWindow op to return the underlying MutableTable")
	}
	assertCol(t, m.Freeze(), "cum", "60", "5", "10", "12", "30")
}

func TestMutableWindow_ResolvesColumnsAtCallTime(t *testing.T) {
	m := windowMutable()
	w := m.PartitionBy("customer")
	m.Rename("customer", "cust").Rename("cust", "customer")
	m.Select("revenue", "customer", "date")
	assertCol(t, w.CumSum("revenue", "cum").Freeze(), "cum", "30", "5", "40", "12", "60")
}

// ── Benchmarks ───────────────────────────────────────────────────────────────

func BenchmarkWindowCumSum(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = tb.PartitionBy("city").CumSum("revenue", "cum")
			}
		})
	}
}

func BenchmarkWindowLag_OrderBy(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = tb.PartitionBy("city").OrderBy(Desc("name")).Lag("revenue", "prev", 7)
			}
		})
	}
}

func BenchmarkWindowRank(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = tb.PartitionBy("city").Rank("revenue", "rank", true)
			}
		})
	}
}

func BenchmarkWindowRollingAgg(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = tb.PartitionBy("city").RollingAgg("roll", 10, Sum("revenue"))
			}
		})
	}
}

func BenchmarkMutableWindowCumSum(b *testing.B) {
	for _, sz := range benchSizes {
		headers, records := benchRecords(sz.n)
		f := benchFixture{src: NewMutable(headers, records)}
		b.Run(sz.name, func(b *testing.B) {
			benchMutableOp(b, f, func(m *MutableTable) {
				m.PartitionBy("city").CumSum("revenue", "cum")
			})
		})
	}
}
