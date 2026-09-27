package etl_test

import (
	"slices"
	"testing"

	"github.com/stefanbethge/gseq-table/etl"
	"github.com/stefanbethge/gseq-table/table"
)

func windowSales() table.Table {
	return table.New([]string{"customer", "date", "revenue"}, [][]string{
		{"a", "03", "30"},
		{"b", "01", "5"},
		{"a", "01", "10"},
		{"b", "02", "7"},
		{"a", "02", "20"},
	})
}

func assertColEq(t *testing.T, tb table.Table, col string, want ...string) {
	t.Helper()
	if got := []string(tb.Col(col)); !slices.Equal(got, want) {
		t.Errorf("column %q: got %q, want %q", col, got, want)
	}
}

func TestWindowSpec_Pipeline(t *testing.T) {
	w := etl.PartitionBy("customer").OrderBy(table.Asc("date"))
	out := etl.From(windowSales()).
		Then(w.CumSum("revenue", "cum")).
		Then(w.Lag("revenue", "prev", 1)).
		Then(w.Lead("revenue", "next", 1)).
		Then(w.Rank("revenue", "rank", false)).
		Then(w.RollingAgg("roll", 2, table.Sum("revenue"))).
		Unwrap()
	assertColEq(t, out, "customer", "a", "b", "a", "b", "a")
	assertColEq(t, out, "cum", "60", "5", "10", "12", "30")
	assertColEq(t, out, "prev", "20", "", "", "5", "10")
	assertColEq(t, out, "next", "", "7", "20", "", "30")
	assertColEq(t, out, "rank", "1", "2", "3", "1", "2")
	assertColEq(t, out, "roll", "50", "5", "10", "12", "30")
}

func TestWindowSpec_Unordered(t *testing.T) {
	out := etl.From(windowSales()).Then(etl.PartitionBy("customer").CumSum("revenue", "cum")).Unwrap()
	assertColEq(t, out, "cum", "30", "5", "40", "12", "60")
}

func TestWindowSpec_OrderByCopies(t *testing.T) {
	base := etl.PartitionBy("customer")
	_ = base.OrderBy(table.Asc("date"))
	out := etl.From(windowSales()).Then(base.CumSum("revenue", "cum")).Unwrap()
	assertColEq(t, out, "cum", "30", "5", "40", "12", "60")
}

func TestMutWindowSpec_Pipeline(t *testing.T) {
	tb := windowSales()
	records := make([][]string, len(tb.Rows))
	for i, r := range tb.Rows {
		records[i] = r.Values()
	}
	w := etl.Mut.PartitionBy("customer").OrderBy(table.Asc("date"))
	out := etl.FromMutable(table.NewMutable(tb.Headers, records)).
		Then(w.CumSum("revenue", "cum")).
		Then(w.Lag("revenue", "prev", 1)).
		Then(w.Lead("revenue", "next", 1)).
		Then(w.Rank("revenue", "rank", false)).
		Then(w.RollingAgg("roll", 2, table.Sum("revenue"))).
		Then(etl.Mut.PartitionBy("customer").CumSum("revenue", "cum_unordered")).
		Frozen().
		Unwrap()
	assertColEq(t, out, "customer", "a", "b", "a", "b", "a")
	assertColEq(t, out, "cum", "60", "5", "10", "12", "30")
	assertColEq(t, out, "prev", "20", "", "", "5", "10")
	assertColEq(t, out, "next", "", "7", "20", "", "30")
	assertColEq(t, out, "rank", "1", "2", "3", "1", "2")
	assertColEq(t, out, "roll", "50", "5", "10", "12", "30")
	assertColEq(t, out, "cum_unordered", "30", "5", "40", "12", "60")
}
