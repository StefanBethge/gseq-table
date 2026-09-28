package gtable

import (
	"math"
	"slices"
	"testing"
	"time"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/testutil"
)

func TestSortPutsNullsLastAndIsStable(t *testing.T) {
	testutil.Proves(t, "T46")

	tbl := NewTable(
		Ints("k", 2, 0, 1, 2, 0, 1).WithNulls(1, 4),
		Texts("id", "a", "b", "c", "d", "e", "f"),
	)
	wantCells(t, tbl.Sort(Asc("k")), "id", "c", "f", "a", "d", "b", "e")
	wantCells(t, tbl.Sort(Desc("k")), "id", "a", "d", "c", "f", "b", "e")
	// A second key orders ties; nulls stay last within them.
	wantCells(t, tbl.Sort(Asc("k"), Desc("id")), "id", "f", "c", "d", "a", "e", "b")
}

func TestSortPlanErrors(t *testing.T) {
	wantPlanError(t, orders().Sort().Err())
	wantPlanError(t, orders().Sort(Asc("x")).Err())
}

func TestCast(t *testing.T) {
	tbl := NewTable(
		Texts("i", "42", "-1", "4.2"),
		Texts("f", "1.5", "NaN", "2"),
		Texts("b", "true", "0", "ja"),
		Texts("d", "2026-09-27", "2026-09-27T10:00:00Z", "27.09.2026"),
		Texts("de", "27.09.2026", "", "-"),
	)
	ci := tbl.Cast("i", TypeInt)
	wantCells(t, ci, "i", "42", "-1")
	if r := ci.Rejects()[0]; r.Value != "4.2" || r.Code != CodeParse {
		t.Errorf("reject = %+v", r)
	}
	wantCells(t, tbl.Cast("f", TypeFloat), "f", "1.5", "2")
	wantCells(t, tbl.Cast("b", TypeBool), "b", "true", "false")
	wantCells(t, tbl.Cast("d", TypeTimestamp), "d", "2026-09-27T00:00:00Z", "2026-09-27T10:00:00Z")
	de := tbl.Cast("de", TypeTimestamp, DateFormat("02.01.2006"), NullTexts("-"))
	wantCells(t, de, "de", "2026-09-27T00:00:00Z", "<null>", "<null>")

	// Casts between types.
	typed := NewTable(
		Ints("i", 3), Floats("f", 2), Floats("g", 2.5),
		Timestamps("t", time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)), Bools("b", true),
	)
	wantCells(t, typed.Cast("i", TypeFloat), "i", "3")
	wantCells(t, typed.Cast("f", TypeInt), "f", "2")
	wantCells(t, typed.Cast("g", TypeInt), "g")
	wantCells(t, typed.Cast("i", TypeText), "i", "3")
	wantCells(t, typed.Cast("b", TypeText), "b", "true")
	wantCells(t, typed.Cast("t", TypeText, DateFormat("02.01.2006")), "t", "27.09.2026")
	wantCells(t, typed.Cast("i", TypeInt), "i", "3")
	wantCells(t, NewTable(Texts("s", "", "NULL")).Cast("s", TypeText, NullTexts("NULL")), "s", "", "<null>")
	wantPlanError(t, typed.Cast("b", TypeInt).Err())
	wantPlanError(t, typed.Cast("t", TypeFloat).Err())
	wantPlanError(t, typed.Cast("x", TypeInt).Err())
	wantPlanError(t, typed.Cast("i", Type(99)).Err())
}

func TestGroupByAggregations(t *testing.T) {
	tbl := NewTable(
		Texts("region", "n", "s", "n", "n", "s"),
		Ints("qty", 1, 5, 3, 3, 7),
		Floats("price", 1, 2, 3, 4, 5),
		Texts("tag", "a", "b", "c", "a", "d"),
	).With("region", Col("region")) // keep the key a text column

	g := tbl.GroupBy([]string{"region"},
		Sum("qty"),
		Sum("price").As("sum_price"),
		Mean("qty").As("mean"),
		Count("tag").As("n"),
		CountDistinct("tag").As("distinct"),
		StringJoin("tag", "|").As("tags"),
		First("tag").As("first"),
		Last("tag").As("last"),
		Min("tag").As("min"),
		Max("qty").As("max"),
		Median("price").As("median"),
		Quantile("price", 0.25).As("q25"),
		Var("qty").As("var"),
		StdDev("qty").As("sd"),
	)
	wantCells(t, g, "region", "n", "s")
	wantCells(t, g, "qty", "7", "12")
	wantCells(t, g, "sum_price", "8", "7")
	wantCells(t, g, "mean", "2.3333333333333335", "6")
	wantCells(t, g, "n", "3", "2")
	wantCells(t, g, "distinct", "2", "2")
	wantCells(t, g, "tags", "a|c|a", "b|d")
	wantCells(t, g, "first", "a", "b")
	wantCells(t, g, "last", "a", "d")
	wantCells(t, g, "min", "a", "b")
	wantCells(t, g, "max", "3", "7")
	wantCells(t, g, "median", "3", "3.5")
	wantCells(t, g, "q25", "2", "2.75")
	wantCells(t, g, "var", "0.8888888888888888", "1")
	wantCells(t, g, "sd", "0.9428090415820634", "1")

	// Null keys form one group, as in SQL.
	nk := NewTable(Ints("k", 1, 0, 0).WithNulls(1, 2), Ints("v", 1, 2, 3)).GroupBy([]string{"k"}, Sum("v"))
	wantCells(t, nk, "k", "1", "<null>")
	wantCells(t, nk, "v", "1", "5")

	// An integer overflow in Sum rejects the group's row with code expr.
	over := NewTable(Texts("k", "a", "a", "b"), Ints("v", math.MaxInt64, 1, 1)).GroupBy([]string{"k"}, Sum("v"))
	wantCells(t, over, "k", "b")
	if !slices.Equal(codes(over.Rejects()), []string{CodeExpr}) {
		t.Errorf("rejects = %v", over.Rejects())
	}

	// Over no values: null, and 0 for Count.
	empty := NewTable(Texts("k", "a"), Ints("v", 0).WithNulls(0)).GroupBy([]string{"k"}, Sum("v"), Count("v").As("n"))
	wantCells(t, empty, "v", "<null>")
	wantCells(t, empty, "n", "0")
}

func TestGroupByPlanErrors(t *testing.T) {
	tbl := NewTable(Texts("k", "a"), Texts("s", "x"), Bools("b", true))
	for name, op := range map[string]Op{
		"no key":        GroupBy(nil, Count("s")),
		"unknown key":   GroupBy([]string{"x"}),
		"repeated key":  GroupBy([]string{"k", "k"}),
		"unknown col":   GroupBy([]string{"k"}, Sum("x")),
		"sum of text":   GroupBy([]string{"k"}, Sum("s")),
		"join of bool":  GroupBy([]string{"k"}, StringJoin("b", ",")),
		"max of bool":   GroupBy([]string{"k"}, Max("b")),
		"bad quantile":  GroupBy([]string{"k"}, Quantile("s", 2)),
		"name clash":    GroupBy([]string{"k"}, Count("k")),
		"empty agg":     GroupBy([]string{"k"}, Agg{}),
		"empty op":      {},
		"quantile type": GroupBy([]string{"k"}, Quantile("s", 0.5)),
	} {
		if _, ok := tbl.Apply(op).Err().(*PlanError); !ok {
			t.Errorf("%s: Err = %v, want a plan error", name, tbl.Apply(op).Err())
		}
	}
}

func TestOperationsOnEmptyTables(t *testing.T) {
	tbl := NewTable(Texts("k", "a"), Ints("v", 1)).Where(Lit(false))
	if tbl.Len() != 0 {
		t.Fatalf("Len = %d", tbl.Len())
	}
	for _, got := range []Table{
		tbl.Sort(Asc("k")),
		tbl.GroupBy([]string{"k"}, Sum("v")),
		tbl.InnerJoin(NewTable(Texts("k", "a")), On("k")),
		tbl.Cast("v", TypeText),
	} {
		if got.Err() != nil || got.Len() != 0 {
			t.Errorf("Err=%v Len=%d", got.Err(), got.Len())
		}
	}
}
