package gtable

import (
	"testing"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/testutil"
)

func TestNullsAreSeparateFromEmptyTextAndBehaveLikeSQL(t *testing.T) {
	testutil.Proves(t, "T27")

	raw := NewTable(
		Texts("k", "a", "a", "b", "b"),
		Texts("qty", "4", "", "n/a", "6"),
	)
	// A raw column keeps an empty field as empty text, not null (D30).
	c, _ := raw.Column("qty")
	if v, ok := c.Text(1); !ok || v != "" || c.NullCount() != 0 {
		t.Fatalf("raw cell = %q, %v; NullCount = %d", v, ok, c.NullCount())
	}

	// Casting makes empty text and the configured null texts null.
	typed := raw.Cast("qty", TypeInt, NullTexts("n/a"))
	wantCells(t, typed, "qty", "4", "<null>", "<null>", "6")
	if len(typed.Rejects()) != 0 {
		t.Fatalf("rejects = %v", typed.Rejects())
	}

	// Arithmetic with null is null.
	wantCells(t, typed.With("x", Col("qty").Add(Lit(1))), "x", "5", "<null>", "<null>", "7")

	// A comparison with null is not true: the filter drops the row.
	wantCells(t, typed.Where(Col("qty").Gt(Lit(0))), "qty", "4", "6")
	wantCells(t, typed.Where(Col("qty").Gt(Lit(0)).Not()), "qty")
	wantCells(t, typed.With("eq", Col("qty").Eq(Col("qty"))), "eq", "true", "<null>", "<null>", "true")

	// Aggregations skip nulls.
	g := typed.GroupBy([]string{"k"},
		Sum("qty"), Count("qty").As("n"), Mean("qty").As("mean"), Min("qty").As("min"), First("qty").As("first"))
	wantCells(t, g, "k", "a", "b")
	wantCells(t, g, "qty", "4", "6")
	wantCells(t, g, "n", "1", "1")
	wantCells(t, g, "mean", "4", "6")
	wantCells(t, g, "min", "4", "6")
	wantCells(t, g, "first", "4", "6")
}

func TestThreeValuedLogic(t *testing.T) {
	tbl := NewTable(
		Bools("a", true, false, true, false, true).WithNulls(4),
		Bools("b", true, true, false, false, false).WithNulls(2, 3),
	)
	// b is null in rows 2 and 3, a is null in row 4.
	wantCells(t, tbl.With("and", Col("a").And(Col("b"))), "and", "true", "false", "<null>", "false", "false")
	wantCells(t, tbl.With("or", Col("a").Or(Col("b"))), "or", "true", "true", "true", "<null>", "<null>")
	wantCells(t, tbl.With("n", Col("a").IsNull()), "n", "false", "false", "false", "false", "true")
	wantCells(t, tbl.With("n", Col("b").IsNotNull()), "n", "true", "true", "false", "false", "true")
}

func TestCoalesce(t *testing.T) {
	tbl := NewTable(Ints("a", 1, 0).WithNulls(1), Floats("b", 9, 2.5))
	wantCells(t, tbl.With("c", Coalesce(Col("a"), Col("b"))), "c", "1", "2.5")
	wantPlanError(t, tbl.With("c", Coalesce(Col("a"), Lit("x"))).Err())
}
