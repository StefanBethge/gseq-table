package gtable

import (
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/testutil"
)

func TestNumbersWidenAndDivisionIsFloat(t *testing.T) {
	testutil.Proves(t, "T45")

	tbl := NewTable(Ints("i", 7, math.MaxInt64), Ints("two", 2, 2), Floats("f", 1.5, 1))

	mul := tbl.With("x", Col("i").Mul(Col("f")))
	if c, _ := mul.Column("x"); c.Type() != TypeFloat {
		t.Errorf("int * float is %v, want float", c.Type())
	}
	wantCells(t, mul, "x", "10.5", "9223372036854776000")

	sum := tbl.Select("two").With("x", Col("two").Add(Lit(3)))
	if c, _ := sum.Column("x"); c.Type() != TypeInt {
		t.Errorf("int + int is %v, want int", c.Type())
	}
	wantCells(t, sum, "x", "5", "5")

	wantCells(t, tbl.With("x", Col("i").Div(Col("two"))), "x", "3.5", "4611686018427388000")

	// An integer overflow rejects the row with code expr (D54).
	over := tbl.With("x", Col("i").Add(Col("two")))
	wantCells(t, over, "x", "9")
	if rs := over.Rejects(); len(rs) != 1 || rs[0].Code != CodeExpr || !strings.Contains(rs[0].Reason, "overflow") {
		t.Errorf("rejects = %v", rs)
	}

	// Text plus a number is a plan error.
	wantPlanError(t, tbl.With("x", Col("i").Add(Lit("1"))).Err())
}

func TestExprRuntimeFailureRejectsWithExprUnlessOrNull(t *testing.T) {
	tbl := NewTable(Ints("a", 6, 1), Ints("b", 3, 0))
	div := tbl.With("q", Col("a").Div(Col("b")))
	wantCells(t, div, "q", "2")
	rs := div.Rejects()
	if len(rs) != 1 || rs[0].Code != CodeExpr || rs[0].Column != "q" || rs[0].Reason != "division by zero: 1 / 0" {
		t.Fatalf("rejects = %v", rs)
	}

	// OrNull turns the failure into null (D54).
	orNull := tbl.With("q", Col("a").Div(Col("b")).OrNull())
	wantCells(t, orNull, "q", "2", "<null>")
	if len(orNull.Rejects()) != 0 {
		t.Errorf("rejects = %v", orNull.Rejects())
	}

	// A failing filter condition rejects the row too.
	w := tbl.Where(Col("a").Mod(Col("b")).Eq(Lit(0)))
	wantCells(t, w, "a", "6")
	if !slices.Equal(codes(w.Rejects()), []string{CodeExpr}) {
		t.Errorf("rejects = %v", w.Rejects())
	}
	wantPlanError(t, tbl.Where(Col("a")).Err())
}

func TestArithmetic(t *testing.T) {
	tbl := NewTable(Ints("i", -7, math.MinInt64), Floats("f", -2.25, 4e3))
	wantCells(t, tbl.With("x", Col("i").Sub(Lit(1))).Select("x"), "x", "-8")
	wantCells(t, tbl.With("x", Col("i").Neg()), "x", "7")
	wantCells(t, tbl.With("x", Col("i").Abs()), "x", "7")
	wantCells(t, tbl.With("x", Col("i").Mod(Lit(4))), "x", "-3", "0")
	wantCells(t, tbl.With("x", Col("f").Abs()), "x", "2.25", "4000")
	wantCells(t, tbl.With("x", Col("f").Neg()), "x", "2.25", "-4000")
	wantCells(t, tbl.With("x", Col("f").Round(1)), "x", "-2.3", "4000")
	wantCells(t, tbl.With("x", Col("i").Round(1).OrNull()), "x", "-7", "-9223372036854775808")
	big := NewTable(Floats("f", -2.25, math.MaxFloat64))
	wantCells(t, big.With("x", Col("f").Mul(Lit(10.0))).Select("x"), "x", "-22.5") // overflows to +Inf
	if c, _ := big.With("x", Col("f").Round(2)).Column("x"); c.Len() != 2 {
		t.Error("Round of a huge float failed")
	}
	wantCells(t, tbl.With("x", Col("f").Mod(Lit(2.0))), "x", "-0.25", "0")
	wantCells(t, tbl.With("x", Col("i").Mul(Lit(2))), "x", "-14")
	wantCells(t, tbl.With("x", Col("i").Mul(Lit(-1))), "x", "7")
	wantPlanError(t, tbl.With("x", Col("f").Round(-1)).Err())
	wantPlanError(t, tbl.With("x", Lit([]int{1})).Err())
	wantPlanError(t, tbl.With("x", Expr{}).Err())
	wantPlanError(t, tbl.With("", Col("i")).Err())
}

func TestIntOverflowDetection(t *testing.T) {
	cases := []struct {
		sym  string
		x, y int64
		ok   bool
	}{
		{"+", math.MaxInt64, 1, false},
		{"+", math.MinInt64, -1, false},
		{"+", -1, 1, true},
		{"-", math.MinInt64, 1, false},
		{"-", math.MaxInt64, -1, false},
		{"-", 0, math.MinInt64, false},
		{"*", math.MaxInt64, 2, false},
		{"*", math.MinInt64, -1, false},
		{"*", -1, math.MinInt64, false},
		{"*", 1 << 31, 1 << 31, true},
		{"*", 0, math.MinInt64, true},
	}
	for _, c := range cases {
		if _, ok := intOp(c.sym, c.x, c.y); ok != c.ok {
			t.Errorf("%d %s %d: ok = %v, want %v", c.x, c.sym, c.y, ok, c.ok)
		}
	}
}

func TestComparisons(t *testing.T) {
	d1 := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	tbl := NewTable(
		Ints("i", 1, 2, 3),
		Floats("f", 2, 2, 2),
		Texts("s", "a", "b", "c"),
		Bools("b", false, true, true),
		Timestamps("t", d1, d1.AddDate(0, 0, 1), d1.AddDate(0, 0, 2)),
	)
	wantCells(t, tbl.Where(Col("i").Lt(Col("f"))), "i", "1")
	wantCells(t, tbl.Where(Col("i").Le(Col("f"))), "i", "1", "2")
	wantCells(t, tbl.Where(Col("i").Eq(Col("f"))), "i", "2")
	wantCells(t, tbl.Where(Col("i").Ne(Lit(2))), "i", "1", "3")
	wantCells(t, tbl.Where(Col("s").Ge(Lit("b"))), "i", "2", "3")
	wantCells(t, tbl.Where(Col("b").Gt(Lit(false))), "i", "2", "3")
	wantCells(t, tbl.Where(Col("b").Eq(Lit(false))), "i", "1")
	wantCells(t, tbl.Where(Col("t").Gt(Lit(d1))), "i", "2", "3")
	wantPlanError(t, tbl.Where(Col("s").Eq(Lit(1))).Err())
	wantPlanError(t, tbl.Where(Col("i").And(Col("b"))).Err())
}

func TestTextAndDateFunctions(t *testing.T) {
	d := time.Date(2026, 2, 27, 0, 0, 0, 0, time.UTC)
	tbl := NewTable(Texts("s", "  Hello Wörld "), Timestamps("d", d))
	wantCells(t, tbl.With("x", Col("s").Trim()), "x", "Hello Wörld")
	wantCells(t, tbl.With("x", Col("s").Trim().Lower()), "x", "hello wörld")
	wantCells(t, tbl.With("x", Col("s").Trim().Upper()), "x", "HELLO WÖRLD")
	wantCells(t, tbl.With("x", Col("s").Trim().Replace("l", "L")), "x", "HeLLo WörLd")
	wantCells(t, tbl.With("x", Col("s").Trim().Len()), "x", "11")
	wantCells(t, tbl.With("x", Col("s").Contains("Wö")), "x", "true")
	wantCells(t, tbl.With("x", Col("s").HasPrefix("  H")), "x", "true")
	wantCells(t, tbl.With("x", Col("s").HasSuffix("x")), "x", "false")
	wantCells(t, tbl.With("x", Concat(Lit("<"), Col("s").Trim(), Lit(">"))), "x", "<Hello Wörld>")
	wantCells(t, tbl.With("x", Col("d").Year()), "x", "2026")
	wantCells(t, tbl.With("x", Col("d").Month()), "x", "2")
	wantCells(t, tbl.With("x", Col("d").Day()), "x", "27")
	wantCells(t, tbl.With("x", Col("d").AddDays(3).Format("2006-01-02")), "x", "2026-03-02")
	wantCells(t, tbl.With("x", Col("d").AddDays(3).DiffDays(Col("d"))), "x", "3")
	wantPlanError(t, tbl.With("x", Col("d").Trim()).Err())
	wantPlanError(t, tbl.With("x", Concat(Col("s"), Col("d"))).Err())
	wantPlanError(t, tbl.With("x", Col("s").Year()).Err())
}

func TestExprString(t *testing.T) {
	for e, want := range map[*Expr]string{
		ptr(Col("a")):                      `Col("a")`,
		ptr(Lit(1)):                        "Lit(1)",
		ptr(Col("a").Add(Lit(1))):          "Add(…)",
		ptr(Col("a").Add(Lit(1)).OrNull()): "Add(…).OrNull()",
		ptr(Expr{}):                        "Expr{}",
	} {
		if e.String() != want {
			t.Errorf("String() = %q, want %q", e.String(), want)
		}
	}
}

func ptr[T any](v T) *T { return &v }
