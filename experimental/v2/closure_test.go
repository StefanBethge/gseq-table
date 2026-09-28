package gtable

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/testutil"
)

func TestClosureErrorRejectsRowWithCodeCustom(t *testing.T) {
	testutil.Proves(t, "T28")

	tbl := NewTable(Texts("customer", "ok", "bad", "boom", "vip", "fine"))
	got := tbl.Apply(WithFunc("label", func(r Row) (string, error) {
		c, _ := r.Text("customer")
		switch c {
		case "bad":
			return "", errors.New("customer is blocked")
		case "boom":
			var m map[string]int
			m["x"] = 1 // panics
		case "vip":
			return "", &CustomError{Code: "vip_check", Err: errors.New("needs a manual check")}
		}
		return "L-" + c, nil
	}))

	wantCells(t, got, "label", "L-ok", "L-fine")
	rs := got.Rejects()
	if !slices.Equal(codes(rs), []string{"custom", "custom", "custom:vip_check"}) {
		t.Fatalf("codes = %v", codes(rs))
	}
	if rs[0].Reason != "customer is blocked" || rs[0].Step != "with_func" || rs[0].Column != "label" {
		t.Errorf("returned error: %+v", rs[0])
	}
	if !strings.Contains(rs[1].Reason, "panic") || !strings.Contains(rs[1].Reason, "nil map") {
		t.Errorf("panic reason = %q, want the panic message (D39)", rs[1].Reason)
	}
	if rs[2].Reason != "needs a manual check" {
		t.Errorf("typed error reason = %q", rs[2].Reason)
	}
}

func TestWhereFuncRejectsOnErrorAndPanic(t *testing.T) {
	tbl := NewTable(Ints("n", 1, 2, 3, 4))
	got := tbl.WhereFunc(func(r Row) (bool, error) {
		n, _ := r.Int("n")
		switch n {
		case 2:
			return false, errors.New("two")
		case 3:
			r.Text("n") // wrong type: panics
		}
		return n != 4, nil
	})
	wantCells(t, got, "n", "1")
	if !slices.Equal(codes(got.Rejects()), []string{"custom", "custom"}) {
		t.Errorf("rejects = %v", got.Rejects())
	}
}

func TestWithFuncTypesAndPlanErrors(t *testing.T) {
	tbl := NewTable(Ints("n", 2))
	got := tbl.Apply(WithFunc("half", func(r Row) (float64, error) {
		n, _ := r.Int("n")
		return float64(n) / 4, nil
	}))
	wantCells(t, got, "half", "0.5")
	if c, _ := got.Column("half"); c.Type() != TypeFloat {
		t.Errorf("type = %v", c.Type())
	}
	wantPlanError(t, tbl.Apply(WithFunc[int64]("", nil)).Err())
	wantPlanError(t, tbl.Apply(WithFunc[int64]("x", nil)).Err())
	wantPlanError(t, tbl.WhereFunc(nil).Err())
}

func TestCustomErrorWithoutCode(t *testing.T) {
	if got := customCode(&CustomError{Err: errors.New("x")}); got != CodeCustom {
		t.Errorf("code = %q", got)
	}
	if (&CustomError{Code: "c"}).Error() != "c" {
		t.Error("Error() without Err")
	}
}

func TestRowAccessorsAndClosureTypes(t *testing.T) {
	d := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	tbl := NewTable(Floats("f", 1.5), Bools("b", true), Timestamps("t", d))
	got := tbl.
		Apply(WithFunc("f2", func(r Row) (float64, error) { v, _ := r.Float("f"); return v * 2, nil })).
		Apply(WithFunc("nb", func(r Row) (bool, error) { v, _ := r.Bool("b"); return !v, nil })).
		Apply(WithFunc("t2", func(r Row) (time.Time, error) { v, _ := r.Timestamp("t"); return v.AddDate(1, 0, 0), nil })).
		Apply(WithFunc("cols", func(r Row) (int64, error) { return int64(len(r.Columns())), nil }))
	wantCells(t, got, "f2", "3")
	wantCells(t, got, "nb", "false")
	wantCells(t, got, "t2", "2027-09-27T00:00:00Z")
	wantCells(t, got, "cols", "6")
}

func TestErrorTexts(t *testing.T) {
	r := Reject{Step: "cast", Column: "a", Value: "x", HasValue: true, Reason: "bad", Code: CodeParse}
	if got := r.String(); got != `step=cast column=a value="x" code=parse reason=bad` {
		t.Errorf("String() = %s", got)
	}
	if got := (Reject{Step: "with"}).String(); !strings.Contains(got, "value=null") {
		t.Errorf("String() = %s", got)
	}
	de := &DataError{Reject: r}
	if !strings.HasPrefix(de.Error(), "data error: step=cast") {
		t.Errorf("DataError = %s", de.Error())
	}
	inner := errors.New("inner")
	if !errors.Is(&PlanError{Step: "s", Err: inner}, inner) || !errors.Is(&CustomError{Code: "c", Err: inner}, inner) {
		t.Error("Unwrap")
	}
}
