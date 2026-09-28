package gtable

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/testutil"
)

func orders() Table {
	return NewTable(
		Texts("id", "1", "2", "3"),
		Texts("amount", "10.5", "abc", "7"),
	)
}

func TestTableCarriesRejectsAndStickyError(t *testing.T) {
	testutil.Proves(t, "T41")

	// A data error rejects the row; the table carries it (D50, D5).
	cast := orders().Cast("amount", TypeFloat)
	wantCells(t, cast, "id", "1", "3")
	rs := cast.Rejects()
	if len(rs) != 1 {
		t.Fatalf("rejects = %v, want one", rs)
	}
	if r := rs[0]; r.Step != "cast" || r.Column != "amount" || r.Value != "abc" || !r.HasValue || r.Code != CodeParse {
		t.Errorf("reject = %+v", r)
	}

	// An unknown column sets the sticky error, and the rest of the chain
	// does not run.
	ran := false
	chain := cast.
		Where(Col("nope").Gt(Lit(1.0))).
		Apply(WhereFunc(func(Row) (bool, error) { ran = true; return true, nil }))
	pe := wantPlanError(t, chain.Err())
	if pe.Step != "where" || !strings.Contains(pe.Error(), `unknown column "nope"`) {
		t.Errorf("plan error = %v", pe)
	}
	if ran {
		t.Error("an operation after the sticky error ran")
	}
	if chain.Len() != 2 || len(chain.Rejects()) != 1 {
		t.Errorf("table after the error: Len=%d rejects=%d, want the data before it", chain.Len(), len(chain.Rejects()))
	}

	// In stop mode the first data error sets the sticky error.
	stop := orders().WithErrorMode(ModeStop).Cast("amount", TypeFloat)
	var de *DataError
	if !errors.As(stop.Err(), &de) {
		t.Fatalf("stop mode: Err = %v, want a *DataError", stop.Err())
	}
	if de.Reject.Value != "abc" || de.Reject.Code != CodeParse {
		t.Errorf("data error = %+v", de.Reject)
	}
	if next := stop.Select("id"); next.Err() != stop.Err() {
		t.Error("stop mode: the chain goes on after the data error")
	}
}

func TestTableIsImmutable(t *testing.T) {
	src := orders()
	_ = src.Cast("amount", TypeFloat).With("amount", Col("amount").Mul(Lit(2.0)))
	wantCells(t, src, "amount", "10.5", "abc", "7")
	if len(src.Rejects()) != 0 {
		t.Error("the source got rejects")
	}
}

func TestNewTableErrors(t *testing.T) {
	wantPlanError(t, NewTable(Texts("a", "x"), Texts("a", "y")).Err())
	wantPlanError(t, NewTable(Texts("a", "x"), Texts("b")).Err())
	wantPlanError(t, NewTable(Texts("", "x")).Err())
	if e := NewTable(); e.Err() != nil || e.Len() != 0 {
		t.Errorf("empty table: %v, %d", e.Err(), e.Len())
	}
}

func TestSelectAndRename(t *testing.T) {
	got := orders().Select("amount", "id").Rename("amount", "betrag")
	if !slices.Equal(got.Columns(), []string{"betrag", "id"}) {
		t.Errorf("columns = %v", got.Columns())
	}
	wantCells(t, got, "betrag", "10.5", "abc", "7")

	wantPlanError(t, orders().Select("id", "id").Err())
	wantPlanError(t, orders().Select().Err())
	wantPlanError(t, orders().Rename("id", "amount").Err())
	wantPlanError(t, orders().Rename("x", "y").Err())
}

func TestRowsAndString(t *testing.T) {
	tbl := NewTable(Texts("a", "x", "y"), Ints("n", 1, 2).WithNulls(1))
	var got []string
	for r := range tbl.Rows() {
		a, _ := r.Text("a")
		if r.IsNull("n") {
			a += "=null"
		}
		got = append(got, a)
	}
	if !slices.Equal(got, []string{"x", "y=null"}) {
		t.Errorf("rows = %v", got)
	}
	want := "a  n\nx  1\ny  <null>\n"
	if s := tbl.String(); s != want {
		t.Errorf("String() =\n%s\nwant\n%s", s, want)
	}
}

func TestErrorModeString(t *testing.T) {
	if ModeReject.String() != "reject" || ModeStop.String() != "stop" {
		t.Error("ErrorMode.String")
	}
	if orders().ErrorMode() != ModeReject {
		t.Error("the default mode is not reject (D5)")
	}
}
