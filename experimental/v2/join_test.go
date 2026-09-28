package gtable

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/testutil"
)

func TestJoinCarriesRejectsAndErrorsOfBothTables(t *testing.T) {
	testutil.Proves(t, "T44")

	left := NewTable(Texts("cid", "1", "2", "x", ""), Texts("item", "a", "b", "c", "d")).
		Cast("cid", TypeInt) // "x" is rejected, "" becomes null
	right := NewTable(Texts("id", "1", "2", "?"), Texts("name", "Ada", "Bo", "Cy")).
		Cast("id", TypeInt) // "?" is rejected

	j := left.LeftJoin(right, OnPair("cid", "id"))
	wantCells(t, j, "item", "a", "b", "d")
	// A null key finds no partner (D72).
	wantCells(t, j, "name", "Ada", "Bo", "<null>")
	if !slices.Equal(j.Columns(), []string{"cid", "item", "name"}) {
		t.Errorf("columns = %v, want the right key dropped", j.Columns())
	}
	// The rejects of the left side come first, then those of the right (D69).
	rs := j.Rejects()
	if len(rs) != 2 || rs[0].Value != "x" || rs[1].Value != "?" {
		t.Fatalf("rejects = %v", rs)
	}

	// A sticky error of the right side stops the join, and the result
	// carries it.
	broken := right.Select("nope")
	if res := left.InnerJoin(broken, OnPair("cid", "id")); !errors.Is(res.Err(), broken.Err()) {
		t.Errorf("Err = %v, want the right side's error %v", res.Err(), broken.Err())
	}
	// A sticky error of the left side wins.
	if res := left.Select("nope").InnerJoin(broken, On("id")); !strings.Contains(res.Err().Error(), "step select") {
		t.Errorf("Err = %v, want the left side's error", res.Err())
	}

	// Clashing columns and keys of different types are plan errors (D72).
	other := NewTable(Ints("id", 1), Texts("item", "z"))
	pe := wantPlanError(t, left.InnerJoin(other, OnPair("cid", "id")).Err())
	if !strings.Contains(pe.Error(), `"item" exists on both sides`) {
		t.Errorf("plan error = %v", pe)
	}
	wantPlanError(t, left.InnerJoin(NewTable(Texts("id", "1")), OnPair("cid", "id")).Err())
}

func TestInnerJoinOneToMany(t *testing.T) {
	orders := NewTable(Texts("cust", "a", "b", "c"), Ints("n", 1, 2, 3))
	lines := NewTable(Texts("cust", "b", "a", "b"), Texts("sku", "s1", "s2", "s3"))
	j := orders.InnerJoin(lines, On("cust"))
	wantCells(t, j, "cust", "a", "b", "b")
	wantCells(t, j, "sku", "s2", "s1", "s3")
	wantCells(t, j, "n", "1", "2", "2")

	wantPlanError(t, orders.InnerJoin(lines).Err())
	wantPlanError(t, orders.InnerJoin(lines, On("zzz")).Err())
	wantPlanError(t, orders.InnerJoin(lines, OnPair("cust", "zzz")).Err())
}

func TestJoinInPipelineWithBrokenRightSideIsPlanError(t *testing.T) {
	broken := NewTable(Texts("id", "1")).Select("nope")
	p := From(NewTable(Texts("id", "1")), 10).Then(InnerJoin(broken, On("id")))
	wantPlanError(t, p.Check())
}
