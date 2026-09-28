package gtable

import (
	"errors"
	"slices"
	"testing"
)

// cells returns the cells of column col as text, with <null> for null.
func cells(t *testing.T, tbl Table, col string) []string {
	t.Helper()
	c, ok := tbl.Column(col)
	if !ok {
		t.Fatalf("no column %q in %v", col, tbl.Columns())
	}
	v := vecOf(c.col)
	out := make([]string, v.n)
	for i := range v.n {
		if s, ok := v.format(i); ok {
			out[i] = s
		} else {
			out[i] = "<null>"
		}
	}
	return out
}

func wantCells(t *testing.T, tbl Table, col string, want ...string) {
	t.Helper()
	if tbl.Err() != nil {
		t.Fatalf("sticky error: %v", tbl.Err())
	}
	if got := cells(t, tbl, col); !slices.Equal(got, want) {
		t.Errorf("column %s = %q, want %q", col, got, want)
	}
}

func wantPlanError(t *testing.T, err error) *PlanError {
	t.Helper()
	var pe *PlanError
	if !errors.As(err, &pe) {
		t.Fatalf("error = %v, want a *PlanError", err)
	}
	return pe
}

func codes(rs []Reject) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.Code
	}
	return out
}
