//go:build !strict

package table_test

import "testing"

// These cases rely on lenient mode producing tables with accumulated errors.
// In strict mode the invalid Select calls panic immediately, so they are covered
// by the strict-specific tests instead of running here.

func TestCoverage_CopyErrsFrom(t *testing.T) {
	errored := newTable([]string{"x"}, nil).Select("nonexistent")
	fresh := newTable([]string{"a"}, [][]string{{"1"}})
	copied := fresh.CopyErrsFrom(errored)
	if !copied.HasErrs() {
		t.Fatal("expected errors to be copied")
	}
}

func TestCoverage_CopyErrsFrom_BothHaveErrs(t *testing.T) {
	src := newTable([]string{"x"}, nil).Select("bad1")
	dst := newTable([]string{"a"}, nil).Select("bad2")
	result := dst.CopyErrsFrom(src)
	if len(result.Errs()) < 2 {
		t.Errorf("expected at least 2 errors, got %d", len(result.Errs()))
	}
}
