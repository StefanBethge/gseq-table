//go:build !strict

package table

import (
	"strings"
	"testing"
)

func assertSingleErr(t *testing.T, errs []error, want string) {
	t.Helper()
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d: %v", len(errs), errs)
	}
	if !strings.Contains(errs[0].Error(), want) {
		t.Errorf("error %q does not contain %q", errs[0], want)
	}
}

func TestWindow_UnknownValueCol(t *testing.T) {
	w := windowTable().PartitionBy("customer")
	cases := map[string]Table{
		"Lag":    w.Lag("nope", "out", 1),
		"Lead":   w.Lead("nope", "out", 1),
		"CumSum": w.CumSum("nope", "out"),
		"Rank":   w.Rank("nope", "out", true),
	}
	for op, out := range cases {
		assertEqual(t, len(out.Headers), 3)
		assertEqual(t, out.Len(), 5)
		assertSingleErr(t, out.Errs(), op+`: unknown column "nope"`)
	}
}

func TestWindow_UnknownPartitionCol(t *testing.T) {
	out := windowTable().PartitionBy("nope").CumSum("revenue", "cum")
	assertSingleErr(t, out.Errs(), `PartitionBy: unknown column "nope"`)
	// Unknown partition columns read as empty: a single partition remains.
	assertCol(t, out, "cum", "30", "35", "45", "52", "72")
}

func TestWindow_UnknownOrderCol(t *testing.T) {
	out := windowTable().PartitionBy("customer").OrderBy(Asc("nope")).CumSum("revenue", "cum")
	assertSingleErr(t, out.Errs(), `OrderBy: unknown column "nope"`)
	assertCol(t, out, "cum", "30", "5", "40", "12", "60")
}

func TestWindow_ErrorPrefixedWithSource(t *testing.T) {
	out := windowTable().WithSource("sales.csv").PartitionBy("nope").CumSum("revenue", "cum")
	assertSingleErr(t, out.Errs(), `[sales.csv] PartitionBy: unknown column "nope"`)
}

func TestMutableWindow_UnknownValueCol(t *testing.T) {
	ops := map[string]func(*MutableWindow) *MutableTable{
		"Lag":    func(w *MutableWindow) *MutableTable { return w.Lag("nope", "out", 1) },
		"Lead":   func(w *MutableWindow) *MutableTable { return w.Lead("nope", "out", 1) },
		"CumSum": func(w *MutableWindow) *MutableTable { return w.CumSum("nope", "out") },
		"Rank":   func(w *MutableWindow) *MutableTable { return w.Rank("nope", "out", true) },
	}
	for op, fn := range ops {
		m := fn(windowMutable().PartitionBy("customer"))
		assertEqual(t, len(m.Headers()), 3)
		assertSingleErr(t, m.Errs(), op+`: unknown column "nope"`)
	}
}

func TestMutableWindow_UnknownPartitionAndOrderCol(t *testing.T) {
	m := windowMutable()
	m.PartitionBy("nope").CumSum("revenue", "cum")
	assertSingleErr(t, m.Errs(), `PartitionBy: unknown column "nope"`)
	assertCol(t, m.Freeze(), "cum", "30", "35", "45", "52", "72")

	m = windowMutable()
	m.PartitionBy("customer").OrderBy(Desc("nope")).Lag("revenue", "prev", 1)
	assertSingleErr(t, m.Errs(), `OrderBy: unknown column "nope"`)
	assertCol(t, m.Freeze(), "prev", "", "", "30", "5", "10")
}
