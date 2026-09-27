//go:build go1.27 && !strict

package table

import "testing"

func TestTypedUnknownColumn_Lenient(t *testing.T) {
	tb := typedTable()
	out := tb.MapAs("missing", func(n int) int { return n })
	assertEqual(t, out.HasErrs(), true)
	assertEqual(t, out.Errs()[0].Error(), `MapAs: unknown column "missing"`)

	m := tb.Mutable()
	m.MapAs("missing", func(n int) int { return n })
	assertEqual(t, m.HasErrs(), true)
	assertEqual(t, m.Errs()[0].Error(), `MapAs: unknown column "missing"`)
}
