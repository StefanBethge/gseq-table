//go:build !strict

package table

import "testing"

func TestFromStructs_Unsupported_Lenient(t *testing.T) {
	type bad struct {
		C chan int
	}
	tb := FromStructs([]bad{{}})
	assertEqual(t, tb.HasErrs(), true)
	assertEqual(t, tb.Errs()[0].Error(), "FromStructs: field C: unsupported type chan int")
	assertEqual(t, len(tb.Rows), 0)

	tb = FromStructs([]int{1})
	assertEqual(t, tb.Errs()[0].Error(), "FromStructs: int is not a struct type")
}
