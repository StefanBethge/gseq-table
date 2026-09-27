//go:build strict

package table

import "testing"

// TestFromStructs_Unsupported_StrictPanics verifies that an unsupported struct
// type panics in the strict build.
func TestFromStructs_Unsupported_StrictPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for unsupported type, got none")
		}
	}()
	FromStructs([]int{1})
}
