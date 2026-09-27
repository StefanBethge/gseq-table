//go:build strict

package table

import "testing"

func TestWindow_StrictPanics(t *testing.T) {
	cases := map[string]func(){
		"PartitionBy":         func() { windowTable().PartitionBy("nope") },
		"OrderBy":             func() { windowTable().PartitionBy("customer").OrderBy(Asc("nope")) },
		"CumSum":              func() { windowTable().PartitionBy("customer").CumSum("nope", "out") },
		"Mutable PartitionBy": func() { windowMutable().PartitionBy("nope") },
		"Mutable OrderBy":     func() { windowMutable().PartitionBy("customer").OrderBy(Asc("nope")) },
		"Mutable Lag":         func() { windowMutable().PartitionBy("customer").Lag("nope", "out", 1) },
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("expected panic, got none")
				}
			}()
			fn()
		})
	}
}
