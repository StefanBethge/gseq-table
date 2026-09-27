//go:build goexperiment.simd && amd64

package simd

import (
	"simd/archsimd"
	"testing"
)

func TestAcceleratedAVX2(t *testing.T) {
	if got, want := Accelerated(), archsimd.X86.AVX2(); got != want {
		t.Fatalf("Accelerated() = %v, want AVX2() = %v", got, want)
	}
}
