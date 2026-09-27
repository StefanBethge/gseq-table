//go:build goexperiment.simd && arm64

package simd

import "testing"

func TestAcceleratedNEON(t *testing.T) {
	if !Accelerated() {
		t.Fatal("Accelerated() = false with GOEXPERIMENT=simd on arm64")
	}
}
