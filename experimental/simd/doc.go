// Package simd provides experimental SIMD kernels for numeric column
// operations on plain []float64 and []int64 slices.
//
// # Stability
//
// This package is EXPERIMENTAL and outside the gseq-table v1 stability
// guarantee. Its API may change or disappear in any release.
//
// # Enabling SIMD
//
// The accelerated kernels are only compiled when building with
//
//	GOEXPERIMENT=simd go build ./...
//
// which uses the standard library's experimental simd/archsimd package. Without the experiment, or on
// architectures without a SIMD implementation, every function uses a
// plain-Go scalar fallback. [Accelerated] reports which path is active.
//
// Supported SIMD targets:
//
//   - amd64: AVX2 (256-bit vectors), selected at runtime; CPUs without AVX2
//     use the scalar fallback.
//   - arm64: NEON (128-bit vectors), always available on arm64.
//
// Some operations always use the scalar code because it is as fast or faster:
// MulInt64 and DotInt64 on both targets (no 64-bit integer multiply without
// AVX-512 or in NEON), and the Compare and Indices kernels on arm64 (NEON has
// no cheap way to turn a vector mask into bits).
//
// # Determinism
//
// All implementations produce bit-identical results for the same input:
// floating-point Sum, Mean and Dot accumulate into eight fixed lanes and
// reduce them in a fixed order, and the scalar fallback reproduces exactly
// that order. The result can therefore differ in the last bits from a naive
// left-to-right loop, but it does not depend on the build or the CPU.
//
// Min and Max follow the semantics of the builtin min and max: any NaN input
// yields NaN, and -0 is treated as smaller than +0.
package simd
