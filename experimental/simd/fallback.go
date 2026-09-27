//go:build !goexperiment.simd || !go1.27 || !(amd64 || arm64)

package simd

// Fallback build: GOEXPERIMENT=simd is not set, the toolchain is older than
// Go 1.27, or the target architecture has no SIMD implementation. Every
// kernel uses the scalar code.

const accelerated = false

func sumFloat64(x []float64) float64 { return sumFloat64Scalar(x) }
func sumInt64(x []int64) int64       { return sumInt64Scalar(x) }
func dotFloat64(a, b []float64) float64 {
	return dotFloat64Scalar(a, b)
}
func minFloat64(x []float64) float64 { return minFloat64Scalar(x) }
func maxFloat64(x []float64) float64 { return maxFloat64Scalar(x) }
func minInt64(x []int64) int64       { return minInt64Scalar(x) }
func maxInt64(x []int64) int64       { return maxInt64Scalar(x) }

func addFloat64(dst, a, b []float64) { addFloat64Scalar(dst, a, b) }
func addInt64(dst, a, b []int64)     { addInt64Scalar(dst, a, b) }
func mulFloat64(dst, a, b []float64) { mulFloat64Scalar(dst, a, b) }

func compareFloat64(mask []bool, x []float64, op Cmp, v float64) {
	compareFloat64Scalar(mask, x, op, v)
}

func compareInt64(mask []bool, x []int64, op Cmp, v int64) {
	compareInt64Scalar(mask, x, op, v)
}

func indicesFloat64(dst []int, x []float64, op Cmp, v float64) []int {
	return indicesFloat64Scalar(dst, x, op, v)
}

func indicesInt64(dst []int, x []int64, op Cmp, v int64) []int {
	return indicesInt64Scalar(dst, x, op, v)
}
