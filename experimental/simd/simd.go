package simd

import "math"

// lanes is the number of independent accumulators every Sum, Mean and Dot
// implementation uses. Changing it changes floating-point results.
const lanes = 8

// Cmp is a comparison operator used by the Compare and Indices kernels.
type Cmp uint8

const (
	Eq Cmp = iota // x == v
	Ne            // x != v
	Lt            // x < v
	Le            // x <= v
	Gt            // x > v
	Ge            // x >= v
)

// Accelerated reports whether the SIMD kernels are active in this build and
// on this CPU. It returns false for the scalar fallback.
func Accelerated() bool { return accelerated }

// SumFloat64 returns the sum of x, or 0 for an empty slice.
func SumFloat64(x []float64) float64 { return sumFloat64(x) }

// SumInt64 returns the sum of x with wrapping two's-complement overflow,
// or 0 for an empty slice.
func SumInt64(x []int64) int64 { return sumInt64(x) }

// MeanFloat64 returns the arithmetic mean of x, or NaN for an empty slice.
func MeanFloat64(x []float64) float64 {
	if len(x) == 0 {
		return math.NaN()
	}
	return sumFloat64(x) / float64(len(x))
}

// MeanInt64 returns the arithmetic mean of x, or NaN for an empty slice.
// The sum is computed in int64 and wraps on overflow.
func MeanInt64(x []int64) float64 {
	if len(x) == 0 {
		return math.NaN()
	}
	return float64(sumInt64(x)) / float64(len(x))
}

// MinFloat64 returns the smallest element of x. ok is false for an empty
// slice. Any NaN element yields NaN.
func MinFloat64(x []float64) (m float64, ok bool) {
	if len(x) == 0 {
		return 0, false
	}
	return minFloat64(x), true
}

// MaxFloat64 returns the largest element of x. ok is false for an empty
// slice. Any NaN element yields NaN.
func MaxFloat64(x []float64) (m float64, ok bool) {
	if len(x) == 0 {
		return 0, false
	}
	return maxFloat64(x), true
}

// MinInt64 returns the smallest element of x. ok is false for an empty slice.
func MinInt64(x []int64) (m int64, ok bool) {
	if len(x) == 0 {
		return 0, false
	}
	return minInt64(x), true
}

// MaxInt64 returns the largest element of x. ok is false for an empty slice.
func MaxInt64(x []int64) (m int64, ok bool) {
	if len(x) == 0 {
		return 0, false
	}
	return maxInt64(x), true
}

// DotFloat64 returns the dot product of a and b. It panics if the lengths
// differ.
func DotFloat64(a, b []float64) float64 {
	checkLen2("DotFloat64", len(a), len(b))
	return dotFloat64(a, b)
}

// DotInt64 returns the dot product of a and b with wrapping overflow. It
// panics if the lengths differ.
func DotInt64(a, b []int64) int64 {
	checkLen2("DotInt64", len(a), len(b))
	return dotInt64Scalar(a, b)
}

// AddFloat64 stores a[i]+b[i] in dst[i]. dst may alias a or b. It panics if
// the lengths differ.
func AddFloat64(dst, a, b []float64) {
	checkLen3("AddFloat64", len(dst), len(a), len(b))
	addFloat64(dst, a, b)
}

// AddInt64 stores a[i]+b[i] in dst[i] with wrapping overflow. dst may alias
// a or b. It panics if the lengths differ.
func AddInt64(dst, a, b []int64) {
	checkLen3("AddInt64", len(dst), len(a), len(b))
	addInt64(dst, a, b)
}

// MulFloat64 stores a[i]*b[i] in dst[i]. dst may alias a or b. It panics if
// the lengths differ.
func MulFloat64(dst, a, b []float64) {
	checkLen3("MulFloat64", len(dst), len(a), len(b))
	mulFloat64(dst, a, b)
}

// MulInt64 stores a[i]*b[i] in dst[i] with wrapping overflow. dst may alias
// a or b. It panics if the lengths differ.
func MulInt64(dst, a, b []int64) {
	checkLen3("MulInt64", len(dst), len(a), len(b))
	mulInt64Scalar(dst, a, b)
}

// CompareFloat64 stores x[i] <op> v in mask[i]. Comparisons follow IEEE 754:
// a NaN compares unequal to everything, including itself. It panics if the
// lengths differ or op is invalid.
func CompareFloat64(mask []bool, x []float64, op Cmp, v float64) {
	checkLen2("CompareFloat64", len(mask), len(x))
	checkCmp(op)
	compareFloat64(mask, x, op, v)
}

// CompareInt64 stores x[i] <op> v in mask[i]. It panics if the lengths differ
// or op is invalid.
func CompareInt64(mask []bool, x []int64, op Cmp, v int64) {
	checkLen2("CompareInt64", len(mask), len(x))
	checkCmp(op)
	compareInt64(mask, x, op, v)
}

// IndicesFloat64 appends to dst the indices i for which x[i] <op> v holds,
// in ascending order, and returns the extended slice. Pass dst[:0] to reuse
// its storage. It panics if op is invalid.
func IndicesFloat64(dst []int, x []float64, op Cmp, v float64) []int {
	checkCmp(op)
	return indicesFloat64(dst, x, op, v)
}

// IndicesInt64 appends to dst the indices i for which x[i] <op> v holds, in
// ascending order, and returns the extended slice. Pass dst[:0] to reuse its
// storage. It panics if op is invalid.
func IndicesInt64(dst []int, x []int64, op Cmp, v int64) []int {
	checkCmp(op)
	return indicesInt64(dst, x, op, v)
}

func checkLen2(fn string, a, b int) {
	if a != b {
		panic("simd." + fn + ": length mismatch")
	}
}

func checkLen3(fn string, a, b, c int) {
	if a != b || a != c {
		panic("simd." + fn + ": length mismatch")
	}
}

func checkCmp(op Cmp) {
	if op > Ge {
		panic("simd: invalid Cmp operator")
	}
}
