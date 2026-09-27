//go:build goexperiment.simd && arm64

package simd

import "simd/archsimd"

// NEON kernels using 128-bit vectors. NEON is mandatory on arm64, so no
// runtime feature check is needed. NEON has no 64-bit integer multiply, so
// MulInt64 and DotInt64 stay scalar, and Compare and Indices are scalar too
// (see below).
//
// Sum and Dot keep four Float64x2 accumulators, i.e. eight lanes laid out as
// [0 1][2 3][4 5][6 7], and reduce them exactly like reduceLanes.

const accelerated = true

func sumFloat64(x []float64) float64 {
	var a0, a1, a2, a3 archsimd.Float64x2
	i := 0
	for ; i+lanes <= len(x); i += lanes {
		c := x[i : i+lanes]
		a0 = a0.Add(archsimd.LoadFloat64x2(c))
		a1 = a1.Add(archsimd.LoadFloat64x2(c[2:]))
		a2 = a2.Add(archsimd.LoadFloat64x2(c[4:]))
		a3 = a3.Add(archsimd.LoadFloat64x2(c[6:]))
	}
	total := reduceFloat64x2(a0, a1, a2, a3)
	for ; i < len(x); i++ {
		total += x[i]
	}
	return total
}

func dotFloat64(a, b []float64) float64 {
	var a0, a1, a2, a3 archsimd.Float64x2
	i := 0
	for ; i+lanes <= len(a); i += lanes {
		x, y := a[i:i+lanes], b[i:i+lanes]
		a0 = a0.Add(archsimd.LoadFloat64x2(x).Mul(archsimd.LoadFloat64x2(y)))
		a1 = a1.Add(archsimd.LoadFloat64x2(x[2:]).Mul(archsimd.LoadFloat64x2(y[2:])))
		a2 = a2.Add(archsimd.LoadFloat64x2(x[4:]).Mul(archsimd.LoadFloat64x2(y[4:])))
		a3 = a3.Add(archsimd.LoadFloat64x2(x[6:]).Mul(archsimd.LoadFloat64x2(y[6:])))
	}
	total := reduceFloat64x2(a0, a1, a2, a3)
	for ; i < len(a); i++ {
		total += float64(a[i] * b[i])
	}
	return total
}

// reduceFloat64x2 mirrors reduceLanes: lanes i+4 onto i, then i+2 onto i,
// then the final pair.
func reduceFloat64x2(a0, a1, a2, a3 archsimd.Float64x2) float64 {
	r := a0.Add(a2).Add(a1.Add(a3))
	return r.GetElem(0) + r.GetElem(1)
}

func sumInt64(x []int64) int64 {
	var a0, a1, a2, a3 archsimd.Int64x2
	i := 0
	for ; i+lanes <= len(x); i += lanes {
		c := x[i : i+lanes]
		a0 = a0.Add(archsimd.LoadInt64x2(c))
		a1 = a1.Add(archsimd.LoadInt64x2(c[2:]))
		a2 = a2.Add(archsimd.LoadInt64x2(c[4:]))
		a3 = a3.Add(archsimd.LoadInt64x2(c[6:]))
	}
	r := a0.Add(a1).Add(a2.Add(a3))
	total := r.GetElem(0) + r.GetElem(1)
	for ; i < len(x); i++ {
		total += x[i]
	}
	return total
}

// FMIN and FMAX propagate NaN and order -0 below +0, which matches the
// builtin min and max, so no fix-up is needed.

func minFloat64(x []float64) float64 {
	if len(x) < 4 {
		return minFloat64Scalar(x)
	}
	m0 := archsimd.LoadFloat64x2(x)
	m1 := archsimd.LoadFloat64x2(x[2:])
	i := 4
	for ; i+4 <= len(x); i += 4 {
		c := x[i : i+4]
		m0 = m0.Min(archsimd.LoadFloat64x2(c))
		m1 = m1.Min(archsimd.LoadFloat64x2(c[2:]))
	}
	r := m0.Min(m1)
	m := min(r.GetElem(0), r.GetElem(1))
	for ; i < len(x); i++ {
		m = min(m, x[i])
	}
	return m
}

func maxFloat64(x []float64) float64 {
	if len(x) < 4 {
		return maxFloat64Scalar(x)
	}
	m0 := archsimd.LoadFloat64x2(x)
	m1 := archsimd.LoadFloat64x2(x[2:])
	i := 4
	for ; i+4 <= len(x); i += 4 {
		c := x[i : i+4]
		m0 = m0.Max(archsimd.LoadFloat64x2(c))
		m1 = m1.Max(archsimd.LoadFloat64x2(c[2:]))
	}
	r := m0.Max(m1)
	m := max(r.GetElem(0), r.GetElem(1))
	for ; i < len(x); i++ {
		m = max(m, x[i])
	}
	return m
}

// NEON has no 64-bit integer min/max, so they are built from a compare and
// a lane select.

func minInt64(x []int64) int64 {
	if len(x) < 4 {
		return minInt64Scalar(x)
	}
	m0 := archsimd.LoadInt64x2(x)
	m1 := archsimd.LoadInt64x2(x[2:])
	i := 4
	for ; i+4 <= len(x); i += 4 {
		c := x[i : i+4]
		v0 := archsimd.LoadInt64x2(c)
		v1 := archsimd.LoadInt64x2(c[2:])
		m0 = v0.IfElse(m0.Greater(v0), m0)
		m1 = v1.IfElse(m1.Greater(v1), m1)
	}
	m := min(m0.GetElem(0), m0.GetElem(1), m1.GetElem(0), m1.GetElem(1))
	for ; i < len(x); i++ {
		m = min(m, x[i])
	}
	return m
}

func maxInt64(x []int64) int64 {
	if len(x) < 4 {
		return maxInt64Scalar(x)
	}
	m0 := archsimd.LoadInt64x2(x)
	m1 := archsimd.LoadInt64x2(x[2:])
	i := 4
	for ; i+4 <= len(x); i += 4 {
		c := x[i : i+4]
		v0 := archsimd.LoadInt64x2(c)
		v1 := archsimd.LoadInt64x2(c[2:])
		m0 = v0.IfElse(v0.Greater(m0), m0)
		m1 = v1.IfElse(v1.Greater(m1), m1)
	}
	m := max(m0.GetElem(0), m0.GetElem(1), m1.GetElem(0), m1.GetElem(1))
	for ; i < len(x); i++ {
		m = max(m, x[i])
	}
	return m
}

func addFloat64(dst, a, b []float64) {
	i := 0
	for ; i+lanes <= len(dst); i += lanes {
		d, x, y := dst[i:i+lanes], a[i:i+lanes], b[i:i+lanes]
		archsimd.LoadFloat64x2(x).Add(archsimd.LoadFloat64x2(y)).Store(d)
		archsimd.LoadFloat64x2(x[2:]).Add(archsimd.LoadFloat64x2(y[2:])).Store(d[2:])
		archsimd.LoadFloat64x2(x[4:]).Add(archsimd.LoadFloat64x2(y[4:])).Store(d[4:])
		archsimd.LoadFloat64x2(x[6:]).Add(archsimd.LoadFloat64x2(y[6:])).Store(d[6:])
	}
	addFloat64Scalar(dst[i:], a[i:], b[i:])
}

func addInt64(dst, a, b []int64) {
	i := 0
	for ; i+lanes <= len(dst); i += lanes {
		d, x, y := dst[i:i+lanes], a[i:i+lanes], b[i:i+lanes]
		archsimd.LoadInt64x2(x).Add(archsimd.LoadInt64x2(y)).Store(d)
		archsimd.LoadInt64x2(x[2:]).Add(archsimd.LoadInt64x2(y[2:])).Store(d[2:])
		archsimd.LoadInt64x2(x[4:]).Add(archsimd.LoadInt64x2(y[4:])).Store(d[4:])
		archsimd.LoadInt64x2(x[6:]).Add(archsimd.LoadInt64x2(y[6:])).Store(d[6:])
	}
	addInt64Scalar(dst[i:], a[i:], b[i:])
}

func mulFloat64(dst, a, b []float64) {
	i := 0
	for ; i+lanes <= len(dst); i += lanes {
		d, x, y := dst[i:i+lanes], a[i:i+lanes], b[i:i+lanes]
		archsimd.LoadFloat64x2(x).Mul(archsimd.LoadFloat64x2(y)).Store(d)
		archsimd.LoadFloat64x2(x[2:]).Mul(archsimd.LoadFloat64x2(y[2:])).Store(d[2:])
		archsimd.LoadFloat64x2(x[4:]).Mul(archsimd.LoadFloat64x2(y[4:])).Store(d[4:])
		archsimd.LoadFloat64x2(x[6:]).Mul(archsimd.LoadFloat64x2(y[6:])).Store(d[6:])
	}
	mulFloat64Scalar(dst[i:], a[i:], b[i:])
}

// Compare and Indices use the scalar code: extracting mask lanes on NEON
// (there is no movemask) costs more than Go's branch-free scalar compare.

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
