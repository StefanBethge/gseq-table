//go:build goexperiment.simd && amd64

package simd

import (
	"math"
	"math/bits"
	"simd/archsimd"
)

// AVX2 kernels using 256-bit vectors, selected at runtime. CPUs without AVX2
// use the scalar code. The 64-bit integer multiply (VPMULLQ) needs AVX-512,
// so MulInt64 and DotInt64 stay scalar.
//
// Sum and Dot keep two Float64x4 accumulators, i.e. eight lanes laid out as
// [0 1 2 3][4 5 6 7], and reduce them exactly like reduceLanes.

var accelerated = archsimd.X86.AVX2()

func sumFloat64(x []float64) float64 {
	if !accelerated {
		return sumFloat64Scalar(x)
	}
	var a0, a1 archsimd.Float64x4
	i := 0
	for ; i+lanes <= len(x); i += lanes {
		c := x[i : i+lanes]
		a0 = a0.Add(archsimd.LoadFloat64x4(c))
		a1 = a1.Add(archsimd.LoadFloat64x4(c[4:]))
	}
	total := reduceFloat64x4(a0, a1)
	for ; i < len(x); i++ {
		total += x[i]
	}
	return total
}

func dotFloat64(a, b []float64) float64 {
	if !accelerated {
		return dotFloat64Scalar(a, b)
	}
	var a0, a1 archsimd.Float64x4
	i := 0
	for ; i+lanes <= len(a); i += lanes {
		x, y := a[i:i+lanes], b[i:i+lanes]
		a0 = a0.Add(archsimd.LoadFloat64x4(x).Mul(archsimd.LoadFloat64x4(y)))
		a1 = a1.Add(archsimd.LoadFloat64x4(x[4:]).Mul(archsimd.LoadFloat64x4(y[4:])))
	}
	total := reduceFloat64x4(a0, a1)
	for ; i < len(a); i++ {
		total += float64(a[i] * b[i])
	}
	return total
}

// reduceFloat64x4 mirrors reduceLanes: lanes i+4 onto i, then i+2 onto i,
// then the final pair.
func reduceFloat64x4(a0, a1 archsimd.Float64x4) float64 {
	s := a0.Add(a1)
	r := s.GetLo().Add(s.GetHi())
	return r.GetElem(0) + r.GetElem(1)
}

func sumInt64(x []int64) int64 {
	if !accelerated {
		return sumInt64Scalar(x)
	}
	var a0, a1 archsimd.Int64x4
	i := 0
	for ; i+lanes <= len(x); i += lanes {
		c := x[i : i+lanes]
		a0 = a0.Add(archsimd.LoadInt64x4(c))
		a1 = a1.Add(archsimd.LoadInt64x4(c[4:]))
	}
	var s [4]int64
	a0.Add(a1).StoreArray(&s)
	total := s[0] + s[1] + s[2] + s[3]
	for ; i < len(x); i++ {
		total += x[i]
	}
	return total
}

// VMINPD and VMAXPD return the second operand when either input is NaN or
// both are zero, unlike the builtin min and max. The float kernels therefore
// track NaN separately and resolve a zero result with the scalar code so the
// sign of zero matches.

func minFloat64(x []float64) float64 {
	if !accelerated || len(x) < 8 {
		return minFloat64Scalar(x)
	}
	m0 := archsimd.LoadFloat64x4(x)
	m1 := archsimd.LoadFloat64x4(x[4:])
	nan := m0.NotEqual(m0).Or(m1.NotEqual(m1))
	i := 8
	for ; i+lanes <= len(x); i += lanes {
		c := x[i : i+lanes]
		v0 := archsimd.LoadFloat64x4(c)
		v1 := archsimd.LoadFloat64x4(c[4:])
		nan = nan.Or(v0.NotEqual(v0)).Or(v1.NotEqual(v1))
		m0 = m0.Min(v0)
		m1 = m1.Min(v1)
	}
	if nan.ToBits() != 0 {
		return math.NaN()
	}
	var s [4]float64
	m0.Min(m1).StoreArray(&s)
	m := min(s[0], s[1], s[2], s[3])
	for ; i < len(x); i++ {
		m = min(m, x[i])
	}
	if m == 0 {
		return minFloat64Scalar(x)
	}
	return m
}

func maxFloat64(x []float64) float64 {
	if !accelerated || len(x) < 8 {
		return maxFloat64Scalar(x)
	}
	m0 := archsimd.LoadFloat64x4(x)
	m1 := archsimd.LoadFloat64x4(x[4:])
	nan := m0.NotEqual(m0).Or(m1.NotEqual(m1))
	i := 8
	for ; i+lanes <= len(x); i += lanes {
		c := x[i : i+lanes]
		v0 := archsimd.LoadFloat64x4(c)
		v1 := archsimd.LoadFloat64x4(c[4:])
		nan = nan.Or(v0.NotEqual(v0)).Or(v1.NotEqual(v1))
		m0 = m0.Max(v0)
		m1 = m1.Max(v1)
	}
	if nan.ToBits() != 0 {
		return math.NaN()
	}
	var s [4]float64
	m0.Max(m1).StoreArray(&s)
	m := max(s[0], s[1], s[2], s[3])
	for ; i < len(x); i++ {
		m = max(m, x[i])
	}
	if m == 0 {
		return maxFloat64Scalar(x)
	}
	return m
}

// VPMINSQ and VPMAXSQ need AVX-512, so the int64 kernels use a compare and
// a lane select instead.

func minInt64(x []int64) int64 {
	if !accelerated || len(x) < 8 {
		return minInt64Scalar(x)
	}
	m0 := archsimd.LoadInt64x4(x)
	m1 := archsimd.LoadInt64x4(x[4:])
	i := 8
	for ; i+lanes <= len(x); i += lanes {
		c := x[i : i+lanes]
		v0 := archsimd.LoadInt64x4(c)
		v1 := archsimd.LoadInt64x4(c[4:])
		m0 = v0.IfElse(m0.Greater(v0), m0)
		m1 = v1.IfElse(m1.Greater(v1), m1)
	}
	var s [4]int64
	m1.IfElse(m0.Greater(m1), m0).StoreArray(&s)
	m := min(s[0], s[1], s[2], s[3])
	for ; i < len(x); i++ {
		m = min(m, x[i])
	}
	return m
}

func maxInt64(x []int64) int64 {
	if !accelerated || len(x) < 8 {
		return maxInt64Scalar(x)
	}
	m0 := archsimd.LoadInt64x4(x)
	m1 := archsimd.LoadInt64x4(x[4:])
	i := 8
	for ; i+lanes <= len(x); i += lanes {
		c := x[i : i+lanes]
		v0 := archsimd.LoadInt64x4(c)
		v1 := archsimd.LoadInt64x4(c[4:])
		m0 = v0.IfElse(v0.Greater(m0), m0)
		m1 = v1.IfElse(v1.Greater(m1), m1)
	}
	var s [4]int64
	m1.IfElse(m1.Greater(m0), m0).StoreArray(&s)
	m := max(s[0], s[1], s[2], s[3])
	for ; i < len(x); i++ {
		m = max(m, x[i])
	}
	return m
}

func addFloat64(dst, a, b []float64) {
	i := 0
	if accelerated {
		for ; i+lanes <= len(dst); i += lanes {
			d, x, y := dst[i:i+lanes], a[i:i+lanes], b[i:i+lanes]
			archsimd.LoadFloat64x4(x).Add(archsimd.LoadFloat64x4(y)).Store(d)
			archsimd.LoadFloat64x4(x[4:]).Add(archsimd.LoadFloat64x4(y[4:])).Store(d[4:])
		}
	}
	addFloat64Scalar(dst[i:], a[i:], b[i:])
}

func addInt64(dst, a, b []int64) {
	i := 0
	if accelerated {
		for ; i+lanes <= len(dst); i += lanes {
			d, x, y := dst[i:i+lanes], a[i:i+lanes], b[i:i+lanes]
			archsimd.LoadInt64x4(x).Add(archsimd.LoadInt64x4(y)).Store(d)
			archsimd.LoadInt64x4(x[4:]).Add(archsimd.LoadInt64x4(y[4:])).Store(d[4:])
		}
	}
	addInt64Scalar(dst[i:], a[i:], b[i:])
}

func mulFloat64(dst, a, b []float64) {
	i := 0
	if accelerated {
		for ; i+lanes <= len(dst); i += lanes {
			d, x, y := dst[i:i+lanes], a[i:i+lanes], b[i:i+lanes]
			archsimd.LoadFloat64x4(x).Mul(archsimd.LoadFloat64x4(y)).Store(d)
			archsimd.LoadFloat64x4(x[4:]).Mul(archsimd.LoadFloat64x4(y[4:])).Store(d[4:])
		}
	}
	mulFloat64Scalar(dst[i:], a[i:], b[i:])
}

func cmpMaskFloat64x4(x archsimd.Float64x4, op Cmp, v archsimd.Float64x4) archsimd.Mask64x4 {
	switch op {
	case Eq:
		return x.Equal(v)
	case Ne:
		return x.NotEqual(v)
	case Lt:
		return x.Less(v)
	case Le:
		return x.LessEqual(v)
	case Gt:
		return x.Greater(v)
	default:
		return x.GreaterEqual(v)
	}
}

func cmpMaskInt64x4(x archsimd.Int64x4, op Cmp, v archsimd.Int64x4) archsimd.Mask64x4 {
	switch op {
	case Eq:
		return x.Equal(v)
	case Ne:
		return x.NotEqual(v)
	case Lt:
		return x.Less(v)
	case Le:
		return x.LessEqual(v)
	case Gt:
		return x.Greater(v)
	default:
		return x.GreaterEqual(v)
	}
}

// bitsToBools maps an 8-bit mask to the eight bools it encodes, so storeBits
// is a single 8-byte copy.
var bitsToBools = func() (t [256][lanes]bool) {
	for b := range t {
		for j := range lanes {
			t[b][j] = b>>j&1 != 0
		}
	}
	return t
}()

// storeBits writes bit j of b to mask[j] for j < 8.
func storeBits(mask []bool, b uint8) {
	*(*[lanes]bool)(mask) = bitsToBools[b]
}

// appendBits appends base+j to dst for every set bit j of b.
func appendBits(dst []int, b uint8, base int) []int {
	for b != 0 {
		dst = append(dst, base+bits.TrailingZeros8(b))
		b &= b - 1
	}
	return dst
}

func compareFloat64(mask []bool, x []float64, op Cmp, v float64) {
	i := 0
	if accelerated {
		vv := archsimd.BroadcastFloat64x4(v)
		for ; i+lanes <= len(x); i += lanes {
			c := x[i : i+lanes]
			b := cmpMaskFloat64x4(archsimd.LoadFloat64x4(c), op, vv).ToBits() | cmpMaskFloat64x4(archsimd.LoadFloat64x4(c[4:]), op, vv).ToBits()<<4
			storeBits(mask[i:i+lanes], b)
		}
	}
	compareFloat64Scalar(mask[i:], x[i:], op, v)
}

func compareInt64(mask []bool, x []int64, op Cmp, v int64) {
	i := 0
	if accelerated {
		vv := archsimd.BroadcastInt64x4(v)
		for ; i+lanes <= len(x); i += lanes {
			c := x[i : i+lanes]
			b := cmpMaskInt64x4(archsimd.LoadInt64x4(c), op, vv).ToBits() | cmpMaskInt64x4(archsimd.LoadInt64x4(c[4:]), op, vv).ToBits()<<4
			storeBits(mask[i:i+lanes], b)
		}
	}
	compareInt64Scalar(mask[i:], x[i:], op, v)
}

func indicesFloat64(dst []int, x []float64, op Cmp, v float64) []int {
	i := 0
	if accelerated {
		vv := archsimd.BroadcastFloat64x4(v)
		for ; i+lanes <= len(x); i += lanes {
			c := x[i : i+lanes]
			if b := cmpMaskFloat64x4(archsimd.LoadFloat64x4(c), op, vv).ToBits() | cmpMaskFloat64x4(archsimd.LoadFloat64x4(c[4:]), op, vv).ToBits()<<4; b != 0 {
				dst = appendBits(dst, b, i)
			}
		}
	}
	for ; i < len(x); i++ {
		if cmpFloat64(x[i], op, v) {
			dst = append(dst, i)
		}
	}
	return dst
}

func indicesInt64(dst []int, x []int64, op Cmp, v int64) []int {
	i := 0
	if accelerated {
		vv := archsimd.BroadcastInt64x4(v)
		for ; i+lanes <= len(x); i += lanes {
			c := x[i : i+lanes]
			if b := cmpMaskInt64x4(archsimd.LoadInt64x4(c), op, vv).ToBits() | cmpMaskInt64x4(archsimd.LoadInt64x4(c[4:]), op, vv).ToBits()<<4; b != 0 {
				dst = appendBits(dst, b, i)
			}
		}
	}
	for ; i < len(x); i++ {
		if cmpInt64(x[i], op, v) {
			dst = append(dst, i)
		}
	}
	return dst
}

// cmpFloat64 and cmpInt64 evaluate a single comparison for the tails of the
// Indices kernels.

func cmpFloat64(e float64, op Cmp, v float64) bool {
	switch op {
	case Eq:
		return e == v
	case Ne:
		return e != v
	case Lt:
		return e < v
	case Le:
		return e <= v
	case Gt:
		return e > v
	default:
		return e >= v
	}
}

func cmpInt64(e int64, op Cmp, v int64) bool {
	switch op {
	case Eq:
		return e == v
	case Ne:
		return e != v
	case Lt:
		return e < v
	case Le:
		return e <= v
	case Gt:
		return e > v
	default:
		return e >= v
	}
}
