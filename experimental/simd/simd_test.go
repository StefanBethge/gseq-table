package simd

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

// testLengths covers empty input, every tail length around the vector and
// block widths, and a few larger sizes.
var testLengths = func() []int {
	var ns []int
	for n := 0; n <= 40; n++ {
		ns = append(ns, n)
	}
	return append(ns, 63, 64, 65, 1000, 1001, 4099)
}()

var allCmps = []Cmp{Eq, Ne, Lt, Le, Gt, Ge}

func sameFloat(a, b float64) bool {
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}
	return math.Float64bits(a) == math.Float64bits(b)
}

func randFloats(r *rand.Rand, n int) []float64 {
	x := make([]float64, n)
	for i := range x {
		x[i] = (r.Float64()*2 - 1) * math.Pow(10, float64(r.IntN(12)-6))
	}
	return x
}

// smallFloats returns integral values from a small range so that equality
// comparisons have hits.
func smallFloats(r *rand.Rand, n int) []float64 {
	x := make([]float64, n)
	for i := range x {
		x[i] = float64(r.IntN(9) - 4)
	}
	return x
}

func randInts(r *rand.Rand, n int) []int64 {
	x := make([]int64, n)
	for i := range x {
		x[i] = r.Int64() - math.MaxInt64/2
	}
	return x
}

func smallInts(r *rand.Rand, n int) []int64 {
	x := make([]int64, n)
	for i := range x {
		x[i] = int64(r.IntN(9) - 4)
	}
	return x
}

// floatInputs returns random inputs of length n plus variants containing
// special values at the first, a middle and the last position.
func floatInputs(r *rand.Rand, n int) [][]float64 {
	inputs := [][]float64{randFloats(r, n), smallFloats(r, n)}
	if n == 0 {
		return inputs
	}
	specials := []float64{math.NaN(), math.Inf(1), math.Inf(-1), 0, math.Copysign(0, -1)}
	for _, s := range specials {
		for _, pos := range []int{0, n / 2, n - 1} {
			x := randFloats(r, n)
			x[pos] = s
			inputs = append(inputs, x)
		}
	}
	// Signed zeros only, to check that Min and Max pick the right sign.
	zeros := make([]float64, n)
	zeros[n/2] = math.Copysign(0, -1)
	negZeros := make([]float64, n)
	for i := range negZeros {
		negZeros[i] = math.Copysign(0, -1)
	}
	negZeros[n/2] = 0
	return append(inputs, zeros, negZeros)
}

func intInputs(r *rand.Rand, n int) [][]int64 {
	inputs := [][]int64{randInts(r, n), smallInts(r, n)}
	if n == 0 {
		return inputs
	}
	for _, s := range []int64{math.MinInt64, math.MaxInt64} {
		for _, pos := range []int{0, n / 2, n - 1} {
			x := smallInts(r, n)
			x[pos] = s
			inputs = append(inputs, x)
		}
	}
	return inputs
}

func TestAccelerated(t *testing.T) {
	t.Logf("Accelerated() = %v", Accelerated())
}

func TestSumFloat64MatchesScalar(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for _, n := range testLengths {
		for _, x := range floatInputs(r, n) {
			got, want := SumFloat64(x), sumFloat64Scalar(x)
			if !sameFloat(got, want) {
				t.Fatalf("n=%d: SumFloat64=%v, scalar=%v", n, got, want)
			}
		}
	}
}

func TestSumFloat64CloseToNaive(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	for _, n := range testLengths {
		x := randFloats(r, n)
		var naive, abs float64
		for _, v := range x {
			naive += v
			abs += math.Abs(v)
		}
		// Both orders are within n*eps*sum|x| of the exact sum.
		tol := 2 * float64(n) * 0x1p-52 * abs
		if got := SumFloat64(x); math.Abs(got-naive) > tol {
			t.Fatalf("n=%d: SumFloat64=%v, naive=%v, tol=%v", n, got, naive, tol)
		}
	}
}

func TestMeanFloat64(t *testing.T) {
	if got := MeanFloat64(nil); !math.IsNaN(got) {
		t.Fatalf("MeanFloat64(nil) = %v, want NaN", got)
	}
	if got := MeanFloat64([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); got != 5.5 {
		t.Fatalf("MeanFloat64 = %v, want 5.5", got)
	}
	r := rand.New(rand.NewPCG(5, 6))
	for _, n := range testLengths[1:] {
		x := randFloats(r, n)
		want := sumFloat64Scalar(x) / float64(n)
		if got := MeanFloat64(x); !sameFloat(got, want) {
			t.Fatalf("n=%d: MeanFloat64=%v, want %v", n, got, want)
		}
	}
}

func TestDotFloat64MatchesScalar(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 8))
	for _, n := range testLengths {
		for _, a := range floatInputs(r, n) {
			b := randFloats(r, n)
			got, want := DotFloat64(a, b), dotFloat64Scalar(a, b)
			if !sameFloat(got, want) {
				t.Fatalf("n=%d: DotFloat64=%v, scalar=%v", n, got, want)
			}
		}
	}
}

func TestDotFloat64CloseToNaive(t *testing.T) {
	r := rand.New(rand.NewPCG(9, 10))
	for _, n := range testLengths {
		a, b := randFloats(r, n), randFloats(r, n)
		var naive, abs float64
		for i := range a {
			naive += a[i] * b[i]
			abs += math.Abs(a[i] * b[i])
		}
		tol := 2 * float64(n+1) * 0x1p-52 * abs
		if got := DotFloat64(a, b); math.Abs(got-naive) > tol {
			t.Fatalf("n=%d: DotFloat64=%v, naive=%v, tol=%v", n, got, naive, tol)
		}
	}
}

func TestMinMaxFloat64MatchesScalar(t *testing.T) {
	if _, ok := MinFloat64(nil); ok {
		t.Fatal("MinFloat64(nil) ok = true")
	}
	if _, ok := MaxFloat64(nil); ok {
		t.Fatal("MaxFloat64(nil) ok = true")
	}
	r := rand.New(rand.NewPCG(11, 12))
	for _, n := range testLengths[1:] {
		for _, x := range floatInputs(r, n) {
			if got, _ := MinFloat64(x); !sameFloat(got, minFloat64Scalar(x)) {
				t.Fatalf("n=%d: MinFloat64=%v, scalar=%v", n, got, minFloat64Scalar(x))
			}
			if got, _ := MaxFloat64(x); !sameFloat(got, maxFloat64Scalar(x)) {
				t.Fatalf("n=%d: MaxFloat64=%v, scalar=%v", n, got, maxFloat64Scalar(x))
			}
		}
	}
}

func TestMinMaxFloat64Semantics(t *testing.T) {
	negZero := math.Copysign(0, -1)
	x := make([]float64, 33)
	x[17] = negZero
	if got, _ := MinFloat64(x); math.Float64bits(got) != math.Float64bits(negZero) {
		t.Fatalf("MinFloat64 with -0 = %v, want -0", got)
	}
	x[17] = math.NaN()
	if got, _ := MaxFloat64(x); !math.IsNaN(got) {
		t.Fatalf("MaxFloat64 with NaN = %v, want NaN", got)
	}
}

func TestSumInt64MatchesScalar(t *testing.T) {
	r := rand.New(rand.NewPCG(13, 14))
	for _, n := range testLengths {
		for _, x := range intInputs(r, n) {
			if got, want := SumInt64(x), sumInt64Scalar(x); got != want {
				t.Fatalf("n=%d: SumInt64=%v, scalar=%v", n, got, want)
			}
		}
	}
}

func TestMeanInt64(t *testing.T) {
	if got := MeanInt64(nil); !math.IsNaN(got) {
		t.Fatalf("MeanInt64(nil) = %v, want NaN", got)
	}
	if got := MeanInt64([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); got != 5.5 {
		t.Fatalf("MeanInt64 = %v, want 5.5", got)
	}
}

func TestMinMaxInt64MatchesScalar(t *testing.T) {
	if _, ok := MinInt64(nil); ok {
		t.Fatal("MinInt64(nil) ok = true")
	}
	if _, ok := MaxInt64(nil); ok {
		t.Fatal("MaxInt64(nil) ok = true")
	}
	r := rand.New(rand.NewPCG(15, 16))
	for _, n := range testLengths[1:] {
		for _, x := range intInputs(r, n) {
			if got, _ := MinInt64(x); got != minInt64Scalar(x) {
				t.Fatalf("n=%d: MinInt64=%v, scalar=%v", n, got, minInt64Scalar(x))
			}
			if got, _ := MaxInt64(x); got != maxInt64Scalar(x) {
				t.Fatalf("n=%d: MaxInt64=%v, scalar=%v", n, got, maxInt64Scalar(x))
			}
		}
	}
}

func TestDotInt64(t *testing.T) {
	if got := DotInt64([]int64{1, 2, 3}, []int64{4, 5, 6}); got != 32 {
		t.Fatalf("DotInt64 = %v, want 32", got)
	}
}

func TestElementwiseFloat64MatchesScalar(t *testing.T) {
	r := rand.New(rand.NewPCG(17, 18))
	for _, n := range testLengths {
		for _, a := range floatInputs(r, n) {
			b := randFloats(r, n)
			got, want := make([]float64, n), make([]float64, n)

			AddFloat64(got, a, b)
			addFloat64Scalar(want, a, b)
			if !slices.EqualFunc(got, want, sameFloat) {
				t.Fatalf("n=%d: AddFloat64 differs from scalar", n)
			}

			MulFloat64(got, a, b)
			mulFloat64Scalar(want, a, b)
			if !slices.EqualFunc(got, want, sameFloat) {
				t.Fatalf("n=%d: MulFloat64 differs from scalar", n)
			}
		}
	}
}

func TestElementwiseInt64MatchesScalar(t *testing.T) {
	r := rand.New(rand.NewPCG(19, 20))
	for _, n := range testLengths {
		for _, a := range intInputs(r, n) {
			b := randInts(r, n)
			got, want := make([]int64, n), make([]int64, n)

			AddInt64(got, a, b)
			addInt64Scalar(want, a, b)
			if !slices.Equal(got, want) {
				t.Fatalf("n=%d: AddInt64 differs from scalar", n)
			}

			MulInt64(got, a, b)
			mulInt64Scalar(want, a, b)
			if !slices.Equal(got, want) {
				t.Fatalf("n=%d: MulInt64 differs from scalar", n)
			}
		}
	}
}

func TestElementwiseAliasing(t *testing.T) {
	a := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9}
	AddFloat64(a, a, a)
	if want := []float64{2, 4, 6, 8, 10, 12, 14, 16, 18}; !slices.Equal(a, want) {
		t.Fatalf("AddFloat64 in place = %v, want %v", a, want)
	}
	b := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9}
	AddInt64(b, b, b)
	if want := []int64{2, 4, 6, 8, 10, 12, 14, 16, 18}; !slices.Equal(b, want) {
		t.Fatalf("AddInt64 in place = %v, want %v", b, want)
	}
}

func TestCompareFloat64MatchesScalar(t *testing.T) {
	r := rand.New(rand.NewPCG(21, 22))
	for _, n := range testLengths {
		for _, x := range floatInputs(r, n) {
			for _, v := range []float64{0, 1, -2.5, math.NaN(), math.Inf(1)} {
				for _, op := range allCmps {
					got, want := make([]bool, n), make([]bool, n)
					CompareFloat64(got, x, op, v)
					compareFloat64Scalar(want, x, op, v)
					if !slices.Equal(got, want) {
						t.Fatalf("n=%d op=%d v=%v: CompareFloat64 differs from scalar", n, op, v)
					}

					idx := IndicesFloat64(nil, x, op, v)
					if wantIdx := indicesFloat64Scalar(nil, x, op, v); !slices.Equal(idx, wantIdx) {
						t.Fatalf("n=%d op=%d v=%v: IndicesFloat64 = %v, scalar = %v", n, op, v, idx, wantIdx)
					}
				}
			}
		}
	}
}

func TestCompareInt64MatchesScalar(t *testing.T) {
	r := rand.New(rand.NewPCG(23, 24))
	for _, n := range testLengths {
		for _, x := range intInputs(r, n) {
			for _, v := range []int64{0, 1, -3, math.MinInt64, math.MaxInt64} {
				for _, op := range allCmps {
					got, want := make([]bool, n), make([]bool, n)
					CompareInt64(got, x, op, v)
					compareInt64Scalar(want, x, op, v)
					if !slices.Equal(got, want) {
						t.Fatalf("n=%d op=%d v=%v: CompareInt64 differs from scalar", n, op, v)
					}

					idx := IndicesInt64(nil, x, op, v)
					if wantIdx := indicesInt64Scalar(nil, x, op, v); !slices.Equal(idx, wantIdx) {
						t.Fatalf("n=%d op=%d v=%v: IndicesInt64 = %v, scalar = %v", n, op, v, idx, wantIdx)
					}
				}
			}
		}
	}
}

func TestCompareNaNSemantics(t *testing.T) {
	x := []float64{math.NaN(), 1, math.NaN(), 2, 3, math.NaN(), 4, 5, math.NaN()}
	if got := IndicesFloat64(nil, x, Ne, 1); !slices.Equal(got, []int{0, 2, 3, 4, 5, 6, 7, 8}) {
		t.Fatalf("Ne 1 = %v", got)
	}
	if got := IndicesFloat64(nil, x, Ge, 1); !slices.Equal(got, []int{1, 3, 4, 6, 7}) {
		t.Fatalf("Ge 1 = %v", got)
	}
	if got := IndicesFloat64(nil, x, Eq, math.NaN()); len(got) != 0 {
		t.Fatalf("Eq NaN = %v, want none", got)
	}
}

func TestIndicesAppends(t *testing.T) {
	x := []int64{5, 1, 5, 2, 5, 3, 5, 4, 5, 6}
	dst := []int{-1}
	got := IndicesInt64(dst, x, Eq, 5)
	if want := []int{-1, 0, 2, 4, 6, 8}; !slices.Equal(got, want) {
		t.Fatalf("IndicesInt64 = %v, want %v", got, want)
	}
}

func TestPanics(t *testing.T) {
	cases := map[string]func(){
		"DotFloat64":     func() { DotFloat64(make([]float64, 2), make([]float64, 3)) },
		"DotInt64":       func() { DotInt64(make([]int64, 2), make([]int64, 3)) },
		"AddFloat64":     func() { AddFloat64(make([]float64, 2), make([]float64, 2), make([]float64, 3)) },
		"AddInt64":       func() { AddInt64(make([]int64, 3), make([]int64, 2), make([]int64, 2)) },
		"MulFloat64":     func() { MulFloat64(make([]float64, 2), make([]float64, 3), make([]float64, 2)) },
		"MulInt64":       func() { MulInt64(make([]int64, 2), make([]int64, 2), make([]int64, 3)) },
		"CompareFloat64": func() { CompareFloat64(make([]bool, 2), make([]float64, 3), Eq, 0) },
		"CompareInt64":   func() { CompareInt64(make([]bool, 3), make([]int64, 2), Eq, 0) },
		"InvalidCmp":     func() { IndicesInt64(nil, make([]int64, 2), Ge+1, 0) },
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			fn()
		})
	}
}
