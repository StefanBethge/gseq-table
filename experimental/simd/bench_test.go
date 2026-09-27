package simd

import (
	"math/rand/v2"
	"strconv"
	"testing"
)

// benchSizes lists the slice lengths used across all sub-benchmarks.
var benchSizes = []int{1_000, 100_000}

// benchImpls runs a kernel through the public API ("simd", which is the
// scalar fallback when Accelerated reports false) and through the scalar
// reference ("scalar").
func benchImpls(b *testing.B, bytesPerOp int, simd, scalar func()) {
	b.Run("simd", func(b *testing.B) {
		b.SetBytes(int64(bytesPerOp))
		for i := 0; i < b.N; i++ {
			simd()
		}
	})
	b.Run("scalar", func(b *testing.B) {
		b.SetBytes(int64(bytesPerOp))
		for i := 0; i < b.N; i++ {
			scalar()
		}
	})
}

func benchFloats(n int) []float64 { return randFloats(rand.New(rand.NewPCG(1, uint64(n))), n) }
func benchInts(n int) []int64     { return randInts(rand.New(rand.NewPCG(2, uint64(n))), n) }

// benchOp is a variable so the compiler cannot specialize the scalar
// reference on a constant operator.
var benchOp = Gt

var (
	sinkFloat float64
	sinkInt   int64
	sinkIdx   []int
)

func BenchmarkSumFloat64(b *testing.B) {
	for _, n := range benchSizes {
		x := benchFloats(n)
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			benchImpls(b, 8*n,
				func() { sinkFloat = SumFloat64(x) },
				func() { sinkFloat = sumFloat64Scalar(x) })
		})
	}
}

func BenchmarkSumInt64(b *testing.B) {
	for _, n := range benchSizes {
		x := benchInts(n)
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			benchImpls(b, 8*n,
				func() { sinkInt = SumInt64(x) },
				func() { sinkInt = sumInt64Scalar(x) })
		})
	}
}

func BenchmarkMinFloat64(b *testing.B) {
	for _, n := range benchSizes {
		x := benchFloats(n)
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			benchImpls(b, 8*n,
				func() { sinkFloat, _ = MinFloat64(x) },
				func() { sinkFloat = minFloat64Scalar(x) })
		})
	}
}

func BenchmarkMaxInt64(b *testing.B) {
	for _, n := range benchSizes {
		x := benchInts(n)
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			benchImpls(b, 8*n,
				func() { sinkInt, _ = MaxInt64(x) },
				func() { sinkInt = maxInt64Scalar(x) })
		})
	}
}

func BenchmarkDotFloat64(b *testing.B) {
	for _, n := range benchSizes {
		x, y := benchFloats(n), benchFloats(n + 1)[:n]
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			benchImpls(b, 16*n,
				func() { sinkFloat = DotFloat64(x, y) },
				func() { sinkFloat = dotFloat64Scalar(x, y) })
		})
	}
}

func BenchmarkAddFloat64(b *testing.B) {
	for _, n := range benchSizes {
		x, y, dst := benchFloats(n), benchFloats(n + 1)[:n], make([]float64, n)
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			benchImpls(b, 24*n,
				func() { AddFloat64(dst, x, y) },
				func() { addFloat64Scalar(dst, x, y) })
		})
	}
}

func BenchmarkMulFloat64(b *testing.B) {
	for _, n := range benchSizes {
		x, y, dst := benchFloats(n), benchFloats(n + 1)[:n], make([]float64, n)
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			benchImpls(b, 24*n,
				func() { MulFloat64(dst, x, y) },
				func() { mulFloat64Scalar(dst, x, y) })
		})
	}
}

func BenchmarkCompareFloat64(b *testing.B) {
	for _, n := range benchSizes {
		x, mask := benchFloats(n), make([]bool, n)
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			benchImpls(b, 8*n,
				func() { CompareFloat64(mask, x, benchOp, 0) },
				func() { compareFloat64Scalar(mask, x, benchOp, 0) })
		})
	}
}

// BenchmarkIndicesFloat64 uses a selective filter (about 1% of rows match),
// the typical shape of a WHERE clause. BenchmarkIndicesInt64 matches about
// half of the rows.
func BenchmarkIndicesFloat64(b *testing.B) {
	for _, n := range benchSizes {
		r := rand.New(rand.NewPCG(3, uint64(n)))
		x := make([]float64, n)
		for i := range x {
			x[i] = r.Float64()
		}
		threshold := 0.99
		dst := make([]int, 0, n)
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			benchImpls(b, 8*n,
				func() { sinkIdx = IndicesFloat64(dst[:0], x, benchOp, threshold) },
				func() { sinkIdx = indicesFloat64Scalar(dst[:0], x, benchOp, threshold) })
		})
	}
}

func BenchmarkIndicesInt64(b *testing.B) {
	for _, n := range benchSizes {
		x := benchInts(n)
		threshold := int64(0)
		dst := make([]int, 0, n)
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			benchImpls(b, 8*n,
				func() { sinkIdx = IndicesInt64(dst[:0], x, benchOp, threshold) },
				func() { sinkIdx = indicesInt64Scalar(dst[:0], x, benchOp, threshold) })
		})
	}
}
