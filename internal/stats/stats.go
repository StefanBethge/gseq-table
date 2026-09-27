// Package stats holds the numeric summary statistics shared by the schema
// column aggregators and the table package's Agg implementations, so both
// always compute median, quantiles and variance the same way.
//
// Rules:
//   - variance is the population variance (divides by n, not n-1)
//   - quantiles use linear interpolation between the closest ranks
//     (h = p·(n-1), the default of R type 7 and NumPy)
//   - functions taking a []float64 may reorder it in place
package stats

import (
	"iter"
	"math"
	"math/rand"
)

// Median returns the median of vals (reordered in place) in expected O(n)
// time using randomized quickselect. Returns 0 for empty input.
func Median(vals []float64) float64 {
	n := len(vals)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return vals[0]
	}
	if n%2 == 1 {
		return quickselect(vals, n/2)
	}
	// For even n: upper median lands at vals[n/2]; vals[0..n/2-1] are all <=
	// that value, so the lower median is max(vals[0..n/2-1]).
	hi := quickselect(vals, n/2)
	lo := vals[0]
	for _, v := range vals[1 : n/2] {
		if v > lo {
			lo = v
		}
	}
	return (lo + hi) / 2
}

// Quantile returns the p-quantile (0 ≤ p ≤ 1) of vals (reordered in place)
// using linear interpolation between the closest ranks, in expected O(n) time.
// Returns 0 for empty input and NaN if p is NaN or outside [0, 1].
func Quantile(vals []float64, p float64) float64 {
	if math.IsNaN(p) || p < 0 || p > 1 {
		return math.NaN()
	}
	n := len(vals)
	if n == 0 {
		return 0
	}
	h := p * float64(n-1)
	k := int(h)
	lo := quickselect(vals, k)
	frac := h - float64(k)
	if frac == 0 || k+1 >= n {
		return lo
	}
	// vals[k+1..] are all >= vals[k], so the next rank is their minimum.
	hi := vals[k+1]
	for _, v := range vals[k+2:] {
		if v < hi {
			hi = v
		}
	}
	return lo + frac*(hi-lo)
}

// Variance returns the population variance of the values yielded by seq and
// how many values it saw. It makes two passes over seq (mean, then squared
// deviations), so seq must yield the same values each time it is ranged over.
// Returns 0, 0 for an empty sequence.
func Variance(seq iter.Seq[float64]) (v float64, n int) {
	var sum float64
	for f := range seq {
		sum += f
		n++
	}
	if n == 0 {
		return 0, 0
	}
	mean := sum / float64(n)
	var sumSq float64
	for f := range seq {
		d := f - mean
		sumSq += d * d
	}
	return sumSq / float64(n), n
}

// quickselect rearranges vals in place and returns the k-th smallest element
// (0-indexed) in expected O(n) time using a randomized Lomuto partition.
func quickselect(vals []float64, k int) float64 {
	lo, hi := 0, len(vals)-1
	for lo < hi {
		p := partition(vals, lo, hi)
		if p == k {
			return vals[k]
		} else if p < k {
			lo = p + 1
		} else {
			hi = p - 1
		}
	}
	return vals[k]
}

// partition partitions vals[lo..hi] around a randomly chosen pivot (avoids
// O(n²) worst case on sorted, reverse-sorted, or cyclic data) and returns the
// pivot's final index.
func partition(vals []float64, lo, hi int) int {
	pivotIdx := lo + rand.Intn(hi-lo+1)
	vals[pivotIdx], vals[hi] = vals[hi], vals[pivotIdx]
	pivot := vals[hi]
	i := lo
	for j := lo; j < hi; j++ {
		if vals[j] <= pivot {
			vals[i], vals[j] = vals[j], vals[i]
			i++
		}
	}
	vals[i], vals[hi] = vals[hi], vals[i]
	return i
}
