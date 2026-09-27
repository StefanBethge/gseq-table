package simd

// Scalar reference kernels. They are always compiled: the fallback build uses
// them directly, the SIMD builds use them for tails, unsupported operations
// and CPUs without the required features, and the tests compare against them.

// reduceLanes combines the eight lane accumulators in the order the vector
// implementations use: first lane i with lane i+4, then i with i+2, then the
// remaining pair.
func reduceLanes(s *[lanes]float64) float64 {
	r0 := (s[0] + s[4]) + (s[2] + s[6])
	r1 := (s[1] + s[5]) + (s[3] + s[7])
	return r0 + r1
}

func sumFloat64Scalar(x []float64) float64 {
	var s [lanes]float64
	i := 0
	for ; i+lanes <= len(x); i += lanes {
		for j := range lanes {
			s[j] += x[i+j]
		}
	}
	total := reduceLanes(&s)
	for ; i < len(x); i++ {
		total += x[i]
	}
	return total
}

func dotFloat64Scalar(a, b []float64) float64 {
	var s [lanes]float64
	i := 0
	for ; i+lanes <= len(a); i += lanes {
		for j := range lanes {
			// The explicit conversion rounds the product and keeps the
			// compiler from fusing it into an FMA, which the vector
			// kernels do not use either.
			s[j] += float64(a[i+j] * b[i+j])
		}
	}
	total := reduceLanes(&s)
	for ; i < len(a); i++ {
		total += float64(a[i] * b[i])
	}
	return total
}

func sumInt64Scalar(x []int64) int64 {
	var s int64
	for _, v := range x {
		s += v
	}
	return s
}

func dotInt64Scalar(a, b []int64) int64 {
	var s int64
	for i := range a {
		s += a[i] * b[i]
	}
	return s
}

func minFloat64Scalar(x []float64) float64 {
	m := x[0]
	for _, v := range x[1:] {
		m = min(m, v)
	}
	return m
}

func maxFloat64Scalar(x []float64) float64 {
	m := x[0]
	for _, v := range x[1:] {
		m = max(m, v)
	}
	return m
}

func minInt64Scalar(x []int64) int64 {
	m := x[0]
	for _, v := range x[1:] {
		m = min(m, v)
	}
	return m
}

func maxInt64Scalar(x []int64) int64 {
	m := x[0]
	for _, v := range x[1:] {
		m = max(m, v)
	}
	return m
}

func addFloat64Scalar(dst, a, b []float64) {
	for i := range dst {
		dst[i] = a[i] + b[i]
	}
}

func addInt64Scalar(dst, a, b []int64) {
	for i := range dst {
		dst[i] = a[i] + b[i]
	}
}

func mulFloat64Scalar(dst, a, b []float64) {
	for i := range dst {
		dst[i] = a[i] * b[i]
	}
}

func mulInt64Scalar(dst, a, b []int64) {
	for i := range dst {
		dst[i] = a[i] * b[i]
	}
}

func compareFloat64Scalar(mask []bool, x []float64, op Cmp, v float64) {
	switch op {
	case Eq:
		for i, e := range x {
			mask[i] = e == v
		}
	case Ne:
		for i, e := range x {
			mask[i] = e != v
		}
	case Lt:
		for i, e := range x {
			mask[i] = e < v
		}
	case Le:
		for i, e := range x {
			mask[i] = e <= v
		}
	case Gt:
		for i, e := range x {
			mask[i] = e > v
		}
	case Ge:
		for i, e := range x {
			mask[i] = e >= v
		}
	}
}

func compareInt64Scalar(mask []bool, x []int64, op Cmp, v int64) {
	switch op {
	case Eq:
		for i, e := range x {
			mask[i] = e == v
		}
	case Ne:
		for i, e := range x {
			mask[i] = e != v
		}
	case Lt:
		for i, e := range x {
			mask[i] = e < v
		}
	case Le:
		for i, e := range x {
			mask[i] = e <= v
		}
	case Gt:
		for i, e := range x {
			mask[i] = e > v
		}
	case Ge:
		for i, e := range x {
			mask[i] = e >= v
		}
	}
}

func indicesFloat64Scalar(dst []int, x []float64, op Cmp, v float64) []int {
	switch op {
	case Eq:
		for i, e := range x {
			if e == v {
				dst = append(dst, i)
			}
		}
	case Ne:
		for i, e := range x {
			if e != v {
				dst = append(dst, i)
			}
		}
	case Lt:
		for i, e := range x {
			if e < v {
				dst = append(dst, i)
			}
		}
	case Le:
		for i, e := range x {
			if e <= v {
				dst = append(dst, i)
			}
		}
	case Gt:
		for i, e := range x {
			if e > v {
				dst = append(dst, i)
			}
		}
	case Ge:
		for i, e := range x {
			if e >= v {
				dst = append(dst, i)
			}
		}
	}
	return dst
}

func indicesInt64Scalar(dst []int, x []int64, op Cmp, v int64) []int {
	switch op {
	case Eq:
		for i, e := range x {
			if e == v {
				dst = append(dst, i)
			}
		}
	case Ne:
		for i, e := range x {
			if e != v {
				dst = append(dst, i)
			}
		}
	case Lt:
		for i, e := range x {
			if e < v {
				dst = append(dst, i)
			}
		}
	case Le:
		for i, e := range x {
			if e <= v {
				dst = append(dst, i)
			}
		}
	case Gt:
		for i, e := range x {
			if e > v {
				dst = append(dst, i)
			}
		}
	case Ge:
		for i, e := range x {
			if e >= v {
				dst = append(dst, i)
			}
		}
	}
	return dst
}
