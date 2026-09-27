package stats

import (
	"math"
	"slices"
	"testing"
)

func TestMedian(t *testing.T) {
	cases := []struct {
		name string
		in   []float64
		want float64
	}{
		{"empty", nil, 0},
		{"single", []float64{42}, 42},
		{"odd", []float64{5, 1, 3}, 3},
		{"even", []float64{4, 1, 3, 2}, 2.5},
		{"duplicates", []float64{2, 2, 2, 1}, 2},
		{"negative", []float64{-3, -1, -2}, -2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Median(slices.Clone(c.in)); got != c.want {
				t.Errorf("Median(%v) = %v, want %v", c.in, got, c.want)
			}
		})
	}
}

func TestQuantile(t *testing.T) {
	vals := []float64{10, 40, 20, 30, 50} // sorted: 10 20 30 40 50
	cases := []struct {
		p    float64
		want float64
	}{
		{0, 10},
		{0.25, 20},
		{0.5, 30},
		{0.9, 46},
		{0.95, 48},
		{1, 50},
		{0.1, 14},
	}
	for _, c := range cases {
		got := Quantile(slices.Clone(vals), c.p)
		if math.Abs(got-c.want) > 1e-9 {
			t.Errorf("Quantile(p=%v) = %v, want %v", c.p, got, c.want)
		}
	}
}

func TestQuantile_EvenMatchesMedian(t *testing.T) {
	vals := []float64{7, 1, 5, 3, 9, 11}
	if got, want := Quantile(slices.Clone(vals), 0.5), Median(slices.Clone(vals)); got != want {
		t.Errorf("Quantile(0.5) = %v, Median = %v", got, want)
	}
}

func TestQuantile_Edge(t *testing.T) {
	if got := Quantile(nil, 0.5); got != 0 {
		t.Errorf("empty: got %v, want 0", got)
	}
	if got := Quantile([]float64{3}, 0.7); got != 3 {
		t.Errorf("single: got %v, want 3", got)
	}
	for _, p := range []float64{-0.1, 1.1, math.NaN(), math.Inf(1)} {
		if got := Quantile([]float64{1, 2}, p); !math.IsNaN(got) {
			t.Errorf("p=%v: got %v, want NaN", p, got)
		}
	}
}

func TestQuantile_Sorted(t *testing.T) {
	vals := make([]float64, 1000)
	for i := range vals {
		vals[i] = float64(i)
	}
	if got := Quantile(slices.Clone(vals), 0.999); math.Abs(got-998.001) > 1e-9 {
		t.Errorf("got %v, want 998.001", got)
	}
	slices.Reverse(vals)
	if got := Quantile(vals, 0.5); got != 499.5 {
		t.Errorf("got %v, want 499.5", got)
	}
}

func TestVariance(t *testing.T) {
	v, n := Variance(slices.Values([]float64{2, 4, 4, 4, 5, 5, 7, 9}))
	if v != 4 || n != 8 {
		t.Errorf("got (%v, %d), want (4, 8)", v, n)
	}
	v, n = Variance(slices.Values([]float64{3}))
	if v != 0 || n != 1 {
		t.Errorf("single: got (%v, %d), want (0, 1)", v, n)
	}
	v, n = Variance(slices.Values[[]float64](nil))
	if v != 0 || n != 0 {
		t.Errorf("empty: got (%v, %d), want (0, 0)", v, n)
	}
}

func BenchmarkQuantile(b *testing.B) {
	src := make([]float64, 10_000)
	for i := range src {
		src[i] = float64((i * 7919) % 10_007)
	}
	vals := make([]float64, len(src))
	b.ReportAllocs()
	for b.Loop() {
		copy(vals, src)
		_ = Quantile(vals, 0.95)
	}
}
