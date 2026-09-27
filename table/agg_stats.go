package table

import (
	"math"
	"slices"
	"strconv"

	"github.com/stefanbethge/gseq-table/internal/cell"
	"github.com/stefanbethge/gseq-table/internal/stats"
)

// Min returns the smallest parseable float value in col, or "" if the group
// has no parseable values.
//
//	table.Min("price")
func Min(col string) Agg { return minAgg{col} }

// Max returns the largest parseable float value in col, or "" if the group
// has no parseable values.
//
//	table.Max("price")
func Max(col string) Agg { return maxAgg{col} }

// Median returns the median of all parseable float values in col, or "" if
// the group has no parseable values. For an even count it is the mean of the
// two middle values.
//
//	table.Median("age")
func Median(col string) Agg { return quantileAgg{col: col, p: 0.5, median: true} }

// Quantile returns the p-quantile (0 ≤ p ≤ 1) of all parseable float values
// in col, linearly interpolated between the closest ranks (NumPy's default).
// Quantile(col, 0.5) equals Median(col). Returns "" if the group has no
// parseable values or if p is NaN or outside [0, 1].
//
//	table.Quantile("latency_ms", 0.95)
func Quantile(col string, p float64) Agg { return quantileAgg{col: col, p: p} }

// Var returns the population variance of all parseable float values in col,
// or "" if the group has no parseable values. A single value yields "0".
//
//	table.Var("revenue")
func Var(col string) Agg { return varianceAgg{col: col} }

// StdDev returns the population standard deviation of all parseable float
// values in col, or "" if the group has no parseable values. A single value
// yields "0".
//
//	table.StdDev("revenue")
func StdDev(col string) Agg { return varianceAgg{col: col, sqrt: true} }

// CountDistinct counts the distinct non-empty values in col. Values are
// compared as raw strings, so "1" and "1.0" are distinct.
//
//	table.CountDistinct("customer_id")
func CountDistinct(col string) Agg { return countDistinctAgg{col} }

// --- Agg implementations ---

type minAgg struct{ col string }

func (a minAgg) reduce(g aggRows) string {
	return reduceAgg(g, a.plan(g))
}

func (a minAgg) plan(cols aggColIndex) aggPlan {
	return aggPlan{colIdx: cols.ColIndex(a.col), state: &extremumAggState{}}
}

type maxAgg struct{ col string }

func (a maxAgg) reduce(g aggRows) string {
	return reduceAgg(g, a.plan(g))
}

func (a maxAgg) plan(cols aggColIndex) aggPlan {
	return aggPlan{colIdx: cols.ColIndex(a.col), state: &extremumAggState{max: true}}
}

type quantileAgg struct {
	col    string
	p      float64
	median bool
}

func (a quantileAgg) reduce(g aggRows) string {
	return reduceAgg(g, a.plan(g))
}

func (a quantileAgg) plan(cols aggColIndex) aggPlan {
	return aggPlan{colIdx: cols.ColIndex(a.col), state: &quantileAggState{p: a.p, median: a.median}}
}

type varianceAgg struct {
	col  string
	sqrt bool
}

func (a varianceAgg) reduce(g aggRows) string {
	return reduceAgg(g, a.plan(g))
}

func (a varianceAgg) plan(cols aggColIndex) aggPlan {
	return aggPlan{colIdx: cols.ColIndex(a.col), state: &varianceAggState{sqrt: a.sqrt}}
}

type countDistinctAgg struct{ col string }

func (a countDistinctAgg) reduce(g aggRows) string {
	return reduceAgg(g, a.plan(g))
}

func (a countDistinctAgg) plan(cols aggColIndex) aggPlan {
	return aggPlan{colIdx: cols.ColIndex(a.col), state: &countDistinctAggState{}}
}

// --- Agg states ---

type extremumAggState struct {
	max   bool
	set   bool
	value float64
}

func (s *extremumAggState) step(value string) {
	f, err := cell.ParseFloat(value, 64)
	if err != nil {
		return
	}
	if !s.set || (s.max && f > s.value) || (!s.max && f < s.value) {
		s.value = f
		s.set = true
	}
}

func (s *extremumAggState) result() string {
	if !s.set {
		return ""
	}
	return cell.FormatFloat(s.value, 64)
}

// floatsAggState collects the parseable float values of a group for
// statistics that need all of them (median, quantiles, variance).
type floatsAggState struct {
	vals []float64
}

func (s *floatsAggState) step(value string) {
	if f, err := cell.ParseFloat(value, 64); err == nil {
		s.vals = append(s.vals, f)
	}
}

type quantileAggState struct {
	floatsAggState
	p      float64
	median bool
}

func (s *quantileAggState) result() string {
	if len(s.vals) == 0 || math.IsNaN(s.p) || s.p < 0 || s.p > 1 {
		return ""
	}
	if s.median {
		return cell.FormatFloat(stats.Median(s.vals), 64)
	}
	return cell.FormatFloat(stats.Quantile(s.vals, s.p), 64)
}

type varianceAggState struct {
	floatsAggState
	sqrt bool
}

func (s *varianceAggState) result() string {
	v, n := stats.Variance(slices.Values(s.vals))
	if n == 0 {
		return ""
	}
	if s.sqrt {
		v = math.Sqrt(v)
	}
	return cell.FormatFloat(v, 64)
}

type countDistinctAggState struct {
	seen map[string]struct{}
}

func (s *countDistinctAggState) step(value string) {
	if value == "" {
		return
	}
	if s.seen == nil {
		s.seen = make(map[string]struct{})
	}
	s.seen[value] = struct{}{}
}

func (s *countDistinctAggState) result() string {
	return strconv.Itoa(len(s.seen))
}
