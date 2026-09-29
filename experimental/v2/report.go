package gtable

import (
	"fmt"
	"math"
	"slices"
)

// DefaultFormatChangeLimit is the share of the rows that went into a step
// from which values of a column failing with the same code are a format
// change (D59, D85). The maintainer set it to a fifth on the sample
// deliveries of the prototype (D105).
const DefaultFormatChangeLimit = 0.2

// maxExamples is the number of failed values a format change shows (D85).
const maxExamples = 5

// formatLimits are the limits for a format change: per pipeline and per
// column (D59).
type formatLimits struct {
	all  float64 // 0: DefaultFormatChangeLimit
	cols map[string]float64
	bad  []float64 // limits out of range, a plan error
}

func (l formatLimits) limit(col string) float64 {
	if v, ok := l.cols[col]; ok {
		return v
	}
	if l.all > 0 {
		return l.all
	}
	return DefaultFormatChangeLimit
}

func checkFormatLimit(v float64) error {
	if math.IsNaN(v) || v <= 0 || v > 1 {
		return fmt.Errorf("format change limit %v is not above 0 and at most 1", v)
	}
	return nil
}

// formatChanges returns a format change for every source, step, column and
// data error code whose failed source rows reach the limit, as a share of
// the rows of that source that went into the step (D23, D85). The entries
// of steps outside the run are not counted.
func formatChanges(entries []rejectEntry, byStep map[*stepRef]*counter, lim formatLimits) []Finding {
	type key struct {
		src  *rawSource
		step *stepRef
		col  string
		code string
	}
	type group struct {
		rows     map[int]bool
		examples []string
	}
	var order []key
	groups := map[key]*group{}
	for _, e := range entries {
		if e.Column == "" || kindOfCode(e.Code) == KindDelivery || byStep[e.step] == nil {
			continue
		}
		for _, r := range columnRefs(e) {
			k := key{r.src, e.step, e.Column, e.Code}
			g, ok := groups[k]
			if !ok {
				g = &group{rows: map[int]bool{}}
				groups[k] = g
				order = append(order, k)
			}
			g.rows[r.row] = true
			if e.HasValue && len(g.examples) < maxExamples && !slices.Contains(g.examples, e.Value) {
				g.examples = append(g.examples, e.Value)
			}
		}
	}
	var out []Finding
	for _, k := range order {
		g := groups[k]
		in := byStep[k.step].perSrc[k.src]
		if in == 0 || float64(len(g.rows))/float64(in) < lim.limit(k.col) {
			continue
		}
		out = append(out, Finding{
			Kind:     FindingFormatChange,
			Source:   k.src.name,
			Column:   k.col,
			Detail:   fmt.Sprintf("%d of %d rows failed with code %s in step %s", len(g.rows), in, k.code, k.step.name),
			Count:    len(g.rows),
			Examples: g.examples,
		})
	}
	return out
}

// columnRefs returns the source rows of e whose source has the failed
// column, or all of them if none has it, such as for a derived column.
func columnRefs(e rejectEntry) []srcRef {
	var out []srcRef
	for _, r := range e.orig.refs {
		if r.src.s.index(e.Column) >= 0 {
			out = append(out, r)
		}
	}
	if out == nil {
		return e.orig.refs
	}
	return out
}
