package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	v1csv "github.com/stefanbethge/gseq-table/csv"
	"github.com/stefanbethge/gseq-table/table"

	gtable "github.com/stefanbethge/gseq-table/experimental/v2"
	"github.com/stefanbethge/gseq-table/experimental/v2/csv"
	"github.com/stefanbethge/gseq-table/experimental/v2/excel"
	"github.com/stefanbethge/gseq-table/experimental/v2/internal/benchknob"
)

// The operations compared (D58, exit criteria of the prototype). Every
// case reads the delivery and applies one operation; read alone is the
// baseline. The implementations:
//
//	v1t     v1 Table, immutable
//	v1m     v1 MutableTable, in place where v1 has the operation; group by
//	        and join exist only on Table and run on a view of the rows
//	v2      v2 with the automatic copy mode (D7) and the raw state (D64)
//	v2noraw v2 without the raw state, by the internal switch (D105)
var caseNames = []string{"read", "filter", "cast", "derive", "sort", "groupby", "join"}

// spec names the columns a case uses in a delivery of one kind.
type spec struct {
	floats, ints []string
	filter       string // float column: keep rows > 0
	sortCol      string
	derive       [2]string // product of two floats, or a text concat for text
	text         bool
}

func specOf(kind string) spec {
	if kind == "text" {
		return spec{floats: []string{"amount"}, ints: []string{"id"}, filter: "amount", sortCol: "name",
			derive: [2]string{"city", "name"}, text: true}
	}
	return spec{floats: []string{"f1", "f2", "f3", "f4", "f5"}, ints: []string{"id", "n"}, filter: "f1",
		sortCol: "code", derive: [2]string{"f1", "f2"}}
}

// aggCols are the columns of the group by: sum, mean and count.
func (s spec) aggCols() (string, string) {
	if s.text {
		return "amount", "amount"
	}
	return "f1", "f2"
}

type runConfig struct {
	impl, kase, kind string
	rows             int
	dir              string
	data             string // path of the delivery; default from dir, kind and rows
	block            int
	sink             bool  // v2: write the result to a sink that discards it
	budget           int64 // v2: cap of the run; 0: the budget of the process
	managed          bool  // v2: let the engine set GOMEMLIMIT
	spill            string
}

func (c runConfig) path() string {
	if c.data != "" {
		return c.data
	}
	ext := "csv"
	if c.kase == "excel" {
		ext = "xlsx"
	}
	return deliveryPath(c.dir, c.kind, c.rows, ext)
}

// runCase runs one case and returns the number of result rows.
func runCase(ctx context.Context, c runConfig) (int, error) {
	switch c.impl {
	case "v1t", "v1m":
		return runV1(c)
	case "v2", "v2noraw":
		benchknob.NoRawState.Store(c.impl == "v2noraw")
		return runV2(ctx, c)
	}
	return 0, fmt.Errorf("unknown implementation %q", c.impl)
}

// --- v1 ---

func parseF(s string) float64 { v, _ := strconv.ParseFloat(s, 64); return v }

func get(r table.Row, col string) string { return r.Get(col).UnwrapOr("") }

// castFloat and castInt are the v1 way to convert a column: parse and
// format the text again; a value that does not parse becomes empty, as in
// v1's lenient conversions (D74).
func castFloat(s string) string {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return ""
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func castInt(s string) string {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return ""
	}
	return strconv.FormatInt(v, 10)
}

func readV1(path string) (table.Table, error) {
	res := v1csv.New().ReadFile(path)
	if res.IsErr() {
		return table.Table{}, res.UnwrapErr()
	}
	return res.Unwrap(), nil
}

func runV1(c runConfig) (int, error) {
	t, err := readV1(c.path())
	if err != nil {
		return 0, err
	}
	s := specOf(c.kind)
	var dim table.Table
	if c.kase == "join" {
		if dim, err = readV1(dimPath(c.dir)); err != nil {
			return 0, err
		}
	}
	sumCol, meanCol := s.aggCols()
	aggs := []table.AggDef{{Col: "sum", Agg: table.Sum(sumCol)}, {Col: "mean", Agg: table.Mean(meanCol)},
		{Col: "count", Agg: table.Count(sumCol)}}
	keep := func(r table.Row) bool { return parseF(get(r, s.filter)) > 0 }
	derive := func(r table.Row) string {
		if s.text {
			return get(r, s.derive[0]) + " / " + get(r, s.derive[1])
		}
		return strconv.FormatFloat(parseF(get(r, s.derive[0]))*parseF(get(r, s.derive[1])), 'g', -1, 64)
	}

	if c.impl == "v1t" {
		switch c.kase {
		case "read":
		case "filter":
			t = t.Where(keep)
		case "cast":
			for _, col := range s.floats {
				t = t.Map(col, castFloat)
			}
			for _, col := range s.ints {
				t = t.Map(col, castInt)
			}
		case "derive":
			t = t.AddCol("total", derive)
		case "sort":
			t = t.Sort(s.sortCol, true)
		case "groupby":
			t = t.GroupByAgg([]string{"code"}, aggs)
		case "join":
			t = t.Join(dim, "code", "code")
		default:
			return 0, fmt.Errorf("unknown case %q", c.kase)
		}
		return t.Len(), errors.Join(t.Errs()...)
	}

	m := t.MutableView()
	switch c.kase {
	case "read":
	case "filter":
		m.Where(keep)
	case "cast":
		for _, col := range s.floats {
			m.Map(col, castFloat)
		}
		for _, col := range s.ints {
			m.Map(col, castInt)
		}
	case "derive":
		m.AddCol("total", derive)
	case "sort":
		m.Sort(s.sortCol, true)
	case "groupby":
		g := m.FreezeView().GroupByAgg([]string{"code"}, aggs)
		return g.Len(), errors.Join(g.Errs()...)
	case "join":
		j := m.FreezeView().Join(dim, "code", "code")
		return j.Len(), errors.Join(j.Errs()...)
	default:
		return 0, fmt.Errorf("unknown case %q", c.kase)
	}
	return m.Len(), errors.Join(m.Errs()...)
}

// --- v2 ---

// discard is a sink that counts the rows and drops them.
type discard struct{ rows *int }

func (d discard) Write(_ context.Context, b gtable.Block) error { *d.rows += b.Rows.Len(); return nil }
func (d discard) Close() error                                  { return nil }

func castOps(s spec) []gtable.Op {
	var ops []gtable.Op
	for _, col := range s.floats {
		ops = append(ops, gtable.Cast(col, gtable.TypeFloat))
	}
	for _, col := range s.ints {
		ops = append(ops, gtable.Cast(col, gtable.TypeInt))
	}
	return ops
}

func runV2(ctx context.Context, c runConfig) (int, error) {
	var src gtable.Source
	switch c.kase {
	case "onebrc":
		src = csv.File(c.path(), csv.Comma(';'), csv.NoHeader()).Expect("station", "measurement")
	case "excel":
		src = excel.Sheet(c.path(), "")
	default:
		src = csv.File(c.path())
	}
	p := gtable.FromSource(src, c.block)
	s := specOf(c.kind)
	sumCol, meanCol := s.aggCols()
	switch c.kase {
	case "read", "excel":
	case "onebrc":
		// The pipeline of examples/onebrc_budget.
		p.Then(gtable.Cast("measurement", gtable.TypeFloat)).
			Then(gtable.GroupBy([]string{"station"},
				gtable.Min("measurement").As("min"),
				gtable.Mean("measurement").As("mean"),
				gtable.Max("measurement").As("max"))).
			Then(gtable.Sort(gtable.Asc("station")))
	case "filter":
		p.Then(gtable.Cast(s.filter, gtable.TypeFloat)).
			Then(gtable.Where(gtable.Col(s.filter).Gt(gtable.Lit(0.0))))
	case "cast":
		p.Then(gtable.CastAll(castOps(s)...))
	case "derive":
		if s.text {
			p.Then(gtable.With("total", gtable.Concat(gtable.Col(s.derive[0]), gtable.Lit(" / "), gtable.Col(s.derive[1]))))
		} else {
			p.Then(gtable.CastAll(gtable.Cast(s.derive[0], gtable.TypeFloat), gtable.Cast(s.derive[1], gtable.TypeFloat))).
				Then(gtable.With("total", gtable.Col(s.derive[0]).Mul(gtable.Col(s.derive[1]))))
		}
	case "sort":
		p.Then(gtable.Sort(gtable.Asc(s.sortCol)))
	case "groupby":
		casts := []gtable.Op{gtable.Cast(sumCol, gtable.TypeFloat)}
		if meanCol != sumCol {
			casts = append(casts, gtable.Cast(meanCol, gtable.TypeFloat))
		}
		p.Then(gtable.CastAll(casts...)).
			Then(gtable.GroupBy([]string{"code"},
				gtable.Sum(sumCol).As("sum"), gtable.Mean(meanCol).As("mean"), gtable.Count(sumCol).As("count")))
	case "join":
		dim, err := csv.File(dimPath(c.dir)).Table(ctx)
		if err != nil {
			return 0, err
		}
		p.Then(gtable.InnerJoin(dim, gtable.On("code")))
	default:
		return 0, fmt.Errorf("unknown case %q", c.kase)
	}
	if c.budget > 0 {
		p.MemoryBudget(c.budget)
	}
	if c.managed {
		p.WithManagedMemory()
	}
	if c.spill != "" {
		p.SpillDir(c.spill)
	}
	n := 0
	if c.sink {
		p.To(discard{&n})
	}
	res, err := p.Run(ctx)
	defer res.Close()
	if err != nil {
		return 0, err
	}
	if res.Status != gtable.StatusOK {
		return 0, fmt.Errorf("status %s: %v", res.Status, res.Causes)
	}
	if !c.sink {
		n = res.Table.Len()
	}
	return n, nil
}
