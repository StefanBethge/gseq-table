package gtable

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

// countingSource is a source that counts the rows it hands out.
type countingSource struct {
	tableSource
	read int
}

func (s *countingSource) open() (opened, error) { return s, nil }

func (s *countingSource) blocks(n int, rx *rejector, yield func(batch) error) error {
	return s.tableSource.blocks(n, rx, func(b batch) error {
		s.read += b.blk.Len()
		return yield(b)
	})
}

func delivery(n int) Table {
	ids := make([]string, n)
	amounts := make([]string, n)
	for i := range n {
		ids[i] = strconv.Itoa(i % 7)
		amounts[i] = strconv.Itoa(i)
		if i%5 == 0 {
			amounts[i] = "x" + amounts[i]
		}
	}
	return NewTable(Texts("id", ids...), Texts("amount", amounts...))
}

func steps() []Op {
	return []Op{
		Cast("amount", TypeInt),
		With("double", Col("amount").Mul(Lit(2))),
		Where(Col("double").Mod(Lit(3)).Ne(Lit(0))),
		GroupBy([]string{"id"}, Sum("double"), Count("amount").As("n")),
		Sort(Desc("double")),
		Select("id", "double", "n"),
	}
}

// A plan error is found before any row is read (D19; prepares T15).
func TestPipelinePlanErrorBeforeAnyRowIsRead(t *testing.T) {
	src := &countingSource{tableSource: tableSource{delivery(20)}}
	for name, op := range map[string]Op{
		"unknown column": With("x", Col("nope").Add(Lit(1))),
		"type conflict":  With("x", Col("amount").Add(Lit(1))), // amount is still text
		"after a step":   Rename("amount", "a"),
	} {
		p := &Pipeline{src: src, blockLen: 4}
		p.Then(Select("id", "amount"))
		if name == "after a step" {
			p.Then(op).Then(Cast("amount", TypeInt))
		} else {
			p.Step("mine", op)
		}
		_, err := p.Run(context.Background())
		pe := wantPlanError(t, err)
		if name != "after a step" && pe.Step != "mine" {
			t.Errorf("%s: step = %q", name, pe.Step)
		}
		if src.read != 0 {
			t.Fatalf("%s: %d rows read before the plan error", name, src.read)
		}
	}
	wantPlanError(t, From(delivery(1), 0).Check())
	wantPlanError(t, From(delivery(1).Select("x"), 1).Check())
	wantPlanError(t, From(delivery(1), 1).Then(Op{}).Check())
}

// The same operations give the same result as Table methods and as pipeline
// steps, for any block length (D31; prepares T22).
func TestPipelineMatchesTableMethods(t *testing.T) {
	src := delivery(100)
	eager := src
	for _, op := range steps() {
		eager = eager.Apply(op)
	}
	if eager.Err() != nil {
		t.Fatal(eager.Err())
	}
	for _, n := range []int{1, 3, 64, 1000} {
		p := From(src, n)
		for _, op := range steps() {
			p.Then(op)
		}
		got, err := p.Run(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range eager.Columns() {
			if a, b := cells(t, got, c), cells(t, eager, c); !slices.Equal(a, b) {
				t.Errorf("block length %d, column %s: %v, want %v", n, c, a, b)
			}
		}
		if !slices.Equal(withoutIDs(got.Rejects()), withoutIDs(eager.Rejects())) {
			t.Errorf("block length %d: rejects differ", n)
		}
	}
}

func TestPipelineRunsBlockByBlock(t *testing.T) {
	src := &countingSource{tableSource: tableSource{delivery(10)}}
	var sizes []int
	p := &Pipeline{src: src, blockLen: 4}
	p.Then(WhereFunc(func(r Row) (bool, error) { return true, nil }))
	p.steps = append(p.steps, step{"probe", Op{probe{&sizes}}})
	if _, err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(sizes, []int{4, 4, 2}) {
		t.Errorf("block sizes = %v, want 4, 4, 2", sizes)
	}
}

type probe struct{ sizes *[]int }

func (probe) kind() string                   { return "probe" }
func (probe) plan(in schema) (schema, error) { return in, nil }
func (p probe) apply(b block.Block, _ schema, _ *stepCtx) (block.Block, []int, error) {
	*p.sizes = append(*p.sizes, b.Len())
	return b, nil, nil
}

// withoutIDs returns rs without their reject_ids, which differ from run to
// run (D76).
func withoutIDs(rs []Reject) []Reject {
	out := make([]Reject, len(rs))
	for i, r := range rs {
		r.ID = ""
		out[i] = r
	}
	return out
}

func TestPipelineCarriesSourceRejectsAndNamesSteps(t *testing.T) {
	src := NewTable(Texts("a", "1", "x")).Cast("a", TypeInt)
	got, err := From(src, 2).Step("double", With("a", Col("a").Div(Lit(0)))).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rs := got.Rejects()
	if len(rs) != 2 || rs[0].Step != "cast" || rs[1].Step != "double" || rs[1].Code != CodeExpr {
		t.Errorf("rejects = %v", rs)
	}
}

func TestPipelineStopsOnCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := From(delivery(10), 2).Then(Select("id")).Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v", err)
	}
}

func TestPipelineOverEmptySource(t *testing.T) {
	got, err := From(delivery(0), 2).Then(Cast("amount", TypeInt)).Then(Sort(Asc("amount"))).Run(context.Background())
	if err != nil || got.Len() != 0 || !slices.Equal(got.Columns(), []string{"id", "amount"}) {
		t.Errorf("got %v, %v", got.Columns(), err)
	}
}
