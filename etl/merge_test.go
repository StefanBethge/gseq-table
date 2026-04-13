package etl

import (
	"testing"

	"github.com/stefanbethge/gseq-table/table"
)

// ─── ConcatPipelines ──────────────────────────────────────────────────────────

func TestConcatPipelines_Empty(t *testing.T) {
	p := ConcatPipelines()
	assertEqual(t, p.IsOk(), true)
}

func TestConcatPipelines_AllOk(t *testing.T) {
	a := table.New([]string{"x"}, [][]string{{"1"}, {"2"}})
	b := table.New([]string{"x"}, [][]string{{"3"}})
	p := ConcatPipelines(From(a), From(b))
	assertEqual(t, p.IsOk(), true)
	assertEqual(t, len(p.Unwrap().Rows), 3)
}

func TestConcatPipelines_FirstErr(t *testing.T) {
	errP := From(makeTable()).AssertColumns("nonexistent")
	ok := From(makeTable())
	p := ConcatPipelines(errP, ok)
	assertEqual(t, p.IsErr(), true)
}

func TestConcatPipelines_SecondErr(t *testing.T) {
	ok := From(makeTable())
	errP := From(makeTable()).AssertColumns("nonexistent")
	p := ConcatPipelines(ok, errP)
	assertEqual(t, p.IsErr(), true)
}

// ─── ConcatWith ───────────────────────────────────────────────────────────────

func TestConcatWith_TwoPipelines(t *testing.T) {
	a := table.New([]string{"v"}, [][]string{{"a"}})
	b := table.New([]string{"v"}, [][]string{{"b"}})
	p := From(a).ConcatWith(From(b))
	assertEqual(t, p.IsOk(), true)
	assertEqual(t, len(p.Unwrap().Rows), 2)
}

// ─── FanOut ───────────────────────────────────────────────────────────────────

func TestFanOut_AllOk(t *testing.T) {
	branches := From(makeTable()).FanOut(
		func(tb table.Table) table.Table {
			return tb.Where(func(r table.Row) bool { return r.Get("city").UnwrapOr("") == "Berlin" })
		},
		func(tb table.Table) table.Table {
			return tb.Where(func(r table.Row) bool { return r.Get("city").UnwrapOr("") == "Munich" })
		},
	)
	assertEqual(t, len(branches), 2)
	assertEqual(t, branches[0].IsOk(), true)
	assertEqual(t, branches[1].IsOk(), true)
	assertEqual(t, len(branches[0].Unwrap().Rows), 2)
	assertEqual(t, len(branches[1].Unwrap().Rows), 1)
}

func TestFanOut_OrderPreserved(t *testing.T) {
	tb := table.New([]string{"v"}, [][]string{{"x"}})
	counter := make([]int, 3)
	branches := From(tb).FanOut(
		func(t table.Table) table.Table { counter[0] = 0; return t },
		func(t table.Table) table.Table { counter[1] = 1; return t },
		func(t table.Table) table.Table { counter[2] = 2; return t },
	)
	assertEqual(t, len(branches), 3)
	for _, b := range branches {
		assertEqual(t, b.IsOk(), true)
	}
}

func TestFanOut_ErrorPropagated(t *testing.T) {
	errP := From(makeTable()).AssertColumns("nonexistent")
	branches := errP.FanOut(
		func(tb table.Table) table.Table { return tb },
		func(tb table.Table) table.Table { return tb },
	)
	for _, b := range branches {
		assertEqual(t, b.IsErr(), true)
	}
}
