package etl

import (
	"testing"

	"github.com/stefanbethge/gseq-table/table"
)

func TestWithTracing_NilByDefault(t *testing.T) {
	p := From(makeTable())
	assertEqual(t, p.Trace() == nil, true)
}

func TestStep_RecordsName(t *testing.T) {
	p := From(makeTable()).WithTracing().
		Step("filter", func(tb table.Table) table.Table {
			return tb.Where(func(r table.Row) bool {
				return r.Get("city").UnwrapOr("") == "Berlin"
			})
		})
	tr := p.Trace()
	assertEqual(t, len(tr), 1)
	assertEqual(t, tr[0].Name, "filter")
}

func TestStep_RowCounts(t *testing.T) {
	p := From(makeTable()).WithTracing().
		Step("filter", func(tb table.Table) table.Table {
			return tb.Where(func(r table.Row) bool {
				return r.Get("city").UnwrapOr("") == "Berlin"
			})
		})
	tr := p.Trace()
	assertEqual(t, tr[0].InputRows, 3)
	assertEqual(t, tr[0].OutputRows, 2)
}

func TestStep_Duration(t *testing.T) {
	p := From(makeTable()).WithTracing().
		Step("noop", func(tb table.Table) table.Table { return tb })
	tr := p.Trace()
	if tr[0].Duration < 0 {
		t.Errorf("expected non-negative duration, got %v", tr[0].Duration)
	}
}

func TestStep_MultipleSteps(t *testing.T) {
	p := From(makeTable()).WithTracing().
		Step("step1", func(tb table.Table) table.Table {
			return tb.Where(func(r table.Row) bool {
				return r.Get("city").UnwrapOr("") == "Berlin"
			})
		}).
		Step("step2", func(tb table.Table) table.Table {
			return tb.Select("name")
		})
	tr := p.Trace()
	assertEqual(t, len(tr), 2)
	assertEqual(t, tr[0].Name, "step1")
	assertEqual(t, tr[1].Name, "step2")
	assertEqual(t, tr[0].OutputRows, tr[1].InputRows)
}

func TestStep_SkippedOnError(t *testing.T) {
	p := From(makeTable()).WithTracing().
		AssertColumns("nonexistent").
		Step("never", func(tb table.Table) table.Table { return tb })
	assertEqual(t, p.IsErr(), true)
	assertEqual(t, len(p.Trace()), 0)
}

func TestStep_ThenContinues(t *testing.T) {
	// Step result should be usable with Then afterwards
	p := From(makeTable()).WithTracing().
		Step("filter", func(tb table.Table) table.Table {
			return tb.Where(func(r table.Row) bool { return r.Get("city").UnwrapOr("") == "Berlin" })
		}).
		Then(func(tb table.Table) table.Table { return tb.Select("name") })
	assertEqual(t, p.IsOk(), true)
	assertEqual(t, len(p.Unwrap().Headers), 1)
}
