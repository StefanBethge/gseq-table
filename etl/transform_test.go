package etl

import (
	"testing"

	"github.com/stefanbethge/gseq-table/table"
)

func TestNewTransform_Apply(t *testing.T) {
	selectName := NewTransform("select-name", func(p Pipeline) Pipeline {
		return p.Then(func(tb table.Table) table.Table { return tb.Select("name") })
	})

	p := From(makeTable()).Apply(selectName)
	assertEqual(t, p.IsOk(), true)
	assertEqual(t, len(p.Unwrap().Headers), 1)
	assertEqual(t, p.Unwrap().Headers[0], "name")
}

func TestTransform_Reusable(t *testing.T) {
	dropAge := NewTransform("drop-age", func(p Pipeline) Pipeline {
		return p.Then(func(tb table.Table) table.Table { return tb.Drop("age") })
	})

	p1 := From(makeTable()).Apply(dropAge)
	p2 := From(makeTable()).Apply(dropAge)

	assertEqual(t, len(p1.Unwrap().Headers), 2)
	assertEqual(t, len(p2.Unwrap().Headers), 2)
}

func TestTransform_ErrorPropagated(t *testing.T) {
	noop := NewTransform("noop", func(p Pipeline) Pipeline { return p })
	errP := From(makeTable()).AssertColumns("nonexistent").Apply(noop)
	assertEqual(t, errP.IsErr(), true)
}

func TestTransform_TracingIntegration(t *testing.T) {
	dropAge := NewTransform("drop-age", func(p Pipeline) Pipeline {
		return p.Then(func(tb table.Table) table.Table { return tb.Drop("age") })
	})

	p := From(makeTable()).WithTracing().Apply(dropAge)
	assertEqual(t, p.IsOk(), true)
	tr := p.Trace()
	assertEqual(t, len(tr), 1)
	assertEqual(t, tr[0].Name, "drop-age")
	assertEqual(t, tr[0].InputRows, 3)
	assertEqual(t, tr[0].OutputRows, 3)
}
