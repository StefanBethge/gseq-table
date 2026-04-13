package etl

import (
	"strings"
	"testing"

	gcsv "github.com/stefanbethge/gseq-table/csv"
	"github.com/stefanbethge/gseq-table/table"
)

func assertEqual[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func makeTable() table.Table {
	return table.New(
		[]string{"name", "city", "age"},
		[][]string{
			{"Alice", "Berlin", "30"},
			{"Bob", "Munich", "25"},
			{"Carol", "Berlin", "35"},
		},
	)
}

func TestFrom(t *testing.T) {
	p := From(makeTable())
	assertEqual(t, p.IsOk(), true)
	assertEqual(t, len(p.Unwrap().Rows), 3)
}

func TestFromResult_Ok(t *testing.T) {
	p := FromResult(gcsv.New().Read(strings.NewReader("a,b\n1,2\n")))
	assertEqual(t, p.IsOk(), true)
}

func TestFromResult_Err(t *testing.T) {
	csvErr := gcsv.New().Read(strings.NewReader("a,\"b\nc")) // parse error
	p := FromResult(csvErr)
	assertEqual(t, p.IsErr(), true)
}

func TestPipeline_Then_Where(t *testing.T) {
	p := From(makeTable()).Then(func(tb table.Table) table.Table {
		return tb.Where(func(r table.Row) bool {
			return r.Get("city").UnwrapOr("") == "Berlin"
		})
	})
	assertEqual(t, p.IsOk(), true)
	assertEqual(t, len(p.Unwrap().Rows), 2)
}

func TestPipeline_Then_Select(t *testing.T) {
	p := From(makeTable()).Then(func(tb table.Table) table.Table {
		return tb.Select("name", "city")
	})
	tb := p.Unwrap()
	assertEqual(t, len(tb.Headers), 2)
	assertEqual(t, tb.Rows[0].Get("age").IsNone(), true)
}

func TestPipeline_Then_Map(t *testing.T) {
	p := From(makeTable()).Then(func(tb table.Table) table.Table {
		return tb.Map("city", func(v string) string { return "DE-" + v })
	})
	assertEqual(t, p.Unwrap().Rows[0].Get("city").UnwrapOr(""), "DE-Berlin")
}

func TestPipeline_Then_AddCol(t *testing.T) {
	p := From(makeTable()).Then(func(tb table.Table) table.Table {
		return tb.AddCol("tag", func(r table.Row) string {
			return r.Get("name").UnwrapOr("") + "_" + r.Get("city").UnwrapOr("")
		})
	})
	tb := p.Unwrap()
	assertEqual(t, len(tb.Headers), 4)
	assertEqual(t, tb.Rows[0].Get("tag").UnwrapOr(""), "Alice_Berlin")
}

func TestPipeline_Then_Sort(t *testing.T) {
	p := From(makeTable()).Then(func(tb table.Table) table.Table {
		return tb.Sort("name", true)
	})
	rows := p.Unwrap().Rows
	assertEqual(t, rows[0].Get("name").UnwrapOr(""), "Alice")
	assertEqual(t, rows[2].Get("name").UnwrapOr(""), "Carol")
}

func TestPipeline_Then_Rename(t *testing.T) {
	p := From(makeTable()).Then(func(tb table.Table) table.Table {
		return tb.Rename("city", "location")
	})
	assertEqual(t, p.Unwrap().Headers[1], "location")
}

func TestPipeline_Then_Join(t *testing.T) {
	cities := table.New(
		[]string{"city", "country"},
		[][]string{{"Berlin", "DE"}, {"Munich", "DE"}},
	)
	p := From(makeTable()).Then(func(tb table.Table) table.Table {
		return tb.Join(cities, "city", "city")
	})
	tb := p.Unwrap()
	assertEqual(t, len(tb.Rows), 3) // Carol@Berlin also matches
	assertEqual(t, tb.Rows[0].Get("country").UnwrapOr(""), "DE")
}

func TestPipeline_GroupBy(t *testing.T) {
	groups := From(makeTable()).GroupBy("city")
	assertEqual(t, len(groups), 2)
	assertEqual(t, len(groups["Berlin"].Unwrap().Rows), 2)
	assertEqual(t, len(groups["Munich"].Unwrap().Rows), 1)
}

func TestPipeline_GroupBy_ErrorForwarded(t *testing.T) {
	errResult := gcsv.New().Read(strings.NewReader("a,\"b\nc"))
	groups := FromResult(errResult).GroupBy("x")
	assertEqual(t, len(groups), 1)
	assertEqual(t, groups[""].IsErr(), true)
}

func TestPipeline_ErrorPropagation(t *testing.T) {
	// error from CSV reader should short-circuit all chained ops
	p := FromResult(gcsv.New().Read(strings.NewReader("a,\"b\nc"))).
		Then(func(tb table.Table) table.Table { return tb.Where(func(r table.Row) bool { return true }) }).
		Then(func(tb table.Table) table.Table { return tb.Select("a") }).
		Then(func(tb table.Table) table.Table { return tb.Map("a", func(v string) string { return v + "!" }) })
	assertEqual(t, p.IsErr(), true)
}

func TestPipeline_FullChain(t *testing.T) {
	const data = `name,city,score
Alice,Berlin,80
Bob,Munich,90
Carol,Berlin,70
Dave,Hamburg,85
`
	p := FromResult(gcsv.New().Read(strings.NewReader(data))).
		Then(func(tb table.Table) table.Table {
			return tb.Where(func(r table.Row) bool {
				return r.Get("city").UnwrapOr("") != "Hamburg"
			})
		}).
		Then(func(tb table.Table) table.Table {
			return tb.Map("score", func(v string) string { return v + "pts" })
		}).
		Then(func(tb table.Table) table.Table {
			return tb.AddCol("summary", func(r table.Row) string {
				return r.Get("name").UnwrapOr("") + ":" + r.Get("score").UnwrapOr("")
			})
		}).
		Then(func(tb table.Table) table.Table {
			return tb.Sort("name", true)
		})

	assertEqual(t, p.IsOk(), true)
	tb := p.Unwrap()
	assertEqual(t, len(tb.Rows), 3)
	assertEqual(t, tb.Rows[0].Get("name").UnwrapOr(""), "Alice")
	assertEqual(t, tb.Rows[0].Get("score").UnwrapOr(""), "80pts")
	assertEqual(t, tb.Rows[0].Get("summary").UnwrapOr(""), "Alice:80pts")
}

func TestPipeline_AssertColumns(t *testing.T) {
	p := From(makeTable()).AssertColumns("name", "city")
	assertEqual(t, p.IsOk(), true)

	pErr := From(makeTable()).AssertColumns("name", "missing")
	assertEqual(t, pErr.IsErr(), true)
}

func TestPipeline_AssertNoEmpty(t *testing.T) {
	p := From(makeTable()).AssertNoEmpty("name")
	assertEqual(t, p.IsOk(), true)

	sparse := table.New(
		[]string{"name", "city"},
		[][]string{{"Alice", ""}, {"", "Berlin"}},
	)
	pErr := From(sparse).AssertNoEmpty("city")
	assertEqual(t, pErr.IsErr(), true)
}

func TestPipeline_Peek(t *testing.T) {
	var seen int
	_ = From(makeTable()).Peek(func(tb table.Table) {
		seen = tb.Len()
	}).Unwrap()
	assertEqual(t, seen, 3)
}
