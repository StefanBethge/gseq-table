package etl

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stefanbethge/gseq-table/table"
	"github.com/stefanbethge/gseq/result"
)

func makeMutable() *table.MutableTable {
	return table.NewMutable(
		[]string{"name", "city", "age"},
		[][]string{
			{"Alice", "Berlin", "30"},
			{"Bob", "Munich", "25"},
			{"Carol", "Berlin", "35"},
		},
	)
}

// ─── Constructors ─────────────────────────────────────────────────────────────

func TestFromMutable_IsOk(t *testing.T) {
	mp := FromMutable(makeMutable())
	assertEqual(t, mp.IsOk(), true)
	assertEqual(t, mp.Unwrap().Len(), 3)
}

// From() now accepts *MutableTable via the Freezable interface.
func TestFrom_AcceptsMutable(t *testing.T) {
	p := From(makeMutable()) // calls Freeze() internally
	assertEqual(t, p.IsOk(), true)
	assertEqual(t, p.Unwrap().Len(), 3)
}

// ─── Then / MutableFunc ───────────────────────────────────────────────────────

func TestMutablePipeline_Then(t *testing.T) {
	mp := FromMutable(makeMutable()).Then(func(m *table.MutableTable) *table.MutableTable {
		return m.Map("city", func(v string) string { return "DE-" + v })
	})
	row, _ := mp.Unwrap().Row(0)
	assertEqual(t, row.Get("city").UnwrapOr(""), "DE-Berlin")
}

// MutableFunc as a named reusable function — the whole point of the type.
func TestMutablePipeline_NamedMutableFunc(t *testing.T) {
	var upperCity MutableFunc = func(m *table.MutableTable) *table.MutableTable {
		return m.Map("city", strings.ToUpper)
	}
	mp := FromMutable(makeMutable()).Then(upperCity)
	row, _ := mp.Unwrap().Row(0)
	assertEqual(t, row.Get("city").UnwrapOr(""), "BERLIN")
}

func TestMutablePipeline_MultiStep(t *testing.T) {
	mp := FromMutable(makeMutable()).
		Then(func(m *table.MutableTable) *table.MutableTable {
			return m.Map("city", strings.ToUpper)
		}).
		Then(func(m *table.MutableTable) *table.MutableTable {
			return m.FillEmpty("age", "0")
		}).
		Then(func(m *table.MutableTable) *table.MutableTable {
			return m.AddCol("label", func(r table.Row) string {
				return r.Get("name").UnwrapOr("") + "@" + r.Get("city").UnwrapOr("")
			})
		})
	m := mp.Unwrap()
	assertEqual(t, m.Len(), 3)
	row, _ := m.Row(0)
	assertEqual(t, row.Get("label").UnwrapOr(""), "Alice@BERLIN")
}

// ─── TableFunc ────────────────────────────────────────────────────────────────

func TestTableFunc_NamedFunction(t *testing.T) {
	// Named TableFunc passed directly — no closure wrapper needed
	var selectName TableFunc = func(t table.Table) table.Table {
		return t.Select("name")
	}
	p := From(makeTable()).Then(selectName)
	assertEqual(t, len(p.Unwrap().Headers), 1)
}

// Regular named function also satisfies TableFunc implicitly.
func selectNameFn(t table.Table) table.Table { return t.Select("name") }

func TestTableFunc_PlainFunction(t *testing.T) {
	p := From(makeTable()).Then(selectNameFn)
	assertEqual(t, len(p.Unwrap().Headers), 1)
}

// ─── Error propagation ────────────────────────────────────────────────────────

func TestMutablePipeline_ErrorPropagation(t *testing.T) {
	sentinel := errors.New("injected")
	mp := FromMutable(makeMutable()).
		ThenErr(func(m *table.MutableTable) result.Result[*table.MutableTable, error] {
			return result.Err[*table.MutableTable, error](sentinel)
		}).
		Then(func(m *table.MutableTable) *table.MutableTable {
			t.Fatal("should not be called after error")
			return m
		})
	assertEqual(t, mp.IsErr(), true)
	assertEqual(t, errors.Is(mp.Result().UnwrapErr(), sentinel), true)
}

// ─── IfThen ───────────────────────────────────────────────────────────────────

func TestMutablePipeline_IfThen_Applied(t *testing.T) {
	mp := FromMutable(makeMutable()).IfThen(true, func(m *table.MutableTable) *table.MutableTable {
		return m.Drop("age")
	})
	assertEqual(t, len(mp.Unwrap().Headers()), 2)
}

func TestMutablePipeline_IfThen_Skipped(t *testing.T) {
	mp := FromMutable(makeMutable()).IfThen(false, func(m *table.MutableTable) *table.MutableTable {
		return m.Drop("age")
	})
	assertEqual(t, len(mp.Unwrap().Headers()), 3) // unchanged
}

// ─── RecoverWith ──────────────────────────────────────────────────────────────

func TestMutablePipeline_RecoverWith(t *testing.T) {
	fallback := table.NewMutable([]string{"x"}, [][]string{{"fallback"}})
	sentinel := errors.New("boom")
	mp := FromMutable(makeMutable()).
		ThenErr(func(m *table.MutableTable) result.Result[*table.MutableTable, error] {
			return result.Err[*table.MutableTable, error](sentinel)
		}).
		RecoverWith(fallback)
	assertEqual(t, mp.IsOk(), true)
	row, _ := mp.Unwrap().Row(0)
	assertEqual(t, row.Get("x").UnwrapOr(""), "fallback")
}

// ─── MapErr ───────────────────────────────────────────────────────────────────

func TestMutablePipeline_MapErr(t *testing.T) {
	sentinel := errors.New("original")
	mp := FromMutable(makeMutable()).
		ThenErr(func(m *table.MutableTable) result.Result[*table.MutableTable, error] {
			return result.Err[*table.MutableTable, error](sentinel)
		}).
		MapErr(func(err error) error {
			return fmt.Errorf("wrapped: %w", err)
		})
	assertEqual(t, mp.IsErr(), true)
	msg := mp.Result().UnwrapErr().Error()
	if !strings.HasPrefix(msg, "wrapped:") {
		t.Errorf("expected wrapped error, got %q", msg)
	}
}

// ─── Tracing ─────────────────────────────────────────────────────────────────

func TestMutablePipeline_Tracing(t *testing.T) {
	mp := FromMutable(makeMutable()).WithTracing().
		Step("upper", func(m *table.MutableTable) *table.MutableTable {
			return m.Map("city", strings.ToUpper)
		}).
		Step("filter", func(m *table.MutableTable) *table.MutableTable {
			return m.Where(func(r table.Row) bool {
				return r.Get("city").UnwrapOr("") == "BERLIN"
			})
		})

	tr := mp.Trace()
	assertEqual(t, len(tr), 2)
	assertEqual(t, tr[0].Name, "upper")
	assertEqual(t, tr[0].InputRows, 3)
	assertEqual(t, tr[0].OutputRows, 3)
	assertEqual(t, tr[1].Name, "filter")
	assertEqual(t, tr[1].InputRows, 3)
	assertEqual(t, tr[1].OutputRows, 2)
}

// ─── Peek ─────────────────────────────────────────────────────────────────────

func TestMutablePipeline_Peek(t *testing.T) {
	var seen int
	_ = FromMutable(makeMutable()).Peek(func(m *table.MutableTable) {
		seen = m.Len()
	}).Unwrap()
	assertEqual(t, seen, 3)
}

// ─── Frozen() — bridge to immutable Pipeline ─────────────────────────────────

func TestMutablePipeline_Frozen(t *testing.T) {
	p := FromMutable(makeMutable()).
		Then(func(m *table.MutableTable) *table.MutableTable {
			return m.Map("city", strings.ToUpper)
		}).
		Frozen(). // → immutable Pipeline
		Then(func(t table.Table) table.Table {
			return t.Select("name", "city")
		})

	assertEqual(t, p.IsOk(), true)
	tb := p.Unwrap()
	assertEqual(t, len(tb.Headers), 2)
	assertEqual(t, tb.Rows[0].Get("city").UnwrapOr(""), "BERLIN")
}

func TestMutablePipeline_Frozen_ErrorPropagated(t *testing.T) {
	sentinel := errors.New("boom")
	p := FromMutable(makeMutable()).
		ThenErr(func(m *table.MutableTable) result.Result[*table.MutableTable, error] {
			return result.Err[*table.MutableTable, error](sentinel)
		}).
		Frozen()
	assertEqual(t, p.IsErr(), true)
	assertEqual(t, errors.Is(p.Result().UnwrapErr(), sentinel), true)
}

func TestMutablePipeline_Frozen_WithSchema(t *testing.T) {
	mp := FromMutable(makeMutable()).
		Then(func(m *table.MutableTable) *table.MutableTable {
			return m.Map("age", func(v string) string { return v })
		}).
		Frozen().
		AssertColumns("name", "city", "age")
	assertEqual(t, mp.IsOk(), true)
}
