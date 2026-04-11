package etl

import (
	"errors"
	"testing"

	"github.com/stefanbethge/gseq-table/table"
	"github.com/stefanbethge/gseq/result"
)

// ─── IfThen ───────────────────────────────────────────────────────────────────

func TestIfThen_Applied(t *testing.T) {
	p := From(makeTable()).IfThen(true, func(tb table.Table) table.Table {
		return tb.Select("name")
	})
	assertEqual(t, len(p.Unwrap().Headers), 1)
}

func TestIfThen_Skipped(t *testing.T) {
	p := From(makeTable()).IfThen(false, func(tb table.Table) table.Table {
		return tb.Select("name")
	})
	assertEqual(t, len(p.Unwrap().Headers), 3) // unchanged
}

func TestIfThen_ErrorUnchanged(t *testing.T) {
	// error pipeline stays error regardless of cond
	errP := From(makeTable()).AssertColumns("nonexistent")
	assertEqual(t, errP.IfThen(true, func(tb table.Table) table.Table { return tb }).IsErr(), true)
	assertEqual(t, errP.IfThen(false, func(tb table.Table) table.Table { return tb }).IsErr(), true)
}

func TestIfThenErr_Applied(t *testing.T) {
	sentinel := errors.New("fail")
	p := From(makeTable()).IfThenErr(true, func(tb table.Table) result.Result[table.Table, error] {
		return result.Err[table.Table, error](sentinel)
	})
	assertEqual(t, p.IsErr(), true)
	assertEqual(t, errors.Is(p.Result().UnwrapErr(), sentinel), true)
}

func TestIfThenErr_Skipped(t *testing.T) {
	sentinel := errors.New("fail")
	p := From(makeTable()).IfThenErr(false, func(tb table.Table) result.Result[table.Table, error] {
		return result.Err[table.Table, error](sentinel)
	})
	assertEqual(t, p.IsOk(), true) // fn not called
}

// ─── RecoverWith ──────────────────────────────────────────────────────────────

func TestRecoverWith_OkPipeline(t *testing.T) {
	fallback := table.New([]string{"x"}, [][]string{{"1"}})
	p := From(makeTable()).RecoverWith(fallback)
	assertEqual(t, len(p.Unwrap().Rows), 3) // original table, not fallback
}

func TestRecoverWith_ErrPipeline(t *testing.T) {
	fallback := table.New([]string{"x"}, [][]string{{"fallback"}})
	p := From(makeTable()).AssertColumns("nonexistent").RecoverWith(fallback)
	assertEqual(t, p.IsOk(), true)
	assertEqual(t, p.Unwrap().Rows[0].Get("x").UnwrapOr(""), "fallback")
}

// ─── OnError ──────────────────────────────────────────────────────────────────

func TestOnError_OkPipeline(t *testing.T) {
	called := false
	p := From(makeTable()).OnError(func(err error) (table.Table, error) {
		called = true
		return table.Table{}, nil
	})
	assertEqual(t, p.IsOk(), true)
	assertEqual(t, called, false)
}

func TestOnError_Clears(t *testing.T) {
	fallback := table.New([]string{"x"}, [][]string{{"recovered"}})
	p := From(makeTable()).AssertColumns("nonexistent").
		OnError(func(err error) (table.Table, error) {
			return fallback, nil
		})
	assertEqual(t, p.IsOk(), true)
	assertEqual(t, p.Unwrap().Rows[0].Get("x").UnwrapOr(""), "recovered")
}

func TestOnError_ReplacesError(t *testing.T) {
	orig := errors.New("original")
	replaced := errors.New("replaced")
	p := From(makeTable()).
		ThenErr(func(tb table.Table) result.Result[table.Table, error] {
			return result.Err[table.Table, error](orig)
		}).
		OnError(func(err error) (table.Table, error) {
			return table.Table{}, replaced
		})
	assertEqual(t, p.IsErr(), true)
	assertEqual(t, errors.Is(p.Result().UnwrapErr(), replaced), true)
}

// ─── MapErr ───────────────────────────────────────────────────────────────────

func TestMapErr_OkPipeline(t *testing.T) {
	p := From(makeTable()).MapErr(func(err error) error {
		return errors.New("should not be called")
	})
	assertEqual(t, p.IsOk(), true)
	assertEqual(t, len(p.Unwrap().Rows), 3)
}

func TestMapErr_ErrPipeline(t *testing.T) {
	p := From(makeTable()).AssertColumns("nonexistent").
		MapErr(func(err error) error {
			return errors.New("wrapped: " + err.Error())
		})
	assertEqual(t, p.IsErr(), true)
	msg := p.Result().UnwrapErr().Error()
	if len(msg) < len("wrapped: ") || msg[:8] != "wrapped:" {
		t.Errorf("expected wrapped error, got %q", msg)
	}
}
