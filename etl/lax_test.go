package etl_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stefanbethge/gseq-table/csv"
	"github.com/stefanbethge/gseq-table/etl"
	"github.com/stefanbethge/gseq-table/table"
)

func TestPipeline_TryMap_Strict(t *testing.T) {
	p := etl.FromResult(csv.New().Read(strings.NewReader("price\n1.5\nbad\n3.0\n")))
	result := p.TryMap("price", func(v string) (string, error) {
		if _, err := strconv.ParseFloat(v, 64); err != nil {
			return "", fmt.Errorf("invalid float: %q", v)
		}
		return v, nil
	})
	if result.IsOk() {
		t.Fatal("expected error, got ok")
	}
}

// Missing column is a structural error: even in lax mode it must short-circuit.
func TestPipeline_TryMap_Lax_MissingColumn(t *testing.T) {
	log := etl.NewErrorLog()
	p := etl.FromResult(csv.New().Read(strings.NewReader("name\nAlice\n")))
	res := p.WithErrorLog(log).TryMap("nonexistent", func(v string) (string, error) {
		return v, nil
	})
	if res.IsOk() {
		t.Fatal("expected pipeline error for missing column, got ok")
	}
	if log.HasErrors() {
		t.Fatal("missing column should not appear in ErrorLog (it short-circuits)")
	}
}

func TestPipeline_TryMap_Lax(t *testing.T) {
	log := etl.NewErrorLog()
	p := etl.FromResult(csv.New().Read(strings.NewReader("name,price\nAlice,1.5\nBob,bad\nCarol,3.0\n")))
	res := p.WithErrorLog(log).TryMap("price", func(v string) (string, error) {
		if _, err := strconv.ParseFloat(v, 64); err != nil {
			return "", fmt.Errorf("invalid float: %q", v)
		}
		return v, nil
	})
	if res.IsErr() {
		t.Fatalf("expected ok pipeline, got error: %v", res.Result().UnwrapErr())
	}
	out := res.Unwrap()
	if out.Len() != 2 {
		t.Fatalf("expected 2 rows (bad row filtered), got %d", out.Len())
	}
	if !log.HasErrors() {
		t.Fatal("expected errors in log")
	}
	entries := log.Entries()
	if len(entries) != 1 {
		t.Fatalf("expected 1 error entry, got %d", len(entries))
	}
	e := entries[0]
	if e.Row != 1 {
		t.Errorf("expected row 1, got %d", e.Row)
	}
	if e.OriginalRow["name"] != "Bob" {
		t.Errorf("expected original row name=Bob, got %q", e.OriginalRow["name"])
	}
	if e.OriginalRow["price"] != "bad" {
		t.Errorf("expected original row price=bad, got %q", e.OriginalRow["price"])
	}
}

func TestPipeline_TryTransform_Lax(t *testing.T) {
	log := etl.NewErrorLog()
	data := "id,val\n1,ok\n2,bad\n3,ok\n"
	res := etl.FromResult(csv.New().Read(strings.NewReader(data))).
		WithErrorLog(log).
		TryTransform(func(r table.Row) (map[string]string, error) {
			v := r.Get("val").UnwrapOr("")
			if v == "bad" {
				return nil, fmt.Errorf("bad value in row")
			}
			return map[string]string{"val": strings.ToUpper(v)}, nil
		})
	if res.IsErr() {
		t.Fatal("expected ok, got error")
	}
	out := res.Unwrap()
	if out.Len() != 2 {
		t.Fatalf("expected 2 rows, got %d", out.Len())
	}
	if !log.HasErrors() {
		t.Fatal("expected error in log")
	}
}

func TestErrorLog_ToTable(t *testing.T) {
	log := etl.NewErrorLog()
	data := "name,price\nAlice,1.5\nBob,bad\nCarol,3.0\n"
	_ = etl.FromResult(csv.New().Read(strings.NewReader(data))).
		WithErrorLog(log).
		TryMap("price", func(v string) (string, error) {
			if _, err := strconv.ParseFloat(v, 64); err != nil {
				return "", err
			}
			return v, nil
		})
	errTable := log.ToTable()
	if errTable.Len() != 1 {
		t.Fatalf("expected 1 error row, got %d", errTable.Len())
	}
	row := errTable.Rows[0]
	if row.Get("_row").UnwrapOr("") != "1" {
		t.Errorf("expected _row=1, got %q", row.Get("_row").UnwrapOr(""))
	}
	if row.Get("name").UnwrapOr("") != "Bob" {
		t.Errorf("expected name=Bob, got %q", row.Get("name").UnwrapOr(""))
	}
	if row.Get("price").UnwrapOr("") != "bad" {
		t.Errorf("expected price=bad, got %q", row.Get("price").UnwrapOr(""))
	}
}
