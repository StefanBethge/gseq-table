package csv

import (
	"os"
	"strings"
	"testing"
)

func assertEqual[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

const csvWithHeader = `name,city,age
Alice,Berlin,30
Bob,Munich,25
`

const csvNoHeader = `Alice,Berlin,30
Bob,Munich,25
`

const csvSemicolon = `name;city;age
Alice;Berlin;30
`

func TestRead_WithHeader(t *testing.T) {
	r := New()
	result := r.Read(strings.NewReader(csvWithHeader))
	assertEqual(t, result.IsOk(), true)
	tb := result.Unwrap()
	assertEqual(t, len(tb.Headers), 3)
	assertEqual(t, len(tb.Rows), 2)
	assertEqual(t, tb.Rows[0].Get("name").UnwrapOr(""), "Alice")
	assertEqual(t, tb.Rows[0].Get("city").UnwrapOr(""), "Berlin")
	assertEqual(t, tb.Rows[1].Get("name").UnwrapOr(""), "Bob")
}

func TestRead_NoHeader_AutoNames(t *testing.T) {
	r := New(WithNoHeader())
	result := r.Read(strings.NewReader(csvNoHeader))
	assertEqual(t, result.IsOk(), true)
	tb := result.Unwrap()
	assertEqual(t, tb.Headers[0], "col_0")
	assertEqual(t, tb.Headers[1], "col_1")
	assertEqual(t, tb.Rows[0].Get("col_0").UnwrapOr(""), "Alice")
}

func TestRead_WithHeaderNames(t *testing.T) {
	r := New(WithHeaderNames("vorname", "ort", "alter"))
	result := r.Read(strings.NewReader(csvNoHeader))
	assertEqual(t, result.IsOk(), true)
	tb := result.Unwrap()
	assertEqual(t, tb.Headers[0], "vorname")
	assertEqual(t, tb.Rows[0].Get("vorname").UnwrapOr(""), "Alice")
	assertEqual(t, tb.Rows[0].Get("ort").UnwrapOr(""), "Berlin")
}

func TestRead_Semicolon(t *testing.T) {
	r := New(WithSeparator(';'))
	result := r.Read(strings.NewReader(csvSemicolon))
	assertEqual(t, result.IsOk(), true)
	tb := result.Unwrap()
	assertEqual(t, tb.Rows[0].Get("name").UnwrapOr(""), "Alice")
	assertEqual(t, tb.Rows[0].Get("city").UnwrapOr(""), "Berlin")
}

func TestRead_Empty(t *testing.T) {
	r := New()
	result := r.Read(strings.NewReader(""))
	assertEqual(t, result.IsOk(), true)
	tb := result.Unwrap()
	assertEqual(t, len(tb.Rows), 0)
}

func TestRead_HeaderOnly(t *testing.T) {
	r := New()
	result := r.Read(strings.NewReader("name,city\n"))
	assertEqual(t, result.IsOk(), true)
	tb := result.Unwrap()
	assertEqual(t, len(tb.Headers), 2)
	assertEqual(t, len(tb.Rows), 0)
}

func TestRead_InvalidCSV(t *testing.T) {
	r := New()
	// mismatched quotes cause a parse error
	result := r.Read(strings.NewReader("a,\"b\nc"))
	assertEqual(t, result.IsErr(), true)
}

func TestReadFile(t *testing.T) {
	f, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	f.WriteString("name,val\nfoo,bar\n")
	f.Close()

	r := New()
	result := r.ReadFile(f.Name())
	assertEqual(t, result.IsOk(), true)
	tb := result.Unwrap()
	assertEqual(t, tb.Rows[0].Get("name").UnwrapOr(""), "foo")
}

func TestReadFile_NotFound(t *testing.T) {
	r := New()
	result := r.ReadFile("/nonexistent/path/file.csv")
	assertEqual(t, result.IsErr(), true)
}