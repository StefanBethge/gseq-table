package csv

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/stefanbethge/gseq-table/table"
)

func makeWriteTable() table.Table {
	return table.New(
		[]string{"name", "city", "age"},
		[][]string{
			{"Alice", "Berlin", "30"},
			{"Bob", "Munich", "25"},
		},
	)
}

func TestWriter_Write_WithHeader(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter()
	if err := w.Write(&buf, makeWriteTable()); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	assertEqual(t, len(lines), 3) // header + 2 rows
	assertEqual(t, lines[0], "name,city,age")
	assertEqual(t, lines[1], "Alice,Berlin,30")
	assertEqual(t, lines[2], "Bob,Munich,25")
}

func TestWriter_Write_WithoutHeader(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(WithoutHeader())
	if err := w.Write(&buf, makeWriteTable()); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	assertEqual(t, len(lines), 2)
	assertEqual(t, lines[0], "Alice,Berlin,30")
}

func TestWriter_Write_Semicolon(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(WithWriteSeparator(';'))
	if err := w.Write(&buf, makeWriteTable()); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	assertEqual(t, lines[0], "name;city;age")
	assertEqual(t, lines[1], "Alice;Berlin;30")
}

func TestWriter_WriteFile(t *testing.T) {
	f, err := os.CreateTemp("", "test_write_*.csv")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	w := NewWriter()
	if err := w.WriteFile(path, makeWriteTable()); err != nil {
		t.Fatal(err)
	}

	// read back with the CSV reader
	r := New()
	res := r.ReadFile(path)
	assertEqual(t, res.IsOk(), true)
	tb := res.Unwrap()
	assertEqual(t, len(tb.Rows), 2)
	assertEqual(t, tb.Rows[0].Get("name").UnwrapOr(""), "Alice")
	assertEqual(t, tb.Rows[1].Get("city").UnwrapOr(""), "Munich")
}

func TestWriter_Roundtrip(t *testing.T) {
	// write with semicolon, read back with semicolon
	var buf bytes.Buffer
	w := NewWriter(WithWriteSeparator(';'))
	if err := w.Write(&buf, makeWriteTable()); err != nil {
		t.Fatal(err)
	}
	r := New(WithSeparator(';'))
	res := r.Read(strings.NewReader(buf.String()))
	assertEqual(t, res.IsOk(), true)
	tb := res.Unwrap()
	assertEqual(t, tb.Rows[0].Get("name").UnwrapOr(""), "Alice")
	assertEqual(t, tb.Rows[1].Get("age").UnwrapOr(""), "25")
}

func TestToString(t *testing.T) {
	tb := makeWriteTable()
	s := ToString(tb)
	lines := strings.Split(strings.TrimSpace(s), "\n")
	assertEqual(t, len(lines), 3)
	assertEqual(t, lines[0], "name,city,age")
}

func TestWriter_Write_QuotesSpecialChars(t *testing.T) {
	tb := table.New([]string{"v"}, [][]string{{"hello, world"}, {"line\nnbreak"}})
	var buf bytes.Buffer
	w := NewWriter(WithoutHeader())
	if err := w.Write(&buf, tb); err != nil {
		t.Fatal(err)
	}
	// encoding/csv should quote values containing comma or newline
	content := buf.String()
	if !strings.Contains(content, `"hello, world"`) {
		t.Errorf("expected quoted value, got: %s", content)
	}
}
