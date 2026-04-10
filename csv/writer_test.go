package csv

import (
	"bytes"
	"os"
	"strconv"
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

func TestWriter_Write_PadsShortRowsToHeaderWidth(t *testing.T) {
	tb := table.New([]string{"a", "b"}, [][]string{{"1"}})

	var buf bytes.Buffer
	if err := NewWriter().Write(&buf, tb); err != nil {
		t.Fatal(err)
	}

	assertEqual(t, buf.String(), "a,b\n1,\n")

	res := New().Read(strings.NewReader(buf.String()))
	assertEqual(t, res.IsOk(), true)
	assertEqual(t, res.Unwrap().Rows[0].Get("b").UnwrapOr("x"), "")
}

func TestWriter_Write_WithoutHeader_PadsToWidestRow(t *testing.T) {
	tb := table.New(nil, [][]string{{"1"}, {"2", "3"}})

	var buf bytes.Buffer
	if err := NewWriter(WithoutHeader()).Write(&buf, tb); err != nil {
		t.Fatal(err)
	}

	assertEqual(t, buf.String(), "1,\n2,3\n")
}

func BenchmarkWriter_Write(b *testing.B) {
	records := make([][]string, 10_000)
	for i := range records {
		records[i] = []string{
			"user_" + strconv.Itoa(i),
			"city_" + strconv.Itoa(i%100),
			strconv.Itoa(20 + i%50),
		}
	}
	tb := table.New([]string{"name", "city", "age"}, records)
	w := NewWriter()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		if err := w.Write(&buf, tb); err != nil {
			b.Fatal(err)
		}
	}
}
