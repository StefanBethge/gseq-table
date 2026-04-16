package json

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stefanbethge/gseq-table/table"
	"github.com/stefanbethge/gseq/slice"
)

func newTable(headers []string, records [][]string) table.Table {
	return table.New(slice.Slice[string](headers), records)
}

func TestWriteArray(t *testing.T) {
	tbl := newTable(
		[]string{"id", "name"},
		[][]string{{"1", "Alice"}, {"2", "Bob"}},
	)
	var buf bytes.Buffer
	if err := NewWriter().Write(&buf, tbl); err != nil {
		t.Fatal(err)
	}
	got := strings.TrimSpace(buf.String())
	want := `[{"id":"1","name":"Alice"},{"id":"2","name":"Bob"}]`
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestWriteNDJSON(t *testing.T) {
	tbl := newTable(
		[]string{"a", "b"},
		[][]string{{"1", "2"}, {"3", "4"}},
	)
	var buf bytes.Buffer
	if err := NewWriter(WithWriteNDJSON()).Write(&buf, tbl); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2", len(lines))
	}
	if want := `{"a":"1","b":"2"}`; lines[0] != want {
		t.Errorf("line[0] = %q, want %q", lines[0], want)
	}
	if want := `{"a":"3","b":"4"}`; lines[1] != want {
		t.Errorf("line[1] = %q, want %q", lines[1], want)
	}
}

func TestWritePrettyPrint(t *testing.T) {
	tbl := newTable([]string{"x"}, [][]string{{"1"}})
	var buf bytes.Buffer
	if err := NewWriter(WithPrettyPrint()).Write(&buf, tbl); err != nil {
		t.Fatal(err)
	}
	got := buf.String()
	if !strings.Contains(got, "\n") {
		t.Error("pretty-printed output should contain newlines")
	}
	if !strings.Contains(got, `  `) {
		t.Error("pretty-printed output should contain indentation")
	}
}

func TestWriteCustomIndent(t *testing.T) {
	tbl := newTable([]string{"x"}, [][]string{{"1"}})
	var buf bytes.Buffer
	if err := NewWriter(WithPrettyPrint(), WithIndent("\t")).Write(&buf, tbl); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "\t") {
		t.Error("output should contain tab indentation")
	}
}

func TestWriteEmptyTable(t *testing.T) {
	tbl := newTable([]string{}, nil)
	var buf bytes.Buffer
	if err := NewWriter().Write(&buf, tbl); err != nil {
		t.Fatal(err)
	}
	got := strings.TrimSpace(buf.String())
	if got != "[]" {
		t.Errorf("got %q, want %q", got, "[]")
	}
}

func TestWriteFile(t *testing.T) {
	tbl := newTable([]string{"k"}, [][]string{{"v"}})
	dir := t.TempDir()
	path := filepath.Join(dir, "out.json")
	if err := NewWriter().WriteFile(path, tbl); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got := strings.TrimSpace(string(data))
	want := `[{"k":"v"}]`
	if got != want {
		t.Errorf("file content = %q, want %q", got, want)
	}
}

func TestRoundTrip(t *testing.T) {
	input := `[{"id":"1","name":"Alice"},{"id":"2","name":"Bob"}]`
	tbl := New(WithSortedHeaders()).ReadString(input).Unwrap()

	var buf bytes.Buffer
	if err := NewWriter().Write(&buf, tbl); err != nil {
		t.Fatal(err)
	}

	tbl2 := New(WithSortedHeaders()).ReadString(strings.TrimSpace(buf.String())).Unwrap()

	if len(tbl.Headers) != len(tbl2.Headers) {
		t.Fatalf("header count mismatch: %d vs %d", len(tbl.Headers), len(tbl2.Headers))
	}
	if len(tbl.Rows) != len(tbl2.Rows) {
		t.Fatalf("row count mismatch: %d vs %d", len(tbl.Rows), len(tbl2.Rows))
	}
	for _, h := range tbl.Headers {
		for j := range tbl.Rows {
			a := tbl.Rows[j].Get(h).UnwrapOr("")
			b := tbl2.Rows[j].Get(h).UnwrapOr("")
			if a != b {
				t.Errorf("row[%d].%s: %q vs %q", j, h, a, b)
			}
		}
	}
}

func TestWriteNDJSONFile(t *testing.T) {
	tbl := newTable([]string{"a"}, [][]string{{"1"}, {"2"}})
	dir := t.TempDir()
	path := filepath.Join(dir, "out.ndjson")
	if err := NewWriter(WithWriteNDJSON()).WriteFile(path, tbl); err != nil {
		t.Fatal(err)
	}

	// Read back as NDJSON.
	res := New(WithNDJSON()).ReadFile(path)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl2 := res.Unwrap()
	if got := len(tbl2.Rows); got != 2 {
		t.Fatalf("rows: got %d, want 2", got)
	}
}

func TestToString(t *testing.T) {
	tbl := newTable([]string{"x"}, [][]string{{"1"}})
	got := strings.TrimSpace(ToString(tbl))
	want := `[{"x":"1"}]`
	if got != want {
		t.Errorf("ToString = %q, want %q", got, want)
	}
}
