package markdown

import (
	"bytes"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stefanbethge/gseq-table/table"
)

var updateGolden = flag.Bool("update", false, "rewrite golden files in testdata/")

// assertGolden compares got with testdata/<name>.golden, rewriting the file
// when the test runs with -update.
func assertGolden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name+".golden")
	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden file (run with -update to create it): %v", err)
	}
	if got != string(want) {
		t.Errorf("%s mismatch\n--- got ---\n%s\n--- want ---\n%s", path, got, want)
	}
}

func fixture() table.Table {
	return table.New(
		[]string{"name", "city", "age", "score"},
		[][]string{
			{"Alice", "Berlin", "30", "1.5"},
			{"Bob", "München", "25", ""},
			{"Chen", "北京", "41", "-2e3"},
		},
	)
}

func render(t *testing.T, w *Writer, tb table.Table) string {
	t.Helper()
	var buf bytes.Buffer
	if err := w.Write(&buf, tb); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

func TestWrite_Golden(t *testing.T) {
	long := make([][]string, 12)
	for i := range long {
		long[i] = []string{strconv.Itoa(i), "row " + strconv.Itoa(i)}
	}
	cases := []struct {
		name string
		w    *Writer
		tb   table.Table
	}{
		{"basic", NewWriter(), fixture()},
		{"no_numeric_align", NewWriter(WithoutNumericAlign()), fixture()},
		{"escaping", NewWriter(), table.New(
			[]string{"a|b", "text"},
			[][]string{
				{"x|y", "line1\nline2"},
				{"crlf", "a\r\nb"},
				{"back\\slash", "tab\there"},
			},
		)},
		{"max_rows", NewWriter(WithMaxRows(10)), table.New([]string{"id", "label"}, long)},
		{"max_col_width", NewWriter(WithMaxColWidth(6)), table.New(
			[]string{"id", "description"},
			[][]string{{"1", "short"}, {"2", "a long description"}, {"3", "日本語テキスト"}},
		)},
		{"short_headers", NewWriter(), table.New(
			[]string{"a", "b"},
			[][]string{{"1", "x"}, {"22", ""}},
		)},
		{"no_rows", NewWriter(), table.New([]string{"name", "city"}, nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { assertGolden(t, tc.name, render(t, tc.w, tc.tb)) })
	}
}

func TestWrite_NoColumns(t *testing.T) {
	assertString(t, render(t, NewWriter(), table.Table{}), "")
}

func TestWrite_OneMoreRow(t *testing.T) {
	got := render(t, NewWriter(WithMaxRows(2)), fixture())
	if !strings.HasSuffix(got, "\n… 1 more row\n") {
		t.Errorf("want singular row summary, got:\n%s", got)
	}
}

func TestWrite_MaxRowsZeroMeansAll(t *testing.T) {
	assertString(t, render(t, NewWriter(WithMaxRows(0)), fixture()), ToString(fixture()))
}

func TestWrite_RaggedRows(t *testing.T) {
	headers := []string{"a", "b"}
	tb := table.NewFromRows(headers, []table.Row{table.NewRow(headers, []string{"1"})})
	want := "|   a | b   |\n| --: | --- |\n|   1 |     |\n"
	assertString(t, render(t, NewWriter(), tb), want)
}

func TestToString(t *testing.T) {
	assertString(t, ToString(fixture()), render(t, NewWriter(), fixture()))
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("boom") }

func TestWrite_Error(t *testing.T) {
	if err := NewWriter().Write(failWriter{}, fixture()); err == nil {
		t.Error("expected write error")
	}
}

func TestWriteFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.md")
	if err := NewWriter().WriteFile(path, fixture()); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	assertString(t, string(data), ToString(fixture()))
}

func TestWriteFile_BadPath(t *testing.T) {
	if err := NewWriter().WriteFile(filepath.Join(t.TempDir(), "missing", "out.md"), fixture()); err == nil {
		t.Error("expected error for missing directory")
	}
}

func assertString(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

func BenchmarkToString(b *testing.B) {
	headers := []string{"name", "city", "id"}
	for _, sz := range []struct {
		name string
		n    int
	}{{"1k", 1_000}, {"10k", 10_000}, {"100k", 100_000}} {
		records := make([][]string, sz.n)
		for i := range records {
			records[i] = []string{"user_" + strconv.Itoa(i), "city_" + strconv.Itoa(i%100), strconv.Itoa(i)}
		}
		tb := table.New(headers, records)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = ToString(tb)
			}
		})
	}
}
