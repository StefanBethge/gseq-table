package table

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
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

func prettyFixture() Table {
	return New(
		[]string{"name", "city", "age", "score"},
		[][]string{
			{"Alice", "Berlin", "30", "1.5"},
			{"Bob", "München", "25", ""},
			{"Chen", "北京", "41", "-2e3"},
		},
	)
}

func TestPretty_Golden(t *testing.T) {
	long := make([][]string, 25)
	for i := range long {
		long[i] = []string{strconv.Itoa(i), "row " + strconv.Itoa(i)}
	}
	cases := []struct {
		name string
		got  string
	}{
		{"pretty/basic", prettyFixture().String()},
		{"pretty/unicode", New(
			[]string{"text", "note"},
			[][]string{
				{"日本語", "wide"},
				{"e\u0301te\u0301", "combining"},
				{"👍 ok", "emoji"},
				{"plain", "ascii"},
			},
		).String()},
		{"pretty/truncated_rows", New([]string{"id", "label"}, long).String()},
		{"pretty/one_more_row", New([]string{"id", "label"}, long[:4]).Pretty(WithPrettyMaxRows(3))},
		{"pretty/col_width", New(
			[]string{"id", "a_very_long_header_name"},
			[][]string{
				{"1", "short"},
				{"2", "a value that is far too long to show"},
				{"3", "日本語の長いテキスト"},
			},
		).Pretty(WithPrettyMaxColWidth(10))},
		{"pretty/control_chars", New(
			[]string{"k", "v"},
			[][]string{{"multi", "line1\nline2"}, {"tab", "a\tb"}, {"bell", "x\ay"}},
		).String()},
		{"pretty/no_rows", New([]string{"name", "city"}, nil).String()},
		{"pretty/ragged", NewFromRows(
			[]string{"a", "b", "c"},
			[]Row{NewRow([]string{"a", "b", "c"}, []string{"1"}), NewRow([]string{"a", "b", "c"}, []string{"2", "x", "y"})},
		).String()},
		{"pretty/unlimited", New([]string{"id", "label"}, long).Pretty(WithPrettyMaxRows(0), WithPrettyMaxColWidth(0))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { assertGolden(t, tc.name, tc.got) })
	}
}

func TestPretty_Empty(t *testing.T) {
	assertEqual(t, Table{}.String(), "(empty table)")
	assertEqual(t, NewMutable(nil, nil).String(), "(empty table)")
}

func TestPretty_Stringer(t *testing.T) {
	tb := prettyFixture()
	var s fmt.Stringer = tb
	assertEqual(t, fmt.Sprint(tb), s.String())
	assertEqual(t, fmt.Sprintf("%v", tb), tb.Pretty())
	var ms fmt.Stringer = tb.Mutable()
	assertEqual(t, ms.String(), tb.String())
}

func TestPretty_Mutable(t *testing.T) {
	tb := prettyFixture()
	m := tb.Mutable()
	opts := []PrettyOption{WithPrettyMaxRows(2), WithPrettyMaxColWidth(4)}
	assertEqual(t, m.Pretty(opts...), tb.Pretty(opts...))
}

func TestPretty_DoesNotMutate(t *testing.T) {
	tb := New([]string{"v"}, [][]string{{"a\nb"}, {"a long value"}})
	_ = tb.Pretty(WithPrettyMaxColWidth(3))
	assertEqual(t, tb.Rows[0].Get("v").UnwrapOr(""), "a\nb")
	assertEqual(t, tb.Rows[1].Get("v").UnwrapOr(""), "a long value")
}

func TestPretty_NumericAlignment(t *testing.T) {
	tb := New([]string{"n", "mixed", "empty", "nan"}, [][]string{
		{"1", "1", "", "NaN"},
		{"100", "x", "", "Inf"},
	})
	lines := strings.Split(tb.Pretty(), "\n")
	// numeric column n is right-aligned (header too), the others left.
	assertEqual(t, lines[1], "|   n | mixed | empty | nan |")
	assertEqual(t, lines[3], "|   1 | 1     |       | NaN |")
	assertEqual(t, lines[4], "| 100 | x     |       | Inf |")
}

func TestPretty_NoTrailingNewline(t *testing.T) {
	s := prettyFixture().String()
	if strings.HasSuffix(s, "\n") {
		t.Errorf("String() should not end in a newline: %q", s)
	}
	s = New([]string{"id"}, [][]string{{"1"}, {"2"}}).Pretty(WithPrettyMaxRows(1))
	if !strings.HasSuffix(s, "… 1 more row") {
		t.Errorf("want trailing row summary, got %q", s)
	}
}

func TestWritePretty(t *testing.T) {
	tb := prettyFixture()
	var buf bytes.Buffer
	if err := tb.WritePretty(&buf, WithPrettyMaxRows(1)); err != nil {
		t.Fatal(err)
	}
	assertEqual(t, buf.String(), tb.Pretty(WithPrettyMaxRows(1))+"\n")

	buf.Reset()
	if err := tb.Mutable().WritePretty(&buf); err != nil {
		t.Fatal(err)
	}
	assertEqual(t, buf.String(), tb.String()+"\n")
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("boom") }

func TestWritePretty_Error(t *testing.T) {
	if err := prettyFixture().WritePretty(failWriter{}); err == nil {
		t.Error("expected write error")
	}
	if err := prettyFixture().Mutable().WritePretty(failWriter{}); err == nil {
		t.Error("expected write error")
	}
}

func BenchmarkPretty(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = tb.String()
			}
		})
		b.Run(sz.name+"/all", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = tb.Pretty(WithPrettyMaxRows(0))
			}
		})
	}
}
