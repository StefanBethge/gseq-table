package csv

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stefanbethge/gseq-table/table"
)

// collectRows drains a row stream, failing the test on any error.
func collectRows(t *testing.T, r *Reader, data string) []table.Row {
	t.Helper()
	var rows []table.Row
	for row, err := range r.Stream(strings.NewReader(data)) {
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	return rows
}

// assertRowsMatchRead checks that rows equal Read's output for the same input.
func assertRowsMatchRead(t *testing.T, r *Reader, data string, rows []table.Row) {
	t.Helper()
	want := r.Read(strings.NewReader(data)).Unwrap()
	if len(rows) != len(want.Rows) {
		t.Fatalf("rows: got %d, want %d", len(rows), len(want.Rows))
	}
	for i, row := range rows {
		if !slices.Equal(row.Headers(), want.Headers) {
			t.Fatalf("row %d headers: got %v, want %v", i, row.Headers(), want.Headers)
		}
		if !slices.Equal(row.Values(), want.Rows[i].Values()) {
			t.Fatalf("row %d values: got %v, want %v", i, row.Values(), want.Rows[i].Values())
		}
	}
}

func TestStream_MatchesRead(t *testing.T) {
	cases := []struct {
		name string
		r    *Reader
		data string
	}{
		{"header", New(), "name,score\nAlice,90\nBob,85\n"},
		{"no header", New(WithNoHeader()), "Alice,90\nBob,85\n"},
		{"header names", New(WithHeaderNames("name", "score")), "Alice,90\nBob,85\n"},
		{"semicolon", New(WithSeparator(';')), "name;score\nAlice;90\nBob;85\n"},
		{"zero separator", &Reader{config: Config{HasHeader: true}}, "a,b\n1,2\n"},
		{"quoted", New(), "name,note\n\"Doe, J\",\"line1\nline2\"\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows := collectRows(t, tc.r, tc.data)
			assertRowsMatchRead(t, tc.r, tc.data, rows)
		})
	}
}

func TestStream_SharesHeaderSlice(t *testing.T) {
	rows := collectRows(t, New(), "a,b\n1,2\n3,4\n")
	assertEqual(t, len(rows), 2)
	assertEqual(t, &rows[0].Headers()[0], &rows[1].Headers()[0])
}

func TestStream_Empty(t *testing.T) {
	for _, r := range []*Reader{New(), New(WithNoHeader()), New(WithHeaderNames("a"))} {
		assertEqual(t, len(collectRows(t, r, "")), 0)
	}
	assertEqual(t, len(collectRows(t, New(), "a,b\n")), 0)
}

func TestStream_MalformedYieldsErrorOnce(t *testing.T) {
	data := "a,b\n1,2\n3\n5,6\n"
	var rows, errs int
	for _, err := range New().Stream(strings.NewReader(data)) {
		if err != nil {
			errs++
			continue
		}
		rows++
	}
	assertEqual(t, rows, 1)
	assertEqual(t, errs, 1)
}

func TestStream_HeaderReadError(t *testing.T) {
	want := errors.New("boom")
	for _, r := range []*Reader{New(), New(WithNoHeader())} {
		var got error
		for _, err := range r.Stream(errReader{want}) {
			got = err
		}
		if !errors.Is(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestStream_EarlyStop(t *testing.T) {
	var count int
	for range New().Stream(strings.NewReader("a\n1\n2\n3\n")) {
		count++
		break
	}
	assertEqual(t, count, 1)

	// early stop inside the pre-read rows (auto-generated names)
	count = 0
	for range New(WithNoHeader()).Stream(strings.NewReader("1\n2\n")) {
		count++
		break
	}
	assertEqual(t, count, 1)
}

func TestStreamFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.csv")
	if err := os.WriteFile(path, []byte("name,score\nAlice,90\nBob,85\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var names []string
	for row, err := range New().StreamFile(path) {
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, row.Get("name").UnwrapOr(""))
	}
	assertEqual(t, strings.Join(names, ","), "Alice,Bob")

	var count int
	for range New().StreamFile(path) {
		count++
		break
	}
	assertEqual(t, count, 1)
}

func TestStreamFile_Missing(t *testing.T) {
	var got error
	for _, err := range New().StreamFile(filepath.Join(t.TempDir(), "missing.csv")) {
		got = err
	}
	if !errors.Is(got, os.ErrNotExist) {
		t.Errorf("got %v, want os.ErrNotExist", got)
	}
}

func TestStream_LargeInput(t *testing.T) {
	const n = 200_000
	data := buildCSV(n, 5, ',')
	rows := collectRows(t, New(), data)
	assertRowsMatchRead(t, New(), data, rows)
	assertEqual(t, rows[n-1].Get("col4").UnwrapOr(""), "val_199999_4")
}

func TestReadStream_LargeInputMatchesRead(t *testing.T) {
	const n = 100_003
	data := buildCSV(n, 5, ',')
	want := New().Read(strings.NewReader(data)).Unwrap()
	var chunks []table.Table
	for chunk, err := range New().ReadStream(strings.NewReader(data), 1000) {
		if err != nil {
			t.Fatal(err)
		}
		if len(chunk.Rows) > 1000 {
			t.Fatalf("chunk has %d rows, want <= 1000", len(chunk.Rows))
		}
		chunks = append(chunks, chunk)
	}
	assertEqual(t, len(chunks), 101)
	got := table.Concat(chunks...)
	assertEqual(t, len(got.Rows), n)
	for i := range got.Rows {
		if !slices.Equal(got.Rows[i].Values(), want.Rows[i].Values()) {
			t.Fatalf("row %d: got %v, want %v", i, got.Rows[i].Values(), want.Rows[i].Values())
		}
	}
	// chunks must not share row storage
	assertEqual(t, chunks[0].Rows[0].Get("col0").UnwrapOr(""), "val_0_0")
}
