package json

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stefanbethge/gseq-table/table"
)

// buildNDJSON generates n NDJSON objects with the given number of fields.
func buildNDJSON(n, fields int) string {
	var sb strings.Builder
	for i := range n {
		sb.WriteByte('{')
		for f := range fields {
			if f > 0 {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, `"f%d":"v_%d_%d"`, f, i, f)
		}
		sb.WriteString("}\n")
	}
	return sb.String()
}

// buildJSONArray generates a JSON array of n objects.
func buildJSONArray(n, fields int) string {
	lines := strings.Split(strings.TrimSpace(buildNDJSON(n, fields)), "\n")
	return "[" + strings.Join(lines, ",") + "]"
}

// streamRows drains a row stream, failing the test on any error.
func streamRows(t *testing.T, r *Reader, data string) []table.Row {
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

// streamChunks drains a chunk stream, failing the test on any error.
func streamChunks(t *testing.T, r *Reader, data string, n int) []table.Table {
	t.Helper()
	var chunks []table.Table
	for chunk, err := range r.ReadStream(strings.NewReader(data), n) {
		if err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, chunk)
	}
	return chunks
}

// assertMatchesRead checks rows against Read's output for the same input.
func assertMatchesRead(t *testing.T, r *Reader, data string, rows []table.Row) {
	t.Helper()
	res := r.ReadString(data)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	want := res.Unwrap()
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
	nested := `[{"id":1,"user":{"name":"A","addr":{"city":"X"}},"tags":["a","b"]},` +
		`{"id":2,"user":{"name":"B","addr":{"city":"Y"}},"tags":["c","d"]}]`
	cases := []struct {
		name string
		r    *Reader
		data string
	}{
		{"array", New(), `[{"id":"1","name":"Alice"},{"id":"2","name":"Bob"}]`},
		{"ndjson", New(WithNDJSON()), "{\"id\":1,\"ok\":true}\n\n{\"id\":2,\"ok\":null}\n"},
		{"sorted", New(WithSortedHeaders()), `[{"z":1,"a":2},{"z":3,"a":4}]`},
		{"nested default", New(), nested},
		{"flatten", New(WithFlatten()), `[{"id":1,"user":{"name":"A"},"tags":["a","b"]},{"id":2,"user":{"name":"B"},"tags":["c","d"]}]`},
		{"flatten sep", New(WithFlatten(), WithFlattenSeparator("_")), `[{"u":{"n":"A"}},{"u":{"n":"B"}}]`},
		{"max depth", New(WithFlatten(), WithMaxDepth(2), WithSortedHeaders()), nested},
		{"mapping", New(WithFieldMapping(map[string]string{"name": ".user.name", "city": ".user.addr.city", "t0": ".tags[0]"})), nested},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertMatchesRead(t, tc.r, tc.data, streamRows(t, tc.r, tc.data))
			chunks := streamChunks(t, tc.r, tc.data, 1)
			var rows []table.Row
			for _, c := range chunks {
				rows = append(rows, c.Rows...)
			}
			assertMatchesRead(t, tc.r, tc.data, rows)
		})
	}
}

func TestStream_EvolvingHeaders(t *testing.T) {
	data := "{\"a\":\"1\"}\n{\"b\":\"2\",\"a\":\"3\"}\n{\"c\":\"4\"}\n"
	rows := streamRows(t, New(WithNDJSON()), data)
	if len(rows) != 3 {
		t.Fatalf("rows: got %d, want 3", len(rows))
	}
	wantHeaders := [][]string{{"a"}, {"a", "b"}, {"a", "b", "c"}}
	wantValues := [][]string{{"1"}, {"3", "2"}, {"", "", "4"}}
	for i, row := range rows {
		if !slices.Equal(row.Headers(), wantHeaders[i]) {
			t.Errorf("row %d headers = %v, want %v", i, row.Headers(), wantHeaders[i])
		}
		if !slices.Equal(row.Values(), wantValues[i]) {
			t.Errorf("row %d values = %v, want %v", i, row.Values(), wantValues[i])
		}
	}
	if got := rows[1].Get("c").IsSome(); got {
		t.Error("row 1 must not see column c added later")
	}
}

func TestStream_EvolvingSortedHeaders(t *testing.T) {
	data := "{\"m\":\"1\"}\n{\"z\":\"2\",\"a\":\"3\"}\n"
	rows := streamRows(t, New(WithNDJSON(), WithSortedHeaders()), data)
	if !slices.Equal(rows[0].Headers(), []string{"m"}) {
		t.Errorf("row 0 headers = %v", rows[0].Headers())
	}
	if !slices.Equal(rows[1].Headers(), []string{"a", "m", "z"}) {
		t.Errorf("row 1 headers = %v", rows[1].Headers())
	}
	if !slices.Equal(rows[1].Values(), []string{"3", "", "2"}) {
		t.Errorf("row 1 values = %v", rows[1].Values())
	}
}

func TestReadStream_EvolvingHeaders(t *testing.T) {
	data := "{\"a\":\"1\"}\n{\"a\":\"2\"}\n{\"b\":\"3\"}\n"
	chunks := streamChunks(t, New(WithNDJSON()), data, 2)
	if len(chunks) != 2 {
		t.Fatalf("chunks: got %d, want 2", len(chunks))
	}
	if !slices.Equal(chunks[0].Headers, []string{"a"}) {
		t.Errorf("chunk 0 headers = %v", chunks[0].Headers)
	}
	if !slices.Equal(chunks[1].Headers, []string{"a", "b"}) {
		t.Errorf("chunk 1 headers = %v", chunks[1].Headers)
	}
	if got := chunks[1].Rows[0].Get("b").UnwrapOr("?"); got != "3" {
		t.Errorf("chunk 1 b = %q, want 3", got)
	}
	// earlier chunk keeps its own values after the buffer is reused
	if got := chunks[0].Rows[1].Get("a").UnwrapOr("?"); got != "2" {
		t.Errorf("chunk 0 row 1 a = %q, want 2", got)
	}
}

func TestReadStream_ChunkSizes(t *testing.T) {
	data := buildNDJSON(5, 2)
	chunks := streamChunks(t, New(WithNDJSON()), data, 2)
	var sizes []int
	for _, c := range chunks {
		sizes = append(sizes, c.Len())
	}
	if !slices.Equal(sizes, []int{2, 2, 1}) {
		t.Errorf("sizes = %v, want [2 2 1]", sizes)
	}
	if got := len(streamChunks(t, New(WithNDJSON()), data, 0)); got != 1 {
		t.Errorf("default chunk size: got %d chunks, want 1", got)
	}
}

func TestStream_Empty(t *testing.T) {
	for _, tc := range []struct {
		name string
		r    *Reader
		data string
	}{
		{"empty input", New(), ""},
		{"empty array", New(), "[]"},
		{"empty ndjson", New(WithNDJSON()), "\n\n"},
		// options are validated lazily, as in Read
		{"invalid options", New(WithFlatten(), WithFieldMapping(map[string]string{"a": ".a"})), "[]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := len(streamRows(t, tc.r, tc.data)); got != 0 {
				t.Errorf("rows: got %d, want 0", got)
			}
			if got := len(streamChunks(t, tc.r, tc.data, 10)); got != 0 {
				t.Errorf("chunks: got %d, want 0", got)
			}
		})
	}
}

func TestStream_Errors(t *testing.T) {
	cases := []struct {
		name     string
		r        *Reader
		data     string
		wantRows int
		wantErr  string
	}{
		{"not array", New(), `{"a":1}`, 0, "expected JSON array"},
		{"non object", New(), `[{"a":1},2]`, 1, "record 1: expected JSON object"},
		{"malformed ndjson", New(WithNDJSON()), "{\"a\":1}\n{\"a\":}\n", 1, "record 1"},
		{"unterminated array", New(), `[{"a":1}`, 1, "unexpected end"},
		{"bad token", New(), `]`, 0, "invalid character"},
		{"exclusive options", New(WithFlatten(), WithFieldMapping(map[string]string{"a": ".a"})), `[{"a":1}]`, 0, "mutually exclusive"},
		{"negative depth", New(WithFlatten(), WithMaxDepth(-1)), `[{"a":1}]`, 0, "WithMaxDepth"},
		{"bad mapping", New(WithFieldMapping(map[string]string{"a": "a["})), `[{"a":1}]`, 0, "field mapping"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var rows, errs int
			var last error
			for _, err := range tc.r.Stream(strings.NewReader(tc.data)) {
				if err != nil {
					errs++
					last = err
					continue
				}
				rows++
			}
			if rows != tc.wantRows || errs != 1 {
				t.Fatalf("rows=%d errs=%d, want rows=%d errs=1", rows, errs, tc.wantRows)
			}
			if !strings.Contains(last.Error(), tc.wantErr) {
				t.Errorf("error %q does not contain %q", last, tc.wantErr)
			}
			if res := tc.r.ReadString(tc.data); res.IsErr() != true {
				t.Error("Read should fail on the same input")
			}

			var chunkErr error
			for _, err := range tc.r.ReadStream(strings.NewReader(tc.data), 10) {
				chunkErr = err
			}
			if chunkErr == nil || !strings.Contains(chunkErr.Error(), tc.wantErr) {
				t.Errorf("ReadStream error = %v, want %q", chunkErr, tc.wantErr)
			}
		})
	}
}

func TestStream_EarlyStop(t *testing.T) {
	data := buildNDJSON(10, 2)
	var rows int
	for range New(WithNDJSON()).Stream(strings.NewReader(data)) {
		rows++
		break
	}
	var chunks int
	for range New(WithNDJSON()).ReadStream(strings.NewReader(data), 3) {
		chunks++
		break
	}
	var arrRows int
	for range New().Stream(strings.NewReader(buildJSONArray(10, 2))) {
		arrRows++
		break
	}
	if rows != 1 || chunks != 1 || arrRows != 1 {
		t.Errorf("rows=%d chunks=%d arrRows=%d, want 1 each", rows, chunks, arrRows)
	}
}

func TestStreamFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "users.ndjson")
	if err := os.WriteFile(path, []byte(buildNDJSON(5, 2)), 0o644); err != nil {
		t.Fatal(err)
	}
	r := New(WithNDJSON())

	var rows int
	for _, err := range r.StreamFile(path) {
		if err != nil {
			t.Fatal(err)
		}
		rows++
	}
	if rows != 5 {
		t.Errorf("rows: got %d, want 5", rows)
	}

	var chunks []table.Table
	for chunk, err := range r.ReadFileStream(path, 2) {
		if err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, chunk)
	}
	if len(chunks) != 3 {
		t.Fatalf("chunks: got %d, want 3", len(chunks))
	}
	for i, c := range chunks {
		if c.Source() != "users.ndjson" {
			t.Errorf("chunk %d source = %q, want users.ndjson", i, c.Source())
		}
	}

	var stopped int
	for range r.StreamFile(path) {
		stopped++
		break
	}
	for range r.ReadFileStream(path, 1) {
		stopped++
		break
	}
	if stopped != 2 {
		t.Errorf("early stop: got %d, want 2", stopped)
	}
}

func TestStreamFile_Missing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.json")
	var rowErr, chunkErr error
	for _, err := range New().StreamFile(path) {
		rowErr = err
	}
	for _, err := range New().ReadFileStream(path, 10) {
		chunkErr = err
	}
	if !errors.Is(rowErr, os.ErrNotExist) || !errors.Is(chunkErr, os.ErrNotExist) {
		t.Errorf("got %v / %v, want os.ErrNotExist", rowErr, chunkErr)
	}
}

func TestStream_LargeInput(t *testing.T) {
	const n = 100_000
	for _, tc := range []struct {
		name string
		r    *Reader
		data string
	}{
		{"ndjson", New(WithNDJSON()), buildNDJSON(n, 5)},
		{"array", New(), buildJSONArray(n, 5)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := streamRows(t, tc.r, tc.data)
			assertMatchesRead(t, tc.r, tc.data, rows)

			chunks := streamChunks(t, tc.r, tc.data, 1000)
			if len(chunks) != 100 {
				t.Fatalf("chunks: got %d, want 100", len(chunks))
			}
			got := table.Concat(chunks...)
			if got.Len() != n {
				t.Fatalf("rows: got %d, want %d", got.Len(), n)
			}
			if v := got.Rows[n-1].Get("f4").UnwrapOr(""); v != "v_99999_4" {
				t.Errorf("last f4 = %q", v)
			}
		})
	}
}
