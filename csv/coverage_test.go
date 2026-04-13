package csv

import (
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stefanbethge/gseq-table/table"
)

// errReader always returns an error on Read.
type errReader struct{ err error }

func (e errReader) Read(p []byte) (int, error) { return 0, e.err }

// --- ReadStream ---

func TestReadStream_MultipleChunks(t *testing.T) {
	data := "name,score\nAlice,90\nBob,85\nCarol,70\nDave,60\nEve,55\n"
	var tables []table.Table
	for chunk, err := range New().ReadStream(strings.NewReader(data), 2) {
		if err != nil {
			t.Fatal(err)
		}
		tables = append(tables, chunk)
	}
	// 5 rows / chunkSize 2 → chunks of 2, 2, 1
	assertEqual(t, len(tables), 3)
	assertEqual(t, len(tables[0].Rows), 2)
	assertEqual(t, len(tables[1].Rows), 2)
	assertEqual(t, len(tables[2].Rows), 1)
	assertEqual(t, tables[0].Rows[0].Get("name").UnwrapOr(""), "Alice")
	assertEqual(t, tables[2].Rows[0].Get("name").UnwrapOr(""), "Eve")
}

func TestReadStream_ExactChunkSize(t *testing.T) {
	data := "name,score\nAlice,90\nBob,85\n"
	var tables []table.Table
	for chunk, err := range New().ReadStream(strings.NewReader(data), 2) {
		if err != nil {
			t.Fatal(err)
		}
		tables = append(tables, chunk)
	}
	assertEqual(t, len(tables), 1)
	assertEqual(t, len(tables[0].Rows), 2)
}

func TestReadStream_EmptyReader_HasHeader(t *testing.T) {
	// Completely empty → resolveHeadersStreaming gets EOF → no yields
	var count int
	for _, err := range New().ReadStream(strings.NewReader(""), 10) {
		if err != nil {
			t.Fatal(err)
		}
		count++
	}
	assertEqual(t, count, 0)
}

func TestReadStream_HeaderOnly(t *testing.T) {
	// Header row but no data rows
	var count int
	for _, err := range New().ReadStream(strings.NewReader("name,age\n"), 10) {
		if err != nil {
			t.Fatal(err)
		}
		count++
	}
	assertEqual(t, count, 0)
}

func TestReadStream_WithNoHeader_AutoNames(t *testing.T) {
	data := "Alice,90\nBob,85\n"
	var tables []table.Table
	for chunk, err := range New(WithNoHeader()).ReadStream(strings.NewReader(data), 10) {
		if err != nil {
			t.Fatal(err)
		}
		tables = append(tables, chunk)
	}
	assertEqual(t, len(tables), 1)
	assertEqual(t, tables[0].Headers[0], "col_0")
	assertEqual(t, tables[0].Headers[1], "col_1")
	assertEqual(t, tables[0].Rows[0].Get("col_0").UnwrapOr(""), "Alice")
	assertEqual(t, tables[0].Rows[1].Get("col_0").UnwrapOr(""), "Bob")
}

func TestReadStream_WithNoHeader_EmptyReader(t *testing.T) {
	// WithNoHeader + empty reader → peek returns EOF → no yields
	var count int
	for _, err := range New(WithNoHeader()).ReadStream(strings.NewReader(""), 10) {
		if err != nil {
			t.Fatal(err)
		}
		count++
	}
	assertEqual(t, count, 0)
}

func TestReadStream_WithHeaderNames(t *testing.T) {
	data := "Alice,90\nBob,85\n"
	var tables []table.Table
	for chunk, err := range New(WithHeaderNames("name", "score")).ReadStream(strings.NewReader(data), 10) {
		if err != nil {
			t.Fatal(err)
		}
		tables = append(tables, chunk)
	}
	assertEqual(t, len(tables), 1)
	assertEqual(t, tables[0].Headers[0], "name")
	assertEqual(t, tables[0].Rows[0].Get("name").UnwrapOr(""), "Alice")
	assertEqual(t, tables[0].Rows[0].Get("score").UnwrapOr(""), "90")
}

func TestReadStream_DefaultChunkSize(t *testing.T) {
	// chunkSize <= 0 → defaults to 1000; still yields one chunk for small data
	data := "name,score\nAlice,90\n"
	var tables []table.Table
	for chunk, err := range New().ReadStream(strings.NewReader(data), 0) {
		if err != nil {
			t.Fatal(err)
		}
		tables = append(tables, chunk)
	}
	assertEqual(t, len(tables), 1)
	assertEqual(t, len(tables[0].Rows), 1)
}

func TestReadStream_EarlyStop(t *testing.T) {
	data := "name,score\nAlice,90\nBob,85\nCarol,70\n"
	var count int
	for _, err := range New().ReadStream(strings.NewReader(data), 1) {
		if err != nil {
			t.Fatal(err)
		}
		count++
		break // stop after first chunk; yield returns false
	}
	assertEqual(t, count, 1)
}

func TestReadStream_ErrorInHeader(t *testing.T) {
	// Reader fails immediately → error yielded during header read
	r := errReader{err: errors.New("header read error")}
	var gotErr bool
	for _, err := range New().ReadStream(r, 10) {
		if err != nil {
			gotErr = true
		}
	}
	assertEqual(t, gotErr, true)
}

func TestReadStream_WithNoHeader_ErrorOnPeek(t *testing.T) {
	// WithNoHeader auto-name: reader fails during peek → error yielded
	r := errReader{err: errors.New("peek error")}
	var gotErr bool
	for _, err := range New(WithNoHeader()).ReadStream(r, 10) {
		if err != nil {
			gotErr = true
		}
	}
	assertEqual(t, gotErr, true)
}

func TestReadStream_ErrorDuringData(t *testing.T) {
	// Use a pipe: write header then close with error to simulate mid-stream failure.
	pr, pw := io.Pipe()
	go func() {
		_, _ = pw.Write([]byte("name,score\n"))
		pw.CloseWithError(errors.New("pipe broken"))
	}()
	var gotErr bool
	for _, err := range New().ReadStream(pr, 10) {
		if err != nil {
			gotErr = true
		}
	}
	assertEqual(t, gotErr, true)
}

func TestReadStream_Semicolon(t *testing.T) {
	data := "name;score\nAlice;90\n"
	var tables []table.Table
	for chunk, err := range New(WithSeparator(';')).ReadStream(strings.NewReader(data), 10) {
		if err != nil {
			t.Fatal(err)
		}
		tables = append(tables, chunk)
	}
	assertEqual(t, len(tables), 1)
	assertEqual(t, tables[0].Rows[0].Get("name").UnwrapOr(""), "Alice")
}

// --- ReadFileStream ---

func TestReadFileStream_HappyPath(t *testing.T) {
	f, err := os.CreateTemp("", "stream_*.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	_, _ = f.WriteString("name,score\nAlice,90\nBob,85\nCarol,70\n")
	f.Close()

	var totalRows int
	for chunk, err := range New().ReadFileStream(f.Name(), 2) {
		if err != nil {
			t.Fatal(err)
		}
		totalRows += len(chunk.Rows)
	}
	assertEqual(t, totalRows, 3)
}

func TestReadFileStream_NotFound(t *testing.T) {
	var gotErr bool
	for _, err := range New().ReadFileStream("/nonexistent/path/file.csv", 10) {
		if err != nil {
			gotErr = true
		}
	}
	assertEqual(t, gotErr, true)
}

func TestReadFileStream_EarlyStop(t *testing.T) {
	f, err := os.CreateTemp("", "stream_stop_*.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	_, _ = f.WriteString("name,score\nAlice,90\nBob,85\nCarol,70\n")
	f.Close()

	var count int
	for _, err := range New().ReadFileStream(f.Name(), 1) {
		if err != nil {
			t.Fatal(err)
		}
		count++
		break
	}
	assertEqual(t, count, 1)
}

// --- WithHeader option ---

func TestWithHeader_ExplicitOption(t *testing.T) {
	// WithHeader() is the explicit form of the default; should work identically.
	r := New(WithHeader())
	res := r.Read(strings.NewReader(csvWithHeader))
	assertEqual(t, res.IsOk(), true)
	tb := res.Unwrap()
	assertEqual(t, len(tb.Headers), 3)
	assertEqual(t, tb.Headers[0], "name")
	assertEqual(t, tb.Rows[0].Get("name").UnwrapOr(""), "Alice")
}

// --- Writer error paths ---

func TestWriteFile_BadPath(t *testing.T) {
	w := NewWriter()
	err := w.WriteFile("/nonexistent/dir/out.csv", makeWriteTable())
	assertEqual(t, err != nil, true)
}

func TestWriter_Write_ZeroSeparator(t *testing.T) {
	// Writer with zero Separator skips setting Comma → encoding/csv defaults to ','
	w := &Writer{config: WriterConfig{HasHeader: true, Separator: 0}}
	var buf strings.Builder
	tb := table.New([]string{"a", "b"}, [][]string{{"1", "2"}})
	err := w.Write(&buf, tb)
	assertEqual(t, err, nil)
	out := buf.String()
	if !strings.Contains(out, "a,b") {
		t.Errorf("expected comma-separated output, got %q", out)
	}
}
