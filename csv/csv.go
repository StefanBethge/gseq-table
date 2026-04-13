// Package csv reads CSV files (or any io.Reader) into a table.Table.
//
// The reader is configured via functional options:
//
//	r := csv.New()                                  // header row, comma separator
//	r := csv.New(csv.WithSeparator(';'))            // semicolon-delimited
//	r := csv.New(csv.WithNoHeader())                // auto-generated col_0, col_1, …
//	r := csv.New(csv.WithHeaderNames("id", "name")) // explicit header names
//
// Reading returns a result.Result so errors are handled in the caller's
// preferred style:
//
//	res := csv.New().ReadFile("sales.csv")
//	if res.IsErr() {
//	    log.Fatal(res.UnwrapErr())
//	}
//	t := res.Unwrap()
package csv

import (
	"bytes"
	gcsv "encoding/csv"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"

	"github.com/stefanbethge/gseq-table/table"
	"github.com/stefanbethge/gseq/result"
	"github.com/stefanbethge/gseq/slice"
)

// Config holds the resolved configuration for a Reader.
// It is exported so callers can inspect defaults; use the With* options to
// change individual fields.
type Config struct {
	// HasHeader controls whether the first row is treated as a header.
	HasHeader bool

	// HeaderNames provides explicit column names when HasHeader is false.
	// If set, these names are used instead of auto-generated col_N names.
	HeaderNames slice.Slice[string]

	// Separator is the field delimiter. Defaults to ','.
	Separator rune
}

// Option is a functional option for configuring a Reader.
type Option func(*Config)

// WithHeader treats the first row of every input as a header row (default).
func WithHeader() Option { return func(c *Config) { c.HasHeader = true } }

// WithNoHeader signals that the input has no header row. Column names will be
// auto-generated as col_0, col_1, … unless WithHeaderNames is also applied.
func WithNoHeader() Option { return func(c *Config) { c.HasHeader = false } }

// WithSeparator sets the field delimiter (e.g. ';' for semicolon-delimited
// files). Defaults to ','.
func WithSeparator(sep rune) Option { return func(c *Config) { c.Separator = sep } }

// WithHeaderNames provides explicit column names for files without a header
// row. Implies WithNoHeader.
//
//	csv.New(csv.WithHeaderNames("id", "name", "amount"))
func WithHeaderNames(names ...string) Option {
	return func(c *Config) {
		c.HasHeader = false
		c.HeaderNames = slice.Slice[string](names)
	}
}

// Reader parses CSV data into a table.Table.
type Reader struct{ config Config }

// New constructs a Reader with the supplied options. Unset options keep their
// defaults: HasHeader=true, Separator=','.
func New(opts ...Option) *Reader {
	cfg := Config{HasHeader: true, Separator: ','}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Reader{config: cfg}
}

// ReadFile opens the file at path and parses it as CSV.
// Returns Err if the file cannot be opened or the CSV is malformed.
// The basename of path is attached as the table's source name so error
// messages include "[filename.csv] ..." context.
func (r *Reader) ReadFile(path string) result.Result[table.Table, error] {
	f, err := os.Open(path)
	if err != nil {
		return result.Err[table.Table, error](err)
	}
	defer f.Close()
	return result.Map(r.Read(f), func(t table.Table) table.Table {
		return t.WithSource(filepath.Base(path))
	})
}


// Read parses CSV from rd and returns a table.Table.
// Returns Err if the CSV is malformed.
func (r *Reader) Read(rd io.Reader) result.Result[table.Table, error] {
	sep := r.config.Separator
	if sep == 0 {
		sep = ','
	}
	cr := gcsv.NewReader(rd)
	cr.Comma = sep
	records, err := cr.ReadAll()
	if err != nil {
		return result.Err[table.Table, error](err)
	}
	headers, dataRows := r.resolveHeaders(records)
	return result.Ok[table.Table, error](table.New(headers, dataRows))
}

// ReadStream reads CSV from rd and yields chunks of at most chunkSize rows as
// Tables. This allows processing large files without loading them fully into
// memory. Each yielded Table shares the same header slice.
//
// The iterator stops early if the caller returns false from yield or an error
// is encountered. If rd contains no data rows, no Tables are yielded.
//
//	for t, err := range csv.New().ReadStream(f, 1000) {
//	    if err != nil { log.Fatal(err) }
//	    process(t)
//	}
func (r *Reader) ReadStream(rd io.Reader, chunkSize int) iter.Seq2[table.Table, error] {
	return func(yield func(table.Table, error) bool) {
		sep := r.config.Separator
		if sep == 0 {
			sep = ','
		}
		if chunkSize <= 0 {
			chunkSize = 1000
		}
		cr := gcsv.NewReader(rd)
		cr.Comma = sep

		headers, dataRows, err := r.resolveHeadersStreaming(cr)
		if err != nil {
			yield(table.Table{}, err)
			return
		}

		// flush any rows already read (e.g. when HasHeader=false and we read
		// the first record to detect width)
		chunk := dataRows

		for {
			if len(chunk) >= chunkSize {
				if !yield(table.New(headers, chunk), nil) {
					return
				}
				chunk = chunk[:0]
			}
			record, err := cr.Read()
			if err == io.EOF {
				if len(chunk) > 0 {
					yield(table.New(headers, chunk), nil)
				}
				return
			}
			if err != nil {
				yield(table.Table{}, err)
				return
			}
			chunk = append(chunk, record)
		}
	}
}

// ReadFileStream opens the file at path and streams it in chunks of chunkSize
// rows. See ReadStream for details.
func (r *Reader) ReadFileStream(path string, chunkSize int) iter.Seq2[table.Table, error] {
	return func(yield func(table.Table, error) bool) {
		f, err := os.Open(path)
		if err != nil {
			yield(table.Table{}, err)
			return
		}
		defer f.Close()
		for t, err := range r.ReadStream(f, chunkSize) {
			if !yield(t, err) {
				return
			}
		}
	}
}

// resolveHeadersStreaming reads the header row (or generates names) from a
// streaming csv.Reader and returns any pre-read data rows.
func (r *Reader) resolveHeadersStreaming(cr *gcsv.Reader) (slice.Slice[string], [][]string, error) {
	if r.config.HasHeader {
		rec, err := cr.Read()
		if err == io.EOF {
			return slice.Slice[string]{}, nil, nil
		}
		if err != nil {
			return nil, nil, err
		}
		return slice.Slice[string](rec), nil, nil
	}
	if len(r.config.HeaderNames) > 0 {
		return r.config.HeaderNames, nil, nil
	}
	// auto-generate names — peek at first row to determine width
	rec, err := cr.Read()
	if err == io.EOF {
		return slice.Slice[string]{}, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	names := make(slice.Slice[string], len(rec))
	for i := range rec {
		names[i] = fmt.Sprintf("col_%d", i)
	}
	return names, [][]string{rec}, nil
}

// ToString serialises t as a CSV string using the default writer settings
// (comma separator, header row included). Useful for tests and debugging.
//
//	fmt.Println(csv.ToString(t))
func ToString(t table.Table) string {
	var buf bytes.Buffer
	_ = NewWriter().Write(&buf, t)
	return buf.String()
}

// resolveHeaders separates the header row (or generates names) from the data
// rows based on the current Config.
func (r *Reader) resolveHeaders(records [][]string) (slice.Slice[string], [][]string) {
	if r.config.HasHeader {
		if len(records) == 0 {
			return slice.Slice[string]{}, nil
		}
		return slice.Slice[string](records[0]), records[1:]
	}
	if len(r.config.HeaderNames) > 0 {
		return r.config.HeaderNames, records
	}
	if len(records) == 0 {
		return slice.Slice[string]{}, nil
	}
	cols := len(records[0])
	names := make(slice.Slice[string], cols)
	for i := range cols {
		names[i] = fmt.Sprintf("col_%d", i)
	}
	return names, records
}
