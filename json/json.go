// Package json reads JSON files (or any io.Reader) into a table.Table.
//
// The reader is configured via functional options:
//
//	r := json.New()                              // JSON array of flat objects
//	r := json.New(json.WithNDJSON())             // one JSON object per line
//	r := json.New(json.WithSortedHeaders())      // alphabetical column order
//
// Reading returns a result.Result so errors are handled in the caller's
// preferred style:
//
//	res := json.New().ReadFile("users.json")
//	if res.IsErr() {
//	    log.Fatal(res.UnwrapErr())
//	}
//	t := res.Unwrap()
package json

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/stefanbethge/gseq-table/table"
	"github.com/stefanbethge/gseq/result"
	"github.com/stefanbethge/gseq/slice"
)

// Config holds the resolved configuration for a Reader.
// It is exported so callers can inspect defaults; use the With* options to
// change individual fields.
type Config struct {
	// NDJSON switches from JSON-array mode to newline-delimited JSON mode.
	NDJSON bool

	// SortedHeaders orders columns alphabetically instead of first-seen.
	SortedHeaders bool

	// Flatten enables recursive flattening of nested objects into
	// dot-separated column names (e.g. "user.addr.city").
	Flatten bool

	// MaxDepth limits the recursion depth during flattening. Zero means
	// unlimited. At the depth limit, values are serialised as compact JSON.
	MaxDepth int

	// FlattenSeparator is the string between nested key segments when
	// flattening. Defaults to ".".
	FlattenSeparator string

	// FieldMapping maps output column names to JSON path expressions
	// (e.g. ".user.name", ".items[0].city"). Only mapped fields appear
	// in the resulting table.
	FieldMapping map[string]string
}

// Option is a functional option for configuring a Reader.
type Option func(*Config)

// WithNDJSON switches the parser to newline-delimited JSON mode where each
// line contains one JSON object.
func WithNDJSON() Option { return func(c *Config) { c.NDJSON = true } }

// WithSortedHeaders orders columns alphabetically instead of first-seen order.
func WithSortedHeaders() Option { return func(c *Config) { c.SortedHeaders = true } }

// WithFlatten enables recursive flattening of nested JSON objects into
// dot-separated column names. Arrays are expanded with index-based keys
// (e.g. "tags.0", "tags.1"). Cannot be combined with WithFieldMapping.
func WithFlatten() Option { return func(c *Config) { c.Flatten = true } }

// WithMaxDepth limits the recursion depth during flattening. At the depth
// limit, remaining nested values are serialised as compact JSON strings.
// Zero (default) means unlimited depth.
func WithMaxDepth(n int) Option { return func(c *Config) { c.MaxDepth = n } }

// WithFlattenSeparator sets the separator between key segments when
// flattening. Defaults to ".".
func WithFlattenSeparator(sep string) Option {
	return func(c *Config) { c.FlattenSeparator = sep }
}

// WithFieldMapping provides explicit JSON path expressions to extract into
// named columns. The map key is the output column name, the value is the
// JSON path (e.g. ".user.name", ".items[0].city"). Only mapped fields
// appear in the resulting table. Cannot be combined with WithFlatten.
func WithFieldMapping(mapping map[string]string) Option {
	return func(c *Config) { c.FieldMapping = mapping }
}

// Reader parses JSON data into a table.Table.
type Reader struct{ config Config }

// New constructs a Reader with the supplied options.
func New(opts ...Option) *Reader {
	cfg := Config{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Reader{config: cfg}
}

// ReadFile opens the file at path and parses it as JSON.
// Returns Err if the file cannot be opened or the JSON is malformed.
// The basename of path is attached as the table's source name so error
// messages include "[filename.json] ..." context.
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

// Read parses JSON from rd and returns a table.Table.
// Returns Err if the JSON is malformed or contains non-object records.
func (r *Reader) Read(rd io.Reader) result.Result[table.Table, error] {
	var (
		records []record
		err     error
	)
	if r.config.NDJSON {
		records, err = decodeNDJSON(rd)
	} else {
		records, err = decodeArray(rd)
	}
	if err != nil {
		return result.Err[table.Table, error](err)
	}
	return r.buildTable(records)
}

// ReadBytes is a convenience wrapper that parses JSON from a byte slice.
func (r *Reader) ReadBytes(data []byte) result.Result[table.Table, error] {
	return r.Read(bytes.NewReader(data))
}

// ReadString is a convenience wrapper that parses JSON from a string.
func (r *Reader) ReadString(s string) result.Result[table.Table, error] {
	return r.Read(strings.NewReader(s))
}

// buildTable converts decoded records into a table.Table.
func (r *Reader) buildTable(records []record) result.Result[table.Table, error] {
	if len(records) == 0 {
		return result.Ok[table.Table, error](table.New(slice.Slice[string]{}, nil))
	}

	// Validate mutually exclusive options.
	if r.config.Flatten && len(r.config.FieldMapping) > 0 {
		return result.Err[table.Table, error](
			errors.New("json: WithFlatten and WithFieldMapping are mutually exclusive"),
		)
	}
	if r.config.MaxDepth < 0 {
		return result.Err[table.Table, error](
			fmt.Errorf("json: WithMaxDepth must be >= 0, got %d", r.config.MaxDepth),
		)
	}

	// Extract flat key-value pairs and collect headers in first-seen order.
	var headers []string
	var rows []map[string]string

	switch {
	case r.config.Flatten:
		sep := r.config.FlattenSeparator
		if sep == "" {
			sep = "."
		}
		headers, rows = extractFlatten(records, sep, r.config.MaxDepth)
	case len(r.config.FieldMapping) > 0:
		var err error
		headers, rows, err = extractMapping(records, r.config.FieldMapping)
		if err != nil {
			return result.Err[table.Table, error](err)
		}
	default:
		headers, rows = extractDefault(records)
	}

	if r.config.SortedHeaders {
		sort.Strings(headers)
	}

	// Build index for O(1) column lookup during record construction.
	headerIdx := make(map[string]int, len(headers))
	for i, h := range headers {
		headerIdx[h] = i
	}

	// Build table records as [][]string.
	tableRecords := make([][]string, len(rows))
	for i, row := range rows {
		rec := make([]string, len(headers))
		for k, v := range row {
			if idx, ok := headerIdx[k]; ok {
				rec[idx] = v
			}
		}
		tableRecords[i] = rec
	}

	return result.Ok[table.Table, error](
		table.New(slice.Slice[string](headers), tableRecords),
	)
}

// ToString serialises t as a JSON string using the default writer settings
// (JSON array, compact). Useful for tests and debugging.
//
//	fmt.Println(json.ToString(t))
func ToString(t table.Table) string {
	var buf bytes.Buffer
	_ = NewWriter().Write(&buf, t)
	return buf.String()
}
