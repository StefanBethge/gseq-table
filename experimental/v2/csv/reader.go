package csv

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	gtable "github.com/stefanbethge/gseq-table/experimental/v2"
	"github.com/stefanbethge/gseq-table/experimental/v2/internal/delivery"
)

// DefaultMaxFieldSize is the default limit for the size of one field, 16 MiB
// (D56).
const DefaultMaxFieldSize = 16 << 20

// Option configures the CSV reader.
type Option func(*config)

type config struct {
	comma      rune
	noHeader   bool
	lazyQuotes bool
	maxField   int
}

func newConfig(opts []Option) config {
	c := config{comma: ',', maxField: DefaultMaxFieldSize}
	for _, opt := range opts {
		opt(&c)
	}
	return c
}

// Comma sets the field separator; the default is ','.
func Comma(r rune) Option { return func(c *config) { c.comma = r } }

// NoHeader reads a delivery without header line. The column names come
// from the expected layout, by position (D53); see gtable.Source.Expect.
func NoHeader() Option { return func(c *config) { c.noHeader = true } }

// LazyQuotes accepts quotes in unquoted fields and unescaped quotes in
// quoted fields, like encoding/csv.
func LazyQuotes() Option { return func(c *config) { c.lazyQuotes = true } }

// MaxFieldSize sets the limit for the size of one field in bytes; the
// default is DefaultMaxFieldSize. A longer field rejects its line with
// code field_too_large, and raw_line is cut to the limit (D56).
func MaxFieldSize(n int) Option { return func(c *config) { c.maxField = n } }

// File returns a source over the CSV file at path (F8). The source is named
// after the file (D53) and opened when a run starts. Every row carries its
// line, the byte offset of its start and a record_key from the fingerprint
// of the file (D10, D61, D81). A line that cannot be split into the columns
// of the header is rejected with its raw bytes (D10).
func File(path string, opts ...Option) gtable.Source {
	return gtable.NewSource(&reader{path: path, cfg: newConfig(opts)})
}

// Reparse returns the option of gtable.FromRejects that reads the raw
// lines of rejected rows again with the given configuration (D16).
func Reparse(opts ...Option) gtable.RejectsOption {
	cfg := newConfig(opts)
	return gtable.Reparse(func(line string) ([]string, error) {
		tz := newTokenizer(strings.NewReader(line), cfg.comma, cfg.lazyQuotes)
		var f delivery.Fields
		rec, err := tz.next(&f)
		switch {
		case err == io.EOF:
			return nil, errors.New("line holds no record")
		case err != nil:
			return nil, err
		case rec.perr != nil:
			return nil, rec.perr
		}
		fields := f.Strings()
		if _, err := tz.next(&f); err != io.EOF {
			return nil, errors.New("line holds more than one record")
		}
		if j := cfg.tooLarge(fields); j >= 0 {
			return nil, fmt.Errorf("field %d is larger than %d bytes", j+1, cfg.maxField)
		}
		return fields, nil
	})
}

// tooLarge returns the index of the first field over the limit, or -1.
func (c config) tooLarge(fields []string) int {
	for j, f := range fields {
		if len(f) > c.maxField {
			return j
		}
	}
	return -1
}

// tooLargeField returns the index of the first field of f over the limit,
// or -1.
func (c config) tooLargeField(f *delivery.Fields) int {
	start := 0
	for j, end := range f.Ends {
		if end-start > c.maxField {
			return j
		}
		start = end
	}
	return -1
}

// reader implements gtable.Reader over a CSV file. It splits records into
// a reused buffer instead of strings (D113).
type reader struct {
	path string
	cfg  config

	f  *os.File
	tz *tokenizer
	fs delivery.Fields
}

func (r *reader) Open() (gtable.Header, error) {
	name := filepath.Base(r.path)
	h := gtable.Header{Source: name}
	if r.cfg.maxField <= 0 {
		return h, &gtable.PlanError{Step: "source", Err: fmt.Errorf("field size limit %d is not positive", r.cfg.maxField)}
	}
	f, err := os.Open(r.path)
	if err != nil {
		return h, err
	}
	r.f = f
	if h.ID, err = delivery.Fingerprint(name, f); err != nil {
		return h, err
	}
	r.tz = newTokenizer(f, r.cfg.comma, r.cfg.lazyQuotes)
	if r.cfg.noHeader {
		return h, nil
	}
	rec, err := r.tz.next(&r.fs)
	switch {
	case err == io.EOF:
		return h, errors.New("no header line")
	case err != nil:
		return h, fmt.Errorf("header: %w", err)
	case rec.perr != nil:
		return h, fmt.Errorf("header: %w", rec.perr)
	}
	cols := r.fs.Strings()
	cols[0] = strings.TrimPrefix(cols[0], "\ufeff")
	h.Columns = cols
	return h, nil
}

// Next returns the next record with its fields as strings. The engine
// reads with NextFields; Next serves other callers of a gtable.Reader.
func (r *reader) Next() (gtable.Record, error) {
	rec, err := r.NextFields(&r.fs)
	if err == nil && rec.Code == "" {
		rec.Fields = r.fs.Strings()
	}
	return rec, err
}

// NextFields reads the next record into fs, which holds its fields unless
// the record is rejected at read time; the returned record has no Fields.
func (r *reader) NextFields(fs *delivery.Fields) (gtable.Record, error) {
	t, err := r.tz.next(fs)
	if err != nil {
		return gtable.Record{}, err
	}
	rec := gtable.Record{Line: t.line, Offset: t.offset, Raw: t.raw, Column: -1}
	if t.perr != nil {
		rec.Code, rec.Reason = gtable.CodeUnparseableLine, t.perr.Err.Error()
		rec.Raw = r.cut(t.raw)
		return rec, nil
	}
	if j := r.cfg.tooLargeField(fs); j >= 0 {
		rec.Code, rec.Column = gtable.CodeFieldTooLarge, j
		rec.Reason = fmt.Sprintf("field is larger than %d bytes", r.cfg.maxField)
		rec.Raw = r.cut(t.raw)
		return rec, nil
	}
	return rec, nil
}

// cut cuts raw bytes to the field size limit (D56).
func (r *reader) cut(raw []byte) []byte { return raw[:min(len(raw), r.cfg.maxField)] }

func (r *reader) Close() error {
	if r.f == nil {
		return nil
	}
	err := r.f.Close()
	r.f, r.tz = nil, nil
	return err
}
