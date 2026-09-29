package csv

import (
	"bytes"
	stdcsv "encoding/csv"
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
		r := cfg.newReader(strings.NewReader(line))
		fields, err := r.Read()
		if err != nil {
			return nil, err
		}
		if _, err := r.Read(); err != io.EOF {
			return nil, errors.New("line holds more than one record")
		}
		if j := cfg.tooLarge(fields); j >= 0 {
			return nil, fmt.Errorf("field %d is larger than %d bytes", j+1, cfg.maxField)
		}
		return fields, nil
	})
}

func (c config) newReader(r io.Reader) *stdcsv.Reader {
	cr := stdcsv.NewReader(r)
	cr.Comma = c.comma
	cr.LazyQuotes = c.lazyQuotes
	cr.FieldsPerRecord = -1 // the source checks the number of fields
	return cr
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

// reader implements gtable.Reader over a CSV file.
type reader struct {
	path string
	cfg  config

	f   *os.File
	rec *recorder
	cr  *stdcsv.Reader
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
	r.rec = &recorder{r: f}
	r.cr = r.cfg.newReader(r.rec)
	if r.cfg.noHeader {
		return h, nil
	}
	cols, err := r.cr.Read()
	switch {
	case err == io.EOF:
		return h, errors.New("no header line")
	case err != nil:
		return h, fmt.Errorf("header: %w", err)
	}
	cols[0] = strings.TrimPrefix(cols[0], "\ufeff")
	h.Columns = cols
	return h, nil
}

func (r *reader) Next() (gtable.Record, error) {
	start := r.cr.InputOffset()
	r.rec.drop(start)
	fields, err := r.cr.Read()
	if err == io.EOF {
		return gtable.Record{}, io.EOF
	}
	raw, offset := trimLine(r.rec.take(start, r.cr.InputOffset()), start)
	rec := gtable.Record{Offset: offset, Raw: raw, Column: -1}
	var pe *stdcsv.ParseError
	switch {
	case errors.As(err, &pe):
		rec.Line = pe.StartLine
		rec.Code, rec.Reason = gtable.CodeUnparseableLine, pe.Err.Error()
		rec.Raw = r.cut(raw)
		return rec, nil
	case err != nil:
		return gtable.Record{}, err
	}
	rec.Line, _ = r.cr.FieldPos(0)
	if j := r.cfg.tooLarge(fields); j >= 0 {
		rec.Code, rec.Column = gtable.CodeFieldTooLarge, j
		rec.Reason = fmt.Sprintf("field is larger than %d bytes", r.cfg.maxField)
		rec.Raw = r.cut(raw)
		return rec, nil
	}
	rec.Fields = fields
	return rec, nil
}

// cut cuts raw bytes to the field size limit (D56).
func (r *reader) cut(raw []byte) []byte { return raw[:min(len(raw), r.cfg.maxField)] }

func (r *reader) Close() error {
	if r.f == nil {
		return nil
	}
	err := r.f.Close()
	r.f, r.rec, r.cr = nil, nil, nil
	return err
}

// trimLine cuts the blank lines encoding/csv skipped before a record and the
// line break after it, and returns the bytes and the offset of the record.
func trimLine(raw []byte, offset int64) ([]byte, int64) {
	for {
		switch {
		case len(raw) > 0 && raw[0] == '\n':
			raw, offset = raw[1:], offset+1
			continue
		case len(raw) > 1 && raw[0] == '\r' && raw[1] == '\n':
			raw, offset = raw[2:], offset+2
			continue
		}
		break
	}
	raw = bytes.TrimSuffix(bytes.TrimSuffix(raw, []byte("\n")), []byte("\r"))
	return raw, offset
}

// recorder keeps the bytes the CSV reader has read and not yet dropped, so
// the raw bytes of a record can be cut out by input offset (D10).
type recorder struct {
	r    io.Reader
	buf  []byte
	base int64 // input offset of buf[0]
}

func (rc *recorder) Read(p []byte) (int, error) {
	n, err := rc.r.Read(p)
	rc.buf = append(rc.buf, p[:n]...)
	return n, err
}

func (rc *recorder) take(from, to int64) []byte { return rc.buf[from-rc.base : to-rc.base] }

func (rc *recorder) drop(to int64) {
	n := copy(rc.buf, rc.buf[to-rc.base:])
	rc.buf, rc.base = rc.buf[:n], to
}
