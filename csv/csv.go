package csv

import (
	gcsv "encoding/csv"
	"fmt"
	"io"
	"os"

	"github.com/stefanbethge/gseq-table/table"
	"github.com/stefanbethge/gseq/result"
	"github.com/stefanbethge/gseq/slice"
)

type Config struct {
	HasHeader   bool
	HeaderNames slice.Slice[string]
	Separator   rune
}

type Option func(*Config)

func WithHeader() Option            { return func(c *Config) { c.HasHeader = true } }
func WithNoHeader() Option          { return func(c *Config) { c.HasHeader = false } }
func WithSeparator(sep rune) Option { return func(c *Config) { c.Separator = sep } }

func WithHeaderNames(names ...string) Option {
	return func(c *Config) {
		c.HasHeader = false
		c.HeaderNames = slice.Slice[string](names)
	}
}

type Reader struct{ config Config }

func New(opts ...Option) *Reader {
	cfg := Config{HasHeader: true, Separator: ','}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Reader{config: cfg}
}

func (r *Reader) ReadFile(path string) result.Result[table.Table, error] {
	f, err := os.Open(path)
	if err != nil {
		return result.Err[table.Table, error](err)
	}
	defer f.Close()
	return r.Read(f)
}

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
