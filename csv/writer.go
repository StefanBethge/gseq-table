package csv

import (
	gcsv "encoding/csv"
	"io"
	"os"

	"github.com/stefanbethge/gseq-table/table"
)

// WriterConfig holds the resolved settings for a Writer.
type WriterConfig struct {
	// HasHeader controls whether the column headers are written as the first
	// row. Defaults to true.
	HasHeader bool

	// Separator is the field delimiter. Defaults to ','.
	Separator rune
}

// WriterOption is a functional option for configuring a Writer.
type WriterOption func(*WriterConfig)

// WithoutHeader suppresses the header row in the output.
func WithoutHeader() WriterOption { return func(c *WriterConfig) { c.HasHeader = false } }

// WithWriteSeparator sets the field delimiter (e.g. ';' for semicolons).
func WithWriteSeparator(sep rune) WriterOption {
	return func(c *WriterConfig) { c.Separator = sep }
}

// Writer serialises a table.Table to CSV.
type Writer struct{ config WriterConfig }

// NewWriter constructs a Writer with the supplied options. Unset options keep
// their defaults: HasHeader=true, Separator=','.
func NewWriter(opts ...WriterOption) *Writer {
	cfg := WriterConfig{HasHeader: true, Separator: ','}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Writer{config: cfg}
}

// WriteFile creates (or truncates) the file at path and writes t as CSV.
func (w *Writer) WriteFile(path string, t table.Table) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return w.Write(f, t)
}

// Write serialises t as CSV to wr.
func (w *Writer) Write(wr io.Writer, t table.Table) error {
	cw := gcsv.NewWriter(wr)
	if w.config.Separator != 0 {
		cw.Comma = w.config.Separator
	}

	if w.config.HasHeader {
		if err := cw.Write([]string(t.Headers)); err != nil {
			return err
		}
	}
	for _, row := range t.Rows {
		if err := cw.Write([]string(row.Values())); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}
