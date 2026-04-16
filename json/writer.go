package json

import (
	stdjson "encoding/json"
	"io"
	"os"

	"github.com/stefanbethge/gseq-table/table"
)

// WriterConfig holds the resolved settings for a Writer.
type WriterConfig struct {
	// NDJSON writes one JSON object per line instead of a JSON array.
	NDJSON bool

	// PrettyPrint indents the output for readability.
	PrettyPrint bool

	// Indent is the indentation string when PrettyPrint is true.
	// Defaults to two spaces.
	Indent string
}

// WriterOption is a functional option for configuring a Writer.
type WriterOption func(*WriterConfig)

// WithWriteNDJSON writes one JSON object per line instead of a JSON array.
func WithWriteNDJSON() WriterOption { return func(c *WriterConfig) { c.NDJSON = true } }

// WithPrettyPrint enables indented output for readability.
func WithPrettyPrint() WriterOption { return func(c *WriterConfig) { c.PrettyPrint = true } }

// WithIndent sets the indentation string used when PrettyPrint is enabled.
// Defaults to two spaces.
func WithIndent(indent string) WriterOption {
	return func(c *WriterConfig) { c.Indent = indent }
}

// Writer serialises a table.Table to JSON.
type Writer struct{ config WriterConfig }

// NewWriter constructs a Writer with the supplied options. Unset options keep
// their defaults: JSON array, compact output, two-space indent.
func NewWriter(opts ...WriterOption) *Writer {
	cfg := WriterConfig{Indent: "  "}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Writer{config: cfg}
}

// WriteFile creates (or truncates) the file at path and writes t as JSON.
func (w *Writer) WriteFile(path string, t table.Table) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return w.Write(f, t)
}

// Write serialises t as JSON to wr.
func (w *Writer) Write(wr io.Writer, t table.Table) error {
	headers := t.Headers
	rows := t.Rows

	if w.config.NDJSON {
		return w.writeNDJSON(wr, headers, rows)
	}
	return w.writeArray(wr, headers, rows)
}

func (w *Writer) writeArray(wr io.Writer, headers []string, rows []table.Row) error {
	objects := make([]map[string]string, len(rows))
	for i, row := range rows {
		obj := make(map[string]string, len(headers))
		for _, h := range headers {
			obj[h] = row.Get(h).UnwrapOr("")
		}
		objects[i] = obj
	}

	enc := stdjson.NewEncoder(wr)
	enc.SetEscapeHTML(false)
	if w.config.PrettyPrint {
		enc.SetIndent("", w.config.Indent)
	}
	return enc.Encode(objects)
}

func (w *Writer) writeNDJSON(wr io.Writer, headers []string, rows []table.Row) error {
	enc := stdjson.NewEncoder(wr)
	enc.SetEscapeHTML(false)
	if w.config.PrettyPrint {
		enc.SetIndent("", w.config.Indent)
	}
	for _, row := range rows {
		obj := make(map[string]string, len(headers))
		for _, h := range headers {
			obj[h] = row.Get(h).UnwrapOr("")
		}
		if err := enc.Encode(obj); err != nil {
			return err
		}
	}
	return nil
}
