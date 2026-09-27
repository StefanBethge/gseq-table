// Package markdown writes a table.Table as a GitHub-flavoured Markdown table,
// for reports, pull request comments and documentation.
//
// The writer is configured via functional options, analogous to the csv and
// json writers:
//
//	w := markdown.NewWriter()                          // all rows, numeric columns right-aligned
//	w := markdown.NewWriter(markdown.WithMaxRows(10))  // first 10 rows plus a "… N more rows" note
//
//	fmt.Print(markdown.ToString(t))
//
// Output:
//
//	| name  | city   | age |
//	| ----- | ------ | --: |
//	| Alice | Berlin |  30 |
//	| Bob   | Munich |  25 |
package markdown

import (
	"bytes"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/stefanbethge/gseq-table/internal/cell"
	"github.com/stefanbethge/gseq-table/table"
)

// WriterConfig holds the resolved settings for a Writer.
type WriterConfig struct {
	// AlignNumeric right-aligns columns whose non-empty values are all
	// numbers. Defaults to true.
	AlignNumeric bool

	// MaxRows is the number of rows written. Remaining rows are summarised in
	// a "… N more rows" line below the table. Zero or less means all rows,
	// which is the default.
	MaxRows int

	// MaxColWidth is the maximum display width of a column. Longer headers
	// and values are truncated with "…". Zero or less means unlimited, which
	// is the default.
	MaxColWidth int
}

// WriterOption is a functional option for configuring a Writer.
type WriterOption func(*WriterConfig)

// WithoutNumericAlign keeps every column left-aligned.
func WithoutNumericAlign() WriterOption { return func(c *WriterConfig) { c.AlignNumeric = false } }

// WithMaxRows limits the output to the first n rows. Zero or less writes all
// rows.
func WithMaxRows(n int) WriterOption { return func(c *WriterConfig) { c.MaxRows = n } }

// WithMaxColWidth truncates headers and values wider than n display columns.
// Zero or less disables truncation.
func WithMaxColWidth(n int) WriterOption { return func(c *WriterConfig) { c.MaxColWidth = n } }

// Writer serialises a table.Table to Markdown.
type Writer struct{ config WriterConfig }

// NewWriter constructs a Writer with the supplied options. Unset options keep
// their defaults: numeric columns right-aligned, all rows, no truncation.
func NewWriter(opts ...WriterOption) *Writer {
	cfg := WriterConfig{AlignNumeric: true}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Writer{config: cfg}
}

// WriteFile creates (or truncates) the file at path and writes t as Markdown.
func (w *Writer) WriteFile(path string, t table.Table) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return w.Write(f, t)
}

// Write serialises t as a Markdown table to wr. Cells are padded so the
// source is aligned too, with widths measured in display columns. Pipes are
// escaped as \| and line breaks become <br>. A table without columns writes
// nothing.
func (w *Writer) Write(wr io.Writer, t table.Table) error {
	cols := len(t.Headers)
	if cols == 0 {
		return nil
	}
	shown := len(t.Rows)
	if w.config.MaxRows > 0 && shown > w.config.MaxRows {
		shown = w.config.MaxRows
	}

	head := make([]string, cols)
	widths := make([]int, cols)
	for c, h := range t.Headers {
		head[c] = w.display(h)
		widths[c] = max(3, cell.Width(head[c])) // a delimiter needs at least 3 characters
	}

	numeric := make([]bool, cols)
	seen := make([]bool, cols)
	for c := range numeric {
		numeric[c] = w.config.AlignNumeric
	}
	cells := make([][]string, shown)
	for r := range shown {
		line := make([]string, cols)
		for c := range cols {
			v := t.Rows[r].At(c).UnwrapOr("")
			if v != "" && numeric[c] {
				seen[c] = true
				numeric[c] = cell.IsNumeric(v)
			}
			line[c] = w.display(v)
			widths[c] = max(widths[c], cell.Width(line[c]))
		}
		cells[r] = line
	}
	for c := range numeric {
		numeric[c] = numeric[c] && seen[c]
	}

	var sb strings.Builder
	line := func(vals []string) {
		for c, v := range vals {
			sb.WriteString("| ")
			sb.WriteString(cell.Pad(v, widths[c], numeric[c]))
			sb.WriteByte(' ')
		}
		sb.WriteString("|\n")
	}

	line(head)
	for c := range cols {
		sb.WriteString("| ")
		if numeric[c] {
			sb.WriteString(strings.Repeat("-", widths[c]-1) + ":")
		} else {
			sb.WriteString(strings.Repeat("-", widths[c]))
		}
		sb.WriteByte(' ')
	}
	sb.WriteString("|\n")
	for _, vals := range cells {
		line(vals)
	}
	if rest := len(t.Rows) - shown; rest > 0 {
		sb.WriteString("\n" + cell.Ellipsis + " " + strconv.Itoa(rest) + " more row")
		if rest != 1 {
			sb.WriteByte('s')
		}
		sb.WriteByte('\n')
	}
	_, err := io.WriteString(wr, sb.String())
	return err
}

// display applies the column width limit to s and escapes it for use inside
// a Markdown table cell. Truncating first keeps escapes such as <br> intact.
func (w *Writer) display(s string) string {
	s = cell.Truncate(s, w.config.MaxColWidth)
	return cell.EscapeControl(mdEscaper.Replace(s))
}

// mdEscaper escapes the characters that would end a cell or a row. Backslashes
// are doubled so a trailing backslash cannot swallow the cell delimiter.
var mdEscaper = strings.NewReplacer(
	`\`, `\\`,
	"|", `\|`,
	"\r\n", "<br>",
	"\n", "<br>",
	"\r", "<br>",
)

// ToString serialises t as a Markdown string using the default writer
// settings. Useful for tests, reports and debugging.
//
//	fmt.Print(markdown.ToString(t))
func ToString(t table.Table) string {
	var buf bytes.Buffer
	_ = NewWriter().Write(&buf, t)
	return buf.String()
}
